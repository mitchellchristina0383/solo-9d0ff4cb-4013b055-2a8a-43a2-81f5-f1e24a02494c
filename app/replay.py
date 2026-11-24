"""离散事件重放引擎（支持按同一抓包向后续算）。

规则（全部时间为整数毫秒，时间轴从 0 开始，仿真区间 [0, replay_end)）：

1. 时间轴被划分为连续周期，每个周期内：
   - 前段 ``[周期起点, 周期起点 + reserve)`` 为告警时段，只允许发告警帧；
   - 后段为图像时段，只允许发图像帧（reserve=period 时没有图像时段；reserve=0 时没有告警时段）。
2. 出口一次只发一帧；同类帧严格按到达顺序发送（相同到达时刻按请求中的先后）。
3. 整帧必须落在当前本类时段内才会开始发送，即 ``now + busy <= 当前时段终点``；
   队首帧放不进当前时段时，同队后续帧也不能插队，本类时段剩余时间空转，等下一个同类时段。
4. 可指定时刻换一套周期/预留。切换只在周期边界生效：
   - 到点若出口正在发帧，等该帧发完（即走到下一个周期边界）再切换；
   - 到点若出口空闲，等到当前周期结束的边界再切换；
   - 排队帧保留；新周期从实际生效时刻重新对齐（生效时刻即新周期的第 0 毫秒）。
5. 告警从到达开始计时；发完即停止计时（耗时 = end - arrival），未发完则算到重放终点
   （耗时 = replay_end - arrival）；耗时严格大于告警时限即超时。

实现分成两层：

- ``_SimState`` 是某个终点时刻的完整仿真快照（配置、换配状态、队列、在发帧、
  未入队到达事件、逐帧已记录结果）。快照大小只与帧数有关，与终点个数无关。
- ``_run`` 从快照时刻继续推进到新的重放终点。终点延长时，跨旧终点的在发帧、
  排队帧和待生效换配都在原快照上接着处理，跨终点前已完成的调度结果直接复用。

进程内最多保留一份抓包快照（按“帧顺序 + 除 replay_end_ms 外的全部请求字段”识别）。
并发长短请求由同一把锁串行化，且快照终点只增不减，短请求不会让已保留的进度回退。
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from typing import Literal

from .models import FrameResult, ReplayRequest, ReplayResponse, ReplayStats, SwitchInfo

ReplayMode = Literal["cold", "resume"]


@dataclass
class _Frame:
    idx: int
    fid: str
    ftype: str  # "alarm" | "image"
    arrival: int
    busy: int
    start: int | None = None
    end: int | None = None
    # 状态标记：未到达 / 已到达排队中 / 发送中 / 已发完
    arrived: bool = False
    transmitting: bool = False
    sent: bool = False


@dataclass
class _SimState:
    """某一重放终点时刻的完整仿真快照。"""

    end_t: int  # 快照对应的重放终点
    period: int
    reserve: int
    anchor: int
    switch_requested: bool
    switch_time: int
    new_period: int | None
    new_reserve: int | None
    switch_armed: bool
    switched: bool
    effective_switch: int | None
    frames: list[_Frame]
    queues: dict[str, list[_Frame]]
    in_flight: _Frame | None
    arrivals: list[int]  # 尚未入队的到达事件（输入下标），末尾为下一个
    admitted_through: int

    def clone(self) -> "_SimState":
        """复制一份可变状态；缓存中的快照始终不被续算直接改写。"""
        frames = [
            _Frame(
                idx=f.idx,
                fid=f.fid,
                ftype=f.ftype,
                arrival=f.arrival,
                busy=f.busy,
                start=f.start,
                end=f.end,
                arrived=f.arrived,
                transmitting=f.transmitting,
                sent=f.sent,
            )
            for f in self.frames
        ]
        return _SimState(
            end_t=self.end_t,
            period=self.period,
            reserve=self.reserve,
            anchor=self.anchor,
            switch_requested=self.switch_requested,
            switch_time=self.switch_time,
            new_period=self.new_period,
            new_reserve=self.new_reserve,
            switch_armed=self.switch_armed,
            switched=self.switched,
            effective_switch=self.effective_switch,
            frames=frames,
            queues={k: [frames[f.idx] for f in q] for k, q in self.queues.items()},
            in_flight=frames[self.in_flight.idx] if self.in_flight is not None else None,
            arrivals=list(self.arrivals),
            admitted_through=self.admitted_through,
        )


@dataclass(frozen=True)
class ReplayMeta:
    """一次计算的旁路信息（不进入响应体，避免影响“响应与从头重放一致”）。"""

    mode: ReplayMode
    scheduling_steps: int
    resumed_from_ms: int | None
    checkpoint_end_ms: int


def _segment_at(now: int, anchor: int, period: int, reserve: int) -> tuple[str, int]:
    """返回 ``now`` 所属时段的类别与时段终点（半开区间）。"""
    offset = (now - anchor) % period
    if offset < reserve:
        return "alarm", now + (reserve - offset)
    return "image", now + (period - offset)


def _period_end_at(now: int, anchor: int, period: int) -> int:
    """返回严格大于 ``now`` 的下一个周期边界。

    ``now`` 恰为周期边界时返回下一个边界；调用方据此判断“当前周期还剩多少”。
    """
    offset = (now - anchor) % period
    if offset == 0:
        return now + period
    return now + (period - offset)


def _is_period_boundary(now: int, anchor: int, period: int) -> bool:
    return (now - anchor) % period == 0


def _initial_state(req: ReplayRequest) -> _SimState:
    frames = [
        _Frame(
            idx=i,
            fid=f.id if f.id is not None else f"F{i + 1}",
            ftype=f.type,
            arrival=f.arrival_ms,
            busy=f.busy_ms,
        )
        for i, f in enumerate(req.frames)
    ]
    # 到达事件按 (时刻, 输入顺序) 排序，保证“同类按到达顺序发送”
    arrivals = sorted(range(len(frames)), key=lambda i: (frames[i].arrival, frames[i].idx))
    arrivals.reverse()  # 末尾为下一个待处理到达
    return _SimState(
        end_t=0,
        period=req.period_ms,
        reserve=req.reserve_ms,
        anchor=0,  # 当前周期对齐起点（0 或换配实际生效时刻）
        switch_requested=req.switch_at_ms is not None,
        switch_time=req.switch_at_ms if req.switch_at_ms is not None else -1,
        new_period=req.new_period_ms,
        new_reserve=req.new_reserve_ms,
        switch_armed=False,  # 换配时刻已到，等待下一个周期边界生效
        switched=False,
        effective_switch=None,
        frames=frames,
        queues={"alarm": [], "image": []},
        in_flight=None,
        arrivals=arrivals,
        admitted_through=-1,  # 已入队到达事件的时间上界
    )


def _run(st: _SimState, end_t: int) -> int:
    """从快照时刻 ``st.end_t`` 继续仿真到 ``end_t``，原地更新 ``st``。

    返回本次续算实际执行的调度步数（主循环迭代次数），用于对比续算与从头重放。
    """
    period = st.period
    reserve = st.reserve
    anchor = st.anchor
    switch_requested = st.switch_requested
    switch_time = st.switch_time
    switch_armed = st.switch_armed
    switched = st.switched
    effective_switch = st.effective_switch
    queues = st.queues
    frames = st.frames
    arrivals = st.arrivals
    in_flight = st.in_flight
    admitted_through = st.admitted_through

    now = st.end_t
    steps = 0

    def apply_switch(t: int) -> None:
        nonlocal period, reserve, anchor, switched, effective_switch, switch_armed
        period = st.new_period  # type: ignore[assignment]
        reserve = st.new_reserve  # type: ignore[assignment]
        anchor = t
        switched = True
        switch_armed = False
        effective_switch = t

    def admit_arrivals(upto: int) -> None:
        """把到达时刻 <= ``upto`` 且尚未入队的帧放入对应类别队列。

        仿真时间单调不减；每次调用都排空所有到达时刻 <= upto 的剩余事件，
        ``admitted_through`` 仅用于在异常回退时保护，正常同刻重复调用也会继续排空。
        """
        nonlocal admitted_through
        if upto < admitted_through:
            return
        while arrivals and frames[arrivals[-1]].arrival <= upto:
            fr = frames[arrivals.pop()]
            fr.arrived = True
            queues[fr.ftype].append(fr)
        admitted_through = upto

    def next_same_class_arrival(seg_class: str) -> int | None:
        """尚未入队的下一个本类帧到达时刻（没有则 None）。"""
        for i in reversed(arrivals):
            if frames[i].ftype == seg_class:
                return frames[i].arrival
        return None

    while now < end_t:
        steps += 1
        # 统一入队点：每次进入循环（含发送完成、空转跳转后）都把截至 now 的到达事件收入队列。
        admit_arrivals(now)
        pending_switch = switch_requested and not switched

        # 1) 出口正在发帧：等待发送完成
        if in_flight is not None:
            fin = in_flight.end
            assert fin is not None
            if fin >= end_t:
                # 帧跨过/正好触及重放终点：未发完，仿真结束；
                # 已到点的换配被本帧推迟，终点前没有“发完”边界，保持 not_switched。
                now = end_t
                break
            now = fin
            in_flight.transmitting = False
            in_flight.sent = True
            in_flight = None
            # 换配时刻可能在本帧发送期间到点：发完即周期边界，到点则在此刻生效
            if pending_switch and now >= switch_time:
                apply_switch(now)
            continue

        # 2) 出口空闲：换配时刻到点（只武装，等当前周期结束的边界再生效）
        if pending_switch and not switch_armed and now >= switch_time:
            switch_armed = True

        # 2.5) 全局快速路径：出口空闲，且已无排队帧、无未到达帧。
        #      武装后的等待由步骤 3 的边界跳转处理（O(1)），这里只覆盖“帧已全部处理完”的收尾。
        if not queues["alarm"] and not queues["image"] and not arrivals:
            if pending_switch and not switch_armed:
                if switch_time >= end_t:
                    # 换配时刻在终点之后，本次重放不会切换
                    now = end_t
                    break
                # 跳到换配时刻，避免在无帧期间逐周期空转
                now = switch_time
                continue
            if not pending_switch:
                # 无换配：剩余时间只会空转，直接收敛到终点
                now = end_t
                break
            # pending_switch 且 switch_armed：落到步骤 3，按周期边界处理

        # 3) 已武装的换配只在周期边界生效
        if pending_switch and switch_armed:
            if _is_period_boundary(now, anchor, period):
                # now 本身就是周期边界：新周期从此刻起算
                apply_switch(now)
                continue
            boundary = _period_end_at(now, anchor, period)
            if boundary >= end_t:
                # 生效边界不早于重放终点：本次重放内不会切换
                now = end_t
                break
            # 边界之前维持旧配置；旧周期剩余时间不再安排跨边界发送，直接跳到边界
            now = boundary
            continue

        seg_class, seg_end = _segment_at(now, anchor, period, reserve)
        head = queues[seg_class][0] if queues[seg_class] else None

        # 4) 能整帧放进当前本类时段：立即开始发送
        if head is not None and now + head.busy <= seg_end:
            head.start = now
            head.end = now + head.busy
            head.transmitting = True
            queues[seg_class].pop(0)
            in_flight = head
            now = head.end
            continue

        # 5) 本时段发不出帧，选择下一个决策点：
        #    a) 队首帧放不进：严格 FIFO，同队后续帧不许插队，本时段剩余时间必然空转 -> 段末；
        #    b) 本类队列为空：可在“下一个本类帧到达”时刻提前唤醒（到达即评估能否即发）；
        #    c) 始终保留段末兜底；若换配已到点，本周期结束就要切换，周期终点也是决策点。
        candidates: list[int] = [seg_end]
        if head is None:
            nxt_arr = next_same_class_arrival(seg_class)
            if nxt_arr is not None and nxt_arr < end_t:
                candidates.append(nxt_arr)
        if pending_switch and switch_armed:
            candidates.append(_period_end_at(now, anchor, period))
        nxt = min(candidates)
        # seg_end 严格大于 now（半开时段），故 nxt > now，循环必然推进
        now = min(nxt, end_t)

    # 收尾：快进到终点可能跳过了若干决策点，仍需把终点前（严格早于终点）已到达的帧
    # 标记为已到达/排队，否则逐段续算与从头重放会得到不同的状态。
    # 到达时刻恰为 end_t 的帧按约定仍算 not_arrived，故只入队到 end_t - 1。
    admit_arrivals(end_t - 1)

    st.end_t = end_t
    st.period = period
    st.reserve = reserve
    st.anchor = anchor
    st.switch_armed = switch_armed
    st.switched = switched
    st.effective_switch = effective_switch
    st.in_flight = in_flight
    st.admitted_through = admitted_through
    return steps


def _build_response(st: _SimState, req: ReplayRequest) -> ReplayResponse:
    """由终点快照生成完整响应（与从头重放到同一终点的输出一致）。"""
    end_t = st.end_t
    results: list[FrameResult] = []
    counts = {"sent": 0, "in_progress": 0, "queued": 0, "not_arrived": 0}
    alarm_total = 0
    alarm_timed_out = 0

    for fr in st.frames:
        if fr.sent:
            status = "sent"
        elif fr.transmitting and st.in_flight is fr:
            status = "in_progress"
        elif fr.arrived:
            status = "queued"
        else:
            status = "not_arrived"
        counts[status] += 1

        elapsed: int | None = None
        timed_out: bool | None = None
        if fr.ftype == "alarm" and status != "not_arrived":
            alarm_total += 1
            if status == "sent":
                assert fr.end is not None
                elapsed = fr.end - fr.arrival
            else:
                # in_progress / queued：未发完，算到重放终点
                elapsed = end_t - fr.arrival
            timed_out = elapsed > req.alarm_deadline_ms
            if timed_out:
                alarm_timed_out += 1

        results.append(
            FrameResult(
                id=fr.fid,
                type=fr.ftype,  # type: ignore[arg-type]
                arrival_ms=fr.arrival,
                busy_ms=fr.busy,
                start_ms=fr.start,
                end_ms=fr.end if status == "sent" else None,
                status=status,  # type: ignore[arg-type]
                alarm_elapsed_ms=elapsed,
                timed_out=timed_out,
            )
        )

    if not st.switch_requested:
        switch_info = SwitchInfo(
            requested_at_ms=None, effective_at_ms=None, switched=False, state="not_requested"
        )
    elif st.switched:
        switch_info = SwitchInfo(
            requested_at_ms=st.switch_time,
            effective_at_ms=st.effective_switch,
            switched=True,
            state="switched",
        )
    else:
        # 未切换：要么换配时刻在终点之后，要么到点后等到的边界不早于终点
        switch_info = SwitchInfo(
            requested_at_ms=st.switch_time,
            effective_at_ms=None,
            switched=False,
            state="not_switched",
        )

    return ReplayResponse(
        replay_end_ms=end_t,
        switch=switch_info,
        initial_period_ms=req.period_ms,
        initial_reserve_ms=req.reserve_ms,
        effective_period_ms=st.period,
        effective_reserve_ms=st.reserve,
        frames=results,
        stats=ReplayStats(
            total=len(st.frames),
            sent=counts["sent"],
            in_progress=counts["in_progress"],
            queued=counts["queued"],
            not_arrived=counts["not_arrived"],
            alarm_total=alarm_total,
            alarm_timed_out=alarm_timed_out,
        ),
    )


def simulate(req: ReplayRequest) -> ReplayResponse:
    """从头执行一次完整重放（不读缓存、不写缓存），返回完整响应。"""
    st = _initial_state(req)
    _run(st, req.replay_end_ms)
    return _build_response(st, req)


# ---------------------------------------------------------------- 单份进度缓存
def capture_key(req: ReplayRequest) -> str:
    """抓包标识：帧顺序与其他全部请求字段相同才算同一份（不含 replay_end_ms）。"""
    payload = req.model_dump(mode="json", exclude={"replay_end_ms"})
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class _ProgressCache:
    """进程内单槽进度缓存；锁同时保证并发长短请求不会让快照终点回退。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.key: str | None = None
        self.state: _SimState | None = None

    def reset(self) -> None:
        with self.lock:
            self.key = None
            self.state = None

    def snapshot(self) -> dict[str, int | str | None]:
        with self.lock:
            state = self.state
            return {
                # 只暴露抓包标识的摘要，避免把整份请求回显出来
                "capture": (
                    None
                    if self.key is None
                    else hashlib.sha256(self.key.encode()).hexdigest()[:16]
                ),
                "end_ms": state.end_t if state is not None else None,
                "retained_states": 0 if state is None else 1,
            }


_PROGRESS = _ProgressCache()


def reset_progress_cache() -> None:
    """清空进程内进度缓存（主要供测试使用）。"""
    _PROGRESS.reset()


def progress_cache_snapshot() -> dict[str, int | str | None]:
    """返回当前保留的唯一一份进度（测试/运维观察用）。"""
    return _PROGRESS.snapshot()


def simulate_cached(req: ReplayRequest) -> tuple[ReplayResponse, ReplayMeta]:
    """带单槽进度缓存的重放：同一抓包终点后移时从已保留快照续算。

    缓存中快照的终点只增不减：若并发/后到的请求终点更短，则本次从头重放，
    但不会用更短的进度覆盖已保留的更长进度。
    """
    key = capture_key(req)
    target = req.replay_end_ms

    with _PROGRESS.lock:
        checkpoint = _PROGRESS.state if _PROGRESS.key == key else None
        if checkpoint is not None and checkpoint.end_t <= target:
            work = checkpoint.clone()
            resumed_from = checkpoint.end_t
            mode: ReplayMode = "resume"
        else:
            work = _initial_state(req)
            resumed_from = None
            mode = "cold"

        steps = _run(work, target)

        # 写入策略（单槽）：换抓包直接替换；同抓包只允许终点前进，防止进度回退。
        if _PROGRESS.key != key:
            _PROGRESS.key = key
            _PROGRESS.state = work
        elif _PROGRESS.state is None or work.end_t > _PROGRESS.state.end_t:
            _PROGRESS.state = work
        # else：保留已有的更长进度

        response = _build_response(work, req)
        checkpoint_end = _PROGRESS.state.end_t if _PROGRESS.state is not None else target

    return response, ReplayMeta(
        mode=mode,
        scheduling_steps=steps,
        resumed_from_ms=resumed_from,
        checkpoint_end_ms=checkpoint_end,
    )
