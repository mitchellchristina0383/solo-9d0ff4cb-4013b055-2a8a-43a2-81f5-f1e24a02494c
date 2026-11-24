"""增量重放（同一抓包终点后移续算）的自测。

核心不变量：对任意终点 E，``/replay`` 的完整响应必须等于以 E 从头重放的结果。
缓存只影响“算了多少步”，不能影响“算出什么”。
"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import ReplayRequest
from app.replay import (
    capture_key,
    progress_cache_snapshot,
    reset_progress_cache,
    simulate,
    simulate_cached,
)


def _frame(fid: str, ftype: str, arrival: int, busy: int) -> dict:
    return {"id": fid, "type": ftype, "arrival_ms": arrival, "busy_ms": busy}


BASE = {
    "period_ms": 100,
    "reserve_ms": 40,
    "alarm_deadline_ms": 80,
    "frames": [
        _frame("A1", "alarm", 0, 30),
        _frame("A2", "alarm", 5, 60),
        _frame("I1", "image", 10, 50),
        _frame("A3", "alarm", 120, 25),
        _frame("I2", "image", 130, 70),
    ],
    "switch_at_ms": 150,
    "new_period_ms": 120,
    "new_reserve_ms": 60,
}


def _req(end: int, **overrides) -> ReplayRequest:
    payload = {**BASE, "replay_end_ms": end, **overrides}
    return ReplayRequest(**payload)


def _dump(resp) -> dict:
    return resp.model_dump()


@pytest.fixture(autouse=True)
def _clear_cache():
    reset_progress_cache()
    yield
    reset_progress_cache()


# ---------------------------------------------------------------- 一致性
def test_resume_equals_cold_for_every_endpoint():
    """逐个终点渐进推进，每一步的响应都等于该终点从头重放。"""
    ends = [1, 50, 100, 120, 149, 150, 200, 260, 320, 400, 1000]
    prev_end = 0
    for end in ends:
        resp, meta = simulate_cached(_req(end))
        assert _dump(resp) == _dump(simulate(_req(end))), f"end={end} 与从头重放不一致"
        if prev_end:
            assert meta.mode == "resume", f"end={end} 应从 {prev_end} 续算"
            assert meta.resumed_from_ms == prev_end
        prev_end = end


def test_cold_request_is_marked_cold_and_resumes_after_same_capture():
    resp, meta = simulate_cached(_req(320))
    assert meta.mode == "cold" and meta.scheduling_steps > 0
    assert meta.resumed_from_ms is None

    resp2, meta2 = simulate_cached(_req(400))
    assert meta2.mode == "resume"
    assert meta2.resumed_from_ms == 320
    assert _dump(resp2) == _dump(simulate(_req(400)))


def test_different_capture_restarts_from_scratch():
    simulate_cached(_req(320))
    # 帧顺序变化 -> 不同抓包
    swapped = [BASE["frames"][1], BASE["frames"][0], *BASE["frames"][2:]]
    _, meta = simulate_cached(_req(400, frames=swapped))
    assert meta.mode == "cold"

    # 其他字段变化（这里是告警时限）-> 不同抓包
    _, meta2 = simulate_cached(_req(400, alarm_deadline_ms=81))
    assert meta2.mode == "cold"


def test_same_frames_different_replay_end_is_same_capture():
    assert capture_key(_req(100)) == capture_key(_req(9999))


# ---------------------------------------------------------------- 跨旧终点续算
def test_resume_continues_in_flight_frame_across_old_endpoint():
    """旧终点 180 时 I_long 正在发（140-200）；推进到 260 必须接着发完。"""
    payload = {
        "period_ms": 100,
        "reserve_ms": 40,
        "alarm_deadline_ms": 1000,
        "frames": [_frame("I_long", "image", 140, 60)],
    }
    first, _ = simulate_cached(ReplayRequest(**payload, replay_end_ms=180))
    fr0 = {f.id: f for f in first.frames}
    assert (fr0["I_long"].start_ms, fr0["I_long"].status) == (140, "in_progress")

    resp, meta = simulate_cached(ReplayRequest(**payload, replay_end_ms=260))
    assert meta.mode == "resume"
    fr = {f.id: f for f in resp.frames}
    assert (fr["I_long"].start_ms, fr["I_long"].end_ms, fr["I_long"].status) == (
        140,
        200,
        "sent",
    )
    # 与从头重放完全一致
    cold = {f.id: f for f in simulate(ReplayRequest(**payload, replay_end_ms=260)).frames}
    assert fr["I_long"].model_dump() == cold["I_long"].model_dump()


def test_resume_continues_queued_frames_across_old_endpoint():
    """旧终点 100 时 A2 仍排队；推进到 400 后应被调度发送并保留此前结果。"""
    simulate_cached(_req(100))
    resp, meta = simulate_cached(_req(400))
    assert meta.mode == "resume"
    assert _dump(resp) == _dump(simulate(_req(400)))
    fr = {f.id: f for f in resp.frames}
    assert fr["A1"].status == "sent"  # 旧终点前已完成的调度被复用
    assert fr["A2"].status == "sent"


def test_resume_finishes_pending_switch_after_old_endpoint():
    """旧终点落在待生效换配之前；延长后换配仍按原规则生效。"""
    payload = {
        "period_ms": 100,
        "reserve_ms": 40,
        "alarm_deadline_ms": 1000,
        "frames": [_frame("A1", "alarm", 170, 20)],
        "switch_at_ms": 170,
        "new_period_ms": 80,
        "new_reserve_ms": 30,
    }
    prev, _ = simulate_cached(ReplayRequest(**payload, replay_end_ms=175))
    assert prev.switch.state == "not_switched"
    assert prev.switch.requested_at_ms == 170

    resp, meta = simulate_cached(ReplayRequest(**payload, replay_end_ms=300))
    assert meta.mode == "resume"
    assert resp.switch.state == "switched"
    assert resp.switch.effective_at_ms == 200
    assert _dump(resp) == _dump(simulate(ReplayRequest(**payload, replay_end_ms=300)))


def test_resume_after_crossing_endpoint_of_in_progress_switch():
    """旧终点把在发帧和换配都截在中间，延长后两者都要正确收尾。"""
    ends = [149, 150, 155, 199, 200, 201, 260, 261, 400]
    seen_resume = False
    for end in ends:
        resp, meta = simulate_cached(_req(end))
        assert _dump(resp) == _dump(simulate(_req(end))), f"end={end}"
        seen_resume = seen_resume or meta.mode == "resume"
    assert seen_resume


# ---------------------------------------------------------------- 缓存形状
def test_only_one_checkpoint_is_retained_regardless_of_tried_endpoints():
    for end in [50, 100, 200, 300, 400, 1000, 5000]:
        simulate_cached(_req(end))
    snap = progress_cache_snapshot()
    assert snap["retained_states"] == 1
    assert snap["end_ms"] == 5000


def test_checkpoint_does_not_regress_on_shorter_request():
    simulate_cached(_req(400))
    resp, meta = simulate_cached(_req(100))
    # 短请求仍给出正确结果，但不回退已保留的进度
    assert meta.resumed_from_ms is None
    assert _dump(resp) == _dump(simulate(_req(100)))
    assert progress_cache_snapshot()["end_ms"] == 400


def test_scheduling_steps_resume_is_less_than_cold():
    """旧终点前已完成的工作被复用：续算步数明显少于从 0 重放到最终终点。"""
    frames = [
        _frame(f"A{i}", "alarm", i * 100, 10) for i in range(30)
    ]
    payload = {
        "period_ms": 100,
        "reserve_ms": 40,
        "alarm_deadline_ms": 1000,
        "frames": frames,
    }
    _, cold_meta = simulate_cached(ReplayRequest(**payload, replay_end_ms=3000))
    _, resume_meta = simulate_cached(ReplayRequest(**payload, replay_end_ms=4000))
    assert resume_meta.mode == "resume"
    assert resume_meta.scheduling_steps < cold_meta.scheduling_steps


# ---------------------------------------------------------------- 随机差分
def test_randomized_resume_matches_cold_across_endpoint_ladders():
    """随机抓包 + 随机终点阶梯：每一步续算结果都必须与从头重放逐字段一致。"""
    import random

    rng = random.Random(20261124)
    for _trial in range(150):
        period = rng.randint(1, 80)
        reserve = rng.randint(0, period)
        frames = [
            _frame(
                f"F{i}",
                rng.choice(["alarm", "image"]),
                rng.randint(0, 500),
                rng.randint(1, 80),
            )
            for i in range(rng.randint(0, 10))
        ]
        payload = {
            "period_ms": period,
            "reserve_ms": reserve,
            "frames": frames,
            "alarm_deadline_ms": rng.randint(0, 200),
        }
        if rng.random() < 0.5:
            p2 = rng.randint(1, 80)
            payload.update(
                switch_at_ms=rng.randint(0, 500),
                new_period_ms=p2,
                new_reserve_ms=rng.randint(0, p2),
            )
        ends = rng.sample(range(1, 800), rng.randint(2, 6))
        for end in ends:
            req = ReplayRequest(**payload, replay_end_ms=end)
            resp, _ = simulate_cached(req)
            assert _dump(resp) == _dump(simulate(req)), f"end={end} payload={payload}"


def test_frame_arriving_while_switch_waits_is_queued_not_not_arrived():
    """换配等待周期边界期间到达的帧：已到达应记 queued，不能因快进到终点记 not_arrived。"""
    req = ReplayRequest(
        period_ms=100,
        reserve_ms=40,
        alarm_deadline_ms=1000,
        replay_end_ms=70,
        frames=[_frame("I_late", "image", 60, 5)],
        switch_at_ms=50,
        new_period_ms=80,
        new_reserve_ms=30,
    )
    resp = simulate(req)
    fr = resp.frames[0]
    assert fr.status == "queued"
    assert resp.stats.queued == 1 and resp.stats.not_arrived == 0


# ---------------------------------------------------------------- 并发
def test_concurrent_short_and_long_requests_never_regress_progress():
    """同一抓包长短请求并发：无论到达顺序如何，保留进度不倒退且响应都正确。"""
    ends = [50, 400, 120, 1000, 200, 800] * 4
    results: dict[int, list] = {e: [] for e in ends}
    errors: list[str] = []
    barrier = threading.Barrier(len(ends))

    def worker(end: int) -> None:
        try:
            barrier.wait()
            resp, meta = simulate_cached(_req(end))
            results[end].append((resp, meta))
        except Exception as exc:  # pragma: no cover - 失败时给出细节
            errors.append(f"end={end}: {exc!r}")

    threads = [threading.Thread(target=worker, args=(e,)) for e in ends]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    for end, got in results.items():
        expected = _dump(simulate(_req(end)))
        for resp, _meta in got:
            assert _dump(resp) == expected, f"并发下 end={end} 结果错乱"
    assert progress_cache_snapshot()["end_ms"] == max(ends)
    assert progress_cache_snapshot()["retained_states"] == 1


# ---------------------------------------------------------------- HTTP
def test_http_headers_expose_resume_without_changing_body():
    client = TestClient(app)
    first = client.post("/replay", json={**BASE, "replay_end_ms": 320})
    assert first.status_code == 200
    assert first.headers["x-replay-mode"] == "cold"

    second = client.post("/replay", json={**BASE, "replay_end_ms": 400})
    assert second.status_code == 200
    assert second.headers["x-replay-mode"] == "resume"
    assert int(second.headers["x-replay-resumed-from-ms"]) == 320
    assert int(second.headers["x-replay-checkpoint-end-ms"]) == 400
    assert int(second.headers["x-replay-scheduling-steps"]) > 0

    # 响应体与从头重放一致
    assert second.json() == simulate(_req(400)).model_dump(mode="json")

    prog = client.get("/progress").json()
    assert prog["end_ms"] == 400 and prog["retained_states"] == 1
