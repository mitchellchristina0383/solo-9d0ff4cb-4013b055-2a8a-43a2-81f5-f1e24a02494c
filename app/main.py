"""FastAPI 入口：通过 /docs 可直接发起重放。"""

from __future__ import annotations

from fastapi import FastAPI, Response

from .models import ReplayRequest, ReplayResponse
from .replay import progress_cache_snapshot, simulate_cached

app = FastAPI(
    title="交换机出口 告警/相机图像 重放试算 API",
    description=(
        "产线告警与相机图像共用交换机出口的重放仿真。\n\n"
        "- 时间一律整数毫秒，时间轴从 0 开始，仿真区间 `[0, replay_end_ms)`。\n"
        "- 每周期前段（长度 reserve_ms）只发告警，其余时间只发图像。\n"
        "- 出口一次只发一帧，整帧必须完全落在本类时段内才会开始发送。\n"
        "- 同类帧按到达顺序发送（同时到达按请求中的先后）。\n"
        "- 可在指定时刻换一套周期/预留：到点若正在发帧则等其发完再切换，排队帧保留，"
        "新周期从实际生效时刻起算。\n"
        "- 告警从到达计时，发完即停止；未发完算到重放终点；耗时严格大于告警时限即超时。\n"
        "- 同一份抓包（帧顺序与其他字段一致）只把 `replay_end_ms` 往后移时，服务从已保留的"
        "进度续算并复用之前的调度结果；响应体仍等于按该终点从头重放的结果。\n"
        "- 续算情况通过响应头 `X-Replay-Mode` / `X-Replay-Scheduling-Steps` / "
        "`X-Replay-Resumed-From-Ms` / `X-Replay-Checkpoint-End-Ms` 透出，不进入响应体。"
    ),
    version="1.0.0",
)

_EXAMPLE_PAYLOAD = {
    "period_ms": 100,
    "reserve_ms": 40,
    "alarm_deadline_ms": 80,
    "replay_end_ms": 320,
    "frames": [
        {"id": "A1", "type": "alarm", "arrival_ms": 0, "busy_ms": 30},
        {"id": "A2", "type": "alarm", "arrival_ms": 5, "busy_ms": 60},
        {"id": "I1", "type": "image", "arrival_ms": 10, "busy_ms": 50},
        {"id": "A3", "type": "alarm", "arrival_ms": 120, "busy_ms": 25},
        {"id": "I2", "type": "image", "arrival_ms": 130, "busy_ms": 70},
    ],
    "switch_at_ms": 150,
    "new_period_ms": 120,
    "new_reserve_ms": 60,
}


@app.get("/health", tags=["运维"], summary="健康检查")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/progress", tags=["运维"], summary="查看进程内保留的一份重放进度")
def progress() -> dict[str, int | str | None]:
    """返回当前缓存的抓包标识与已推进到的终点；没有缓存时均为 null。"""
    return progress_cache_snapshot()


@app.post(
    "/replay",
    response_model=ReplayResponse,
    tags=["重放"],
    summary="执行一次重放试算",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "含换配的完整示例": {"value": _EXAMPLE_PAYLOAD},
                        "同一抓包先看短终点": {
                            "summary": "终点 320：先发一次，建立进度",
                            "value": {**_EXAMPLE_PAYLOAD, "replay_end_ms": 320},
                        },
                        "同一抓包终点后移": {
                            "summary": "终点 420：只改 replay_end_ms，服务续算",
                            "value": {**_EXAMPLE_PAYLOAD, "replay_end_ms": 420},
                        },
                        "不换配（最简）": {
                            "value": {
                                "period_ms": 100,
                                "reserve_ms": 40,
                                "alarm_deadline_ms": 80,
                                "replay_end_ms": 200,
                                "frames": [
                                    {"type": "alarm", "arrival_ms": 0, "busy_ms": 30},
                                    {"type": "image", "arrival_ms": 0, "busy_ms": 50},
                                ],
                            }
                        },
                    }
                }
            }
        }
    },
    responses={
        200: {
            "description": "重放结果；续算信息见响应头",
            "headers": {
                "X-Replay-Mode": {"description": "resume=从已保留进度续算；cold=从头重放"},
                "X-Replay-Scheduling-Steps": {"description": "本次实际执行的调度步数"},
                "X-Replay-Resumed-From-Ms": {"description": "续算起点毫秒；cold 时为 none"},
                "X-Replay-Checkpoint-End-Ms": {"description": "当前保留进度对应的终点毫秒"},
            },
        }
    },
)
def replay(req: ReplayRequest, response: Response) -> ReplayResponse:
    """按请求参数仿真，返回实际切换时刻、各帧发送起止、终点状态与超时告警。

    同一份抓包（帧顺序与其他字段一致）只把 ``replay_end_ms`` 往后移时，服务会从
    已保留的进度续算并复用之前的调度结果；响应体与按该终点从头重放完全一致。
    续算情况只通过响应头透出，不影响响应体：

    - ``X-Replay-Mode``：``resume`` / ``cold``
    - ``X-Replay-Scheduling-Steps``：本次实际执行的调度步数
    - ``X-Replay-Resumed-From-Ms``：续算起点（cold 时为 ``none``）
    - ``X-Replay-Checkpoint-End-Ms``：当前保留进度对应的终点
    """
    result, meta = simulate_cached(req)
    response.headers["X-Replay-Mode"] = meta.mode
    response.headers["X-Replay-Scheduling-Steps"] = str(meta.scheduling_steps)
    response.headers["X-Replay-Resumed-From-Ms"] = (
        "none" if meta.resumed_from_ms is None else str(meta.resumed_from_ms)
    )
    response.headers["X-Replay-Checkpoint-End-Ms"] = str(meta.checkpoint_end_ms)
    return result
