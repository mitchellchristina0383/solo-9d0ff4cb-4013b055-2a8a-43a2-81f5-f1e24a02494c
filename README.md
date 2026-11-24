# 交换机出口 告警/相机图像 重放试算 API

产线告警与相机图像共用同一台交换机出口。本服务用离散事件仿真重放一段时间内的帧发送过程，
供网管在**改配前试算**：调整周期与告警预留时长后，哪些告警会超时。

所有时间均为**整数毫秒**，时间轴从 `0` 开始，仿真区间为半开区间 `[0, replay_end_ms)`。

## 调度规则

1. 时间轴按周期划分，每个周期内：
   - 前段 `[周期起点, 周期起点 + reserve_ms)` 为**告警时段**，只允许发告警帧；
   - 其余为**图像时段**，只允许发图像帧。
   - `reserve=0` 表示没有告警时段；`reserve=period` 表示没有图像时段。
2. 出口同一时刻只发一帧；同类帧严格按到达顺序发送（同时到达按请求中的先后）。
3. **整帧必须落在本类时段内**才会开始发送：`now + busy_ms <= 时段终点`。
   队首帧放不进当前时段时，同队后续帧不许插队，本时段剩余时间空转，等下一个同类时段。
4. 可在指定时刻换一套周期/预留时长。切换只在**周期边界**生效：
   - 到点若出口正在发帧，等该帧发完（走到下一个周期边界）再切换；
   - 到点若出口空闲，等当前周期结束的边界再切换；
   - 排队帧保留；新周期从实际生效时刻重新对齐（生效时刻即新周期的第 0 毫秒）。
5. 告警从到达开始计时：发完即停止（耗时 = 发送终点 − 到达）；未发完（发送中或排队）算到重放终点
   （耗时 = `replay_end_ms − 到达`）。耗时**严格大于**告警时限即超时。

帧在终点时刻的状态：

| status | 含义 |
|---|---|
| `sent` | 已发完，返回 `start_ms` / `end_ms` |
| `in_progress` | 终点时仍在发送（未发完），有 `start_ms`，`end_ms=null` |
| `queued` | 已到达但排队未发 |
| `not_arrived` | 到达时刻不早于重放终点，未进入仿真 |

## 运行

### Docker Compose（推荐交付方式）

```bash
docker compose up --build
# 打开浏览器
#   http://localhost:8000/docs
```

`/docs` 内置两个可直接 “Try it out” 的示例（含换配 / 不换配）。

### 本地（无 Docker 时）

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 接口

### `POST /replay`

请求字段：

| 字段 | 说明 |
|---|---|
| `period_ms` / `reserve_ms` | 初始周期、每周期告警预留时长（`0 <= reserve <= period`） |
| `frames[]` | 每帧：`type`（`alarm`/`image`）、`arrival_ms`、`busy_ms`、可选 `id`（缺省 F1、F2…） |
| `alarm_deadline_ms` | 告警时限，耗时严格大于该值判超时 |
| `replay_end_ms` | 重放终点 |
| `switch_at_ms` + `new_period_ms` + `new_reserve_ms` | 可选，三者须同时给出 |

响应返回：`switch`（计划/实际切换时刻与 `switched`/`not_switched`/`not_requested` 状态）、
终点时刻实际生效的周期与预留、逐帧发送起止与终点状态、告警计时与超时标记、汇总统计。

另有 `GET /health` 健康检查，`GET /progress` 查看当前保留的一份进度（抓包摘要、已推进终点）。

### 增量续算（同一份抓包只把终点往后移）

网管通常从短终点开始逐步拉长，观察告警从哪一刻起积压。为避免每次都从 0 重算，
`/replay` 在进程内保留**最多一份**抓包进度：

- **同一份抓包**：帧顺序与除 `replay_end_ms` 外的全部请求字段都相同（`id`/`type`/
  `arrival_ms`/`busy_ms` 及周期、预留、时限、换配参数）才算同一份；仅终点不同不影响。
- **续算**：新终点不早于已保留终点时，从快照直接推进：跨过旧终点的在发帧继续发完、
  排队帧继续调度、已武装的换配继续等边界生效；旧终点前已完成的逐帧结果原样复用。
- **结果一致**：响应体始终等于“按本次终点从头重放”的完整结果（有随机差分测试守护）。
- **缓存形状**：单槽，后一份抓包替换前一份；同抓包只允许快照终点前进，
  已试过的终点再多也不增加内存，进度大小只与帧数有关。
- **并发**：同一把锁串行化“读取/续算/写回”，长短请求并发时短请求照常返回正确结果，
  但不会用更短的进度覆盖已保留的更长进度（终点只增不减）。
- **可观察性**：续算信息只放在响应头，不进入响应体：
  `X-Replay-Mode`（`resume`/`cold`）、`X-Replay-Scheduling-Steps`（本次调度步数）、
  `X-Replay-Resumed-From-Ms`（续算起点，cold 为 `none`）、
  `X-Replay-Checkpoint-End-Ms`（当前保留进度的终点）。

`/docs` 中“同一抓包先看短终点 / 同一抓包终点后移”两个示例可依次 Try it out，
对比响应头里的调度步数。


### 请求示例

```json
{
  "period_ms": 100,
  "reserve_ms": 40,
  "alarm_deadline_ms": 80,
  "replay_end_ms": 320,
  "frames": [
    {"id": "A1", "type": "alarm", "arrival_ms": 0,   "busy_ms": 30},
    {"id": "A2", "type": "alarm", "arrival_ms": 5,   "busy_ms": 60},
    {"id": "I1", "type": "image", "arrival_ms": 10,  "busy_ms": 50},
    {"id": "A3", "type": "alarm", "arrival_ms": 120, "busy_ms": 25},
    {"id": "I2", "type": "image", "arrival_ms": 130, "busy_ms": 70}
  ],
  "switch_at_ms": 150,
  "new_period_ms": 120,
  "new_reserve_ms": 60
}
```

## 目录结构

```
app/
  main.py      FastAPI 入口（/replay、/health、/progress、/docs）
  models.py    Pydantic 请求/响应模型与交叉字段校验
  replay.py    离散事件仿真引擎 + 单槽进度缓存（续算）
tests/
  test_replay.py             18 个基础用例
  test_replay_incremental.py 15 个续算/缓存/并发/差分用例
Dockerfile / docker-compose.yml / requirements.txt
```

## 自测

```bash
pip install -r requirements.txt pytest httpx
pytest -q
```

覆盖重点：

- **整帧放不进本类时段**：队首阻塞（后续同类帧不插队）、放不进的帧持续 `queued` 且不影响异类、
  换配扩大预留后队首先于后续帧发出；
- **发帧中改配**：换配时刻落在发送区间内时，等帧发完的周期边界才切换、排队帧保留、
  新周期从生效时刻重对齐；空闲时换配等当前周期结束；恰好落在边界则当刻生效；
  被推迟到终点之后则 `not_switched`；
- 告警超时边界（等于时限不超时、严格大于才超时）、`in_progress`/`queued` 计时到终点；
- `reserve=0`、`reserve=period`、到达时刻等于终点、缺省 id、参数校验 422；
- 发送区间互不重叠、区间长度等于占线时长等不变量；
- 大终点（10^7 ms）快速收敛，不随周期数空转；
- **续算一致性**：终点阶梯上每一步续算的完整响应都等于该终点从头重放；
  跨旧终点的在发帧 / 排队帧 / 待生效换配都正确收尾；帧顺序或其他字段变化视为新抓包；
- **缓存与并发**：只保留一份进度（`retained_states == 1`）；短请求不回退保留进度；
  长短请求并发交错后逐请求结果正确、保留终点等于最大终点；
- **随机差分**：随机抓包 × 随机终点阶梯，续算与从头重放逐字段一致。

实测（本机 Python 3.11 / FastAPI 0.165 / Pydantic 2.14 / uvicorn 0.54）：
**33 passed**（18 基础 + 15 续算），全量约 1s；此外另跑了 2000 组随机终点阶梯差分、
500 组乱序长短请求与 60 组并发交错，均无不一致。
真实 uvicorn 起服务后 `/health`、`/docs`、`/openapi.json` 均 200，`/progress` 正常，
示例抓包按 180→200→260→400 ms 逐段拉长时响应头依次为
`cold(8 步)`、`resume(1 步)`、`resume(2 步)`、`resume(6 步)`，响应体与从头重放一致。

## 余项 / 约定说明

- 本交付环境无 Docker 守护进程，`docker compose build/up` 仅做了 compose YAML 与 Dockerfile
  静态核对，未实机构建；在有 Docker 的机器上执行上文命令即可。本地已用同一 requirements
  与 uvicorn 命令实起服务验证 `/docs` 与 `/replay`。
- “切换只在周期边界生效”是对“到点若有帧在发，等它发完再切换”的一致化推广：
  出口空闲时也等到当前周期结束，避免在时段中间凭空产生新的时段边界。
- 仿真为纯函数式计算、无外部状态；单次续算耗时只与“新增时间窗内的帧/事件数”有关，
  与 `replay_end_ms` 绝对值无关。
- 缓存按进程内存保留，多 worker（如 `uvicorn --workers N`）时每个进程各有一份，互不共享；
  Compose 默认单 worker。缓存随进程重启丢失，属于纯性能优化，不影响结果正确性。
- 当前按“最近一次使用的抓包”单槽保留：切换回旧抓包会从头重算（规则要求单份缓存，不做多抓包 LRU）。
