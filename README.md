# 分布式缓存系统（课程场景 8）

一个三节点的临时缓存服务：单机侧提供 TTL 过期、LRU/LFU 淘汰、回源与请求合并，集群侧通过异步复制同步数据。每个节点是独立进程，使用 HTTP + JSON 通信，运行时不依赖第三方库。

- 5 套测试，67 项自动断言
- 11 个故障注入实验，覆盖节点宕机、网络分区、重复、乱序、时钟回拨、热点 key、响应丢失
- 详细设计与取舍见 [docs/report.md](docs/report.md)

## 目录结构

```
.
├── src/
│   ├── cache_store.py      存储内核：TTL / LRU / LFU / 统计
│   ├── cache_node.py       HTTP 服务、回源、故障开关
│   ├── replication.py      异步复制（每副本一条队列 + 独立线程）
│   ├── singleflight.py     请求合并
│   └── origin.py           模拟数据源（慢查询 + 并发上限 + 访问计数）
├── tests/                  5 套测试
├── start_node.py           启动单个节点
├── start_cluster.py        一键启动集群（可选带数据源）
├── start_origin.py         单独启动数据源
├── verify_replication.py   复制效果验证
├── verify_stampede.py      击穿保护验证
├── fault_experiments.py    11 个故障注入实验
└── docs/                   报告与演示文稿
```

## 环境要求

- Python 3.9+
- 运行时只用标准库；测试与验证脚本需要 `requests`

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 快速开始

启动 3 个节点和数据源（端口 9301–9303，数据源 9311）：

```powershell
.venv\Scripts\python.exe start_cluster.py --nodes 3 --base-port 9301 --with-origin
```

另开一个终端验证：

```powershell
curl.exe http://127.0.0.1:9301/health
Invoke-RestMethod -Method Put -Uri http://127.0.0.1:9301/cache/foo -ContentType "application/json" -Body (@{ value = "hello"; ttl = 60 } | ConvertTo-Json)
curl.exe http://127.0.0.1:9301/cache/foo
curl.exe http://127.0.0.1:9302/stats
```

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/stats` | 存储、复制、回源、请求合并指标 |
| GET | `/dump` | 导出当前全部数据 |
| GET | `/cache/{key}` | 读取；未命中且配置了数据源时自动回源 |
| PUT | `/cache/{key}` | 写入，body：`{"value": ..., "ttl": 60}` |
| DELETE | `/cache/{key}` | 删除 |
| POST | `/replicate` | 接收副本写入（集群内部） |
| POST | `/admin/fault` | 设置吞掉的 PUT 响应数：`{"drop_responses": 1}` |
| POST | `/admin/clock` | 调整墙钟偏移（秒）：`{"offset": -60}` |

数据源接口：`/data/{key}`、`/health`、`/stats`、`/reset`。

## 启动参数

集群（`start_cluster.py`）：

| 参数 | 说明 | 默认 |
|---|---|---|
| `--nodes N` | 节点个数 | 3 |
| `--base-port P` | 起始端口（数据源为 P+10） | 9001 |
| `--capacity N` | 每个节点最大条数 | 50 |
| `--policy lru\|lfu` | 淘汰策略 | lru |
| `--repl-delay S` | 复制人为延迟（秒） | 0 |
| `--isolate NODE` | 把指定节点与其余节点双向隔离 | 关 |
| `--repl-dup N` | 每条复制消息重复发送 N 次 | 1 |
| `--repl-reorder` | 复制消息乱序发送 | 关 |
| `--repl-reorder-hold S` | 乱序缓冲区最长停留（秒） | 0.5 |
| `--with-origin` | 启动数据源并开启回源 | 关 |
| `--origin-delay S` | 数据源单次查询耗时（秒） | 0.2 |
| `--no-singleflight` | 关闭请求合并（对照组） | 关 |

单节点（`start_node.py`）另有 `--clock mono|wall` 和 `--drop-response N`，用于时钟回拨与响应丢失实验。

## 验证脚本

```powershell
# 异步复制：写入后立刻读副本 404，等待后再读 200
.venv\Scripts\python.exe verify_replication.py --base-port 9301 --wait 8

# 缓存击穿：有/无请求合并的回源次数与尾延迟对比
.venv\Scripts\python.exe verify_stampede.py --base-port 9301
```

## 测试

```powershell
.venv\Scripts\python.exe tests\test_cache_store.py
.venv\Scripts\python.exe tests\test_singleflight.py
.venv\Scripts\python.exe tests\test_cache_node.py
.venv\Scripts\python.exe tests\test_replication.py
.venv\Scripts\python.exe tests\test_stampede.py
```

## 故障注入实验

```powershell
.venv\Scripts\python.exe fault_experiments.py
```

脚本会自动启动数据源和 3 个节点，依次执行 11 个实验，每个实验都按“预测 → 注入 → 对照 → 解释”输出。全程约 110 秒。

实验按阶段隔离环境，避免回源掩盖复制层的缺口：

- 实验 1–3：纯缓存集群，测 TTL、不一致窗口、副本故障
- 实验 4–5：接上数据源，测数据源故障与缓存击穿
- 实验 6–11：再切回纯缓存，测分区、重复、乱序、时钟回拨、热点 key、响应丢失

## 设计边界

- 异步复制：写只等本地成功，存在不一致窗口，崩溃时队列内数据会丢
- 无 CAS / 无版本比较：同一 key 的并发写按到达顺序覆盖
- 不支持在线迁移与弹性扩容：固定 3 节点，每个节点全量副本
- 复制失败不重试、无反熵：离线期间的写入不会自动补回

以上取舍的理由、代价和补法见报告第五章。

## 文档

- 设计与实验报告：[docs/report.md](docs/report.md)
- 答辩演示文稿：`docs/分布式缓存系统-课程答辩.pptx`
