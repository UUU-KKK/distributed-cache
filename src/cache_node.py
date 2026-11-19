"""
缓存节点：把 CacheStore 包装成一个可被网络访问的 HTTP 服务。

概念对应（报告可用）：
  - 一个「缓存节点」= 一份内存缓存(CacheStore) + 一个 HTTP 服务 + 一个后台过期清理线程。
  - 多个节点分别监听不同端口，彼此通过网络通信，就构成「缓存集群」。

HTTP 接口一览：
    GET    /health            健康检查
    GET    /stats             统计指标（含复制与回源指标）
    GET    /cache/{key}       读取一条数据（未命中且配置了数据源时自动回源）
    PUT    /cache/{key}       写入一条数据    body: {"value": ..., "ttl": 60}
    DELETE /cache/{key}       删除一条数据
    GET    /dump              导出全部数据（调试用）
    POST   /replicate         接收其它节点复制过来的写入（内部使用）

阶段3 的变化：
  节点多了一个 peers 参数（其它节点的地址列表）。本节点收到 PUT / DELETE 时，
  除了写自己的内存，还会把这次操作丢进复制队列，由 replication.py 的后台线程
  异步推送给所有副本。推送过程中本节点不等回应，所以客户端感知的写延迟不变。

阶段4 的变化：
  节点多了一个 origin 参数（数据源地址）。读取未命中时不再直接返回 404，
  而是先去数据源取（回源），取到后写进缓存再返回。
  为防止「缓存击穿」（热点 key 缺失的瞬间，大量并发回源打爆数据源），
  回源动作经过 singleflight.py 的请求合并：同一时刻同一个 key 只回源一次，
  其余请求原地等待、共享同一份结果。
"""

import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote, urlparse

from cache_store import CacheStore
from replication import ReplicationSender
from singleflight import SingleFlight


class _NodeHTTPServer(ThreadingHTTPServer):
    """演示时会有几十个请求同时涌进来，把 TCP 等待队列调大，避免连接排队造成额外延迟。"""
    request_queue_size = 64


class NodeClock:
    """TTL 使用的时钟。

    mode="mono"：单调时钟（默认）。系统时间被改、被 NTP 往回拨，它都只增不减，
                 所以 TTL 计时不会被"时间倒退"影响。
    mode="wall"：墙钟 time.time()，并叠加一个可调偏移。仅用于实验对照组：
                 把 offset 调成 -60，就等价于"系统时间突然倒退一分钟"。
    """

    def __init__(self, mode="mono"):
        self.mode = mode
        self.offset = 0.0

    def __call__(self):
        if self.mode == "wall":
            return time.time() + self.offset
        return time.monotonic()


class CacheNode:
    """一个可独立运行的缓存节点。"""

    def __init__(self, node_id, port, host="127.0.0.1", capacity=128, policy="lru",
                 peers=None, repl_delay=0.0, origin=None, origin_ttl=30.0, singleflight=True,
                 repl_block=None, repl_dup=1, repl_reorder=False,
                 repl_reorder_window=2, repl_reorder_hold=0.5, clock="mono",
                 drop_response=0):
        self.node_id = node_id
        self.host = host
        self.port = int(port)
        self.clock = NodeClock(clock)
        # 实验用：吞掉接下来 N 个 PUT 的成功响应，
        # 模拟「磁盘写成功，但响应在返回途中丢失」。
        self._fault_lock = threading.Lock()
        self.drop_responses = int(drop_response)
        self.store = CacheStore(capacity=capacity, policy=policy, clock=self.clock)

        self.sweep_interval = 1.0        # 后台过期清理线程的扫描周期（秒）
        self.allow_replication = True    # 是否响应 /replicate（副本节点需要能收）
        self.repl_received = 0           # 收到其它节点复制的次数（统计用）

        # 阶段3：异步复制。peers 为空时该节点就是单机模式（阶段2的行为）。
        # 阶段5：可选的故障注入（分区 / 重复 / 乱序）。
        self.repl = ReplicationSender(
            node_id, peers, delay=repl_delay,
            block_peers=repl_block, dup=repl_dup,
            reorder=repl_reorder, reorder_window=repl_reorder_window,
            reorder_hold=repl_reorder_hold,
        )

        # 阶段4：回源与请求合并。
        self.origin = origin or ""            # 数据源地址；留空=未命中直接 404（阶段2的行为）
        self.origin_ttl = float(origin_ttl)   # 回源取到的数据写入缓存时的存活时间
        self.origin_timeout = 5.0             # 访问数据源的单次超时（秒）
        self.singleflight_enabled = bool(singleflight)
        self.sf = SingleFlight()              # 合并同一 key 的并发回源
        self.origin_fetched = 0               # 真正发往数据源的查询次数

        self._httpd = None
        self._server_thread = None
        self._sweeper_thread = None
        self._stop_event = threading.Event()
        self.started_at = None

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    @property
    def url(self):
        return "http://%s:%d" % (self.host, self.port)

    def _start_sweeper(self):
        """后台线程：定期清理过期条目，避免过期数据长期占容量。"""
        def loop():
            while not self._stop_event.wait(self.sweep_interval):
                removed = self.store.sweep_expired()
                if removed:
                    print("[%s] 后台清理 %d 条过期数据" % (self.node_id, removed))

        self._sweeper_thread = threading.Thread(
            target=loop, name="%s-sweeper" % self.node_id, daemon=True)
        self._sweeper_thread.start()

    def start_background(self):
        """在后台线程中启动 HTTP 服务（用于同一进程内起多个节点/测试）。"""
        self._httpd = _NodeHTTPServer((self.host, self.port), make_handler(self))
        self.started_at = time.time()
        self._start_sweeper()
        self.repl.start()
        self._server_thread = threading.Thread(
            target=self._httpd.serve_forever, name="%s-http" % self.node_id, daemon=True)
        self._server_thread.start()
        print("[%s] 已启动，监听 %s (容量=%d, 策略=%s)"
              % (self.node_id, self.url, self.store.capacity, self.store.policy))
        return self

    def serve_forever(self):
        """阻塞式启动（用于独立进程运行一个节点）。"""
        self._httpd = _NodeHTTPServer((self.host, self.port), make_handler(self))
        self.started_at = time.time()
        self._start_sweeper()
        self.repl.start()
        print("[%s] 已启动，监听 %s (容量=%d, 策略=%s)"
              % (self.node_id, self.url, self.store.capacity, self.store.policy))
        try:
            self._httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self):
        self._stop_event.set()
        self.repl.stop()
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        print("[%s] 已停止" % self.node_id)

    # ------------------------------------------------------------------ #
    # 阶段4：回源（带请求合并）
    # ------------------------------------------------------------------ #
    def _fetch_from_origin(self, key):
        """真正访问一次数据源（这是"慢"操作）。"""
        print("[%s] 回源 key=%s" % (self.node_id, key))
        url = "%s/data/%s" % (self.origin.rstrip("/"), quote(key, safe=""))
        with urllib.request.urlopen(url, timeout=self.origin_timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        self.origin_fetched += 1
        return data["value"]

    def take_drop_response(self):
        """如果还有待丢弃的响应，消耗一个并返回 True。"""
        with self._fault_lock:
            if self.drop_responses > 0:
                self.drop_responses -= 1
                return True
        return False

    def remaining_ttl(self, key):
        """把 store.ttl() 的 -1/-2 约定转换成复制报文用的 None / 剩余秒数。

        复制必须携带「剩余存活时间」：如果直接把原始 TTL 发出去，副本会在
        收到时重新起算，导致同一份数据在副本上比源节点多活一个复制延迟。
        """
        ttl = self.store.ttl(key)
        return None if ttl < 0 else ttl

    def fill_from_origin(self, key):
        """未命中时从数据源补齐：取回 -> 写缓存 -> 复制给副本，返回取到的值。"""
        def do_fill():
            value = self._fetch_from_origin(key)
            self.store.set(key, value, ttl=self.origin_ttl)
            self.repl.enqueue("set", key, value, self.remaining_ttl(key))
            return value

        if self.singleflight_enabled:
            return self.sf.do(key, do_fill)    # 并发时只放 1 个请求去执行 do_fill
        return do_fill()


def make_handler(node):
    """为某个节点生成配套的请求处理器。"""

    class Handler(BaseHTTPRequestHandler):
        server_version = "TinyCacheNode/1.0"

        # -------- 工具方法 -------- #
        def _send_json(self, code, obj):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return None

        @staticmethod
        def _split(path):
            """/cache/foo -> ('cache', 'foo')"""
            parts = [unquote(p) for p in path.strip("/").split("/")]
            return parts

        def log_message(self, fmt, *args):
            # 关闭默认的逐条访问日志，让控制台干净；需要时可自行打开
            pass

        # -------- GET -------- #
        def do_GET(self):
            parts = self._split(urlparse(self.path).path)

            if parts == ["health"]:
                return self._send_json(200, {
                    "ok": True, "node_id": node.node_id,
                    "uptime": round(time.time() - node.started_at, 2),
                })

            if parts == ["stats"]:
                merged = node.store.stats()
                merged.update(node.repl.stats())
                merged.update(node.sf.stats())
                merged["repl_received"] = node.repl_received
                merged["origin"] = node.origin
                merged["origin_fetched"] = node.origin_fetched
                merged["singleflight"] = node.singleflight_enabled
                merged["clock"] = node.clock.mode
                merged["drop_responses_remaining"] = node.drop_responses
                return self._send_json(200, merged)

            if parts == ["dump"]:
                return self._send_json(200, {"node_id": node.node_id, "items": node.store.dump()})

            if len(parts) == 2 and parts[0] == "cache":
                key = parts[1]
                value = node.store.get(key)
                if value is None and key not in node.store:
                    if not node.origin:
                        return self._send_json(404, {"found": False, "key": key})
                    # 阶段4：未命中且配置了数据源 -> 回源（并发时由 singleflight 合并）
                    try:
                        filled = node.fill_from_origin(key)
                    except Exception as exc:
                        return self._send_json(502, {"found": False, "key": key,
                                                     "error": "回源失败: %s" % exc})
                    return self._send_json(200, {
                        "found": True, "key": key, "value": filled,
                        "ttl": node.store.ttl(key), "source": "origin",
                    })
                return self._send_json(200, {
                    "found": True, "key": key, "value": value,
                    "ttl": node.store.ttl(key), "source": "cache",
                })

            return self._send_json(404, {"error": "未知路径: %s" % self.path})

        # -------- PUT 写入 -------- #
        def do_PUT(self):
            parts = self._split(urlparse(self.path).path)
            if not (len(parts) == 2 and parts[0] == "cache"):
                return self._send_json(404, {"error": "未知路径: %s" % self.path})

            payload = self._read_json()
            if payload is None:
                return self._send_json(400, {"error": "请求体不是合法 JSON"})
            if "value" not in payload:
                return self._send_json(400, {"error": "缺少字段 value"})

            key = parts[1]
            ttl = payload.get("ttl")
            node.store.set(key, payload["value"], ttl=ttl)
            # 阶段3：本地写成功后立刻返回，同时把这次写入丢进复制队列异步抄给副本。
            # 注意复制的是「剩余存活时间」，不是客户端传入的原始 ttl。
            node.repl.enqueue("set", key, payload["value"], node.remaining_ttl(key))

            if node.take_drop_response():
                # 关键：本地已经写成功、复制也已入队，只是不回响应直接断开。
                # 客户端看到的是「连接异常/超时」，但这次写入其实已经生效。
                self.close_connection = True
                try:
                    self.wfile.flush()
                except Exception:
                    pass
                return

            return self._send_json(200, {"ok": True, "key": key, "ttl": node.store.ttl(key)})

        # -------- DELETE -------- #
        def do_DELETE(self):
            parts = self._split(urlparse(self.path).path)
            if len(parts) == 2 and parts[0] == "cache":
                ok = node.store.delete(parts[1])
                node.repl.enqueue("delete", parts[1])
                return self._send_json(200, {"ok": ok, "key": parts[1]})
            return self._send_json(404, {"error": "未知路径: %s" % self.path})

        # -------- POST 复制写入 -------- #
        def do_POST(self):
            parts = self._split(urlparse(self.path).path)

            # 实验用：设置「要吞掉几个 PUT 响应」，模拟响应丢失
            if parts == ["admin", "fault"]:
                payload = self._read_json() or {}
                node.drop_responses = int(payload.get("drop_responses", 0))
                return self._send_json(200, {
                    "ok": True,
                    "drop_responses": node.drop_responses,
                    "note": "接下来的 PUT 会写成功但不回响应",
                })

            # 实验用：调整墙钟偏移，模拟「系统时间突然倒退一分钟」
            if parts == ["admin", "clock"]:
                payload = self._read_json() or {}
                offset = float(payload.get("offset", 0))
                if node.clock.mode == "wall":
                    node.clock.offset += offset
                    return self._send_json(200, {
                        "ok": True, "mode": "wall",
                        "offset": node.clock.offset,
                        "note": "墙钟已偏移，TTL 立即受影响",
                    })
                return self._send_json(200, {
                    "ok": False, "mode": "mono",
                    "note": "单调时钟不可被拨动，TTL 不受影响",
                })

            if parts != ["replicate"]:
                return self._send_json(404, {"error": "未知路径: %s" % self.path})

            if not node.allow_replication:
                return self._send_json(200, {"ok": True, "skipped": "本节点未开启复制"})

            payload = self._read_json()
            if payload is None:
                return self._send_json(400, {"error": "请求体不是合法 JSON"})

            key = payload.get("key")
            if key is None:
                return self._send_json(400, {"error": "缺少字段 key"})

            # 副本节点只做本地写入，不再向外扩散（避免复制风暴）
            if payload.get("op") == "delete":
                node.store.delete(key)
            else:
                node.store.set(key, payload.get("value"), ttl=payload.get("ttl"))
            node.repl_received += 1
            return self._send_json(200, {"ok": True, "key": key})

    return Handler