"""
模拟「数据源」（真实系统里这一步就是数据库）。

它是缓存回源的目标：缓存没命中时，缓存节点就来这里要数据。
为了能演示「缓存击穿」，它故意做了三件事：
  1. 每次查询都慢一拍（--delay，模拟慢查询）；
  2. 并发处理能力有上限（--parallel，默认 1，模拟数据库一次只能处理一个查询）；
  3. 记录被访问的次数（总数 + 每个 key 的次数）——这是衡量击穿强度的仪表盘。

HTTP 接口：
    GET  /data/{key}    取数据（先睡 delay 秒，受并发上限约束）
    GET  /health        健康检查
    GET  /stats         访问统计（calls_total / calls_by_key / delay / parallel）
    POST /reset         清零统计（做对比实验前先清零）
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse


class _OriginHTTPServer(ThreadingHTTPServer):
    """演示时会有几十个请求同时涌进来，把 TCP 等待队列调大，避免连接排队造成额外延迟。"""
    request_queue_size = 64


class OriginServer:
    """一个可独立运行的数据源服务。"""

    def __init__(self, host="127.0.0.1", port=9311, delay=0.2, parallel=1):
        self.host = host
        self.port = int(port)
        self.delay = float(delay)
        self.parallel = max(1, int(parallel))
        self._lock = threading.Lock()
        self._sem = threading.Semaphore(self.parallel)
        self.calls_total = 0
        self.calls_by_key = {}
        self._httpd = None
        self._server_thread = None
        self.started_at = None

    @property
    def url(self):
        return "http://%s:%d" % (self.host, self.port)

    def _record_call(self, key):
        with self._lock:
            self.calls_total += 1
            self.calls_by_key[key] = self.calls_by_key.get(key, 0) + 1
            return self.calls_total

    def reset_stats(self):
        with self._lock:
            self.calls_total = 0
            self.calls_by_key = {}

    def serve_data(self, key):
        n = self._record_call(key)
        print("[origin] 回源请求 #%d: key=%s" % (n, key))
        # 模拟慢查询 + 数据库处理能力有限（并发上限）
        with self._sem:
            time.sleep(self.delay)
        return {"key": key, "value": "value-for-%s" % key, "delay": self.delay}

    def start_background(self):
        """在后台线程中启动 HTTP 服务（用于同一进程内测试）。"""
        self._httpd = _OriginHTTPServer((self.host, self.port), make_handler(self))
        self.started_at = time.time()
        self._server_thread = threading.Thread(
            target=self._httpd.serve_forever, name="origin-http", daemon=True)
        self._server_thread.start()
        print("[origin] 已启动，监听 %s (每次查询 %.2fs, 并发上限 %d)"
              % (self.url, self.delay, self.parallel))
        return self

    def serve_forever(self):
        """阻塞式启动（用于独立进程运行数据源）。"""
        self._httpd = _OriginHTTPServer((self.host, self.port), make_handler(self))
        self.started_at = time.time()
        print("[origin] 已启动，监听 %s (每次查询 %.2fs, 并发上限 %d)"
              % (self.url, self.delay, self.parallel))
        try:
            self._httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self):
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        print("[origin] 已停止")


def make_handler(origin):
    """为某个数据源生成配套的请求处理器。"""

    class Handler(BaseHTTPRequestHandler):
        server_version = "TinyOrigin/1.0"

        def _send_json(self, code, obj):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            pass

        @staticmethod
        def _split(path):
            return [unquote(p) for p in path.strip("/").split("/")]

        def do_GET(self):
            parts = self._split(urlparse(self.path).path)

            if parts == ["health"]:
                return self._send_json(200, {"ok": True, "service": "origin"})

            if parts == ["stats"]:
                with origin._lock:
                    return self._send_json(200, {
                        "calls_total": origin.calls_total,
                        "calls_by_key": dict(origin.calls_by_key),
                        "delay": origin.delay,
                        "parallel": origin.parallel,
                    })

            if len(parts) == 2 and parts[0] == "data":
                return self._send_json(200, origin.serve_data(parts[1]))

            return self._send_json(404, {"error": "未知路径: %s" % self.path})

        def do_POST(self):
            parts = self._split(urlparse(self.path).path)
            if parts == ["reset"]:
                origin.reset_stats()
                print("[origin] 统计已清零")
                return self._send_json(200, {"ok": True, "reset": True})
            return self._send_json(404, {"error": "未知路径: %s" % self.path})

    return Handler