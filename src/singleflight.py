"""
请求合并（singleflight）：把「同一时刻、同一个 key」的多次回源合并成一次。

背景（缓存击穿）：热点 key 一过期，很多并发请求同时发现「缓存里没有」，
如果每个请求都去回源，数据源（数据库）瞬间会被打很多次。
singleflight 的规则：第一个发现缺失的请求去回源（领跑者），
同一时刻其它请求原地等待，等结果回来后所有人共享同一份结果（搭车）。

对应关系（报告可用）：
  - 领跑者（leader）：真正调用回源函数的那一个请求；
  - 搭车者（follower）：等待并共享结果的其余请求；
  - 只有「时间上重叠」的请求才会合并；错开的请求各查各的（那时通常已命中缓存）。
"""

import threading


class _Call:
    """一次正在进行的回源（领跑者的工作位）。"""
    __slots__ = ("event", "result", "error")

    def __init__(self):
        self.event = threading.Event()
        self.result = None
        self.error = None


class SingleFlight:
    def __init__(self):
        self._lock = threading.Lock()
        self._calls = {}      # key -> 正在进行的 _Call
        self.executed = 0     # 真正执行了回源的次数（领跑次数）
        self.shared = 0       # 搭车（共享了结果）的请求数

    def do(self, key, fn):
        """执行 fn()，保证同一时刻同一个 key 只会真正执行一次。"""
        with self._lock:
            call = self._calls.get(key)
            is_follower = call is not None
            if is_follower:
                self.shared += 1
            else:
                call = _Call()
                self._calls[key] = call
                self.executed += 1

        if is_follower:
            call.event.wait()              # 等领跑者把结果拿回来
            if call.error is not None:
                raise call.error
            return call.result

        try:
            call.result = fn()
            return call.result
        except Exception as exc:
            call.error = exc
            raise
        finally:
            with self._lock:
                self._calls.pop(key, None)
            call.event.set()               # 叫醒所有搭车者

    def stats(self):
        return {"sf_executed": self.executed, "sf_shared": self.shared}