"""
缓存核心存储层（不涉及网络，纯内存操作）。

职责：
  1. 以 key -> value 的形式保存数据；
  2. 支持 TTL：每条数据可以设置存活时间，到期即失效；
  3. 支持容量上限：当条数超过 capacity 时，按淘汰策略丢弃某条数据；
  4. 提供 LRU / LFU 两种淘汰策略；
  5. 统计命中/未命中/过期/淘汰次数，供演示与报告使用。

设计说明（报告可用）：
  - 线程安全：所有公开方法都用可重入锁（RLock）保护。因为后面缓存节点会被
    一个多线程的 HTTP 服务器调用，多线程并发读写同一个 dict 会出问题。
  - 过期时间用注入时钟记录的绝对时间戳。默认时钟是 time.monotonic()（单调时钟），
    因为它不受系统时间被修改的影响，TTL 计时更可靠；实验里可以注入「墙钟 + 可调
    偏移」的对照时钟，用来演示系统时间回拨会发生什么。
  - 惰性删除：get 时才判断是否过期；另提供 sweep_expired() 供后台线程定期清理，
    避免大量过期数据长期占用容量。
"""

import time
import threading
from collections import OrderedDict


class _Entry:
    """缓存中的一条记录（内部使用）。"""

    __slots__ = ("value", "expire_at", "freq", "seq")

    def __init__(self, value, expire_at, seq):
        self.value = value
        self.expire_at = expire_at  # 绝对过期时间戳；None 表示永不过期
        self.freq = 1               # LFU 的访问频次计数
        self.seq = seq              # 全局单调递增序号，用于 LFU 平局时的“最久未使用”比较


class CacheStore:
    """内存缓存存储。"""

    POLICIES = ("lru", "lfu")

    def __init__(self, capacity=128, policy="lru", clock=None):
        if policy not in self.POLICIES:
            raise ValueError("未知淘汰策略: %r，仅支持 %s" % (policy, self.POLICIES))
        if int(capacity) <= 0:
            raise ValueError("capacity 必须为正整数")

        self.capacity = int(capacity)
        self.policy = policy
        # 注入时钟：默认单调时钟；实验里可以换成「墙钟 + 可调偏移」对照
        self._clock = clock or time.monotonic
        # OrderedDict 兼顾两种用途：
        #   - 当 policy == "lru" 时，靠它维护访问顺序；
        #   - 当 policy == "lfu" 时，只当普通 dict 使用，顺序不重要。
        self._data = OrderedDict()
        self._seq = 0
        self._lock = threading.RLock()

        # 统计指标
        self.hits = 0
        self.misses = 0
        self.expirations = 0  # 因过期被清除的次数
        self.evictions = 0    # 因容量满被淘汰的次数
        self.sets = 0         # 写入次数

    # ------------------------------------------------------------------ #
    # 内部工具方法
    # ------------------------------------------------------------------ #
    def _next_seq(self):
        self._seq += 1
        return self._seq

    @staticmethod
    def _is_expired(entry, now):
        return entry.expire_at is not None and now >= entry.expire_at

    def _purge_expired(self, now):
        """一次性扫描删除所有已过期条目，返回删除数量。"""
        dead = [k for k, e in self._data.items() if self._is_expired(e, now)]
        for k in dead:
            del self._data[k]
            self.expirations += 1
        return len(dead)

    def _evict_one(self):
        """按策略淘汰一条数据，返回被淘汰的 key。"""
        if self.policy == "lru":
            # OrderedDict 的队头就是“最久未使用”的那条
            key, _ = self._data.popitem(last=False)
        else:  # lfu
            # 选出 (访问频次, 序号) 最小者：频次最低；频次相同则最久未使用
            key = min(self._data, key=lambda k: (self._data[k].freq, self._data[k].seq))
            del self._data[key]
        self.evictions += 1
        return key

    # ------------------------------------------------------------------ #
    # 对外接口
    # ------------------------------------------------------------------ #
    def get(self, key, default=None):
        """读取一条数据；不存在或已过期返回 default。"""
        with self._lock:
            now = self._clock()
            entry = self._data.get(key)

            if entry is None:
                self.misses += 1
                return default

            if self._is_expired(entry, now):
                # 惰性删除：正好访问到一条已过期的数据，顺手清掉
                del self._data[key]
                self.expirations += 1
                self.misses += 1
                return default

            # 命中：更新访问信息
            entry.freq += 1
            if self.policy == "lru":
                self._data.move_to_end(key)  # 移到队尾 = 标记为“最近刚用过”
            self.hits += 1
            return entry.value

    def set(self, key, value, ttl=None):
        """写入一条数据。ttl 为存活秒数；None 表示永不过期。"""
        with self._lock:
            now = self._clock()
            expire_at = None if ttl is None else now + float(ttl)

            if key in self._data:
                # 已存在则原地更新
                entry = self._data[key]
                entry.value = value
                entry.expire_at = expire_at
                entry.freq += 1
                entry.seq = self._next_seq()
                if self.policy == "lru":
                    self._data.move_to_end(key)
            else:
                self._data[key] = _Entry(value, expire_at, self._next_seq())

            self.sets += 1

            # 容量检查：先清理过期数据，仍然超限才按策略淘汰
            if len(self._data) > self.capacity:
                self._purge_expired(now)
                while len(self._data) > self.capacity:
                    self._evict_one()
            return True

    def delete(self, key):
        """删除一条数据，返回是否删除成功。"""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def ttl(self, key):
        """返回剩余存活秒数；-1 表示永不过期；-2 表示不存在/已过期。"""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return -2
            if entry.expire_at is None:
                return -1
            remaining = entry.expire_at - self._clock()
            if remaining <= 0:
                return -2
            return remaining

    def sweep_expired(self):
        """主动清理所有过期条目（供后台线程定期调用）。"""
        with self._lock:
            return self._purge_expired(self._clock())

    def __len__(self):
        with self._lock:
            return len(self._data)

    def __contains__(self, key):
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            return not self._is_expired(entry, self._clock())

    def stats(self):
        """返回当前统计指标快照。"""
        with self._lock:
            total = self.hits + self.misses
            return {
                "policy": self.policy,
                "capacity": self.capacity,
                "size": len(self._data),
                "sets": self.sets,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / total, 4) if total else 0.0,
                "expirations": self.expirations,
                "evictions": self.evictions,
            }

    def dump(self):
        """导出全部数据，便于调试与测试。"""
        with self._lock:
            now = self._clock()
            result = []
            for key, entry in self._data.items():
                if entry.expire_at is None:
                    remaining = -1
                else:
                    remaining = round(entry.expire_at - now, 3)
                result.append({
                    "key": key,
                    "value": entry.value,
                    "ttl": remaining,
                    "freq": entry.freq,
                })
            return result