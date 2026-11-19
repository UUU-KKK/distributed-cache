"""
阶段1 测试：验证缓存核心的 TTL 与 LRU/LFU 淘汰行为。

运行方式（在项目根目录执行）：
    .venv\\Scripts\\python.exe tests\\test_cache_store.py

采用最朴素的 assert 写法，不依赖任何测试框架：
每通过一项就打印 [PASS]，失败会直接抛出 AssertionError。
"""

import os
import sys
import time

# 让脚本能 import 到 src 目录下的模块
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from cache_store import CacheStore  # noqa: E402

passed = 0


def check(name, condition):
    global passed
    if condition:
        passed += 1
        print("[PASS] %s" % name)
    else:
        raise AssertionError("测试失败：%s" % name)


def test_basic_set_get():
    c = CacheStore(capacity=10, policy="lru")
    c.set("name", "华南师范大学")
    check("基本读写：能取回写入的值", c.get("name") == "华南师范大学")
    check("基本读写：不存在的 key 返回 None", c.get("不存在的key") is None)
    check("基本读写：支持删除", c.delete("name") is True and c.get("name") is None)


def test_ttl_expire():
    c = CacheStore(capacity=10)
    c.set("temp", "只在0.2秒内有效", ttl=0.2)
    check("TTL：刚写入时未过期", c.get("temp") == "只在0.2秒内有效")
    time.sleep(0.25)
    check("TTL：超过存活时间后读不到", c.get("temp") is None)
    check("TTL：过期被计入 expirations", c.stats()["expirations"] >= 1)
    check("TTL：永不过期的数据 ttl 为 -1",
          c.set("forever", 1) and c.ttl("forever") == -1)


def test_lru_eviction():
    c = CacheStore(capacity=3, policy="lru")
    for k in ("a", "b", "c"):
        c.set(k, k)
    c.get("a")          # 访问 a，使 a 变成“最近使用”
    c.set("d", "d")     # 容量已满，应淘汰“最久未使用”的 b
    check("LRU：淘汰最久未使用的 b", c.get("b") is None)
    check("LRU：最近使用过的 a 仍在", c.get("a") == "a")
    check("LRU：新写入的 d 在", c.get("d") == "d")
    check("LRU：淘汰计数为 1", c.stats()["evictions"] == 1)


def test_lfu_eviction():
    c = CacheStore(capacity=3, policy="lfu")
    for k in ("a", "b", "c"):
        c.set(k, k)     # 三条频次均为 1
    c.get("a")
    c.get("a")          # a 频次 -> 3
    c.get("b")          # b 频次 -> 2
    # 此时 c 的频次最低（仍为 1），写入 d 应淘汰 c
    c.set("d", "d")
    check("LFU：淘汰访问次数最少的 c", c.get("c") is None)
    check("LFU：高频的 a 仍在", c.get("a") == "a")
    check("LFU：次高频的 b 仍在", c.get("b") == "b")


def test_stats_hit_rate():
    c = CacheStore(capacity=10)
    c.set("x", 1)
    c.get("x")          # 命中
    c.get("y")          # 未命中
    s = c.stats()
    check("统计：命中数为 1", s["hits"] == 1)
    check("统计：未命中数为 1", s["misses"] == 1)
    check("统计：命中率为 0.5", s["hit_rate"] == 0.5)


def test_eviction_not_resurrect_expired():
    """过期数据应优先被清理，而不是占用容量把有效数据淘汰掉。"""
    c = CacheStore(capacity=2)
    c.set("old", 1, ttl=0.05)
    time.sleep(0.08)
    c.set("new1", 1)
    c.set("new2", 1)
    # old 已过期，容量 2 应容纳 new1/new2，二者都还在
    check("过期优先清理：new1 未被误淘汰", c.get("new1") == 1)
    check("过期优先清理：new2 未被误淘汰", c.get("new2") == 1)


def main():
    tests = [
        test_basic_set_get,
        test_ttl_expire,
        test_lru_eviction,
        test_lfu_eviction,
        test_stats_hit_rate,
        test_eviction_not_resurrect_expired,
    ]
    for t in tests:
        print("\n--- %s ---" % t.__name__)
        t()
    print("\n全部通过：共 %d 项断言" % passed)


if __name__ == "__main__":
    main()