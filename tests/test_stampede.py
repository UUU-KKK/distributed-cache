"""
阶段4 集成测试：验证「缓存击穿」场景下请求合并的实际效果。

在同一进程内后台启动：
  - 1 个模拟数据源（OriginServer）：每次查询耗时 0.15s，一次只处理 1 个查询；
  - 1 个开启 singleflight 的缓存节点（默认）；
  - 1 个关闭 singleflight 的缓存节点（对照组）。

然后分别对两个节点发起 20 个并发 GET（同一个缺失的 key），检验：
  - 开启保护：数据源只被访问 1 次，20 个请求都拿到正确结果；
  - 关闭保护：数据源被每个请求各访问一次（缓存击穿）。

说明：个别请求如果到得稍晚（错过合并窗口），会直接命中缓存返回——
这同样是正确行为，所以对"搭车数量"留一点余量，但"数据源只被访问 1 次"
这一核心性质是严格断言的。

运行：
    .venv\\Scripts\\python.exe tests\\test_stampede.py
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import requests  # noqa: E402
from cache_node import CacheNode  # noqa: E402
from origin import OriginServer  # noqa: E402

passed = 0


def check(name, condition):
    global passed
    if condition:
        passed += 1
        print("[PASS] %s" % name)
    else:
        raise AssertionError("测试失败：%s" % name)


def burst(url, key, n):
    """同一瞬间发起 n 个 GET 请求，返回 (状态码, 值, 数据来源, 耗时秒) 列表。"""
    barrier = threading.Barrier(n)

    def one(_):
        barrier.wait()
        t0 = time.time()
        r = requests.get(url + "/cache/" + key, timeout=15)
        body = r.json()
        return r.status_code, body.get("value"), body.get("source"), time.time() - t0

    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(one, range(n)))


def main():
    origin = OriginServer(port=9601, delay=0.15, parallel=1)
    node_sf = CacheNode("node-sf", port=9602, origin="http://127.0.0.1:9601")
    node_raw = CacheNode("node-raw", port=9603,
                         origin="http://127.0.0.1:9601", singleflight=False)

    origin.start_background()
    node_sf.start_background()
    node_raw.start_background()

    u_origin = "http://127.0.0.1:9601"
    u_sf = "http://127.0.0.1:9602"
    u_raw = "http://127.0.0.1:9603"

    try:
        # ---------- 1. 开启保护：20 个并发 → 回源 1 次 ---------- #
        requests.post(u_origin + "/reset", timeout=3)
        key1 = "hot-key-sf"
        results = burst(u_sf, key1, 20)

        check("开启保护：20 个请求全部成功（HTTP 200）",
              all(code == 200 for code, _, _, _ in results))
        check("开启保护：所有请求都拿到正确结果",
              all(v == "value-for-%s" % key1 for _, v, _, _ in results))

        s = requests.get(u_origin + "/stats", timeout=3).json()
        check("开启保护：数据源总共只被访问 1 次", s["calls_total"] == 1)
        check("开启保护：数据源只收到 1 次该 key 的查询",
              s["calls_by_key"].get(key1, 0) == 1)

        n = requests.get(u_sf + "/stats", timeout=3).json()
        via_origin = sum(1 for _, _, src, _ in results if src == "origin")
        print("  [统计] executed=%d, shared=%d, 走回源路径的响应=%d, 最慢请求=%.2fs"
              % (n["sf_executed"], n["sf_shared"], via_origin,
                 max(t for _, _, _, t in results)))
        check("节点统计：executed = 1（只有 1 个请求真正回源）", n["sf_executed"] == 1)
        check("节点统计：绝大多数请求搭车（shared >= 15）", n["sf_shared"] >= 15)
        check("一致性：走回源路径的响应数 = 领跑 1 + 搭车 shared",
              via_origin == n["sf_shared"] + 1)

        # 再次读同一个 key：这次应该直接命中缓存，连回源都不需要
        before = requests.get(u_origin + "/stats", timeout=3).json()["calls_total"]
        r = requests.get(u_sf + "/cache/" + key1, timeout=3).json()
        after = requests.get(u_origin + "/stats", timeout=3).json()["calls_total"]
        check("第二次读同一个 key：命中缓存（source=cache），不回源",
              r.get("source") == "cache" and after == before)

        # ---------- 2. 关闭保护：20 个并发 → 每个请求都回源 ---------- #
        requests.post(u_origin + "/reset", timeout=3)
        key2 = "hot-key-raw"
        results2 = burst(u_raw, key2, 20)

        check("关闭保护：20 个请求依然都能成功",
              all(code == 200 for code, _, _, _ in results2))
        check("关闭保护：结果依然正确（只是代价大）",
              all(v == "value-for-%s" % key2 for _, v, _, _ in results2))

        s2 = requests.get(u_origin + "/stats", timeout=3).json()
        hits2 = s2["calls_by_key"].get(key2, 0)
        via_origin2 = sum(1 for _, _, src, _ in results2 if src == "origin")
        print("  [统计] 数据源被打 %d 次（%d 个请求走了回源路径）, 最慢请求=%.2fs"
              % (hits2, via_origin2, max(t for _, _, _, t in results2)))
        check("关闭保护：几乎没有合并——数据源被反复打（>= 15 次）", hits2 >= 15)
        check("一致性：数据源被打次数 = 走回源路径的请求数", hits2 == via_origin2)

    finally:
        for obj in (node_sf, node_raw, origin):
            obj.stop()

    print("\n全部通过：共 %d 项断言" % passed)


if __name__ == "__main__":
    main()