"""
阶段4 验证脚本：亲眼看到「缓存击穿」与「请求合并（singleflight）」的区别。

前提：集群带数据源在运行。
  终端A（默认：带保护）：
      .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port 9301 --with-origin
  终端A（想看"没有保护"的对比）：
      .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port 9301 --with-origin --no-singleflight

用法（终端B）：
    .venv\\Scripts\\python.exe verify_stampede.py --base-port 9301

它做的事：把 --requests 个请求"同一瞬间"打向同一个缺失的 key，
然后数一数数据源被访问了几次：
  - 有保护：应该是 1 次（其余请求搭车）；
  - 没保护：应该是 --requests 次（每个请求都穿透）。
"""

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request_json(method, url, body=None, timeout=15):
    """发一个 HTTP 请求，返回 (状态码, 解析后的 JSON 字典)。"""
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, method=method, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description="验证缓存击穿保护（singleflight）")
    parser.add_argument("--base-port", type=int, default=9301, help="集群起始端口")
    parser.add_argument("--requests", type=int, default=20, help="并发请求数量")
    args = parser.parse_args()

    node1 = "http://127.0.0.1:%d" % args.base_port
    origin = "http://127.0.0.1:%d" % (args.base_port + 10)
    key = "burst-%d" % time.time()

    print("=" * 60)
    print("阶段4 验证：缓存击穿保护（singleflight 请求合并）")
    print("=" * 60)

    # ---- 第 0 步：确认节点和数据源都在 ----
    print("\n[0/4] 检查集群与数据源 ...")
    try:
        _, health = request_json("GET", node1 + "/health")
        print("  node-1 在线（node_id=%s）" % health.get("node_id"))
    except (URLError, OSError):
        print("  node-1 连不上！请先启动集群：")
        print("    .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port %d --with-origin"
              % args.base_port)
        sys.exit(1)
    try:
        request_json("GET", origin + "/health")
        print("  数据源在线（%s）" % origin)
    except (URLError, OSError):
        print("  数据源连不上！启动集群时请带上 --with-origin（数据源在 起始端口+10 上）。")
        sys.exit(1)

    # ---- 第 1 步：清零数据源统计 ----
    print("\n[1/4] 把数据源的访问计数清零 ...")
    request_json("POST", origin + "/reset")
    print("  已清零。")

    # ---- 第 2 步：同时打 N 个请求 ----
    print("\n[2/4] 同时向 node-1 打 %d 个请求（同一个缺失的 key=%s）..." % (args.requests, key))
    _, ns_before = request_json("GET", node1 + "/stats")   # 记下当前计数，用来算"本次"增量
    barrier = threading.Barrier(args.requests)

    def one(_):
        barrier.wait()                       # 让所有请求"同一瞬间"出发
        t0 = time.time()
        try:
            code, resp = request_json("GET", node1 + "/cache/" + key)
        except (URLError, OSError):
            return None, None, time.time() - t0
        return code, resp, time.time() - t0

    with ThreadPoolExecutor(max_workers=args.requests) as pool:
        results = list(pool.map(one, range(args.requests)))

    codes = [r[0] for r in results]
    latencies = [r[2] for r in results]
    ok = sum(1 for c in codes if c == 200)
    print("  完成：%d/%d 个请求返回 200；最慢一个耗时 %.2f 秒，平均 %.2f 秒"
          % (ok, args.requests, max(latencies), sum(latencies) / len(latencies)))

    # ---- 第 3 步：数数据源被打了几次 ----
    print("\n[3/4] 看看数据源被访问了几次 ...")
    _, os_stats = request_json("GET", origin + "/stats")
    calls = os_stats["calls_by_key"].get(key, 0)
    print("  数据源这次的访问记录: 总次数=%d, 本 key=%d 次" % (os_stats["calls_total"], calls))
    _, ns = request_json("GET", node1 + "/stats")
    fetched_delta = ns.get("origin_fetched", 0) - ns_before.get("origin_fetched", 0)
    shared_delta = ns.get("sf_shared", 0) - ns_before.get("sf_shared", 0)
    print("  node-1 的记录（仅本次）: singleflight=%s, 回源次数=%d, 搭车=%d 个请求"
          % (ns.get("singleflight"), fetched_delta, shared_delta))

    # ---- 第 4 步：结论 ----
    print("\n[4/4] 结论 ...")
    if calls == 1:
        print("  数据源只被访问了 1 次 —— 请求合并在工作：")
        print("  %d 个请求同时到达，只有 1 个去回源，其余 %d 个搭车共享结果。"
              % (args.requests, args.requests - 1))
    elif calls >= args.requests:
        print("  数据源被访问了 %d 次 —— 每个请求都自己回源（没有合并）：" % calls)
        print("  这就是「缓存击穿」：热点 key 缺失的一瞬间，全部请求一起穿透打到数据源。")
        print("  对比一下：刚才最慢的请求耗时被数据源的处理能力拖长了。")
    else:
        print("  数据源被访问了 %d 次（介于 1 和 %d 之间，说明请求没有完全同时到达）。"
              % (calls, args.requests))

    print("\n" + "=" * 60)
    print("想看另一种情况：Ctrl+C 关掉集群，改用")
    if calls == 1:
        print("    .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port %d --with-origin --no-singleflight"
              % args.base_port)
        print("重启后重跑本脚本，就能看到「全部穿透」的样子。")
    else:
        print("    .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port %d --with-origin"
              % args.base_port)
        print("重启后重跑本脚本，就能看到「只回源 1 次」的样子。")
    print("=" * 60)


if __name__ == "__main__":
    main()