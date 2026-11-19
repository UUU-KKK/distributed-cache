"""
阶段3 验证脚本：亲眼看到「异步复制」的两个关键现象：
  1) 不一致窗口：写入 node-1 后立刻读 node-2 -> 读不到
  2) 最终一致性：稍后（等后台复制线程发货完）再读 node-2 -> 读到了，且值正确

为什么用 Python 脚本而不是 curl：
  curl 在 PowerShell 里的引号 / 转义 / 多行粘贴非常容易出错（JSON 请求体尤其容易踩坑），
  本脚本用 Python 直接发 HTTP 请求，你只需要粘贴一行命令，不存在这些问题。

用法（必须开两个终端）：
  终端A（先跑，别关）：
      .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port 9301 --repl-delay 5
  终端B（集群起来后再跑）：
      .venv\\Scripts\\python.exe verify_replication.py --base-port 9301 --wait 8

参数说明：
  --base-port  集群起始端口（要和终端A的 --base-port 一致）
  --wait       写入后等多少秒再读副本（要大于集群的 --repl-delay，留一点余量）
  --key        要写入的 key；默认自动生成一个每次都不同的 key，避免上一次的残留数据干扰
"""

import argparse
import json
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request_json(method, url, body=None, timeout=3):
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
        # 4xx / 5xx 也会带 JSON 响应体（例如 404 时返回 {"found": false}）
        return exc.code, json.loads(exc.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description="验证异步复制：不一致窗口 + 最终一致性")
    parser.add_argument("--base-port", type=int, default=9301,
                        help="集群起始端口（与 start_cluster.py 的 --base-port 一致）")
    parser.add_argument("--wait", type=float, default=8.0,
                        help="写入后等待的秒数，要大于集群的 --repl-delay")
    parser.add_argument("--key", default=None,
                        help="写入的 key；默认为每次都不同的 demo-<时间戳>")
    args = parser.parse_args()

    key = args.key or ("demo-%d" % time.time())
    node1 = "http://127.0.0.1:%d" % args.base_port
    node2 = "http://127.0.0.1:%d" % (args.base_port + 1)

    print("=" * 60)
    print("阶段3 验证：异步复制（不一致窗口 -> 最终一致性）")
    print("=" * 60)

    # ---- 第 0 步：确认集群在运行 ----
    print("\n[0/4] 检查集群是否在运行 ...")
    for name, base in (("node-1", node1), ("node-2", node2)):
        try:
            _, health = request_json("GET", base + "/health")
            print("  %s 在线（node_id=%s）" % (name, health.get("node_id")))
        except (URLError, OSError):
            print("  %s（%s）连不上！" % (name, base))
            print()
            print("  这说明集群没有在运行。请先用「另一个终端」启动它：")
            print("    .venv\\Scripts\\python.exe start_cluster.py --nodes 3 --base-port %d --repl-delay 5"
                  % args.base_port)
            print("  看到「集群已启动」之后，再回到本终端重跑本脚本。")
            sys.exit(1)

    # ---- 第 1 步：写入 node-1 ----
    print("\n[1/4] 向 node-1 写入 key=%s（值=hello，TTL=60 秒） ..." % key)
    code, resp = request_json("PUT", node1 + "/cache/" + key, {"value": "hello", "ttl": 60})
    print("  写入结果：HTTP %d  %s" % (code, json.dumps(resp, ensure_ascii=False)))

    # ---- 第 2 步：立刻读 node-2 ----
    print("\n[2/4] 立刻去读 node-2（预期：读不到）...")
    code, resp = request_json("GET", node2 + "/cache/" + key)
    if code == 404:
        print("  读取结果：HTTP 404  %s" % json.dumps(resp, ensure_ascii=False))
        print("  >> 读不到是正常的：数据还在 node-1 的复制队列里没发货，")
        print("  >> 这一段就叫「异步复制的不一致窗口」。")
    else:
        print("  读取结果：HTTP %d  %s" % (code, json.dumps(resp, ensure_ascii=False)))
        print("  >> 这次副本直接读到了。通常是因为集群的 --repl-delay 很小（或为 0），")
        print("  >> 复制太快了。想看到「读不到」的瞬间，请用 --repl-delay 5 重新启动集群。")

    # ---- 第 3 步：等待复制线程发货 ----
    print("\n[3/4] 等 %.0f 秒，让后台复制线程把数据送过去 ..." % args.wait)
    time.sleep(args.wait)

    # ---- 第 4 步：再读 node-2（轮询等待，最多再等 10 秒） ----
    print("\n[4/4] 再读一次 node-2（预期：读到了）...")
    deadline = time.time() + 10
    while True:
        code, resp = request_json("GET", node2 + "/cache/" + key)
        if code == 200:
            print("  读取结果：HTTP 200  %s" % json.dumps(resp, ensure_ascii=False))
            if resp.get("value") == "hello":
                print("  >> 值也正确（hello）。副本追上了：这就是「最终一致性」。")
            break
        if time.time() > deadline:
            print("  读取结果：HTTP %d（还没复制到）" % code)
            print("  >> 集群的 --repl-delay 可能比 --wait 还大，把 --wait 调大一些（比如 15）再重跑。")
            break
        print("  还没到（HTTP %d），再等 1 秒 ..." % code)
        time.sleep(1)

    # ---- 附加：看一眼 node-1 的复制指标 ----
    print("\n[附加] node-1 的复制指标（/stats 的一部分）：")
    _, stats = request_json("GET", node1 + "/stats")
    for field in ("repl_enabled", "repl_delay", "repl_enqueued", "repl_backlog",
                  "repl_sent_ok", "repl_sent_fail", "repl_received"):
        if field in stats:
            print("  %-16s = %s" % (field, stats[field]))

    print("\n" + "=" * 60)
    print("验证结束。你刚刚亲眼看到的就是：")
    print("  写入后立刻读副本 -> 读不到（不一致窗口）")
    print("  稍后再读副本     -> 读到了（最终一致性）")
    print("=" * 60)


if __name__ == "__main__":
    main()