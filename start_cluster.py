"""
一键启动一个小型缓存集群（可选带模拟数据源）。

每个节点都是**独立的 Python 进程**（不是一个进程里开三个线程），
这样才能真实模拟「一台机器上跑着三个互相独立的缓存服务」，
也才能在阶段5里真正「杀掉其中一个节点」看会发生什么。

用法：
    # 基础集群（阶段2/3的用法）
    .venv\\Scripts\\python.exe start_cluster.py

    # 带模拟数据源（阶段4：缓存击穿演示）
    .venv\\Scripts\\python.exe start_cluster.py --with-origin --origin-delay 0.2

    # 对比实验：关闭请求合并，看"全部穿透"的原始样子
    .venv\\Scripts\\python.exe start_cluster.py --with-origin --no-singleflight

参数说明：
    --repl-delay 2      给异步复制加 2 秒人为延迟，用于演示「不一致窗口」。
    --with-origin       额外启动一个模拟数据源（默认在「起始端口 + 10」上），
                        并让每个缓存节点开启回源。
    --origin-delay 0.2  数据源每次查询的耗时（秒），模拟慢查询。
    --no-singleflight   关闭请求合并，让每个未命中请求都各自回源（仅用于对比）。

按 Ctrl+C 会一次性停掉所有进程（含数据源）。
"""

import argparse
import os
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))


def port_in_use(host, port):
    """尝试连接该端口：能连上说明已经有人在用。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0


def build_plan(count, base_port, host):
    """规划每个节点的名字、端口，以及它要复制给谁。"""
    plan = []
    for i in range(count):
        plan.append({"node_id": "node-%d" % (i + 1), "port": base_port + i})

    # 每个节点的 peers = 除自己以外的所有节点（全互联，任意节点写入都会抄给其它所有节点）
    for item in plan:
        item["peers"] = ["%s:%d" % (host, p["port"]) for p in plan if p["port"] != item["port"]]
    return plan


def main():
    parser = argparse.ArgumentParser(description="一键启动缓存集群")
    parser.add_argument("--nodes", type=int, default=3, help="节点个数")
    parser.add_argument("--base-port", type=int, default=9001, help="起始端口")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--capacity", type=int, default=50, help="每个节点的最大缓存条数")
    parser.add_argument("--policy", default="lru", choices=("lru", "lfu"), help="淘汰策略")
    parser.add_argument("--repl-delay", type=float, default=0.0, help="异步复制人为延迟（秒）")
    parser.add_argument("--isolate", default="",
                        help="模拟双向网络分区：把指定节点（如 node-3）与其余节点完全隔离")
    parser.add_argument("--repl-dup", type=int, default=1,
                        help="模拟重复投递：每条复制消息重复发送 N 次")
    parser.add_argument("--repl-reorder", action="store_true",
                        help="模拟乱序到达：消息先进缓冲区，随机挑一条发送")
    parser.add_argument("--repl-reorder-hold", type=float, default=0.5,
                        help="乱序缓冲区最长停留时间（秒）")
    parser.add_argument("--with-origin", action="store_true",
                        help="额外启动模拟数据源（在 起始端口+10 上），并让节点开启回源")
    parser.add_argument("--origin-delay", type=float, default=0.2, help="数据源每次查询耗时（秒）")
    parser.add_argument("--no-singleflight", action="store_true",
                        help="关闭请求合并（对比实验用）")
    args = parser.parse_args()

    plan = build_plan(args.nodes, args.base_port, args.host)
    origin_port = args.base_port + 10

    # 启动前先检查端口：如果上一次的进程没关干净，这里能立刻发现问题，
    # 而不是等到「某个节点悄悄启动失败、复制怎么都不生效」才去排查。
    check_list = [(item["node_id"], item["port"]) for item in plan]
    if args.with_origin:
        check_list.append(("origin", origin_port))
    busy = [(name, port) for name, port in check_list if port_in_use(args.host, port)]
    if busy:
        print("启动失败：以下端口已被占用（多半是上一次的进程没关干净）：")
        for name, port in busy:
            print("  %-7s -> %s:%d" % (name, args.host, port))
        print("\n解决办法：先关掉占用端口的进程（在运行它的窗口按 Ctrl+C），")
        print("或者换一组端口，例如加上 --base-port 9101")
        return

    procs = []

    # 先启动数据源（如果有）
    if args.with_origin:
        origin_cmd = [
            sys.executable, os.path.join(ROOT, "start_origin.py"),
            "--host", args.host,
            "--port", str(origin_port),
            "--delay", str(args.origin_delay),
        ]
        procs.append(({"node_id": "origin", "port": origin_port, "peers": []},
                      subprocess.Popen(origin_cmd, cwd=ROOT)))

    # 再启动各个缓存节点
    for item in plan:
        cmd = [
            sys.executable, os.path.join(ROOT, "start_node.py"),
            "--node-id", item["node_id"],
            "--host", args.host,
            "--port", str(item["port"]),
            "--capacity", str(args.capacity),
            "--policy", args.policy,
            "--peers", ",".join(item["peers"]),
            "--repl-delay", str(args.repl_delay),
        ]
        block = []
        if args.isolate:
            if item["node_id"] == args.isolate:
                # 被隔离的节点：发往所有副本的消息都丢掉
                block = [str(p["port"]) for p in plan if p["port"] != item["port"]]
            elif any(p["node_id"] == args.isolate for p in plan):
                # 其余节点：发往被隔离节点的消息都丢掉（两个方向都断 = 双向分区）
                block = [str(next(p["port"] for p in plan if p["node_id"] == args.isolate))]
        if block:
            cmd += ["--block", ",".join(block)]
        if args.repl_dup > 1:
            cmd += ["--repl-dup", str(args.repl_dup)]
        if args.repl_reorder:
            cmd += ["--repl-reorder", "--repl-reorder-hold", str(args.repl_reorder_hold)]
        if args.with_origin:
            cmd += ["--origin", "http://%s:%d" % (args.host, origin_port)]
        if args.no_singleflight:
            cmd += ["--no-singleflight"]
        procs.append((item, subprocess.Popen(cmd, cwd=ROOT)))

    print("\n================ 集群已启动 ================")
    for item, proc in procs:
        if item["node_id"] == "origin":
            print("  %-7s pid=%-6d %-24s 模拟数据源（每次查询 %ss）"
                  % ("origin", proc.pid, "http://%s:%d" % (args.host, item["port"]),
                     args.origin_delay))
        else:
            print("  %-7s pid=%-6d %-24s 副本 -> %s"
                  % (item["node_id"], proc.pid,
                     "http://%s:%d" % (args.host, item["port"]),
                     ", ".join(item["peers"])))
    print("============================================")
    print("按 Ctrl+C 停止整个集群。\n")

    try:
        while True:
            time.sleep(0.5)
            for item, proc in procs:
                if proc.poll() is not None:
                    print("[警告] %s (pid=%d) 已经退出了！" % (item["node_id"], proc.pid))
    except KeyboardInterrupt:
        print("\n正在停止集群……")
    finally:
        for item, proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for item, proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print("集群已全部停止")


if __name__ == "__main__":
    main()