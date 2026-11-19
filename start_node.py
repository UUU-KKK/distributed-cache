"""
启动单个缓存节点（独立进程）。

用法示例（单机模式）：
    .venv\\Scripts\\python.exe start_node.py --node-id node-1 --port 9001 --capacity 100 --policy lru

用法示例（集群 + 数据源，也就是阶段4的场景）：
    .venv\\Scripts\\python.exe start_node.py --node-id node-1 --port 9001 ^
        --peers 127.0.0.1:9002,127.0.0.1:9003 --origin http://127.0.0.1:9011

启动后用浏览器或 curl 访问 http://127.0.0.1:9001/health 即可看到节点状态。
按 Ctrl+C 停止。
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

from cache_node import CacheNode  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="启动一个缓存节点")
    parser.add_argument("--node-id", default="node-1", help="节点名字")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=9001, help="监听端口")
    parser.add_argument("--capacity", type=int, default=128, help="最大缓存条数")
    parser.add_argument("--policy", default="lru", choices=("lru", "lfu"), help="淘汰策略")
    parser.add_argument("--peers", default="",
                        help="副本节点地址，逗号分隔，例如 127.0.0.1:9002,127.0.0.1:9003；留空=单机模式")
    parser.add_argument("--repl-delay", type=float, default=0.0,
                        help="异步复制的人为发货延迟（秒），仅用于演示不一致窗口")
    parser.add_argument("--block", default="",
                        help="模拟网络分区：命中该字符串的副本（如 9303）复制消息直接丢弃，逗号分隔")
    parser.add_argument("--repl-dup", type=int, default=1,
                        help="模拟重复投递：每条复制消息重复发送 N 次（默认 1）")
    parser.add_argument("--repl-reorder", action="store_true",
                        help="模拟乱序到达：消息先进缓冲区，随机挑一条发送")
    parser.add_argument("--repl-reorder-hold", type=float, default=0.5,
                        help="乱序缓冲区最长停留时间（秒），默认 0.5")
    parser.add_argument("--clock", choices=["mono", "wall"], default="mono",
                        help="TTL 时钟：mono=单调时钟（默认，免疫时间回拨），wall=墙钟（对照实验用）")
    parser.add_argument("--drop-response", type=int, default=0,
                        help="吞掉接下来 N 个 PUT 的成功响应，模拟「写成功但响应丢失」")
    parser.add_argument("--origin", default="",
                        help="数据源地址（如 http://127.0.0.1:9311）；留空=未命中直接 404，不回源")
    parser.add_argument("--origin-ttl", type=float, default=30.0,
                        help="回源取到的数据写入缓存时的存活时间（秒）")
    parser.add_argument("--no-singleflight", action="store_true",
                        help="关闭请求合并（同一 key 的并发回源不合并），用于对比实验")
    args = parser.parse_args()

    peers = [p for p in args.peers.split(",") if p.strip()]

    node = CacheNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        capacity=args.capacity,
        policy=args.policy,
        peers=peers,
        repl_delay=args.repl_delay,
        origin=args.origin or None,
        origin_ttl=args.origin_ttl,
        singleflight=not args.no_singleflight,
        repl_block=[t for t in args.block.split(",") if t.strip()],
        repl_dup=args.repl_dup,
        repl_reorder=args.repl_reorder,
        repl_reorder_hold=args.repl_reorder_hold,
        clock=args.clock,
        drop_response=args.drop_response,
    )
    node.serve_forever()


if __name__ == "__main__":
    main()