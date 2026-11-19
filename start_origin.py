"""
启动模拟数据源（独立进程）。

用法：
    .venv\\Scripts\\python.exe start_origin.py --port 9311 --delay 0.2 --parallel 1

一般不用单独启动：start_cluster.py 加 --with-origin 会一起把它拉起来。
按 Ctrl+C 停止。
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))

from origin import OriginServer  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="启动一个模拟数据源")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=9311, help="监听端口")
    parser.add_argument("--delay", type=float, default=0.2, help="每次查询的耗时（秒）")
    parser.add_argument("--parallel", type=int, default=1, help="并发处理上限（模拟数据库处理能力）")
    args = parser.parse_args()

    OriginServer(host=args.host, port=args.port,
                 delay=args.delay, parallel=args.parallel).serve_forever()


if __name__ == "__main__":
    main()