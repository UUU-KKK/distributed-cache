"""
阶段3 测试：验证三个节点之间的异步复制。

在同一进程内后台启动 3 个节点（各自监听不同端口、彼此作为副本），
然后检验：
  1. 写入 node-1 后，数据最终会出现在 node-2 / node-3 上（最终一致）；
  2. 删除操作也会被复制；
  3. 复制指标（发出、积压、接收）统计正确。

注意：因为是「异步」复制，写入之后不能立刻断言副本上一定有了，
必须**轮询等待**（这正是异步复制与同步复制的本质区别）。

运行：
    .venv\\Scripts\\python.exe tests\\test_replication.py
"""

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import requests  # noqa: E402
from cache_node import CacheNode  # noqa: E402

passed = 0


def check(name, condition):
    global passed
    if condition:
        passed += 1
        print("[PASS] %s" % name)
    else:
        raise AssertionError("测试失败：%s" % name)


def wait_until_get_ok(url, timeout=3.0):
    """轮询直到 GET 返回 200（数据已复制过来），超时返回 False。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=2).status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.05)
    return False


def wait_until_get_404(url, timeout=3.0):
    """轮询直到 GET 返回 404（删除已复制过来），超时返回 False。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=2).status_code == 404:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.05)
    return False


def main():
    ports = {1: 9201, 2: 9202, 3: 9203}
    nodes = {}

    for i in (1, 2, 3):
        peers = ["127.0.0.1:%d" % ports[j] for j in ports if j != i]
        node = CacheNode("node-%d" % i, port=ports[i], capacity=10, peers=peers)
        node.start_background()
        nodes[i] = node

    u1, u2, u3 = ("http://127.0.0.1:%d" % ports[i] for i in (1, 2, 3))

    try:
        # ---------- 1. 写入后最终一致 ---------- #
        requests.put(u1 + "/cache/city", json={"value": "广州", "ttl": 30}, timeout=3)

        check("写入节点立刻可读（本地写优先，不等副本）",
              requests.get(u1 + "/cache/city", timeout=3).json()["value"] == "广州")

        check("异步复制：node-2 最终也读到了", wait_until_get_ok(u2 + "/cache/city"))
        check("异步复制：node-3 最终也读到了", wait_until_get_ok(u3 + "/cache/city"))

        # 从副本上读到的值必须和源节点一致
        check("副本上的值正确",
              requests.get(u2 + "/cache/city", timeout=3).json()["value"] == "广州")

        # ---------- 2. 复制指标 ---------- #
        s1 = requests.get(u1 + "/stats", timeout=3).json()
        check("node-1 已把写入放进复制队列", s1["repl_enqueued"] >= 1)
        check("node-1 向 2 个副本各发一次，成功次数 >= 2", s1["repl_sent_ok"] >= 2)
        check("node-1 复制队列已清空（积压 = 0）", s1["repl_backlog"] == 0)

        s2 = requests.get(u2 + "/stats", timeout=3).json()
        check("node-2 记录了收到的复制次数", s2["repl_received"] >= 1)
        check("node-2 自己没有向外扩散（它的队列应为空）", s2["repl_backlog"] == 0)

        # ---------- 3. 删除也会被复制 ---------- #
        requests.delete(u1 + "/cache/city", timeout=3)
        check("删除被复制：node-2 上的数据消失", wait_until_get_404(u2 + "/cache/city"))
        check("删除被复制：node-3 上的数据消失", wait_until_get_404(u3 + "/cache/city"))

        # ---------- 4. 从任意节点写入都会广播 ---------- #
        requests.put(u3 + "/cache/city2", json={"value": "深圳", "ttl": 30}, timeout=3)
        check("从 node-3 写入，node-1 也能收到", wait_until_get_ok(u1 + "/cache/city2"))

    finally:
        for node in nodes.values():
            node.stop()

    print("\n全部通过：共 %d 项断言" % passed)


if __name__ == "__main__":
    main()