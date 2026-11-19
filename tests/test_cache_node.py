"""
阶段2 测试：验证缓存节点的 HTTP 接口。

在同一进程内后台启动一个节点，然后用 requests 真实发 HTTP 请求，
覆盖 写入/读取/删除/健康检查/统计/TTL过期 等接口。

运行：
    .venv\\Scripts\\python.exe tests\\test_cache_node.py
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


def main():
    node = CacheNode("test-node", port=9101, capacity=10, policy="lru")
    node.allow_replication = True
    node.start_background()
    base = node.url

    try:
        # 健康检查
        r = requests.get(base + "/health", timeout=3)
        check("GET /health 返回 200", r.status_code == 200)
        check("GET /health 报告 node_id", r.json()["node_id"] == "test-node")

        # 写入
        r = requests.put(base + "/cache/city", json={"value": "广州", "ttl": 5}, timeout=3)
        check("PUT /cache/city 成功", r.status_code == 200 and r.json()["ok"] is True)

        # 读取
        r = requests.get(base + "/cache/city", timeout=3)
        check("GET /cache/city 命中", r.status_code == 200 and r.json()["found"] is True)
        check("GET /cache/city 值正确", r.json()["value"] == "广州")

        # 未命中
        r = requests.get(base + "/cache/不存在", timeout=3)
        check("GET 不存在的 key 返回 404", r.status_code == 404)

        # 统计
        r = requests.get(base + "/stats", timeout=3)
        s = r.json()
        check("GET /stats 命中数为 1", s["hits"] == 1)
        check("GET /stats 未命中数为 1", s["misses"] == 1)

        # 复制写入接口
        r = requests.post(base + "/replicate",
                          json={"key": "repl", "value": "来自副本", "ttl": 5}, timeout=3)
        check("POST /replicate 成功", r.status_code == 200 and r.json()["ok"] is True)
        check("POST /replicate 已写入本地", node.store.get("repl") == "来自副本")

        # 删除
        r = requests.delete(base + "/cache/city", timeout=3)
        check("DELETE /cache/city 成功", r.status_code == 200 and r.json()["ok"] is True)

        # TTL 过期（用一个短 TTL 再等待）
        requests.put(base + "/cache/short", json={"value": 1, "ttl": 0.3}, timeout=3)
        time.sleep(0.5)
        r = requests.get(base + "/cache/short", timeout=3)
        check("TTL 到期后 GET 返回 404", r.status_code == 404)

        # 非法请求体
        r = requests.put(base + "/cache/bad", data="{不是json", timeout=3,
                         headers={"Content-Type": "application/json"})
        check("非法 JSON 请求体返回 400", r.status_code == 400)

    finally:
        node.stop()

    print("\n全部通过：共 %d 项断言" % passed)


if __name__ == "__main__":
    main()