"""
阶段4 单元测试：验证请求合并器 SingleFlight 的行为（不启动网络）。

检验：
  1. 同一时刻同一个 key：20 个线程只有 1 个真正执行（领跑），其余搭车；
  2. 不同 key 互不合并；
  3. 领跑者出错时，错误会共享给搭车者，之后可以重新执行；
  4. 时间上错开的两次调用不会合并（各自执行）。

运行：
    .venv\\Scripts\\python.exe tests\\test_singleflight.py
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from singleflight import SingleFlight  # noqa: E402

passed = 0


def check(name, condition):
    global passed
    if condition:
        passed += 1
        print("[PASS] %s" % name)
    else:
        raise AssertionError("测试失败：%s" % name)


def main():
    # ---------- 1. 同一个 key 并发：只执行 1 次 ---------- #
    sf = SingleFlight()
    calls = {"n": 0}
    barrier = threading.Barrier(20)

    def slow_fetch():
        calls["n"] += 1
        time.sleep(0.3)
        return "value"

    def one(i):
        barrier.wait()                      # 让 20 个线程同一瞬间出发
        return sf.do("hot", slow_fetch)

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(one, range(20)))

    check("20 个并发请求：回源函数只真正执行了 1 次", calls["n"] == 1)
    check("20 个并发请求：全部拿到同一份结果", results == ["value"] * 20)
    check("统计：executed = 1（领跑 1 次）", sf.executed == 1)
    check("统计：shared = 19（其余都搭车）", sf.shared == 19)

    # ---------- 2. 不同 key 互不合并 ---------- #
    sf2 = SingleFlight()
    calls2 = {"n": 0}
    lock2 = threading.Lock()
    barrier2 = threading.Barrier(20)

    def fetch2(key):
        with lock2:
            calls2["n"] += 1
        time.sleep(0.2)
        return "v-" + key

    def one2(i):
        key = "k%d" % (i % 5)
        barrier2.wait()
        return sf2.do(key, lambda: fetch2(key))

    with ThreadPoolExecutor(max_workers=20) as pool:
        rs = list(pool.map(one2, range(20)))

    check("5 个不同的 key：一共真正执行 5 次", calls2["n"] == 5)
    check("5 个不同的 key：每个结果都正确",
          all(rs[i] == "v-k%d" % (i % 5) for i in range(20)))
    check("统计：executed = 5", sf2.executed == 5)

    # ---------- 3. 出错时错误共享，之后可以重试 ---------- #
    sf3 = SingleFlight()
    attempts = {"n": 0}
    barrier3 = threading.Barrier(10)

    def failing():
        attempts["n"] += 1
        time.sleep(0.2)
        raise RuntimeError("数据源挂了")

    def one3(i):
        barrier3.wait()
        try:
            sf3.do("bad", failing)
            return None
        except RuntimeError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=10) as pool:
        errs = list(pool.map(one3, range(10)))

    check("领跑者出错：也只真正执行 1 次", attempts["n"] == 1)
    check("错误被共享：10 个请求都收到了同样的错误", errs == ["数据源挂了"] * 10)

    def good():
        return "finally-ok"

    check("出错后同一 key 可以重新发起（不会卡死）", sf3.do("bad", good) == "finally-ok")

    # ---------- 4. 时间错开的调用不合并 ---------- #
    sf4 = SingleFlight()
    seq = {"n": 0}

    def quick():
        seq["n"] += 1
        return "x"

    sf4.do("s", quick)
    sf4.do("s", quick)
    check("时间错开的两次调用不会合并（各执行一次）", seq["n"] == 2)

    print("\n全部通过：共 %d 项断言" % passed)


if __name__ == "__main__":
    main()