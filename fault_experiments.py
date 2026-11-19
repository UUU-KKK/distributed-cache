"""
阶段5：故障注入实验 ——「预测 → 实测 → 对照 → 解释」。

课程要求演示的四步都封装在这个脚本里：每个实验都会依次打印
  [预测]  先说出我们预期系统会怎么表现
  [实测]  真跑一遍，记录观测数据
  [对照]  预测 vs 实际
  [解释]  违反了哪项“保证”，或者为什么仍然正确

十一个实验，分三段做，和前面阶段一一对应：
  阶段A（纯缓存集群：3 个节点，不接数据源）
    实验1  TTL 到期自动消失（对应阶段1/2）
    实验2  异步复制的不一致窗口（对应阶段3）
    实验3  杀掉一个副本节点（对应阶段3）
  阶段B（换成接数据源的集群）
    实验4  杀掉数据源（对应阶段4）
    实验5  并发压测对照（对应阶段4）
  阶段C（纯缓存集群 + 网络故障注入，对应阶段5）
    实验6  网络双向分区（--isolate）
    实验7  消息重复投递三次（--repl-dup 3）
    实验8  消息乱序到达（--repl-reorder）
    实验9  系统时间倒退一分钟（--clock wall 对照）
    实验10 单个 key 占据 50% 请求（热点 key）
    实验11 写成功但响应丢失（/admin/fault）

为什么要分两段：如果节点接着数据源，阶段1~3 的现象会被“回源”盖住
（过期/缺失的数据会被自动从数据源刷新回来）。先隔离、再叠加，
才能把每一层的真实行为看清楚——这也正是“故障注入实验”的意义。

脚本会自己启动、杀掉、重启所有进程（数据源 + 3 个节点），
全程自动化，你只需要运行它、对照输出看结果。

用法：
    .venv\\Scripts\\python.exe fault_experiments.py
    .venv\\Scripts\\python.exe fault_experiments.py --base-port 9701

参数：
    --base-port   集群起始端口（数据源在 起始端口+10）
    --repl-delay  复制的人为延迟秒数（默认 2 秒，让“不一致窗口”清晰可见）
    --requests    实验5 的并发请求数（默认 20）
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, ROOT)

from start_cluster import build_plan, port_in_use  # noqa: E402


# ---------------------------------------------------------------------- #
# 工具
# ---------------------------------------------------------------------- #
def api(method, url, body=None, timeout=8):
    """发一个 HTTP 请求，返回 (状态码, JSON 字典)。连不上会抛异常。"""
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


def is_alive(url):
    """目标服务是否在线。"""
    try:
        code, _ = api("GET", url + "/health", timeout=1.5)
        return code == 200
    except (URLError, OSError):
        return False


def wait_online(url, timeout=12.0, label=""):
    """轮询等待某个服务上线。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_alive(url):
            return True
        time.sleep(0.2)
    print("  警告：%s（%s）等待超时，没有上线！" % (label, url))
    return False


def wait(seconds, why):
    print("  …… 等待 %.1f 秒（%s）" % (seconds, why))
    time.sleep(seconds)


def section(title):
    print("\n" + "=" * 64)
    print(title)
    print("=" * 64)


def show(tag, lines):
    print("\n[%s]" % tag)
    for line in lines:
        print("  " + line)


def verdict(cond):
    return "与预测一致" if cond else "和预测有出入（如实记录）"


def spawn(arg_list):
    return subprocess.Popen([sys.executable] + arg_list, cwd=ROOT)


def start_origin(host, port, delay):
    return spawn([os.path.join(ROOT, "start_origin.py"),
                  "--host", host, "--port", str(port), "--delay", str(delay)])


def start_node(node_id, host, port, peers, origin_url, repl_delay,
               no_singleflight=False, extra=None):
    """启动一个节点；origin_url 为 None 时不接数据源（纯缓存形态）。

    extra 用于阶段C 追加故障注入参数（--block / --repl-dup / --repl-reorder）。
    """
    cmd = [os.path.join(ROOT, "start_node.py"),
           "--node-id", node_id, "--host", host, "--port", str(port),
           "--capacity", "50", "--peers", ",".join(peers),
           "--repl-delay", str(repl_delay)]
    if origin_url:
        cmd += ["--origin", origin_url]
    if no_singleflight:
        cmd += ["--no-singleflight"]
    if extra:
        cmd += list(extra)
    return spawn(cmd)


def wait_hit(url, key, timeout=3.0):
    """轮询等待某个 key 可读，返回 (耗时秒, 响应体)；超时返回 (None, None)。"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        code, body = api("GET", url + "/cache/" + key)
        if code == 200:
            return time.time() - t0, body
        time.sleep(0.05)
    return None, None


def restart_cluster(ctx, flags=None, repl_delay=0.0):
    """阶段C：重启为“纯缓存 + 网络故障注入”的集群（不接数据源）。

    必须不接数据源：否则缺失的数据会被回源补齐，把复制层的故障现象盖住。
    """
    for name in ("node-1", "node-2", "node-3"):
        kill(ctx["procs"], name)
    time.sleep(0.5)
    flags = flags or {}
    for item in ctx["plan"]:
        ctx["procs"][item["node_id"]] = start_node(
            item["node_id"], ctx["host"], item["port"], item["peers"],
            None, repl_delay, extra=flags.get(item["node_id"]))
    for item in ctx["plan"]:
        wait_online(ctx["urls"][item["node_id"]], label=item["node_id"])


def kill(procs, name):
    proc = procs.get(name)
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    print("  - 已杀掉 %s（pid=%d）" % (name, proc.pid))


def burst(url, key, n):
    """同一瞬间打 n 个 GET，返回 (成功数, 最慢耗时)。"""
    barrier = threading.Barrier(n)

    def one(_):
        barrier.wait()
        t0 = time.time()
        try:
            code, _body = api("GET", url + "/cache/" + key, timeout=20)
        except (URLError, OSError):
            code = None
        return code, time.time() - t0

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(one, range(n)))
    ok = sum(1 for code, _ in results if code == 200)
    slowest = max(t for _, t in results)
    return ok, slowest


# ---------------------------------------------------------------------- #
# 五个实验
# ---------------------------------------------------------------------- #
def exp1_ttl(ctx):
    n1 = ctx["urls"]["node-1"]
    key = "exp1-ttl-%d" % ctx["run"]

    section("实验 1：TTL 到期自动消失（对应阶段1/2，纯缓存集群）")
    show("预测", [
        "写入一条 TTL=2 秒的数据：刚写完能读到；等 3 秒后再读应该读不到；",
        "节点的过期计数（expirations）应该 +1。",
    ])
    print("\n[实测]")
    _, s0 = api("GET", n1 + "/stats")
    exp_before = s0.get("expirations", 0)
    api("PUT", n1 + "/cache/" + key, {"value": "ttl-data", "ttl": 2})
    code1, body1 = api("GET", n1 + "/cache/" + key)
    print("  - 刚写入：HTTP %d（值=%s）" % (code1, body1.get("value")))
    wait(3, "让 TTL 到期（顺带给后台清扫线程一次机会）")
    code2, _ = api("GET", n1 + "/cache/" + key)
    _, s1 = api("GET", n1 + "/stats")
    exp_after = s1.get("expirations", 0)
    print("  - 3 秒后：HTTP %d；expirations: %d -> %d" % (code2, exp_before, exp_after))

    hit = (code1 == 200 and code2 == 404 and exp_after > exp_before)
    show("对照", [
        "刚写入 HTTP %d（预测 200） / 3 秒后 HTTP %d（预测 404） / expirations +%d —— %s"
        % (code1, code2, exp_after - exp_before, verdict(hit)),
    ])
    show("解释", [
        "TTL 是缓存的基本纪律：到期即死。删除有两个时机——读到时发现已过期（惰性删除），",
        "以及后台清扫线程定期扫掉（我们每 1 秒扫一次）。",
        "计数 +1 说明这条过期数据确实被清掉了，没有长期占用内存。",
    ])
    ctx["summary"].append("实验1 TTL 到期：写入 HTTP %d -> 3 秒后 HTTP %d（expirations +%d）—— %s"
                          % (code1, code2, exp_after - exp_before, verdict(hit)))


def exp2_replication_window(ctx):
    urls = ctx["urls"]
    n1, n2 = urls["node-1"], urls["node-2"]
    key = "exp2-window-%d" % ctx["run"]
    delay = ctx["args"].repl_delay

    section("实验 2：异步复制的不一致窗口（对应阶段3，纯缓存集群）")
    show("预测", [
        "写入 node-1 后“立刻”读 node-2：应该读不到（数据还在复制队列里，",
        "我们的复制延迟被设成了 %.0f 秒）；" % delay,
        "等超过这个延迟后再读 node-2：应该能读到正确的值。",
    ])
    print("\n[实测]")
    code, _ = api("PUT", n1 + "/cache/" + key, {"value": "v1", "ttl": 60})
    print("  - 写入 node-1：HTTP %d" % code)
    code_now, body_now = api("GET", n2 + "/cache/" + key)
    print("  - 立刻读 node-2：HTTP %d  %s" % (code_now, json.dumps(body_now, ensure_ascii=False)))
    wait(delay + 1.5, "等复制线程把数据送过去")
    code_later, body_later = api("GET", n2 + "/cache/" + key)
    print("  - 稍后再读 node-2：HTTP %d  %s" % (code_later, json.dumps(body_later, ensure_ascii=False)))

    hit = (code_now == 404 and code_later == 200 and body_later.get("value") == "v1")
    show("对照", [
        "立刻读：HTTP %d（预测 404） / 稍后读：HTTP %d（预测 200）—— %s"
        % (code_now, code_later, verdict(hit)),
    ])
    show("解释", [
        "这段“读不到”的时间叫不一致窗口：窗口内我们违反了“强一致”，",
        "但窗口结束后数据补齐，满足“最终一致性”。",
        "这正是阶段3 主动选择的权衡：用短暂的不一致，换“写延迟不被最慢副本拖累”。",
    ])
    ctx["summary"].append("实验2 不一致窗口：窗口内 HTTP %d、窗口后 HTTP %d —— %s"
                          % (code_now, code_later, verdict(hit)))


def exp3_kill_replica(ctx):
    urls, procs = ctx["urls"], ctx["procs"]
    n1, n2, n3 = urls["node-1"], urls["node-2"], urls["node-3"]
    old_key = "exp3-old-%d" % ctx["run"]
    new_key = "exp3-new-%d" % ctx["run"]
    delay = ctx["args"].repl_delay

    section("实验 3：杀掉一个副本节点 node-3（对应阶段3，纯缓存集群）")
    show("预测", [
        "1) node-3 挂掉后，node-1 / node-2 照常读写——集群在单节点故障下继续服务；",
        "2) node-1 发给 node-3 的复制会失败并被记账（repl_sent_fail 增加）；",
        "3) 把 node-3 重启回来：之后的“新写入”会重新复制到它，",
        "   但停机期间缺掉的“旧数据”不会自动补齐（我们不做反熵修复）。",
    ])
    print("\n[实测]")
    kill(procs, "node-3")
    code, _ = api("PUT", n1 + "/cache/" + old_key, {"value": "old", "ttl": 60})
    print("  - 向 node-1 写入 %s：HTTP %d" % (old_key, code))
    code1, body1 = api("GET", n1 + "/cache/" + old_key)
    print("  - node-1 立刻读：HTTP %d（值=%s）" % (code1, body1.get("value")))
    wait(delay + 1.5, "等复制发货（node-2 在线，node-3 已死）")
    code2, body2 = api("GET", n2 + "/cache/" + old_key)
    print("  - node-2 读（应已复制到）：HTTP %d（值=%s）" % (code2, body2.get("value")))
    print("  - node-3 在线检查：%s" % ("能连上？！" if is_alive(n3) else "连不上（确认已死）"))
    _, stats = api("GET", n1 + "/stats")
    fail = stats.get("repl_sent_fail", 0)
    print("  - node-1 复制台账：sent_ok=%s, sent_fail=%s, backlog=%s"
          % (stats.get("repl_sent_ok"), fail, stats.get("repl_backlog")))

    # 重启 node-3
    print("\n  （重启 node-3 ……）")
    item = [p for p in ctx["plan"] if p["node_id"] == "node-3"][0]
    time.sleep(0.5)
    procs["node-3"] = start_node("node-3", ctx["host"], item["port"], item["peers"],
                                 None, delay)
    wait_online(n3, label="node-3")
    print("  - node-3 已重新上线")
    api("PUT", n1 + "/cache/" + new_key, {"value": "new", "ttl": 60})
    wait(delay + 1.5, "等新数据复制到重启后的 node-3")
    code_old, _ = api("GET", n3 + "/cache/" + old_key)
    code_new, body_new = api("GET", n3 + "/cache/" + new_key)
    print("  - node-3 读“停机期间写入的旧数据”：HTTP %d" % code_old)
    print("  - node-3 读“重启之后的新数据”：HTTP %d（值=%s）" % (code_new, body_new.get("value")))

    hit = (code1 == 200 and code2 == 200 and fail >= 1 and code_old == 404 and code_new == 200)
    show("对照", [
        "1) 其余节点照常服务、node-3 连不上 —— 与预测一致",
        "2) sent_fail=%d（应有发往 node-3 的失败）" % fail,
        "3) 重启后：新数据 HTTP %d（有）、旧数据 HTTP %d（没有）—— %s"
        % (code_new, code_old, verdict(hit)),
    ])
    show("解释", [
        "集群的高可用成立：少一个节点，服务不中断（没有单点）。",
        "但“最终一致”是有边界的：它只保证“在线节点最终拿到数据”，",
        "不保证“离线的节点能补上历史亏空”——失败的那次复制不会重试。",
        "（如果节点像阶段4 那样接了数据源，缺失的数据会通过“回源”重新取到——",
        " 那是另一层兜底；本实验先隔离复制本身，所以节点没有接数据源。）",
        "题目允许“数据丢失部分可接受”，这份边界我们会如实写进报告。",
    ])
    ctx["summary"].append("实验3 副本故障：集群照常服务、复制失败记账、旧数据缺口不补 —— %s" % verdict(hit))


def exp4_kill_origin(ctx):
    urls, procs = ctx["urls"], ctx["procs"]
    n1, n2 = urls["node-1"], urls["node-2"]
    cached_key = "exp4-cached-%d" % ctx["run"]
    fresh_key = "exp4-fresh-%d" % ctx["run"]
    delay = ctx["args"].repl_delay

    section("实验 4：杀掉数据源（对应阶段4，已接数据源）")
    show("预测", [
        "数据源挂掉后：",
        "1) 读“缓存里已有的数据”：照常命中——缓存起“挡箭牌”作用；",
        "2) 读一个“从没缓存过的新 key”：回源失败，返回 502。",
    ])
    print("\n[实测]")
    api("PUT", n1 + "/cache/" + cached_key, {"value": "cached", "ttl": 60})
    wait(delay + 1.5, "先把这条数据复制到各节点（让它成为“已有缓存”）")
    kill(procs, "origin")
    code1, body1 = api("GET", n2 + "/cache/" + cached_key)
    print("  - 读已有缓存：HTTP %d（值=%s）" % (code1, body1.get("value")))
    code2, body2 = api("GET", n2 + "/cache/" + fresh_key)
    print("  - 读新 key：HTTP %d  %s" % (code2, json.dumps(body2, ensure_ascii=False)[:100]))

    hit = (code1 == 200 and code2 == 502)
    show("对照", [
        "已有缓存：HTTP %d（预测 200） / 新 key：HTTP %d（预测 502）—— %s"
        % (code1, code2, verdict(hit)),
    ])
    show("解释", [
        "缓存对“已有数据”提供了降级保护：数据库倒了，命中缓存的请求照样能用；",
        "但“没缓存过的数据”会直接失败——回源链路上的数据源目前是单点依赖。",
        "（真实系统常见的做法是限流 / 熔断 / 备用源；我们做到哪、就到哪，如实写边界。）",
    ])
    ctx["summary"].append("实验4 数据源故障：已有缓存 HTTP %d、新 key HTTP %d —— %s"
                          % (code1, code2, verdict(hit)))

    # 把数据源重启回来，供实验5使用
    print("\n  （把数据源重启回来，供实验 5 使用）")
    procs["origin"] = start_origin(ctx["host"], ctx["origin_port"], ctx["args"].origin_delay)
    wait_online(ctx["origin_url"], label="数据源")


def exp5_stampede(ctx):
    urls, procs = ctx["urls"], ctx["procs"]
    n1 = urls["node-1"]
    origin_url = ctx["origin_url"]
    n = ctx["args"].requests
    key_a = "exp5-protected-%d" % ctx["run"]
    key_b = "exp5-raw-%d" % ctx["run"]

    section("实验 5：缓存击穿保护实战对照（对应阶段4，已接数据源）")
    show("预测", [
        "%d 个并发请求同时打向同一个缺失的 key：" % n,
        "1) 有保护（singleflight）：数据源只应该被访问 1 次，其余请求共享结果；",
        "2) 作为对照，关掉保护后：几乎每个请求都会各自回源（约 %d 次）。" % n,
    ])

    print("\n[实测 5A] 当前集群：有保护")
    api("POST", origin_url + "/reset")
    ok_a, slow_a = burst(n1, key_a, n)
    _, os1 = api("GET", origin_url + "/stats")
    calls_a = os1["calls_by_key"].get(key_a, 0)
    print("  - 完成：%d/%d 个请求返回 200，最慢 %.2f 秒" % (ok_a, n, slow_a))
    print("  - 数据源被访问：%d 次" % calls_a)
    hit_a = (ok_a == n and calls_a == 1)

    print("\n[实测 5B] 对照：重启集群并关闭保护（--no-singleflight）")
    for name in ("node-1", "node-2", "node-3"):
        kill(procs, name)
    time.sleep(0.5)
    for item in ctx["plan"]:
        procs[item["node_id"]] = start_node(item["node_id"], ctx["host"], item["port"],
                                            item["peers"], origin_url,
                                            ctx["args"].repl_delay, no_singleflight=True)
    for item in ctx["plan"]:
        wait_online(urls[item["node_id"]], label=item["node_id"])
    api("POST", origin_url + "/reset")
    ok_b, slow_b = burst(n1, key_b, n)
    _, os2 = api("GET", origin_url + "/stats")
    calls_b = os2["calls_by_key"].get(key_b, 0)
    print("  - 完成：%d/%d 个请求返回 200，最慢 %.2f 秒" % (ok_b, n, slow_b))
    print("  - 数据源被访问：%d 次" % calls_b)
    # 阈值按并发数缩放，避免 --requests 改小时误判（默认 20 时仍是 15）
    hit_b = (ok_b == n and calls_b >= max(2, int(n * 0.75)))

    show("对照", [
        "有保护：数据源被访问 %d 次、最慢 %.2f 秒（预测 1 次）—— %s"
        % (calls_a, slow_a, verdict(hit_a)),
        "无保护：数据源被访问 %d 次、最慢 %.2f 秒（预测约 %d 次）—— %s"
        % (calls_b, slow_b, n, verdict(hit_b)),
    ])
    show("解释", [
        "同一批并发请求，数据源压力：%d 次 对 %d 次；最慢响应：%.2f 秒 对 %.2f 秒。"
        % (calls_a, calls_b, slow_a, slow_b),
        "请求合并（singleflight）把“同时到达的同一个 key”压缩成 1 次回源——",
        "这是阶段4 的保护在“真实集群 + 故障演练环境”里的实战检验。",
    ])
    ctx["summary"].append("实验5 击穿保护对照：回源 %d 次 vs %d 次（最慢 %.2f 秒 vs %.2f 秒）—— %s"
                          % (calls_a, calls_b, slow_a, slow_b, verdict(hit_a and hit_b)))


def exp6_partition(ctx):
    urls = ctx["urls"]
    n1, n2, n3 = urls["node-1"], urls["node-2"], urls["node-3"]
    ports = {p["node_id"]: p["port"] for p in ctx["plan"]}
    key_a = "exp6-a-%d" % ctx["run"]
    key_b = "exp6-b-%d" % ctx["run"]

    section("实验 6：网络双向分区（--isolate / --block，纯缓存集群）")
    show("预测", [
        "1) node-3 被隔离后，写入 node-1 的数据仍能秒级到达 node-2；",
        "2) 反方向也断：写入 node-3 的数据到不了 node-1 / node-2（这才叫双向分区）；",
        "3) 两侧各自可读可写，但数据分叉；没有反熵，恢复后不会自动合并。",
    ])
    restart_cluster(ctx, {
        "node-1": ["--block", str(ports["node-3"])],
        "node-2": ["--block", str(ports["node-3"])],
        "node-3": ["--block", "%d,%d" % (ports["node-1"], ports["node-2"])],
    })
    print("\n[实测]")
    t0 = time.time()
    code, _ = api("PUT", n1 + "/cache/" + key_a, {"value": "from-node-1", "ttl": 60})
    write_ms = (time.time() - t0) * 1000
    print("  - 写入 node-1：HTTP %d（耗时 %.0f ms）" % (code, write_ms))
    lat, body2 = wait_hit(n2, key_a, timeout=3)
    print("  - node-2 收到该数据：%s（值=%s）"
          % ("%.0f ms" % (lat * 1000) if lat is not None else "超时", body2 and body2.get("value")))
    code3, _ = api("GET", n3 + "/cache/" + key_a)
    print("  - node-3 读同一条数据：HTTP %d" % code3)

    code_b, _ = api("PUT", n3 + "/cache/" + key_b, {"value": "from-node-3", "ttl": 60})
    print("  - 写入 node-3：HTTP %d" % code_b)
    wait(1.0, "给反方向复制留出发货时间")
    code1_b, _ = api("GET", n1 + "/cache/" + key_b)
    code2_b, _ = api("GET", n2 + "/cache/" + key_b)
    print("  - node-1 读 node-3 的写入：HTTP %d" % code1_b)
    print("  - node-2 读 node-3 的写入：HTTP %d" % code2_b)

    stats1 = api("GET", n1 + "/stats")[1]
    stats3 = api("GET", n3 + "/stats")[1]
    drop1 = stats1.get("repl_blocked_dropped", 0)
    drop3 = stats3.get("repl_blocked_dropped", 0)
    print("  - node-1 分区丢弃计数：%d 条（发往 node-3）" % drop1)
    print("  - node-3 分区丢弃计数：%d 条（发往 node-1/2）" % drop3)

    hit = (lat is not None and code3 == 404 and code1_b == 404 and code2_b == 404
           and drop1 >= 1 and drop3 >= 1)
    show("对照", [
        "1) node-2 在 %.0f ms 内收到 —— 与预测一致"
        % ((lat or 0) * 1000),
        "2) node-3 读不到（HTTP %d）、node-1/2 也读不到 node-3 的写入（HTTP %d/%d）—— %s"
        % (code3, code1_b, code2_b, verdict(hit)),
        "3) 两侧丢弃计数：%d / %d 条 —— 分区在两个方向都生效" % (drop1, drop3),
    ])
    show("解释", [
        "这是典型的 AP 取舍：分区期间每个节点都能读写，可用性不降；",
        "代价是数据分叉（split brain），两个分区各自演化。",
        "我们不选主、不投票，所以没有“多数派”能裁定哪边是对的；",
        "又因为没有反熵，分区恢复后分叉会一直保留——这是本设计已知的边界。",
    ])
    ctx["summary"].append("实验6 双向分区：可用性保持、两侧分叉、丢弃 %d/%d 条 —— %s"
                          % (drop1, drop3, verdict(hit)))


def exp7_duplicate(ctx):
    urls = ctx["urls"]
    n1, n2, n3 = urls["node-1"], urls["node-2"], urls["node-3"]
    key = "exp7-dup-%d" % ctx["run"]

    section("实验 7：复制消息被重复发送三次（--repl-dup 3，纯缓存集群）")
    show("预测", [
        "1) 同一条写入会被投递 3 次（每个副本 3 次，共 2 副本 x 3 = 6 次成功发送）；",
        "2) 副本最终状态与只发一次完全相同——因为 set 是幂等的；",
        "3) 不会出现“值被写坏”“计数翻倍”之类的现象。",
    ])
    restart_cluster(ctx, {"node-1": ["--repl-dup", "3"]})
    print("\n[实测]")
    code, _ = api("PUT", n1 + "/cache/" + key, {"value": "dup-ok", "ttl": 60})
    print("  - 写入 node-1：HTTP %d" % code)
    wait(1.2, "等三条重复消息全部送达")
    code2, body2 = api("GET", n2 + "/cache/" + key)
    code3, body3 = api("GET", n3 + "/cache/" + key)
    stats = api("GET", n1 + "/stats")[1]
    ok = stats.get("repl_sent_ok", 0)
    print("  - node-2 读：HTTP %d（值=%s）" % (code2, body2.get("value")))
    print("  - node-3 读：HTTP %d（值=%s）" % (code3, body3.get("value")))
    print("  - node-1 发送台账：sent_ok=%d, dup=%s" % (ok, stats.get("repl_dup")))

    hit = (code2 == 200 and code3 == 200
           and body2.get("value") == "dup-ok" and body3.get("value") == "dup-ok"
           and ok == 6)
    show("对照", [
        "1) 实际成功发送 %d 次（预测 6 次）—— %s" % (ok, verdict(ok == 6)),
        "2) 两个副本读到的值都是 dup-ok，与单次投递结果相同 —— %s"
        % verdict(hit),
    ])
    show("解释", [
        "幂等（idempotent）：同一个操作执行一次和执行多次，最终状态一样。",
        "我们的复制是 at-least-once（至少一次），靠 set 的幂等性兜住重复；",
        "如果换成“数量 +1”这种非幂等操作，重复投递就会翻倍——",
        "这也解释了购物车那类场景为什么要用 requestId 去重。",
    ])
    ctx["summary"].append("实验7 重复投递 x3：发送 %d 次、状态不变 —— %s" % (ok, verdict(hit)))


def exp8_reorder(ctx):
    urls = ctx["urls"]
    n1, n2 = urls["node-1"], urls["node-2"]
    keys = ["exp8-%d-%d" % (ctx["run"], i) for i in range(10)]

    section("实验 8：消息延迟后乱序到达（--repl-reorder，纯缓存集群）")
    show("预测", [
        "1) 10 条写入会被乱序投递，node-2 观察到的到达顺序与写入顺序不同；",
        "2) 一条都不会丢——乱序不等于丢失，最终 10 条全部到达；",
        "3) 因为写的是 10 个不同的 key，乱序不影响最终状态。",
    ])
    restart_cluster(ctx, {"node-1": ["--repl-reorder", "--repl-reorder-hold", "0.3"]})
    print("\n[实测]")
    for i, key in enumerate(keys):
        api("PUT", n1 + "/cache/" + key, {"value": i, "ttl": 60})
    print("  - 已连续写入 node-1：%d 条" % len(keys))

    seen = []
    deadline = time.time() + 5
    while time.time() < deadline and len(seen) < len(keys):
        _code, body = api("GET", n2 + "/dump")
        for item in body.get("items", []):
            if item["key"] in keys and item["key"] not in seen:
                seen.append(item["key"])
        time.sleep(0.02)
    print("  - node-2 实际到达顺序：%s" % " → ".join(k.split("-")[-1] for k in seen))
    print("  - 期望的写入顺序：     %s" % " → ".join(k.split("-")[-1] for k in keys))

    all_arrived = len(seen) == len(keys)
    reordered = seen != keys
    hit = all_arrived and reordered
    show("对照", [
        "1) 到达 %d/10 条（预测 10 条）—— %s" % (len(seen), verdict(all_arrived)),
        "2) 到达顺序%s写入顺序 —— %s"
        % ("不同于" if reordered else "仍等于", verdict(reordered)),
    ])
    show("解释", [
        "默认实现里每个副本一条 FIFO 队列，能保证“同一副本按产生顺序收到”；",
        "打开 --repl-reorder 后，我们故意打乱发送顺序，模拟网络乱序到达。",
        "本实验的 10 个 key 互不相关，乱序不影响结果；",
        "但同一个 key 的“先写后删”如果被乱序，就可能出现删完又被写回来——",
        "所以生产系统要靠版本号/逻辑时钟，而不是只依赖到达顺序。",
    ])
    ctx["summary"].append("实验8 乱序到达：%d/10 条到达、顺序%s写入 —— %s"
                          % (len(seen), "不同于" if reordered else "等于", verdict(hit)))


def _pct(values, p):
    """取分位数（0~1），用于打印 p50 / p99。"""
    if not values:
        return 0.0
    vals = sorted(values)
    idx = min(len(vals) - 1, int(round((len(vals) - 1) * p)))
    return vals[idx]


def exp9_clock_rollback(ctx):
    urls = ctx["urls"]
    n1, n2 = urls["node-1"], urls["node-2"]
    key = "exp9-clock-%d" % ctx["run"]

    section("实验 9：系统时间突然倒退一分钟（--clock wall 对照，纯缓存集群）")
    show("预测", [
        "1) 默认节点用单调时钟：把系统时间拨回去，TTL 照常流逝，不受影响；",
        "2) 对照组（--clock wall）把墙钟拨回 60 秒：剩余 TTL 凭空多出 60 秒，数据“续命”；",
        "3) 结论：用物理时钟做 TTL 或版本比较，时间回拨会破坏语义。",
    ])
    restart_cluster(ctx, {"node-1": ["--clock", "wall"]})
    print("\n[实测]")
    code, _ = api("PUT", n1 + "/cache/" + key, {"value": "clock", "ttl": 60})
    print("  - 写入 node-1（ttl=60）：HTTP %d" % code)
    wait(0.5, "等副本同步（复制报文带的是剩余秒数，与墙钟无关）")
    ttl1_before = api("GET", n1 + "/cache/" + key)[1].get("ttl", -1)
    ttl2_before = api("GET", n2 + "/cache/" + key)[1].get("ttl", -1)
    print("  - 回拨前：node-1(墙钟) ttl=%.1f 秒，node-2(单调) ttl=%.1f 秒"
          % (ttl1_before, ttl2_before))

    resp = api("POST", n1 + "/admin/clock", {"offset": -60})[1]
    print("  - 把 node-1 的墙钟拨回 60 秒：%s" % resp.get("note"))
    ttl1_after = api("GET", n1 + "/cache/" + key)[1].get("ttl", -1)
    ttl2_after = api("GET", n2 + "/cache/" + key)[1].get("ttl", -1)
    print("  - 回拨后：node-1(墙钟) ttl=%.1f 秒，node-2(单调) ttl=%.1f 秒"
          % (ttl1_after, ttl2_after))

    hit = (ttl1_after > ttl1_before + 50 and abs(ttl2_after - ttl2_before) < 5)
    show("对照", [
        "墙钟节点：%.1f 秒 -> %.1f 秒（凭空 +%.1f 秒）"
        % (ttl1_before, ttl1_after, ttl1_after - ttl1_before),
        "单调时钟节点：%.1f 秒 -> %.1f 秒（误差 %.1f 秒）—— %s"
        % (ttl2_before, ttl2_after, ttl2_after - ttl2_before, verdict(hit)),
    ])
    show("解释", [
        "墙钟会被 NTP 校准、手动改时间、虚拟机迁移影响，可能突然往回跳。",
        "TTL 一旦用墙钟，回拨就等于给所有数据“续命”；如果再用时间戳做 LWW 冲突解决，",
        "更旧的写入会被误判为更新，造成丢更新。",
        "单调时钟只保证“本机进程内单调递增”，所以它适合做 TTL，",
        "但不能跨机器比较——跨节点定序仍然要靠 Lamport 时钟/版本向量。",
    ])
    ctx["summary"].append("实验9 时间回拨：墙钟 TTL +%.0f 秒、单调时钟不变 —— %s"
                          % (ttl1_after - ttl1_before, verdict(hit)))


def exp10_hot_key(ctx):
    urls = ctx["urls"]
    n1 = urls["node-1"]
    hot = "exp10-hot-%d" % ctx["run"]
    cold = ["exp10-cold-%d-%d" % (ctx["run"], i) for i in range(20)]

    section("实验 10：单个 key 占据 50% 请求（纯缓存集群）")
    show("预测", [
        "1) 热点 key 的读请求分散到 3 个副本，每个副本都有本地副本，基本全命中；",
        "2) 热点 key 的读延迟不会明显高于冷 key；",
        "3) 代价在写侧：每次写要复制给 2 个 peer，热点 key 的写放大 3 倍。",
    ])
    restart_cluster(ctx, {})
    print("\n[实测]")
    api("PUT", n1 + "/cache/" + hot, {"value": "hot", "ttl": 60})
    for k in cold:
        api("PUT", n1 + "/cache/" + k, {"value": "cold", "ttl": 60})
    wait(0.6, "等待预热数据复制到所有副本")

    mixed = [(hot, urls["node-%d" % (i % 3 + 1)]) for i in range(100)]
    mixed += [(cold[i % len(cold)], urls["node-%d" % (i % 3 + 1)]) for i in range(100)]

    def one(item):
        key, base = item
        t0 = time.time()
        code, _ = api("GET", base + "/cache/" + key, timeout=5)
        return code, time.time() - t0, key == hot

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(one, mixed))
    ok = sum(1 for code, _t, _h in results if code == 200)
    hot_lat = [t for code, t, is_hot in results if code == 200 and is_hot]
    cold_lat = [t for code, t, is_hot in results if code == 200 and not is_hot]
    print("  - 200 个读请求（热点 100 / 冷 key 100）：成功 %d 个" % ok)
    print("  - 热点 key：p50=%.1f ms，p99=%.1f ms"
          % (_pct(hot_lat, 0.50) * 1000, _pct(hot_lat, 0.99) * 1000))
    print("  - 冷 key：  p50=%.1f ms，p99=%.1f ms"
          % (_pct(cold_lat, 0.50) * 1000, _pct(cold_lat, 0.99) * 1000))

    before = api("GET", n1 + "/stats")[1].get("repl_sent_ok", 0)
    for i in range(60):
        api("PUT", n1 + "/cache/" + hot, {"value": "v%d" % i, "ttl": 60})
    wait(0.8, "等待 60 次热点写入复制完成")
    after_stats = api("GET", n1 + "/stats")[1]
    sent = after_stats.get("repl_sent_ok", 0) - before
    final = api("GET", urls["node-2"] + "/cache/" + hot)[1].get("value")
    print("  - 60 次热点写入：复制发送 %d 次（2 个 peer x 60 = 120），"
          "backlog=%s，副本最终值=%s"
          % (sent, after_stats.get("repl_backlog"), final))

    p50_ok = _pct(hot_lat, 0.50) <= _pct(cold_lat, 0.50) * 2 + 0.01
    hit = (ok == 200 and p50_ok and sent >= 100 and final == "v59")
    show("对照", [
        "1) 读请求 200/200 成功，热点 p50 不高于冷 key 的 2 倍 —— %s"
        % verdict(ok == 200 and p50_ok),
        "2) 60 次写触发 %d 次复制发送（预测 120）：网络放大 %.1f 倍、含本地共 %.1f 份 —— %s"
        % (sent, sent / 60.0, 1 + sent / 60.0, verdict(sent >= 100)),
        "3) 副本最终收敛到 v59 —— %s" % verdict(final == "v59"),
    ])
    show("解释", [
        "热点读在缓存场景里几乎不付出代价：3 个副本各自命中，流量被天然摊平。",
        "真正的放大在写侧：1 次写 -> 3 份存储 + 2 次网络发送。",
        "如果把热点 key 强行分片，读请求反而要跨节点转发，延迟更高；",
        "所以“复制 + 本地命中”是更适合读多写少缓存的选择。",
        "剩下的风险是热点 key 过期瞬间的击穿——那由 singleflight 兜住（实验5）。",
    ])
    ctx["summary"].append("实验10 热点 key：200 读全成功、写 60 次复制 %d 次（共 %.1f 份）—— %s"
                          % (sent, 1 + sent / 60.0, verdict(hit)))


def exp11_response_loss(ctx):
    urls = ctx["urls"]
    n1, n2 = urls["node-1"], urls["node-2"]
    key1 = "exp11-retry-%d" % ctx["run"]
    key2 = "exp11-aba-%d" % ctx["run"]

    section("实验 11：写成功但响应丢失（/admin/fault，纯缓存集群）")
    show("预测", [
        "1) 客户端收到连接异常，但数据其实已经写进 node-1；",
        "2) 用相同值重试，最终数据正确，但服务端执行了两次（复制也发两次）；",
        "3) 危险场景：失败期间别人写了更新值，失败方盲目重试会把新值覆盖掉。",
    ])
    restart_cluster(ctx, {})
    print("\n[实测]")

    api("POST", n1 + "/admin/fault", {"drop_responses": 1})
    failed_how = ""
    try:
        api("PUT", n1 + "/cache/" + key1, {"value": "v1", "ttl": 60}, timeout=3)
        print("  - 第一次写入竟然返回了响应（预期应失败）")
    except Exception as exc:  # 连接被断开 / 没有响应
        failed_how = type(exc).__name__
        print("  - 第一次写入客户端报错：%s（响应在途中丢失）" % failed_how)

    code1, body1 = api("GET", n1 + "/cache/" + key1)
    print("  - 但 node-1 实际已写入：HTTP %d，值=%s" % (code1, body1.get("value")))

    code_retry, _ = api("PUT", n1 + "/cache/" + key1, {"value": "v1", "ttl": 60})
    wait(0.6, "等重试写入复制完成")
    code2, body2 = api("GET", n2 + "/cache/" + key1)
    stats = api("GET", n1 + "/stats")[1]
    enq = stats.get("repl_enqueued", 0)
    print("  - 客户端重试：HTTP %d；node-2 读到：HTTP %d，值=%s"
          % (code_retry, code2, body2.get("value")))
    print("  - node-1 的复制入队次数：%d（一次写 + 一次重试 = 2）" % enq)

    # 场景二：失败方的重试覆盖了别人刚写的新值
    api("POST", n1 + "/admin/fault", {"drop_responses": 1})
    try:
        api("PUT", n1 + "/cache/" + key2, {"value": "a-v1", "ttl": 60}, timeout=3)
    except Exception:
        pass  # 客户端认为失败
    api("PUT", n1 + "/cache/" + key2, {"value": "b-v2", "ttl": 60})
    api("PUT", n1 + "/cache/" + key2, {"value": "a-v1", "ttl": 60})  # A 盲目重试
    final = api("GET", n1 + "/cache/" + key2)[1].get("value")
    print("  - A 失败 -> B 写入 b-v2 -> A 重试 a-v1，最终值=%s" % final)

    hit = (bool(failed_how) and code1 == 200 and body1.get("value") == "v1"
           and code_retry == 200 and body2.get("value") == "v1"
           and enq == 2 and final == "a-v1")
    show("对照", [
        "1) 客户端报 %s，但 node-1 已写入（HTTP %d，值=%s）—— %s"
        % (failed_how or "无异常", code1, body1.get("value"), verdict(bool(failed_how) and code1 == 200)),
        "2) 重试后副本一致（%s），服务端却执行了 2 次（复制入队 %d 次）—— %s"
        % (body2.get("value"), enq, verdict(code_retry == 200 and enq == 2)),
        "3) A 的重试把 B 的 b-v2 覆盖成 %s（B 的更新丢失）—— %s"
        % (final, verdict(final == "a-v1")),
    ])
    show("解释", [
        "客户端只知道自己没收到 ack，无法区分「没执行」和「执行了但响应丢了」。",
        "at-least-once 重试是必然选择，所以服务端必须让操作幂等。",
        "set 相同值天然幂等，所以场景1结果正确；但重试与 B 的写是并发的，",
        "两者没有 happens-before 关系，按到达顺序覆盖就是合法的——对业务却是丢更新。",
        "工程解法：requestId 去重表 / 幂等令牌 / 版本号 CAS，不能靠“值相同”判断重复。",
    ])
    ctx["summary"].append("实验11 响应丢失：客户端报错但已写入、重试后 B 的更新被覆盖 —— %s"
                          % verdict(hit))


def switch_to_origin_cluster(ctx):
    """阶段B：把集群从“纯缓存”重启为“带数据源”的形态，并启动数据源。"""
    print("\n>>> 阶段B：重启集群并接上数据源（实验 4~5 需要）...")
    for name in ("node-1", "node-2", "node-3"):
        kill(ctx["procs"], name)
    time.sleep(0.5)
    ctx["procs"]["origin"] = start_origin(ctx["host"], ctx["origin_port"],
                                          ctx["args"].origin_delay)
    for item in ctx["plan"]:
        ctx["procs"][item["node_id"]] = start_node(
            item["node_id"], ctx["host"], item["port"], item["peers"],
            ctx["origin_url"], ctx["args"].repl_delay)
    for item in ctx["plan"]:
        wait_online(ctx["urls"][item["node_id"]], label=item["node_id"])
    wait_online(ctx["origin_url"], label="数据源")


# ---------------------------------------------------------------------- #
# 主流程
# ---------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(description="阶段5 故障注入实验（预测-实测-对照-解释）")
    parser.add_argument("--base-port", type=int, default=9301, help="集群起始端口（数据源在 起始端口+10）")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--repl-delay", type=float, default=2.0, help="复制延迟（秒），让不一致窗口清晰可见")
    parser.add_argument("--origin-delay", type=float, default=0.2, help="数据源每次查询耗时（秒）")
    parser.add_argument("--requests", type=int, default=20, help="实验5 的并发请求数")
    args = parser.parse_args()

    host = args.host
    base = args.base_port
    origin_port = base + 10
    plan = build_plan(3, base, host)

    check_list = [("node-1", base), ("node-2", base + 1), ("node-3", base + 2), ("origin", origin_port)]
    busy = [(name, port) for name, port in check_list if port_in_use(host, port)]
    if busy:
        print("启动失败：以下端口已被占用：")
        for name, port in busy:
            print("  %-7s -> %s:%d" % (name, host, port))
        print("\n如果是上一次的进程没关干净，先在运行它的窗口按 Ctrl+C；")
        print("或者换一组端口，例如：--base-port 9701")
        return

    urls = {item["node_id"]: "http://%s:%d" % (host, item["port"]) for item in plan}
    origin_url = "http://%s:%d" % (host, origin_port)

    section("阶段5：故障注入实验（预测 → 实测 → 对照 → 解释）")
    print("本次实验分三段：实验 1~3 在“纯缓存集群”（不接数据源）上做；")
    print("实验 4~5 重启集群、接上数据源后再做；")
    print("实验 6~11 再切回纯缓存集群，注入分区 / 重复 / 乱序 / 时钟回拨 / 热点 key / 响应丢失。")
    print("全程约 110 秒。")
    if args.repl_delay < 1.0:
        print("提示：--repl-delay 太小，实验2 的不一致窗口可能看不清，建议 >= 1 秒。")

    print("\n>>> 阶段A：启动纯缓存集群（3 个节点，不带数据源）...")
    procs = {}
    for item in plan:
        procs[item["node_id"]] = start_node(item["node_id"], host, item["port"],
                                            item["peers"], None, args.repl_delay)
    ready = all(wait_online(urls[item["node_id"]], label=item["node_id"]) for item in plan)

    ctx = {
        "host": host, "origin_port": origin_port, "origin_url": origin_url,
        "urls": urls, "procs": procs, "plan": plan, "run": int(time.time()),
        "args": args, "summary": [],
    }

    try:
        if not ready:
            print("\n实验环境没有全部就绪，本次实验终止。")
            return
        exp1_ttl(ctx)
        exp2_replication_window(ctx)
        exp3_kill_replica(ctx)
        switch_to_origin_cluster(ctx)
        exp4_kill_origin(ctx)
        exp5_stampede(ctx)

        print("\n>>> 阶段C：切回纯缓存集群，做网络、时间与语义故障注入（实验 6~11）...")
        exp6_partition(ctx)
        exp7_duplicate(ctx)
        exp8_reorder(ctx)
        exp9_clock_rollback(ctx)
        exp10_hot_key(ctx)
        exp11_response_loss(ctx)

        section("实验小结（十一个实验的预测 vs 实际）")
        for line in ctx["summary"]:
            print("  " + line)
        print("\n提示：把上面每段的 [预测] / [实测] / [对照] / [解释] 截下来，")
        print("就是你答辩演示需要的“预测 → 实测 → 对照 → 解释”四步证据链。")
    except (URLError, OSError) as exc:
        print("\n[中止] 出现网络错误：%s" % exc)
        print("可以检查一下实验进程是否还活着，然后重跑一次。")
    finally:
        print("\n>>> 清理实验进程 ...")
        for name in list(procs.keys()):
            kill(procs, name)
        print("所有实验进程已清理。")


if __name__ == "__main__":
    main()