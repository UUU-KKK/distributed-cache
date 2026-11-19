"""
异步复制（阶段3核心）：把本节点收到的写入，抄送给其它节点。

「异步」到底是什么意思？
  客户端 PUT 到 node-1 时：
    - node-1 只保证「我自己本地写好了」就立刻返回 200；
    - 抄送给 node-2 / node-3 的任务被丢进内存队列，由后台线程慢慢发货，
      node-1 不等对方回应。
  好处：写延迟低（不会被最慢的节点拖累）。
  代价：从「node-1 写成功」到「node-2 也有这条数据」之间存在一个时间窗口。
        这个窗口内两个节点看到的数据不一致；如果 node-1 此刻崩溃，
        队列里还没发出去的数据就永久丢失了。
        这正是题目所说「数据丢失部分可接受」的来源。

为什么用「每副本一条独立队列 + 一个专用线程」：
  1. 每个副本单独排队，某个副本挂掉时，发往它的超时不会拖慢其它副本；
  2. 单线程消费保证「同一个副本收到的操作顺序 = 本节点产生操作的顺序」，
     避免「先删后写」乱序到达；
  3. 队列积压长度（backlog）本身就是可观测指标：它越大，
     说明副本越跟不上，「不一致窗口」越长。

故障注入开关（阶段5用）：
  --block      模拟网络分区：命中该字符串的副本，复制消息直接丢弃（计入失败）；
  --repl-dup   模拟重复投递：每条复制消息重复发送 N 次（副本应幂等，结果不变）；
  --repl-reorder 模拟乱序：消息先进入一个小缓冲区，随机取一条发送，
                 让「后产生」的消息可能先到达。

依赖：只用标准库（queue / threading / urllib），节点本身不需要装任何第三方库。
"""

import json
import queue
import random
import threading
import time
import urllib.request


class _PeerWorker:
    """一个副本对应一条队列 + 一个后台线程（故障隔离）。"""

    def __init__(self, sender, peer):
        self.sender = sender
        self.peer = peer
        self.queue = queue.Queue()
        self.pending = []          # [(item, 到达时间)]，仅在 reorder 模式下使用
        self._thread = None

    @property
    def backlog(self):
        return self.queue.qsize() + len(self.pending)

    def start(self):
        self._thread = threading.Thread(
            target=self._loop,
            name="%s-repl-%s" % (self.sender.node_id, self.peer),
            daemon=True,
        )
        self._thread.start()

    def _loop(self):
        sender = self.sender
        while not sender._stop_event.is_set():
            try:
                item = self.queue.get(timeout=0.2)
            except queue.Empty:
                item = None

            if item is not None:
                if sender.delay and sender._stop_event.wait(sender.delay):
                    return
                if sender.reorder:
                    # 先进缓冲区，等缓冲区够大或停留够久再随机挑一条发
                    self.pending.append((item, time.monotonic()))
                else:
                    self._deliver(item)

            if self.pending:
                now = time.monotonic()
                due = (len(self.pending) >= sender.reorder_window
                       or now - self.pending[0][1] >= sender.reorder_hold)
                if due:
                    idx = random.randrange(len(self.pending))
                    held, _ = self.pending.pop(idx)
                    self._deliver(held)

    def _deliver(self, item):
        sender = self.sender
        # TTL 必须在「真正发送的这一刻」重新扣减：消息可能在队列里排队、
        # 被人为延迟或乱序缓冲区压了很久，按入队时的剩余时间发送会让副本多活。
        payload = dict(item)
        if payload["ttl"] is not None:
            remain = payload["ttl"] - (time.monotonic() - payload.pop("created"))
            payload["ttl"] = max(0.0, remain)
        else:
            payload.pop("created", None)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        for _ in range(sender.dup):
            try:
                req = urllib.request.Request(
                    "http://%s/replicate" % self.peer,
                    data=body,
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=2) as resp:
                    resp.read()
                sender.sent_ok += 1
            except Exception as exc:  # 副本离线、超时等：计入失败，不阻塞其它副本
                sender.sent_fail += 1
                print("[%s] 复制到 %s 失败: %s" % (sender.node_id, self.peer, exc))
                break


class ReplicationSender:
    """把写入异步推送到若干副本节点。"""

    def __init__(self, node_id, peers, delay=0.0, block_peers=None,
                 dup=1, reorder=False, reorder_window=2, reorder_hold=0.5):
        self.node_id = node_id
        self.peers = [p.strip() for p in (peers or []) if p and p.strip()]
        self.delay = float(delay)      # 人为发货延迟（秒），仅用于演示「不一致窗口」

        self.block_tokens = [t.strip() for t in (block_peers or []) if t and t.strip()]
        self.dup = max(1, int(dup))    # 每条消息重复发送次数
        self.reorder = bool(reorder)   # 是否随机乱序发送
        self.reorder_window = max(2, int(reorder_window))
        self.reorder_hold = float(reorder_hold)

        self._stop_event = threading.Event()
        self._workers = {}
        for peer in self.peers:
            self._workers[peer] = _PeerWorker(self, peer)

        # 统计指标
        self.enqueued = 0      # 进入队列的复制任务数（按操作计，不按副本计）
        self.sent_ok = 0       # 成功送达次数（按「副本 x 条消息」计，含重复投递）
        self.sent_fail = 0     # 发送失败次数
        self.blocked_dropped = 0  # 因网络分区被丢弃的消息数

    @property
    def enabled(self):
        return bool(self.peers)

    @property
    def backlog(self):
        """还没发出去的复制任务数（所有副本队列之和）。"""
        return sum(w.backlog for w in self._workers.values())

    def _is_blocked(self, peer):
        """--block 的值按「子串匹配」生效，例如 --block 9303 或 --block node-3。"""
        return any(tok in peer for tok in self.block_tokens)

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def start(self):
        if not self.enabled:
            return
        for worker in self._workers.values():
            worker.start()
        print("[%s] 异步复制已开启，副本节点: %s (发货延迟=%.1fs%s)"
              % (self.node_id, ", ".join(self.peers), self.delay,
                 self._fault_desc()))

    def _fault_desc(self):
        parts = []
        if self.block_tokens:
            parts.append("分区=%s" % ",".join(self.block_tokens))
        if self.dup > 1:
            parts.append("重复x%d" % self.dup)
        if self.reorder:
            parts.append("乱序")
        return ("；故障注入：" + "，".join(parts)) if parts else ""

    def stop(self):
        self._stop_event.set()

    # ------------------------------------------------------------------ #
    # 生产端：写入节点调用
    # ------------------------------------------------------------------ #
    def enqueue(self, op, key, value=None, ttl=None):
        """把一次写操作放进每个副本的队列，立即返回（不等待发送）。

        ttl 必须是「剩余存活秒数」，不是客户端传入的原始时长，否则副本会
        比源节点多活一个复制延迟；ttl=None 表示永不过期。
        """
        if not self.enabled:
            return
        item = {"op": op, "key": key, "value": value, "ttl": ttl,
                "origin": self.node_id, "created": time.monotonic()}
        for peer, worker in self._workers.items():
            if self._is_blocked(peer):
                # 网络分区：消息直接丢弃，不进入队列
                self.sent_fail += 1
                self.blocked_dropped += 1
                continue
            worker.queue.put(item)
        self.enqueued += 1

    def stats(self):
        return {
            "repl_enabled": self.enabled,
            "repl_peers": self.peers,
            "repl_delay": self.delay,
            "repl_enqueued": self.enqueued,
            "repl_backlog": self.backlog,
            "repl_sent_ok": self.sent_ok,
            "repl_sent_fail": self.sent_fail,
            "repl_dup": self.dup,
            "repl_reorder": self.reorder,
            "repl_blocked": self.block_tokens,
            "repl_blocked_dropped": self.blocked_dropped,
        }
