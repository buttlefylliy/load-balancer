#!/usr/bin/env python3
"""确定性的后端池与普通轮询调度。

只依赖 Python 标准库；不进行网络通信、不读取墙上时钟、不做持久化或重试，
相同输入必定产生相同结果。

公开接口：

* ``ConfigurationError``：后端配置非法时抛出。
* ``NoAvailableBackendError``：池中没有任何健康后端时抛出。
* ``BackendPool(configs)``：由后端配置列表建池，O(n) 时间、O(n) 空间。
* ``BackendPool.set_healthy(backend_id, healthy)``：按 id 原子更新健康标记。
* ``RoundRobinScheduler(pool)``：绑定后端池的轮询调度器。
* ``RoundRobinScheduler.select()``：返回下一个健康后端，最坏 O(n) 时间、
  额外空间 O(1)。
* ``WeightedRoundRobinScheduler(pool)``：按 weight 构造逻辑循环的加权轮询
  调度器，额外空间 O(n)，单次选择最坏 O(n) 时间、额外空间 O(1)。
* ``LeastConnectionsScheduler(pool)``：最少连接调度器，为每个后端维护
  从零开始的活动连接数，额外空间 O(n)，单次选择最坏 O(n) 时间、
  额外空间 O(1)，单次释放 O(1)。
* ``ConsistentHashScheduler(pool)``：一致性哈希调度器，``select(key)``
  按会话键对当前健康后端评分（SHA-256 摘要取无符号大端整数，分数最大者
  胜出），不维护游标、连接计数或按键增长的缓存，单次选择最坏 O(n) 时间、
  额外空间 O(1)。
* ``ConnectionStateError``：连接计数或连接生命周期状态非法时抛出。
* ``ConnectionTable(pool, idle_timeout, hard_timeout)``：以五元组标识
  连接并绑定后端池中后端的连接生命周期表。时间只由显式注入的 ``now``
  驱动；按五元组定位、记录活动和关闭平均 O(1)，``advance`` 与完整查询
  O(n)，空间 O(n)。
"""

import argparse
import bisect
import hashlib
import json
import sys

__all__ = [
    "ConfigurationError",
    "NoAvailableBackendError",
    "ConnectionStateError",
    "BackendPool",
    "RoundRobinScheduler",
    "WeightedRoundRobinScheduler",
    "LeastConnectionsScheduler",
    "ConsistentHashScheduler",
    "ConnectionTable",
]

_POLICY = "round_robin"
_WEIGHTED_POLICY = "weighted_round_robin"
_LEAST_POLICY = "least_connections"
_FIELDS = ("id", "address", "port", "healthy")
_OPTIONAL_FIELDS = ("weight",)
_KNOWN_FIELDS = _FIELDS + _OPTIONAL_FIELDS
_COMPLEXITY = (
    "复杂度：建池为 O(n) 时间与 O(n) 空间；"
    "单次选择最坏 O(n) 时间且额外空间 O(1)；"
    "加权调度器额外保存 O(n) 的前缀和；"
    "最少连接调度器额外保存 O(n) 的活动连接计数，单次释放 O(1)。"
)


class ConfigurationError(ValueError):
    """后端配置不合法（字段缺失、类型错误、端口越界或 id 重复等）。"""


class NoAvailableBackendError(Exception):
    """一次有界扫描内没有找到任何健康后端。"""


class ConnectionStateError(Exception):
    """连接状态非法：计数会低于零、连接非 active 或时间发生回退。"""


def _validate_configs(configs):
    """校验全部后端配置，返回与调用方对象完全隔离的新列表。

    只有全部配置合法时才返回；任何一项非法都抛出 ConfigurationError，
    不会产生部分结果。
    """
    if not isinstance(configs, list):
        raise ConfigurationError("backend configuration must be a list")

    prepared = []
    seen_ids = set()
    for position, entry in enumerate(configs):
        location = f"backend at position {position}"
        if not isinstance(entry, dict):
            raise ConfigurationError(f"{location}: expected an object")

        unknown = [key for key in entry if key not in _KNOWN_FIELDS]
        if unknown:
            raise ConfigurationError(
                f"{location}: unknown field {unknown[0]!r}"
            )
        missing = [key for key in _FIELDS if key not in entry]
        if missing:
            raise ConfigurationError(
                f"{location}: missing field {missing[0]!r}"
            )

        backend_id = entry["id"]
        if not isinstance(backend_id, str) or backend_id == "":
            raise ConfigurationError(
                f"{location}: id must be a non-empty string"
            )

        address = entry["address"]
        if not isinstance(address, str) or address == "":
            raise ConfigurationError(
                f"{location}: address must be a non-empty string"
            )

        port = entry["port"]
        # bool 是 int 的子类，健康后端的端口必须显式排除布尔值。
        if isinstance(port, bool) or not isinstance(port, int):
            raise ConfigurationError(
                f"{location}: port must be an integer between 1 and 65535"
            )
        if not 1 <= port <= 65535:
            raise ConfigurationError(
                f"{location}: port must be an integer between 1 and 65535"
            )

        healthy = entry["healthy"]
        if not isinstance(healthy, bool):
            raise ConfigurationError(
                f"{location}: healthy must be a boolean"
            )

        # weight 为可选字段，缺省按 1 处理；布尔值是 int 的子类，必须显式排除。
        weight = entry.get("weight", 1)
        if isinstance(weight, bool) or not isinstance(weight, int):
            raise ConfigurationError(
                f"{location}: weight must be an integer between 1 and 10000"
            )
        if not 1 <= weight <= 10000:
            raise ConfigurationError(
                f"{location}: weight must be an integer between 1 and 10000"
            )

        if backend_id in seen_ids:
            raise ConfigurationError(
                f"duplicate backend id: {backend_id!r}"
            )
        seen_ids.add(backend_id)

        prepared.append(
            {
                "id": backend_id,
                "address": address,
                "port": port,
                "healthy": healthy,
                "weight": weight,
            }
        )

    return prepared


class BackendPool:
    """静态后端池：保存声明顺序，并按 id 维护健康标记。"""

    def __init__(self, backends):
        # 先完成全部校验并取得防御性拷贝，再建立任何可见状态；
        # 校验失败时对象不会被部分构造。
        prepared = _validate_configs(backends)
        self._backends = prepared
        self._index = {backend["id"]: i for i, backend in enumerate(prepared)}

    def __len__(self):
        return len(self._backends)

    def set_healthy(self, backend_id, healthy):
        """按 id 原子更新健康标记。

        重复设置相同状态是幂等的，且不影响任何调度器游标。
        未知 id 抛出 KeyError。
        """
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        if not isinstance(healthy, bool):
            raise TypeError("healthy must be a boolean")
        index = self._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        self._backends[index]["healthy"] = healthy

    def is_healthy(self, backend_id):
        """返回某个 id 当前的健康标记；未知 id 抛出 KeyError。"""
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        index = self._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        return self._backends[index]["healthy"]


class RoundRobinScheduler:
    """普通轮询调度器。

    游标指向上一次成功选择的位置，初始位于首项之前（-1）。每次选择
    从游标后继位置开始做至多 n 次检查的有界扫描，跳过不健康后端；
    找不到健康后端时抛出 NoAvailableBackendError 且保持游标不变。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        self._cursor = -1

    def select(self):
        """选择并返回下一个健康后端。

        结果是键序固定为 id、address、port 的新字典；成功后游标推进到
        所选位置。没有健康后端时抛出 NoAvailableBackendError，
        池状态与游标均不改变。
        """
        backends = self._pool._backends
        size = len(backends)
        for offset in range(size):
            index = (self._cursor + 1 + offset) % size
            backend = backends[index]
            if backend["healthy"]:
                self._cursor = index
                return {
                    "id": backend["id"],
                    "address": backend["address"],
                    "port": backend["port"],
                }
        raise NoAvailableBackendError("no healthy backend available")


class WeightedRoundRobinScheduler:
    """加权轮询调度器。

    按声明顺序把每个后端映射到逻辑循环中连续 weight 个位置（不展开
    保存重复项，只保存每个后端的段首偏移，O(n) 空间）。游标记录上一次
    成功选择的逻辑位置，初始位于循环起点之前（-1）。每次选择从游标
    后继位置开始，借助段首偏移整段跳过不健康后端占有的位置，至多检查
    n 个后端：单次选择最坏 O(n) 时间、额外空间 O(1)。恢复健康标记的
    变更立即影响下一次选择；没有健康后端时抛出 NoAvailableBackendError
    且游标不变。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        offsets = []
        position = 0
        for backend in pool._backends:
            offsets.append(position)
            position += backend["weight"]
        self._offsets = offsets
        self._total = position
        self._cursor = -1

    def select(self):
        """选择并返回下一个健康后端。

        结果是键序固定为 id、address、port 的新字典；成功后游标推进到
        所选逻辑位置。没有健康后端时抛出 NoAvailableBackendError，
        池状态与游标均不改变。
        """
        backends = self._pool._backends
        size = len(backends)
        if self._total == 0:
            raise NoAvailableBackendError("no healthy backend available")
        position = (self._cursor + 1) % self._total
        # 二分定位 position 所属的后端段，之后逐个后端检查；
        # 不健康则直接跳到下一个后端的段首，至多检查 n 个后端。
        index = bisect.bisect_right(self._offsets, position) - 1
        for _ in range(size):
            backend = backends[index]
            if backend["healthy"]:
                self._cursor = position
                return {
                    "id": backend["id"],
                    "address": backend["address"],
                    "port": backend["port"],
                }
            index += 1
            if index == size:
                index = 0
                position = 0
            else:
                position = self._offsets[index]
        raise NoAvailableBackendError("no healthy backend available")


class LeastConnectionsScheduler:
    """最少连接调度器。

    按声明顺序为每个后端维护一个从零开始的活动连接数（O(n) 空间，
    不展开保存重复项）。每次选择只考察当前健康的后端，取活动连接数
    最小者，计数相同时取声明顺序最前者；选定后先把该后端的计数加一，
    再返回结果。健康标记的变更立即影响后续选择，但不清除或改写已有
    计数；不健康的后端仍允许释放既有连接，恢复健康后以保留的计数
    继续参与比较。没有健康后端时抛出 NoAvailableBackendError，
    所有计数与选择状态均不改变。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        self._counts = [0] * len(pool._backends)

    def select(self):
        """选择当前健康且活动连接数最小的后端。

        结果是键序固定为 id、address、port 的新字典；成功后被选后端的
        活动连接数先加一再返回。没有健康后端时抛出
        NoAvailableBackendError，所有计数均不改变。
        """
        backends = self._pool._backends
        counts = self._counts
        chosen = -1
        for index, backend in enumerate(backends):
            if backend["healthy"] and (
                chosen < 0 or counts[index] < counts[chosen]
            ):
                chosen = index
        if chosen < 0:
            raise NoAvailableBackendError("no healthy backend available")
        counts[chosen] += 1
        backend = backends[chosen]
        return {
            "id": backend["id"],
            "address": backend["address"],
            "port": backend["port"],
        }

    def release_connection(self, backend_id):
        """按 id 释放一个活动连接，对应计数减一。

        不健康的后端同样允许释放。id 不是字符串时抛出 TypeError，
        id 不存在时抛出 KeyError；计数已为零时抛出
        ConnectionStateError，且任何计数都不改变。
        """
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        index = self._pool._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        if self._counts[index] == 0:
            raise ConnectionStateError(
                f"backend {backend_id!r} has no active connection to release"
            )
        self._counts[index] -= 1

    def active_connections(self):
        """返回全部后端的活动连接数。

        结果是与内部状态隔离的新字典，键按声明顺序排列，值为非负整数。
        """
        return {
            backend["id"]: self._counts[index]
            for index, backend in enumerate(self._pool._backends)
        }


class ConsistentHashScheduler:
    """一致性哈希调度器。

    不维护轮询游标、连接计数或任何按键增长的缓存：每次 select 都只依据
    池当前的健康标记重新评分。评分输入是由会话键与后端 id 组成的 JSON
    数组（``[key, backend_id]``），以 ensure_ascii=False 和紧凑分隔符
    序列化为 UTF-8 后计算 SHA-256，摘要按无符号大端整数解释；分数最大
    的后端胜出，摘要相同时按配置声明顺序取最前者。address、port、weight
    与声明位置都不参与评分。只比较当前健康后端，因此某个后端转为不健康
    时，原本未选择它的键选择不变，原本选择它的键在其余健康后端中重新
    映射；恢复健康后按同一评分规则确定性地回到原选择。健康变化在下一次
    select 立即生效，选择过程不改写池或本调度器的任何状态。没有健康后端
    时抛出 NoAvailableBackendError。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool

    def select(self, key):
        """按会话键选择一个当前健康的后端。

        结果是键序固定为 id、address、port 的新字典；同一键在后端集合
        与健康状态不变时，无论调用次数以及与其他键的调用顺序如何，都
        返回逐字段相同的结果。key 不是字符串时抛出 TypeError，为空
        字符串时抛出 ValueError，两种失败均不产生状态变化；没有健康
        后端时抛出 NoAvailableBackendError，池与调度器状态均不改变。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")
        backends = self._pool._backends
        chosen = -1
        best_score = -1
        for index, backend in enumerate(backends):
            if not backend["healthy"]:
                continue
            # 仅键与后端 id 参与评分；ensure_ascii=False 与紧凑分隔符
            # 固定序列化形态，摘要按无符号大端整数比较。
            payload = json.dumps(
                [key, backend["id"]],
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            score = int.from_bytes(
                hashlib.sha256(payload).digest(), "big", signed=False
            )
            # 只在严格更大时替换，摘要相同保留声明顺序最前者。
            if score > best_score:
                best_score = score
                chosen = index
        if chosen < 0:
            raise NoAvailableBackendError("no healthy backend available")
        backend = backends[chosen]
        return {
            "id": backend["id"],
            "address": backend["address"],
            "port": backend["port"],
        }


_FLOW_FIELDS = ("src_addr", "src_port", "dst_addr", "dst_port", "protocol")
_PROTOCOLS = ("tcp", "udp")


def _validate_flow(flow):
    """校验五元组并返回规范化后的元组键。

    flow 必须是恰好包含 src_addr、src_port、dst_addr、dst_port、protocol
    五个字段的字典；类型错误抛出 TypeError，值错误（含布尔端口）抛出
    ValueError，两种失败都不产生任何状态变化。
    """
    if not isinstance(flow, dict):
        raise TypeError("flow must be a dict of five-tuple fields")
    for key in flow:
        if key not in _FLOW_FIELDS:
            raise ValueError(f"flow: unknown field {key!r}")
    for key in _FLOW_FIELDS:
        if key not in flow:
            raise ValueError(f"flow: missing field {key!r}")

    for name in ("src_addr", "dst_addr"):
        address = flow[name]
        if not isinstance(address, str):
            raise TypeError(f"flow {name} must be a string")
        if address == "":
            raise ValueError(f"flow {name} must be a non-empty string")

    for name in ("src_port", "dst_port"):
        port = flow[name]
        # bool 是 int 的子类，端口必须显式排除布尔值。
        if isinstance(port, bool):
            raise ValueError(
                f"flow {name} must be an integer between 1 and 65535"
            )
        if not isinstance(port, int):
            raise TypeError(
                f"flow {name} must be an integer between 1 and 65535"
            )
        if not 1 <= port <= 65535:
            raise ValueError(
                f"flow {name} must be an integer between 1 and 65535"
            )

    protocol = flow["protocol"]
    if not isinstance(protocol, str):
        raise TypeError("flow protocol must be a string")
    if protocol not in _PROTOCOLS:
        raise ValueError("flow protocol must be 'tcp' or 'udp'")

    return (
        flow["src_addr"],
        flow["src_port"],
        flow["dst_addr"],
        flow["dst_port"],
        protocol,
    )


class ConnectionTable:
    """连接生命周期表：以五元组标识连接并绑定后端池中的后端。

    时间只由显式注入的 now 驱动（非布尔的非负整数，且不得回退，回退抛出
    ConnectionStateError），不读取墙上时钟。idle_timeout 与 hard_timeout
    必须为正整数，否则抛出 ConfigurationError。

    open_connection 创建 active 记录，后端须存在（否则 KeyError）且健康
    （否则 ConnectionStateError）；同一五元组已 active 时，相同后端返回
    原记录，不同后端抛出 ConnectionStateError。record_activity 只更新
    active 连接的最后活动时间，close_connection 把 active 连接置为
    closed 且重复关闭幂等；对非 active 连接记录活动抛出
    ConnectionStateError。每个操作先按 now 处理目标连接的到期再执行：
    空闲或硬期限不晚于 now 时 active 连接转为 expired（同时命中时
    end_reason 为 hard_timeout），截止时刻的活动不能挽救连接。单连接
    操作只惰性处理目标五元组的到期（平均 O(1)）；advance 做全量扫描
    （O(n)）并按建立顺序返回本次到期的记录。查询按建立顺序返回与内部
    状态隔离的副本，记录键序固定为 flow、backend_id、state、
    created_at、last_activity_at、ended_at、end_reason，flow 保持
    五元组顺序。空间 O(n)。
    """

    def __init__(self, pool, idle_timeout, hard_timeout):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        for name, value in (
            ("idle_timeout", idle_timeout),
            ("hard_timeout", hard_timeout),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise ConfigurationError(
                    f"{name} must be a positive integer"
                )
        self._pool = pool
        self._idle_timeout = idle_timeout
        self._hard_timeout = hard_timeout
        # 五元组键 -> 记录；字典插入顺序即建立顺序，重新建立时先删后插。
        self._records = {}
        self._last_now = None

    def _checked_now(self, now):
        """校验 now 的类型、值与单调性；失败时不产生任何状态变化。"""
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be a non-negative integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        if self._last_now is not None and now < self._last_now:
            raise ConnectionStateError("now must not move backwards")
        return now

    def _expire_if_due(self, record, now):
        """若 active 记录在 now 前已到期则置为 expired，返回是否本次到期。"""
        if record["state"] != "active":
            return False
        if record["created_at"] + self._hard_timeout <= now:
            reason = "hard_timeout"
        elif record["last_activity_at"] + self._idle_timeout <= now:
            reason = "idle_timeout"
        else:
            return False
        record["state"] = "expired"
        record["ended_at"] = now
        record["end_reason"] = reason
        return True

    @staticmethod
    def _copy(record):
        """返回与内部状态隔离、键序固定的记录副本。"""
        return {
            "flow": dict(record["flow"]),
            "backend_id": record["backend_id"],
            "state": record["state"],
            "created_at": record["created_at"],
            "last_activity_at": record["last_activity_at"],
            "ended_at": record["ended_at"],
            "end_reason": record["end_reason"],
        }

    def open_connection(self, flow, backend_id, now):
        """按五元组建立 active 连接并返回记录副本。

        后端不存在抛出 KeyError，不健康抛出 ConnectionStateError；同一
        五元组已 active 时，相同后端返回原记录，不同后端抛出
        ConnectionStateError；已 closed 或 expired 的五元组重新建立新
        记录并排到建立顺序末尾。参数校验失败不产生任何状态变化。
        """
        now = self._checked_now(now)
        key = _validate_flow(flow)
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        index = self._pool._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        if not self._pool._backends[index]["healthy"]:
            raise ConnectionStateError(
                f"backend {backend_id!r} is not healthy"
            )
        self._last_now = now
        record = self._records.get(key)
        if record is not None:
            self._expire_if_due(record, now)
            if record["state"] == "active":
                if record["backend_id"] == backend_id:
                    return self._copy(record)
                raise ConnectionStateError(
                    "flow already has an active connection on backend "
                    f"{record['backend_id']!r}"
                )
            # 已结束的记录被新记录替换，并移到建立顺序末尾。
            del self._records[key]
        record = {
            "flow": {
                "src_addr": key[0],
                "src_port": key[1],
                "dst_addr": key[2],
                "dst_port": key[3],
                "protocol": key[4],
            },
            "backend_id": backend_id,
            "state": "active",
            "created_at": now,
            "last_activity_at": now,
            "ended_at": None,
            "end_reason": None,
        }
        self._records[key] = record
        return self._copy(record)

    def record_activity(self, flow, now):
        """把 active 连接的最后活动时间更新为 now 并返回记录副本。

        连接不存在或已非 active 时抛出 ConnectionStateError；截止时刻
        的活动不能挽救连接（先按 now 处理到期再执行）。
        """
        now = self._checked_now(now)
        key = _validate_flow(flow)
        self._last_now = now
        record = self._records.get(key)
        if record is None:
            raise ConnectionStateError("no connection for this flow")
        self._expire_if_due(record, now)
        if record["state"] != "active":
            raise ConnectionStateError(
                f"connection is {record['state']}, not active"
            )
        record["last_activity_at"] = now
        return self._copy(record)

    def close_connection(self, flow, now):
        """把 active 连接置为 closed 并返回记录副本。

        重复关闭幂等，返回既有 closed 记录且不改变任何状态；连接不存在
        或已 expired 时抛出 ConnectionStateError。
        """
        now = self._checked_now(now)
        key = _validate_flow(flow)
        self._last_now = now
        record = self._records.get(key)
        if record is None:
            raise ConnectionStateError("no connection for this flow")
        self._expire_if_due(record, now)
        if record["state"] == "closed":
            return self._copy(record)
        if record["state"] != "active":
            raise ConnectionStateError(
                f"connection is {record['state']}, not active"
            )
        record["state"] = "closed"
        record["ended_at"] = now
        record["end_reason"] = "closed"
        return self._copy(record)

    def advance(self, now):
        """按 now 处理全部连接的到期，返回本次到期的记录副本列表。

        结果按建立顺序排列；已被此前操作惰性到期的记录不会重复出现。
        """
        now = self._checked_now(now)
        self._last_now = now
        expired = []
        for record in self._records.values():
            if self._expire_if_due(record, now):
                expired.append(self._copy(record))
        return expired

    def connections(self):
        """返回全部记录的隔离副本列表，按建立顺序排列。"""
        return [self._copy(record) for record in self._records.values()]

    def active_connections(self):
        """返回每个后端的 active 连接数，键按后端声明顺序排列。"""
        counts = {backend["id"]: 0 for backend in self._pool._backends}
        for record in self._records.values():
            if record["state"] == "active":
                counts[record["backend_id"]] += 1
        return counts


def _positive_int(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            "count must be a positive integer"
        ) from None
    if number <= 0:
        raise argparse.ArgumentTypeError("count must be a positive integer")
    return number


def _emit_error(error_type, message, exit_code):
    """失败时只在 stderr 写键序固定为 type、message 的紧凑 JSON。"""
    payload = json.dumps(
        {"type": error_type, "message": str(message)},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    sys.stderr.write(payload + "\n")
    raise SystemExit(exit_code)


class _JSONArgumentParser(argparse.ArgumentParser):
    """参数错误也统一输出 type/message JSON 信封并以退出码 2 结束。"""

    def error(self, message):
        _emit_error("ArgumentError", message, 2)


def _build_parser():
    parser = _JSONArgumentParser(
        prog="load_balancer.py",
        description="确定性后端池与普通轮询调度（仅依赖 Python 标准库）。",
        epilog=_COMPLEXITY,
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command")

    schedule = subparsers.add_parser(
        "schedule",
        help="按轮询策略连续选择后端，输出一个紧凑 JSON 对象",
        description=(
            "从 UTF-8 JSON 配置文件建立后端池，执行 N 次轮询选择，"
            "输出键序固定为 policy、selections 的紧凑 JSON 对象。"
        ),
        epilog=_COMPLEXITY,
    )
    schedule.add_argument(
        "--config",
        required=True,
        metavar="PATH",
        help="后端配置文件路径（UTF-8 JSON，内容为后端对象数组）",
    )
    schedule.add_argument(
        "--count",
        required=True,
        type=_positive_int,
        metavar="N",
        help="连续选择的次数，必须为正整数",
    )
    schedule.add_argument(
        "--policy",
        choices=(_POLICY, _WEIGHTED_POLICY, _LEAST_POLICY),
        default=_POLICY,
        help="调度策略，省略时为 round_robin",
    )
    schedule.set_defaults(handler=_handle_schedule)
    return parser


def _read_config(path):
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except OSError:
        _emit_error("FileReadError", f"cannot read config file: {path}", 2)


def _handle_schedule(args):
    raw = _read_config(args.config)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        _emit_error("FileReadError", "config file is not valid UTF-8", 2)
    try:
        configs = json.loads(text)
    except json.JSONDecodeError as exc:
        _emit_error(
            "JSONDecodeError",
            f"config file is not valid JSON: line {exc.lineno} "
            f"column {exc.colno}",
            2,
        )

    try:
        pool = BackendPool(configs)
    except ConfigurationError as exc:
        _emit_error("ConfigurationError", exc, 3)

    if args.policy == _WEIGHTED_POLICY:
        scheduler = WeightedRoundRobinScheduler(pool)
    elif args.policy == _LEAST_POLICY:
        scheduler = LeastConnectionsScheduler(pool)
    else:
        scheduler = RoundRobinScheduler(pool)
    selections = []
    try:
        for _ in range(args.count):
            selections.append(scheduler.select())
    except NoAvailableBackendError as exc:
        _emit_error("NoAvailableBackendError", exc, 4)

    result = {"policy": args.policy, "selections": selections}
    sys.stdout.write(
        json.dumps(result, separators=(",", ":"), ensure_ascii=False) + "\n"
    )


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "handler", None) is None:
        parser.error("a subcommand is required")
    args.handler(args)


if __name__ == "__main__":
    main()
