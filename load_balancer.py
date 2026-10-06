#!/usr/bin/env python3
"""确定性的后端池与普通轮询调度。

只依赖 Python 标准库；不进行网络通信、不读取墙上时钟、不做持久化或重试，
相同输入必定产生相同结果。

公开接口：

* ``ConfigurationError``：后端配置非法时抛出。
* ``NoAvailableBackendError``：池中没有任何健康后端时抛出。
* ``BackendOverloadedError``：存在健康后端但全部达到自身并发上限时抛出。
* ``BackendPool(configs)``：由后端配置列表建池，O(n) 时间、O(n) 空间；
  可选的 max_connections 为每后端声明并发上限，省略表示不限量。
* ``BackendPool.set_healthy(backend_id, healthy)``：按 id 原子更新健康标记。
* ``RoundRobinScheduler(pool)``：绑定后端池的轮询调度器。
* ``RoundRobinScheduler.select()``：返回下一个健康后端，最坏 O(n) 时间、
  额外空间 O(1)。
* ``WeightedRoundRobinScheduler(pool)``：按 weight 构造逻辑循环的加权轮询
  调度器，额外空间 O(n)，单次选择最坏 O(n) 时间、额外空间 O(1)。
* ``LeastConnectionsScheduler(pool)``：最少连接调度器，为每个后端维护
  从零开始的活动连接数，只在健康且未达 max_connections 上限的后端中
  取计数最小者，额外空间 O(n)，单次选择最坏 O(n) 时间、额外空间
  O(1)，单次释放 O(1)；``explain()`` 返回键序固定的可重放解释
  （全部候选与计数、selected、outcome、reason），与 select 同规则但
  不可调度时不抛错，单次查询 O(n) 时间、除返回结果外额外空间 O(1)。
* ``ConsistentHashScheduler(pool)``：一致性哈希调度器，``select(key)``
  按会话键对当前健康后端评分（SHA-256 摘要取无符号大端整数，分数最大者
  胜出），不维护游标、连接计数或按键增长的缓存，单次选择最坏 O(n) 时间、
  额外空间 O(1)；``explain(key)`` 返回该次选择键序固定的可重放解释
  （全部候选与分数、selected、outcome、reason），与 select 同校验但
  无健康后端时不抛错，单次查询 O(n) 时间、除返回结果外额外空间 O(1)。
* ``StickySessionScheduler(pool, max_sessions)``：有界会话绑定调度器，
  以 OrderedDict 保存至多 max_sessions 个 key→backend_id 绑定并按成功
  选择维护最近使用顺序：命中（绑定后端仍健康）平均 O(1)，首次选择与
  故障转移 O(n)（一致性哈希评分），超限时淘汰最久未成功使用的 key；
  ``bindings()`` 按最久到最近使用顺序返回 O(max_sessions) 的隔离副本，
  ``explain(key)`` 返回键序固定的可重放解释且不改变状态。
* ``ConnectionTable(pool, idle_timeout, hard_timeout)``：以五元组标识连接、
  绑定后端池的连接生命周期表。时间只由显式 ``now`` 驱动；按五元组定位、
  记录活动与关闭平均 O(1) 时间，``advance`` 与完整查询 O(n) 时间，
  空间 O(n)。
* ``ConnectionStateError``：连接状态非法时抛出（释放使活动连接数低于零、
  对非 active 连接记录活动、五元组冲突、now 回退等）。
* ``HealthCheckTracker(pool, failure_threshold, recovery_threshold)``：
  只消费显式检查结果的健康跟踪器，按连续失败/成功次数在共享池中自动
  摘除与回切后端；不发起网络请求、不读取墙上时钟。单次记录平均 O(1)
  时间，完整查询 O(n) 时间，空间 O(n)。
"""

import argparse
import bisect
import hashlib
import heapq
import json
import sys
from collections import OrderedDict

__all__ = [
    "ConfigurationError",
    "NoAvailableBackendError",
    "BackendOverloadedError",
    "ConnectionStateError",
    "BackendPool",
    "RoundRobinScheduler",
    "WeightedRoundRobinScheduler",
    "LeastConnectionsScheduler",
    "ConsistentHashScheduler",
    "StickySessionScheduler",
    "ConnectionTable",
    "HealthCheckTracker",
]

_POLICY = "round_robin"
_WEIGHTED_POLICY = "weighted_round_robin"
_LEAST_POLICY = "least_connections"
_FIELDS = ("id", "address", "port", "healthy")
_OPTIONAL_FIELDS = ("weight", "max_connections")
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


class BackendOverloadedError(Exception):
    """存在健康后端，但它们的活动连接数都已达到自身上限。"""


class ConnectionStateError(Exception):
    """连接状态非法。

    释放会使活动连接数低于零，或连接生命周期操作与当前状态冲突
    （对非 active 连接记录活动、同一五元组绑定到不同后端、
    目标后端不健康、now 回退等）。
    """


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

        # max_connections 为可选字段，缺省（None）表示不限量；
        # 布尔值是 int 的子类，必须显式排除。
        has_capacity = "max_connections" in entry
        max_connections = entry["max_connections"] if has_capacity else None
        if has_capacity:
            if isinstance(max_connections, bool) or not isinstance(
                max_connections, int
            ):
                raise ConfigurationError(
                    f"{location}: max_connections must be an integer "
                    "between 1 and 1000000"
                )
            if not 1 <= max_connections <= 1000000:
                raise ConfigurationError(
                    f"{location}: max_connections must be an integer "
                    "between 1 and 1000000"
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
                "max_connections": max_connections,
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
    不展开保存重复项）。每次选择只考察当前健康且活动连接数低于自身
    max_connections 上限的后端（省略上限的后端始终通过容量筛选），
    取活动连接数最小者，计数相同时取声明顺序最前者；选定后先把该
    后端的计数加一，再返回结果。健康标记的变更立即影响后续选择，但
    不清除或改写已有计数；不健康的后端仍允许释放既有连接，恢复健康
    后以保留的计数与上限重新参与比较。没有健康后端时抛出
    NoAvailableBackendError；存在健康后端但全部达到上限时抛出
    BackendOverloadedError，两种失败均不改变任何计数。

    ``explain()`` 复用同一筛选与比较规则，但不抛出
    NoAvailableBackendError 或 BackendOverloadedError：返回包含全部
    候选及其计数与资格、选中结果与失败原因的可重放解释字典，不增加
    任何计数，也不修改池或本调度器的任何状态。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        self._counts = [0] * len(pool._backends)

    def select(self):
        """选择当前健康且未满容量、活动连接数最小的后端。

        结果是键序固定为 id、address、port 的新字典；成功后被选后端的
        活动连接数先加一再返回。没有健康后端时抛出
        NoAvailableBackendError；存在健康后端但计数全部达到各自
        max_connections 上限时抛出 BackendOverloadedError。两种失败
        都不改变任何计数。
        """
        backends = self._pool._backends
        counts = self._counts
        chosen = -1
        saw_healthy = False
        for index, backend in enumerate(backends):
            if not backend["healthy"]:
                continue
            saw_healthy = True
            limit = backend["max_connections"]
            if limit is not None and counts[index] >= limit:
                continue
            if chosen < 0 or counts[index] < counts[chosen]:
                chosen = index
        if chosen < 0:
            if not saw_healthy:
                raise NoAvailableBackendError("no healthy backend available")
            raise BackendOverloadedError(
                "all healthy backends are at capacity"
            )
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

    def explain(self):
        """返回一次最少连接选择的可重放解释，不改变任何状态。

        结果是键序固定为 policy、candidates、selected、outcome、reason
        的新字典，与内部状态完全隔离：policy 固定为 least_connections；
        candidates 按后端声明顺序排列，每项键序固定为 backend_id、
        healthy、active_connections、max_connections、eligible，未配置
        容量上限的后端 max_connections 为 None，eligible 仅在后端健康
        且活动连接数低于自身上限时为 True。选择规则与紧随其后的 select
        完全相同：在 eligible 候选中取活动连接数最小者，计数相同时取
        声明顺序最前者。可以调度时 selected 是键序固定为 id、address、
        port 的新字典，outcome 为 selected、reason 为
        least_connections；没有健康后端时 selected 为 None、outcome
        为 failed、reason 为 no_healthy_backend；存在健康后端但全部
        达到上限时 selected 为 None、outcome 为 failed、reason 为
        all_healthy_backends_at_capacity，两种失败都不抛出调度异常。
        单次查询 O(n) 时间，除返回的 O(n) 解释结果外只使用 O(1) 额外
        空间；解释不增加任何计数，重复调用在状态不变时逐字段一致。
        """
        backends = self._pool._backends
        counts = self._counts
        candidates = []
        chosen = -1
        saw_healthy = False
        for index, backend in enumerate(backends):
            healthy = backend["healthy"]
            limit = backend["max_connections"]
            eligible = healthy and (
                limit is None or counts[index] < limit
            )
            candidates.append(
                {
                    "backend_id": backend["id"],
                    "healthy": healthy,
                    "active_connections": counts[index],
                    "max_connections": limit,
                    "eligible": eligible,
                }
            )
            if not healthy:
                continue
            saw_healthy = True
            if not eligible:
                continue
            # 只在严格更小时替换，计数相同保留声明顺序最前者。
            if chosen < 0 or counts[index] < counts[chosen]:
                chosen = index
        if chosen < 0:
            selected = None
            outcome = "failed"
            reason = (
                "all_healthy_backends_at_capacity"
                if saw_healthy
                else "no_healthy_backend"
            )
        else:
            backend = backends[chosen]
            selected = {
                "id": backend["id"],
                "address": backend["address"],
                "port": backend["port"],
            }
            outcome = "selected"
            reason = "least_connections"
        return {
            "policy": _LEAST_POLICY,
            "candidates": candidates,
            "selected": selected,
            "outcome": outcome,
            "reason": reason,
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

    ``explain(key)`` 复用同一校验与评分规则，但不抛出
    NoAvailableBackendError：返回包含全部候选及其分数、选中结果与失败
    原因的可重放解释字典，同样不修改任何状态，也不维护按键累计的状态。
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

    def explain(self, key):
        """返回一次一致性哈希选择的可重放解释，不改变任何状态。

        与 select 采用相同的 key 类型与值校验：非字符串抛出 TypeError，
        空字符串抛出 ValueError，校验失败不改变池或调度器状态。结果是
        键序固定为 policy、key、candidates、selected、outcome、reason 的
        新字典：policy 固定为 consistent_hash；candidates 按后端声明顺序
        排列，每项键序固定为 backend_id、healthy、score，健康后端的 score
        为 SHA-256 摘要保留前导零的 64 位小写十六进制字符串，不健康后端
        不参与比较且 score 为 None。存在健康后端时 selected 是键序固定为
        id、address、port 的新字典（池状态不变时与 select 对同一 key 的
        返回逐字段一致），outcome 为 selected，reason 为 highest_score，
        最高分相同时取声明顺序最前的健康后端；没有健康后端时不抛出
        NoAvailableBackendError，selected 为 None、outcome 为 failed、
        reason 为 no_healthy_backend。单次查询 O(n) 时间，除返回的 O(n)
        解释结果外只使用 O(1) 额外空间。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")
        backends = self._pool._backends
        candidates = []
        chosen = -1
        best_score = -1
        for index, backend in enumerate(backends):
            healthy = backend["healthy"]
            if not healthy:
                candidates.append(
                    {
                        "backend_id": backend["id"],
                        "healthy": healthy,
                        "score": None,
                    }
                )
                continue
            # 评分与 select 完全一致：仅键与后端 id 参与评分，摘要按
            # 无符号大端整数比较；十六进制形态保留前导零共 64 个字符。
            payload = json.dumps(
                [key, backend["id"]],
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            digest = hashlib.sha256(payload).digest()
            score = int.from_bytes(digest, "big", signed=False)
            candidates.append(
                {
                    "backend_id": backend["id"],
                    "healthy": healthy,
                    "score": digest.hex(),
                }
            )
            # 只在严格更大时替换，摘要相同保留声明顺序最前者。
            if score > best_score:
                best_score = score
                chosen = index
        if chosen < 0:
            selected = None
            outcome = "failed"
            reason = "no_healthy_backend"
        else:
            backend = backends[chosen]
            selected = {
                "id": backend["id"],
                "address": backend["address"],
                "port": backend["port"],
            }
            outcome = "selected"
            reason = "highest_score"
        return {
            "policy": "consistent_hash",
            "key": key,
            "candidates": candidates,
            "selected": selected,
            "outcome": outcome,
            "reason": reason,
        }


class StickySessionScheduler:
    """有界会话绑定调度器。

    以 OrderedDict 保存至多 max_sessions 个 key→backend_id 绑定，迭代
    顺序即成功选择顺序（首项最久未成功使用，末项最近使用）。首次见到
    的 key 委托现有 ConsistentHashScheduler 按当前健康后端评分并绑定；
    之后只要绑定后端仍健康就始终返回它（命中 O(1)），健康集合增加或
    其他后端变化都不迁移。绑定后端不健康时，再次按一致性哈希规则在
    当前健康后端中重新选择并原子替换绑定，原后端恢复后也不主动迁回。

    每次成功选择都把 key 置为最近使用；新绑定会使绑定数超过
    max_sessions 时，先淘汰最久未成功使用的 key 再写入，相同事件序列
    下淘汰顺序确定。没有健康后端时抛出 NoAvailableBackendError，
    绑定与使用顺序均不改变；任何校验或选择失败都不产生部分修改。
    """

    def __init__(self, pool, max_sessions):
        # 先完成 pool 与 max_sessions 的全部校验，再建立任何实例状态；
        # 校验失败时对象不会被部分构造。
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        # bool 是 int 的子类，上限必须显式排除布尔值。
        if (
            isinstance(max_sessions, bool)
            or not isinstance(max_sessions, int)
            or max_sessions <= 0
        ):
            raise ConfigurationError(
                "max_sessions must be a positive integer"
            )
        self._pool = pool
        self._max_sessions = max_sessions
        self._hash = ConsistentHashScheduler(pool)
        # 首项为最久未成功使用的绑定，末项为最近使用的绑定。
        self._bindings = OrderedDict()

    def select(self, key):
        """返回 key 绑定的健康后端，必要时按一致性哈希首次选择或迁移。

        key 校验与 ConsistentHashScheduler.select 完全相同：非字符串
        抛出 TypeError，空字符串抛出 ValueError，失败不改变状态。
        绑定后端仍健康时直接命中并把 key 置为最近使用，平均 O(1)；
        未绑定或绑定后端不健康时按一致性哈希在当前健康后端中选择
        （O(n)），没有健康后端抛出 NoAvailableBackendError 且绑定与
        使用顺序都不改变。成功后新绑定超过 max_sessions 时淘汰最久未
        成功使用的 key。结果是键序固定为 id、address、port 的新字典。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")

        bindings = self._bindings
        bound_id = bindings.get(key)
        if bound_id is not None:
            bound = self._pool._backends[self._pool._index[bound_id]]
            if bound["healthy"]:
                # 命中：只刷新最近使用顺序，不重新评分，O(1)。
                bindings.move_to_end(key)
                return {
                    "id": bound["id"],
                    "address": bound["address"],
                    "port": bound["port"],
                }

        # 首次选择或故障转移：先取得一致性哈希决策（可能抛出
        # NoAvailableBackendError），在此之前不改动任何绑定。
        selected = self._hash.select(key)
        selected_id = selected["id"]
        if bound_id is None:
            # 新绑定：超限时先淘汰最久未成功使用的 key（首项）。
            if len(bindings) >= self._max_sessions:
                bindings.popitem(last=False)
            bindings[key] = selected_id
        else:
            # 故障转移：原子替换绑定并刷新为最近使用，绑定数不变。
            bindings[key] = selected_id
            bindings.move_to_end(key)
        return selected

    def bindings(self):
        """按最久到最近成功使用顺序返回全部绑定的隔离副本。

        每项是键序固定为 key、backend_id 的新字典；列表与字典都与
        内部状态隔离，O(max_sessions) 时间与空间。
        """
        return [
            {"key": key, "backend_id": backend_id}
            for key, backend_id in self._bindings.items()
        ]

    def explain(self, key):
        """返回一次会话绑定选择的可重放解释，不改变任何状态。

        与 select 采用相同的 key 校验：非字符串抛出 TypeError，空
        字符串抛出 ValueError，失败不改变池或调度器状态。结果是键序
        固定为 policy、key、previous_backend_id、selected、outcome、
        reason、evicted_key、base_decision 的新字典：policy 固定为
        sticky_session；previous_backend_id 为该 key 当前绑定的后端
        id，未绑定时为 None；命中时 selected 为绑定后端结果、reason
        为 sticky_hit、base_decision 为 None；未绑定时 reason 为
        new_binding，绑定后端不健康时 reason 为 unhealthy_failover，
        两种重新选择的 base_decision 为当前一致性哈希 explain 结果；
        evicted_key 给出紧随其后的成功 select 将淘汰的 key（命中、
        替换绑定或未超限时为 None）。没有健康后端时不抛出
        NoAvailableBackendError，selected 为 None、outcome 为 failed、
        reason 为 no_healthy_backend。池状态不变时，解释与紧随其后的
        select 逐字段一致。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")

        bindings = self._bindings
        bound_id = bindings.get(key)
        if bound_id is not None:
            bound = self._pool._backends[self._pool._index[bound_id]]
            if bound["healthy"]:
                return {
                    "policy": "sticky_session",
                    "key": key,
                    "previous_backend_id": bound_id,
                    "selected": {
                        "id": bound["id"],
                        "address": bound["address"],
                        "port": bound["port"],
                    },
                    "outcome": "selected",
                    "reason": "sticky_hit",
                    "evicted_key": None,
                    "base_decision": None,
                }

        # 未绑定或绑定后端不健康：复用一致性哈希的可重放解释。
        base_decision = self._hash.explain(key)
        if base_decision["selected"] is None:
            return {
                "policy": "sticky_session",
                "key": key,
                "previous_backend_id": bound_id,
                "selected": None,
                "outcome": "failed",
                "reason": "no_healthy_backend",
                "evicted_key": None,
                "base_decision": base_decision,
            }

        reason = (
            "unhealthy_failover" if bound_id is not None else "new_binding"
        )
        # 只有新绑定且绑定数已达上限时，紧随其后的 select 才会淘汰
        # 当前首项（最久未成功使用的 key）；替换绑定不改变绑定数。
        if bound_id is None and len(bindings) >= self._max_sessions:
            evicted_key = next(iter(bindings))
        else:
            evicted_key = None
        return {
            "policy": "sticky_session",
            "key": key,
            "previous_backend_id": bound_id,
            "selected": base_decision["selected"],
            "outcome": "selected",
            "reason": reason,
            "evicted_key": evicted_key,
            "base_decision": base_decision,
        }


_FLOW_FIELDS = ("src_address", "src_port", "dst_address", "dst_port", "protocol")
_PROTOCOLS = ("tcp", "udp")
_RECORD_FIELDS = (
    "flow",
    "backend_id",
    "state",
    "created_at",
    "last_activity_at",
    "ended_at",
    "end_reason",
)


def _validate_flow(flow):
    """校验五元组并返回键序固定的规范化新字典。

    类型错误抛出 TypeError，值错误抛出 ValueError；不会修改调用方对象。
    """
    if not isinstance(flow, dict):
        raise TypeError("flow must be a dict")
    unknown = [key for key in flow if key not in _FLOW_FIELDS]
    if unknown:
        raise ValueError(f"flow: unknown field {unknown[0]!r}")
    missing = [key for key in _FLOW_FIELDS if key not in flow]
    if missing:
        raise ValueError(f"flow: missing field {missing[0]!r}")

    for field in ("src_address", "dst_address"):
        address = flow[field]
        if not isinstance(address, str):
            raise TypeError(f"flow: {field} must be a string")
        if address == "":
            raise ValueError(f"flow: {field} must be a non-empty string")

    for field in ("src_port", "dst_port"):
        port = flow[field]
        # bool 是 int 的子类，端口必须显式排除布尔值。
        if isinstance(port, bool) or not isinstance(port, int):
            raise TypeError(
                f"flow: {field} must be an integer between 1 and 65535"
            )
        if not 1 <= port <= 65535:
            raise ValueError(
                f"flow: {field} must be an integer between 1 and 65535"
            )

    protocol = flow["protocol"]
    if not isinstance(protocol, str):
        raise TypeError("flow: protocol must be a string")
    if protocol not in _PROTOCOLS:
        raise ValueError("flow: protocol must be 'tcp' or 'udp'")

    return {key: flow[key] for key in _FLOW_FIELDS}


def _flow_key(flow):
    return (
        flow["src_address"],
        flow["src_port"],
        flow["dst_address"],
        flow["dst_port"],
        flow["protocol"],
    )


def _copy_record(record):
    """按固定键序生成与内部状态隔离的记录副本。"""
    return {
        "flow": dict(record["flow"]),
        "backend_id": record["backend_id"],
        "state": record["state"],
        "created_at": record["created_at"],
        "last_activity_at": record["last_activity_at"],
        "ended_at": record["ended_at"],
        "end_reason": record["end_reason"],
    }


class ConnectionTable:
    """以五元组标识连接、绑定 BackendPool 的连接生命周期表。

    五元组为 src_address、src_port、dst_address、dst_port、protocol
    （仅小写 "tcp"/"udp"）。时间只由调用方显式传入的 ``now`` 驱动，
    不读墙上时钟；``now`` 必须是非布尔的非负整数且不得回退。

    每条连接记录依次处于 active、closed 或 expired 状态。空闲期限
    （last_activity_at + idle_timeout）或硬期限（created_at +
    hard_timeout）不晚于 now 时连接到期：每个携带 now 的操作都先按
    now 处理到期再执行，因此截止时刻的活动不能挽救连接；两个期限
    同时命中时 end_reason 为 hard_timeout。

    按五元组定位、记录活动与关闭平均 O(1) 时间（到期处理由最小堆
    惰性完成），advance 与完整查询 O(n) 时间，空间 O(n)。
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
        self._now = None
        # 全部记录按建立顺序保存；_by_flow 只指向每个五元组的最新记录。
        self._records = []
        self._deadlines = []  # 与 _records 平行的 [空闲期限, 硬期限]
        self._by_flow = {}
        # (期限, 建立序号, 期限种类) 的最小堆，惰性失效：0 空闲、1 硬期限。
        self._expiry_heap = []
        self._active_counts = {
            backend["id"]: 0 for backend in pool._backends
        }

    def _validate_now(self, now):
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be an integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        if self._now is not None and now < self._now:
            raise ConnectionStateError("now must not move backwards")

    def _process_expirations(self, now):
        """把所有期限不晚于 now 的 active 连接置为 expired。

        返回本次到期的内部记录列表（按建立顺序）；堆中已关闭或已被
        更新的期限条目被惰性跳过。
        """
        expired = []
        heap = self._expiry_heap
        records = self._records
        deadlines = self._deadlines
        while heap and heap[0][0] <= now:
            deadline, seq, kind = heapq.heappop(heap)
            record = records[seq]
            if record["state"] != "active":
                continue
            pair = deadlines[seq]
            if pair[kind] != deadline:
                continue
            idle_deadline, hard_deadline = pair
            record["state"] = "expired"
            record["ended_at"] = now
            # 两个期限同时命中时硬期限优先。
            if hard_deadline <= now:
                record["end_reason"] = "hard_timeout"
            else:
                record["end_reason"] = "idle_timeout"
            self._active_counts[record["backend_id"]] -= 1
            expired.append(record)
        expired.sort(key=lambda record: record["seq"])
        return expired

    def open_connection(self, flow, backend_id, now):
        """为五元组建立一条绑定到指定后端的 active 连接并返回记录副本。

        后端必须存在且当前健康。同一五元组已有 active 连接时：后端相同
        则直接返回原记录的副本（不产生任何状态变化），后端不同则抛出
        ConnectionStateError。五元组最新记录已 closed 或 expired 时
        建立新记录。所有校验失败都不改变任何状态。
        """
        self._validate_now(now)
        prepared = _validate_flow(flow)
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        index = self._pool._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        if not self._pool._backends[index]["healthy"]:
            raise ConnectionStateError(
                f"backend {backend_id!r} is not healthy"
            )

        self._now = now
        self._process_expirations(now)

        key = _flow_key(prepared)
        existing = self._by_flow.get(key)
        if existing is not None and existing["state"] == "active":
            if existing["backend_id"] == backend_id:
                return _copy_record(existing)
            raise ConnectionStateError(
                "flow already has an active connection on backend "
                f"{existing['backend_id']!r}"
            )

        seq = len(self._records)
        record = {
            "seq": seq,
            "flow": prepared,
            "backend_id": backend_id,
            "state": "active",
            "created_at": now,
            "last_activity_at": now,
            "ended_at": None,
            "end_reason": None,
        }
        idle_deadline = now + self._idle_timeout
        hard_deadline = now + self._hard_timeout
        self._records.append(record)
        self._deadlines.append([idle_deadline, hard_deadline])
        heapq.heappush(self._expiry_heap, (idle_deadline, seq, 0))
        heapq.heappush(self._expiry_heap, (hard_deadline, seq, 1))
        self._by_flow[key] = record
        self._active_counts[backend_id] += 1
        return _copy_record(record)

    def record_activity(self, flow, now):
        """把五元组当前 active 连接的最后活动时间更新为 now。

        先按 now 处理到期：截止时刻不晚于 now 的连接已到期，活动不能
        挽救。连接不存在或不是 active 时抛出 ConnectionStateError。
        """
        self._validate_now(now)
        prepared = _validate_flow(flow)
        self._now = now
        self._process_expirations(now)

        record = self._by_flow.get(_flow_key(prepared))
        if record is None or record["state"] != "active":
            raise ConnectionStateError("flow has no active connection")
        record["last_activity_at"] = now
        idle_deadline = now + self._idle_timeout
        self._deadlines[record["seq"]][0] = idle_deadline
        heapq.heappush(self._expiry_heap, (idle_deadline, record["seq"], 0))
        return _copy_record(record)

    def close_connection(self, flow, now):
        """把五元组当前 active 连接置为 closed 并返回记录副本。

        先按 now 处理到期。重复关闭（连接已 closed 或 expired）是幂等
        的，直接返回现有记录；五元组从未建立连接时抛出
        ConnectionStateError。
        """
        self._validate_now(now)
        prepared = _validate_flow(flow)
        self._now = now
        self._process_expirations(now)

        record = self._by_flow.get(_flow_key(prepared))
        if record is None:
            raise ConnectionStateError("flow has no connection")
        if record["state"] == "active":
            record["state"] = "closed"
            record["ended_at"] = now
            record["end_reason"] = "closed"
            self._active_counts[record["backend_id"]] -= 1
        return _copy_record(record)

    def advance(self, now):
        """把时间推进到 now，返回本次到期的记录副本（按建立顺序）。"""
        self._validate_now(now)
        self._now = now
        expired = self._process_expirations(now)
        return [_copy_record(record) for record in expired]

    def connections(self):
        """按建立顺序返回全部记录的隔离副本。"""
        return [_copy_record(record) for record in self._records]

    def active_connections(self):
        """按后端声明顺序返回每个后端的 active 连接数（新字典）。"""
        return {
            backend["id"]: self._active_counts[backend["id"]]
            for backend in self._pool._backends
        }


class HealthCheckTracker:
    """只消费显式检查结果、在共享池中自动摘除与回切后端的跟踪器。

    不发起网络请求、不读取墙上时钟，也不接入 schedule 命令：时间与
    状态完全由 record_result 的显式事件驱动。``now`` 必须是非布尔的
    非负整数且全局单调不减。

    连续计数按当前状态解释：健康后端的失败累计连续失败并清零连续
    成功，达到 failure_threshold 时立即在共享池中标为不健康；成功
    清零连续失败。不健康后端的成功累计连续成功并清零连续失败，达到
    recovery_threshold 时立即恢复健康；失败清零连续成功。状态变化
    只通过 BackendPool.set_healthy 写入共享池，立即影响绑定同一池的
    调度器，但不重置既有游标、连接记录或最少连接计数。外部
    set_healthy 改变状态后，下一次记录以池中实际状态为准并重置该
    后端的连续计数。

    单次记录平均 O(1) 时间，statuses 完整查询 O(n) 时间，空间 O(n)。
    """

    def __init__(self, pool, failure_threshold, recovery_threshold):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        for name, value in (
            ("failure_threshold", failure_threshold),
            ("recovery_threshold", recovery_threshold),
        ):
            # bool 是 int 的子类，阈值必须显式排除布尔值。
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
            ):
                raise ConfigurationError(
                    f"{name} must be a positive integer"
                )
        self._pool = pool
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold
        self._now = None
        # 每个后端的跟踪状态，按声明顺序平行于 pool._backends。
        self._states = [
            {
                "known_healthy": backend["healthy"],
                "consecutive_failures": 0,
                "consecutive_successes": 0,
                "checked_at": None,
                "changed": False,
                "last_now": None,
                "last_success": None,
                "last_result": None,
            }
            for backend in pool._backends
        ]

    def record_result(self, backend_id, success, now):
        """提交一次健康检查结果，返回键序固定的状态副本。

        结果字典的键序固定为 backend_id、healthy、changed、
        consecutive_failures、consecutive_successes、checked_at，
        每次返回与内部状态隔离的新字典。同一后端在同一 now 重复提交
        相同结果时幂等地返回上次结果的副本；success 冲突、now 回退
        时抛出 ConnectionStateError。backend_id 非字符串、success
        非布尔或 now 类型错误抛出 TypeError，负 now 抛出 ValueError，
        未知 id 抛出 KeyError；任何失败都不改变计数、时间或池状态。
        """
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        if not isinstance(success, bool):
            raise TypeError("success must be a boolean")
        # bool 是 int 的子类，时间戳必须显式排除布尔值。
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be an integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        index = self._pool._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        if self._now is not None and now < self._now:
            raise ConnectionStateError("now must not move backwards")

        state = self._states[index]
        if state["last_now"] is not None and now == state["last_now"]:
            if success != state["last_success"]:
                raise ConnectionStateError(
                    f"conflicting result for backend {backend_id!r} "
                    f"at now={now}"
                )
            return dict(state["last_result"])

        # 外部 set_healthy 造成的状态偏移在下一次记录时被发现：
        # 以池中实际状态为准，并重置该后端的连续计数。
        actual = self._pool._backends[index]["healthy"]
        if actual != state["known_healthy"]:
            state["known_healthy"] = actual
            state["consecutive_failures"] = 0
            state["consecutive_successes"] = 0

        if success:
            state["consecutive_successes"] += 1
            state["consecutive_failures"] = 0
        else:
            state["consecutive_failures"] += 1
            state["consecutive_successes"] = 0

        changed = False
        if (
            actual
            and state["consecutive_failures"] >= self._failure_threshold
        ):
            self._pool.set_healthy(backend_id, False)
            state["known_healthy"] = False
            changed = True
        elif (
            not actual
            and state["consecutive_successes"] >= self._recovery_threshold
        ):
            self._pool.set_healthy(backend_id, True)
            state["known_healthy"] = True
            changed = True

        state["checked_at"] = now
        state["changed"] = changed
        result = {
            "backend_id": backend_id,
            "healthy": state["known_healthy"],
            "changed": changed,
            "consecutive_failures": state["consecutive_failures"],
            "consecutive_successes": state["consecutive_successes"],
            "checked_at": now,
        }
        state["last_now"] = now
        state["last_success"] = success
        state["last_result"] = result
        self._now = now
        return dict(result)

    def statuses(self):
        """按后端声明顺序返回全部后端的状态（新字典组成的新列表）。

        healthy 反映共享池中的当前标记；从未检查的后端 checked_at
        为 None、changed 为 False、连续计数为零。
        """
        report = []
        for position, backend in enumerate(self._pool._backends):
            state = self._states[position]
            report.append(
                {
                    "backend_id": backend["id"],
                    "healthy": backend["healthy"],
                    "changed": state["changed"],
                    "consecutive_failures": state["consecutive_failures"],
                    "consecutive_successes": state["consecutive_successes"],
                    "checked_at": state["checked_at"],
                }
            )
        return report


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
    except BackendOverloadedError as exc:
        _emit_error("BackendOverloadedError", exc, 5)

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
