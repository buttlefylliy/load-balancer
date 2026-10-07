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
* ``BackendPool.to_json()``：把池的当前配置快照导出为无末尾换行的紧凑
  JSON 字符串（顶层为按声明顺序排列的后端数组，键序固定为 id、address、
  port、healthy、weight，有限容量时末尾追加 max_connections），不保存
  任何调度器游标、连接计数、会话绑定、健康检查计数或事件时间。
* ``BackendPool.from_json(text)``：从 JSON 文本重建后端池；text 非字符串
  抛出 TypeError，非法 JSON、顶层非数组或任一后端不满足既有校验规则时
  统一抛出 ConfigurationError，全部校验完成后才创建与已有池隔离的新实例。
* ``BackendPool.reload_json(text)``：在不替换池对象的前提下热加载兼容
  快照（库接口，不接入 schedule，也不读写文件）；text 沿用 from_json
  的输入与字段语义，非字符串抛出 TypeError，JSON 语法、字段或取值非法
  统一抛出 ConfigurationError。新快照必须与当前池具有相同的后端数量、
  id 与声明顺序且每个后端 weight 不变，新增、删除、改名、重排或改变
  weight 也抛出 ConfigurationError。解析、配置校验与兼容性校验全部
  完成后才一次性提交 address、port、healthy、max_connections 四个允许
  变化的字段，任何失败都使 to_json 保持逐字节不变；已绑定该池的调度器、
  连接表与健康检查跟踪器仍是原实例，其游标、统计、活动连接、等待队列、
  会话绑定、检查状态与事件时钟均保持不变。成功返回键序固定为 changed、
  backends 的新字典，backends 仅按声明顺序列出实际变化的后端，每项键序
  为 backend_id、changed_fields，changed_fields 按 address、port、
  healthy、max_connections 的固定顺序排列；等价配置重复加载返回 changed
  为 false 且 backends 为空且不改变状态。O(n) 时间与 O(n) 临时空间，
  不读取墙上时钟。
* ``RoundRobinScheduler(pool)``：绑定后端池的轮询调度器。
* ``RoundRobinScheduler.select()``：返回下一个健康后端，最坏 O(n) 时间、
  额外空间 O(1)；``explain()`` 返回键序固定的可重放解释（调用前游标、
  全部候选的稳定声明位置、selected、outcome、reason、next_cursor），
  与 select 同规则但无健康后端时不抛错，不推进游标或改变任何状态，
  单次查询 O(n) 时间、除返回结果外额外空间 O(1)；``statistics()``
  返回键序固定（policy、attempts、succeeded、failed、failures、
  backends）的累计统计隔离副本，只由本实例的 select 累计，选择只
  增加 O(1) 统计开销，完整查询 O(n) 时间与 O(n) 返回空间，摘除或
  恢复后端保留累计值。
* ``WeightedRoundRobinScheduler(pool)``：按 weight 构造逻辑循环的加权轮询
  调度器，额外空间 O(n)，单次选择最坏 O(n) 时间、额外空间 O(1)；
  ``explain()`` 返回键序固定的可重放解释（调用前游标、全部候选的权重
  段边界、selected、outcome、reason、next_cursor），与 select 同规则
  但无健康后端时不抛错，不推进游标或改变任何状态，单次查询 O(n) 时间、
  除返回结果外额外空间 O(1)；``statistics()`` 返回键序固定（policy、
  attempts、succeeded、failed、failures、backends）的累计统计隔离
  副本，只由本实例的 select 累计，选择只增加 O(1) 统计开销，完整查询
  O(n) 时间与 O(n) 返回空间，摘除或恢复后端保留累计值。
* ``LeastConnectionsScheduler(pool)``：最少连接调度器，为每个后端维护
  从零开始的活动连接数，只在健康且未达 max_connections 上限的后端中
  取计数最小者，额外空间 O(n)，单次选择最坏 O(n) 时间、额外空间
  O(1)，单次释放 O(1)；``explain()`` 返回键序固定的可重放解释
  （全部候选的连接数、容量与 eligible、selected、outcome、reason），
  与 select 同规则但没有健康后端或全部已满时不抛错，不改变任何状态，
  单次查询 O(n) 时间、除返回结果外额外空间 O(1)；``statistics()``
  返回键序固定（policy、attempts、succeeded、failed、released、
  failures、backends）的累计统计隔离副本，只由 select 与成功的
  release_connection 累计，选择与释放只增加 O(1) 统计开销，完整查询
  O(n) 时间与 O(n) 返回空间，摘除或恢复后端保留累计值。
* ``QueuedLeastConnectionsScheduler(pool, max_queue, queue_timeout)``：
  带等待队列的最少连接调度器（库接口，不接入 schedule），维护与其他
  调度器及连接表互不影响的独立连接计数。max_queue 与 queue_timeout
  必须是排除布尔值的正整数，否则抛出 ConfigurationError。
  ``submit(request_id, now)`` 接收非空字符串标识与显式非布尔非负整数
  时间，先清理截止时间不晚于 now 的请求，再沿用最少连接规则选择健康
  且未满载的后端；成功时计数加一，返回键序为 request_id、outcome、
  backend、queued_at、expires_at、reason 的新字典，outcome 为
  selected。仅当存在健康后端但均满载时才在队列未满时入队，outcome
  为 queued、backend 为 null、reason 为
  all_healthy_backends_at_capacity；同一标识仍在等待时重复提交幂等
  返回原结果。无健康后端抛 NoAvailableBackendError，队列已满抛
  BackendOverloadedError，失败不改变状态。
  ``release_and_dispatch(backend_id, now)`` 释放一个连接，清理到期项
  后把队首有效请求按同一规则分配给任一可用后端，返回键序为
  released_backend_id、dispatched 的新字典；暂不可分配时保留请求且
  dispatched 为 null。``advance(now)`` 只推进时间并按入队顺序返回本次
  到期请求；``pending()`` 返回队列顺序的隔离副本。全部时间入口共享
  单调时钟，回退抛出 ConnectionStateError；类型和值错误、未知后端或
  无连接可释放均在完整校验后抛出，不改变时钟、队列或计数。选择与释放
  后分配最坏 O(n)，入队与逐项过期摊销 O(1)，查询 O(q)，状态空间
  O(n+max_queue)。
* ``ConsistentHashScheduler(pool)``：一致性哈希调度器，``select(key)``
  按会话键对当前健康后端评分（SHA-256 摘要取无符号大端整数，分数最大者
  胜出），不维护游标、连接计数或按键增长的缓存，单次选择最坏 O(n) 时间、
  额外空间 O(1)；``explain(key)`` 返回该次选择键序固定的可重放解释
  （全部候选与分数、selected、outcome、reason），与 select 同校验但
  无健康后端时不抛错，单次查询 O(n) 时间、除返回结果外额外空间 O(1)；
  ``statistics()`` 返回键序固定（policy、attempts、succeeded、failed、
  failures、backends）的累计统计隔离副本，只由本实例的 select 累计，
  选择只增加 O(1) 统计开销，完整查询 O(n) 时间与 O(n) 返回空间，
  摘除或恢复后端保留累计值。
* ``StickySessionScheduler(pool, max_sessions)``：有界会话绑定调度器，
  以 OrderedDict 保存至多 max_sessions 个 key→backend_id 绑定并按成功
  选择维护最近使用顺序：命中（绑定后端仍健康）平均 O(1)，首次选择与
  故障转移 O(n)（一致性哈希评分），超限时淘汰最久未成功使用的 key；
  ``bindings()`` 按最久到最近使用顺序返回 O(max_sessions) 的隔离副本，
  ``explain(key)`` 返回键序固定的可重放解释且不改变状态。
* ``RetryChainScheduler(pool, max_attempts)``：跨后端有界重试链调度器
  （库接口，不接入 schedule），自身无状态：``next_backend(key,
  failed_backend_ids)`` 在当前健康且未在本链失败的后端中沿用一致性
  哈希评分取最高者，max_attempts 包含首次选择；无健康后端抛
  NoAvailableBackendError，达到尝试上限或健康后端均已失败时抛
  RetryExhaustedError；``explain(key, failed_backend_ids)`` 同校验
  同规则，但无候选时返回解释而不抛选择异常。单次查询 O(n+m) 时间、
  除 explain 的 O(n) 返回值外额外 O(m) 空间，m 受 max_attempts 限制。
* ``ConnectionTable(pool, idle_timeout, hard_timeout)``：以五元组标识连接、
  绑定后端池的连接生命周期表。时间只由显式 ``now`` 驱动；按五元组定位、
  记录活动与关闭平均 O(1) 时间，``advance`` 与完整查询 O(n) 时间，
  空间 O(n)。
* ``ConnectionTable.export_log()`` / 类级入口
  ``ConnectionTable.replay_log(pool, idle_timeout, hard_timeout, text)``：
  确定性的内存事件日志与重建入口。open_connection、record_activity、
  close_connection、advance 每次成功返回后按调用顺序追加一个事件
  （幂等的重复建立与重复关闭也各记录一次，抛出异常的调用不记录）；
  导出入口返回无末尾换行的紧凑 JSON 数组，事件键序固定为 sequence、
  operation、now、flow、backend_id；重放入口先完成解析与结构校验，
  再按 sequence 在新 ConnectionTable 上重放，日志结构非法或事件无法
  按给定池与超时合法执行时统一抛出 ConfigurationError（文本非字符串
  抛出 TypeError），失败不修改传入池也不暴露部分结果。单次追加
  O(1) 时间与空间，导出 O(e)，重放状态空间 O(e+n)，e 为事件数。
* ``ConnectionStateError``：连接状态非法时抛出（释放使活动连接数低于零、
  对非 active 连接记录活动、五元组冲突、now 回退等）。
* ``HealthCheckTracker(pool, failure_threshold, recovery_threshold,
  stale_timeout=None)``：只消费显式检查结果的健康跟踪器，按连续失败/
  成功次数在共享池中自动摘除与回切后端；不发起网络请求、不读取墙上
  时钟。可选的 stale_timeout 为检查失联超时（排除布尔值的正整数，
  省略或 None 表示不启用）：``advance(now)`` 沿与 record_result 共享
  的单调时间线推进事件时钟，把当前健康、已有检查结果且
  checked_at + stale_timeout <= now 的后端在共享池中摘除，按池声明
  顺序返回键序固定为 backend_id、healthy、changed、checked_at、
  expired_at、reason 的新摘除项，没有变化时返回空列表。单次记录平均
  O(1) 时间，advance 与完整查询 O(n) 时间，空间 O(n)；
  ``explain(backend_id, success, now)`` 返回键序固定为 backend_id、
  success、checked_at、healthy、changed、consecutive_failures、
  consecutive_successes、reason 的只读预览，与 record_result 同校验
  同判定但不修改任何状态，单次 O(1) 时间与 O(1) 额外空间；
  ``record_batch(results, now)`` 在同一事件时刻原子提交一批检查
  结果：先完成整批校验与判定，再一次性提交全部结果与健康变更，
  任一失败不留下部分修改；成功时按输入顺序返回与 record_result
  相同键序的结果列表，k 个条目使用 O(k) 时间与 O(k) 暂存及返回
   空间。
* ``CircuitBreakerScheduler(pool, failure_threshold, reset_timeout)``：
  按后端维护 closed、open、half_open 熔断状态与连续失败计数的调度器
  （库接口，不接入 schedule），failure_threshold 与 reset_timeout 必须是
  排除布尔值的正整数，否则抛出 ConfigurationError；``record_result
  (backend_id, success, now)`` 记录请求结果：closed 成功清零，连续失败
  达到阈值转为 open（open_until 为 now + reset_timeout），到期经一次
  half_open 探测，探测成功关闭并清零、失败重新打开；同一后端同一 now
  的相同结果幂等、相反结果抛出 ConnectionStateError。``select(key, now)``
  沿用一致性哈希的 key 校验与评分，只比较健康且 closed 的后端，并允许
  到期后端的一次半开探测（选中即占用，记录结果前不再参与选择）；没有
  健康后端抛 NoAvailableBackendError，有健康后端但全部熔断或探测占用
  抛 CircuitOpenError。``explain(key, now)`` 返回键序固定为 policy、
  key、candidates、selected、outcome、reason 的只读解释，不推进时间或
  占用探测，与随后 select 一致。
"""

import argparse
import bisect
import hashlib
import heapq
import json
import sys
from collections import OrderedDict, deque

__all__ = [
    "ConfigurationError",
    "NoAvailableBackendError",
    "BackendOverloadedError",
    "ConnectionStateError",
    "RetryExhaustedError",
    "CircuitOpenError",
    "BackendPool",
    "RoundRobinScheduler",
    "WeightedRoundRobinScheduler",
    "LeastConnectionsScheduler",
    "QueuedLeastConnectionsScheduler",
    "ConsistentHashScheduler",
    "StickySessionScheduler",
    "RetryChainScheduler",
    "ConnectionTable",
    "HealthCheckTracker",
    "CircuitBreakerScheduler",
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


class RetryExhaustedError(Exception):
    """重试链达到尝试上限，或当前健康后端均已在本链失败。"""


class CircuitOpenError(Exception):
    """存在健康后端，但它们全部熔断或唯一到期后端的半开探测已被占用。"""


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

    def to_json(self):
        """把池的当前配置快照导出为紧凑 JSON 字符串（无末尾换行）。

        顶层为按声明顺序排列的后端数组；每个后端对象的键固定按 id、
        address、port、healthy、weight 排列，仅当 max_connections 有
        有限值时才在末尾追加该键。缺省权重导出为 1，非 ASCII 字符直接
        保留不转义。只导出配置快照：不保存任何调度器游标、连接计数、
        会话绑定、健康检查计数或事件时间。池状态不变时重复调用逐字节
        一致，set_healthy 的变更立即反映到下一次导出。O(n) 时间，
        除返回字符串外额外空间 O(n)。
        """
        backends = []
        for backend in self._backends:
            entry = {
                "id": backend["id"],
                "address": backend["address"],
                "port": backend["port"],
                "healthy": backend["healthy"],
                "weight": backend["weight"],
            }
            if backend["max_connections"] is not None:
                entry["max_connections"] = backend["max_connections"]
            backends.append(entry)
        return json.dumps(
            backends, separators=(",", ":"), ensure_ascii=False
        )

    @classmethod
    def from_json(cls, text):
        """从 JSON 文本重建后端池，返回与任何已有池隔离的新实例。

        text 必须是字符串，其他类型抛出 TypeError；文本不是合法 JSON、
        顶层不是数组，或任一后端不满足 BackendPool 的字段、类型、范围、
        未知字段及重复 id 规则时，统一抛出 ConfigurationError。解析与
        全部语义校验完成后才创建新池，任何失败都不产生可观察的部分
        实例，也不改变已有池；重复导入同一文本得到彼此隔离但内容相同
        的新对象。O(n) 时间，除返回对象外额外空间 O(n)。
        """
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        try:
            configs = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                f"invalid JSON: {exc}"
            ) from None
        # 构造器先完成全部校验再建立任何可见状态，失败时不会留下
        # 可观察的部分实例。
        return cls(configs)

    def reload_json(self, text):
        """在不替换池对象的前提下热加载兼容快照，返回变化清单。

        text 沿用 from_json 的输入与字段语义：非字符串抛出 TypeError；
        文本不是合法 JSON、顶层不是数组，或任一后端不满足既有字段、
        类型、范围、未知字段及重复 id 规则时，统一抛出
        ConfigurationError。新快照还必须与当前池兼容：后端数量相同，
        按声明顺序逐位具有相同 id，且每个后端的 weight 不变；新增、
        删除、改名、重排或改变 weight 同样抛出 ConfigurationError。

        调用先在与内部状态隔离的新列表上完成整份文本的解析、配置校验
        与兼容性校验，再一次性提交 address、port、healthy、
        max_connections 四个允许变化的字段；任何失败都不写入池，to_json
        保持逐字节一致，已绑定该池的调度器、连接表与健康检查跟踪器的
        可观察状态也不改变。提交在原有后端字典上原地进行：池对象、声明
        顺序与各后端身份不变，因此游标、统计、活动连接、等待队列、会话
        绑定、检查状态与事件时钟全部保留，绑定对象从下一次公开操作开始
        看到新字段。max_connections 可以降到当前活动连接数以下，已有
        连接继续保留，依赖容量的调度器随后按既有满载规则处理新选择。

        成功时返回与内部状态隔离的新字典，键序固定为 changed、
        backends：changed 表示规范化后的配置是否有语义变化；backends
        仅按池声明顺序列出实际变化的后端，每项键序固定为 backend_id、
        changed_fields，changed_fields 按 address、port、healthy、
        max_connections 的固定顺序排列。重复加载等价配置返回 changed
        为 false 且 backends 为空，不改变状态；修改返回对象不污染后续
        结果。提交后 to_json 立即反映新快照。单次热加载 O(n) 时间与
        O(n) 临时空间，不读取墙上时钟。
        """
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        try:
            configs = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                f"invalid JSON: {exc}"
            ) from None
        # 先在与内部状态隔离的新列表上完成配置校验；任何非法取值都在
        # 触碰池状态之前抛出，不产生部分修改。
        prepared = _validate_configs(configs)

        current = self._backends
        # 兼容性校验：后端数量、逐位 id 与逐位 weight 必须完全一致；
        # 这也保证加权调度器预计算的权重段边界在热加载后仍然有效。
        if len(prepared) != len(current):
            raise ConfigurationError(
                "reload requires the same number of backends"
            )
        for position, (incoming, existing) in enumerate(
            zip(prepared, current)
        ):
            if incoming["id"] != existing["id"]:
                raise ConfigurationError(
                    f"backend at position {position}: id must remain "
                    f"{existing['id']!r}, got {incoming['id']!r}"
                )
            if incoming["weight"] != existing["weight"]:
                raise ConfigurationError(
                    f"backend at position {position}: weight must remain "
                    f"{existing['weight']}"
                )

        # 全部校验完成：先在暂存列表上计算变化清单，再一次性提交，保证
        # 返回结果与内部状态隔离。
        variable_fields = ("address", "port", "healthy", "max_connections")
        changed_backends = []
        pending = []
        changed = False
        for existing, incoming in zip(current, prepared):
            changed_fields = [
                field
                for field in variable_fields
                if incoming[field] != existing[field]
            ]
            if changed_fields:
                changed = True
                changed_backends.append(
                    {
                        "backend_id": existing["id"],
                        "changed_fields": changed_fields,
                    }
                )
                pending.append((existing, incoming))

        # 一次性提交允许变化的字段；在原字典上原地更新，保留池列表、
        # 声明顺序、各后端身份及 weight，绑定对象持有的索引与计数等
        # 平行数组继续有效。
        for existing, incoming in pending:
            for field in variable_fields:
                existing[field] = incoming[field]

        return {"changed": changed, "backends": changed_backends}


class RoundRobinScheduler:
    """普通轮询调度器。

    游标指向上一次成功选择的位置，初始位于首项之前（-1）。每次选择
    从游标后继位置开始做至多 n 次检查的有界扫描，跳过不健康后端；
    找不到健康后端时抛出 NoAvailableBackendError 且保持游标不变。

    ``explain()`` 复用与 select 完全相同的起点、跳过与回绕规则，但不
    抛出调度异常、不推进游标：返回包含调用前游标、全部候选及其声明
    位置与选中结果的可重放解释字典，健康变化立即体现在下一次解释中；
    池状态不变时，解释与紧随其后的 select 选中同一后端，且
    next_cursor 即该次 select 将写入的位置。

    ``statistics()`` 返回本实例构造以来累计的选择统计：每次 select
    调用（无论成功或抛出失败）都计入 attempts，成功时同时计入
    succeeded 与被选后端的 selected，无健康后端的失败只计入 failed
    与 failures 中的 no_healthy_backend。explain、statistics 与健康
    标记变化均不累计任何事件；摘除或恢复后端保留全部累计值。统计只
    描述通过本实例发生的 select 调用，不影响共享同一后端池的其他
    调度器实例。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        self._cursor = -1
        # 累计统计按声明顺序与后端平行保存；统计只增不减，健康标记变化
        # 与只读查询都不触碰这些值，额外空间 O(n)。
        self._stats_attempts = 0
        self._stats_succeeded = 0
        self._stats_failed = 0
        self._stats_failures = {"no_healthy_backend": 0}
        self._stats_selected = [0] * len(pool._backends)

    def select(self):
        """选择并返回下一个健康后端。

        结果是键序固定为 id、address、port 的新字典；成功后游标推进到
        所选位置。每次调用（无论成功或失败）都先把 attempts 加一；成功
        时同时把 succeeded 与被选后端的 selected 加一。没有健康后端时
        抛出 NoAvailableBackendError，只增加 failed 与 failures 中的
        no_healthy_backend，池状态、游标与各后端计数均不改变。
        """
        backends = self._pool._backends
        size = len(backends)
        # 每次 select 调用都计入尝试，包括随后抛出失败异常的调用。
        self._stats_attempts += 1
        for offset in range(size):
            index = (self._cursor + 1 + offset) % size
            backend = backends[index]
            if backend["healthy"]:
                self._cursor = index
                self._stats_succeeded += 1
                self._stats_selected[index] += 1
                return {
                    "id": backend["id"],
                    "address": backend["address"],
                    "port": backend["port"],
                }
        self._stats_failed += 1
        self._stats_failures["no_healthy_backend"] += 1
        raise NoAvailableBackendError("no healthy backend available")

    def statistics(self):
        """返回本实例构造以来累计的选择统计的隔离副本。

        结果是键序固定为 policy、attempts、succeeded、failed、
        failures、backends 的新字典：policy 固定为 round_robin；
        attempts、succeeded、failed 为从零开始的非负整数累计计数；
        failures 是只含 no_healthy_backend 一个固定键的新字典；
        backends 按池声明顺序排列，每项键序固定为 backend_id、
        selected，即使后端从未被选中、当前不健康或随后恢复，也保留
        对应的零值或累计值。每次 select 调用增加 attempts：成功时
        同时增加 succeeded 与被选后端的 selected，无健康后端的失败
        只增加 failed 与 failures 中的 no_healthy_backend。explain、
        statistics 与健康标记变化均不累计任何事件；摘除或恢复后端
        保留累计值。返回字典与列表全部为新建对象，修改它们不污染
        内部状态或后续结果；池状态不变时重复调用逐字段相同。单次
        查询 O(n) 时间与 O(n) 返回空间。
        """
        return {
            "policy": "round_robin",
            "attempts": self._stats_attempts,
            "succeeded": self._stats_succeeded,
            "failed": self._stats_failed,
            "failures": {
                "no_healthy_backend":
                    self._stats_failures["no_healthy_backend"],
            },
            "backends": [
                {
                    "backend_id": backend["id"],
                    "selected": self._stats_selected[index],
                }
                for index, backend in enumerate(self._pool._backends)
            ],
        }

    def explain(self):
        """返回一次普通轮询选择的可重放解释，不推进游标或改变任何状态。

        结果是键序固定为 policy、cursor、candidates、selected、outcome、
        reason、next_cursor 的新字典：policy 固定为 round_robin；cursor
        为调用前上一次成功选择的位置（初始为 -1）；candidates 按后端
        声明顺序排列，每项键序固定为 backend_id、healthy、position，
        position 是从零开始且稳定不变的声明位置。解释从 cursor 的后继
        位置出发并在末尾回绕，沿用与 select 完全相同的规则跳过不健康
        后端：存在健康后端时 selected 是键序固定为 id、address、port 的
        新字典，outcome 为 selected，reason 为 round_robin，next_cursor
        为预计选中位置；池状态不变时紧随其后的 select 返回同一后端并把
        游标推进到 next_cursor。空池或全部后端均不健康时不抛出
        NoAvailableBackendError：返回全部候选，selected 为 None、
        outcome 为 failed、reason 为 no_healthy_backend、next_cursor 等于
        cursor；同一情形下 select 仍抛出 NoAvailableBackendError。解释
        不修改游标、池或健康标记，修改返回对象不污染后续结果；池状态
        不变时重复调用逐字段一致，set_healthy 的变化立即反映在下一次
        解释中。单次查询 O(n) 时间，除返回的 O(n) 解释结果外只使用
        O(1) 额外空间。
        """
        backends = self._pool._backends
        size = len(backends)
        cursor = self._cursor
        candidates = [
            {
                "backend_id": backend["id"],
                "healthy": backend["healthy"],
                "position": position,
            }
            for position, backend in enumerate(backends)
        ]
        chosen = -1
        # 起点、跳过不健康后端与回绕与 select 完全一致，只是不写回游标。
        for offset in range(size):
            index = (cursor + 1 + offset) % size
            if backends[index]["healthy"]:
                chosen = index
                break
        if chosen < 0:
            return {
                "policy": "round_robin",
                "cursor": cursor,
                "candidates": candidates,
                "selected": None,
                "outcome": "failed",
                "reason": "no_healthy_backend",
                "next_cursor": cursor,
            }
        backend = backends[chosen]
        return {
            "policy": "round_robin",
            "cursor": cursor,
            "candidates": candidates,
            "selected": {
                "id": backend["id"],
                "address": backend["address"],
                "port": backend["port"],
            },
            "outcome": "selected",
            "reason": "round_robin",
            "next_cursor": chosen,
        }


class WeightedRoundRobinScheduler:
    """加权轮询调度器。

    按声明顺序把每个后端映射到逻辑循环中连续 weight 个位置（不展开
    保存重复项，只保存每个后端的段首偏移，O(n) 空间）。游标记录上一次
    成功选择的逻辑位置，初始位于循环起点之前（-1）。每次选择从游标
    后继位置开始，借助段首偏移整段跳过不健康后端占有的位置，至多检查
    n 个后端：单次选择最坏 O(n) 时间、额外空间 O(1)。恢复健康标记的
    变更立即影响下一次选择；没有健康后端时抛出 NoAvailableBackendError
    且游标不变。

    ``explain()`` 复用与 select 完全相同的起点定位、整段跳过与回绕
    规则，但不抛出调度异常、不推进游标：返回包含调用前游标、全部候选
    的权重段边界与选中结果的可重放解释字典，健康变化立即体现在下一次
    解释中；池状态不变时，解释与紧随其后的 select 选中同一后端，且
    next_cursor 即该次 select 将写入的位置。

    ``statistics()`` 返回本实例构造以来累计的选择统计：每次 select
    调用（无论成功或抛出失败）都计入 attempts，成功时同时计入
    succeeded 与被选后端的 selected（一次调用只计一次，weight 只决定
    命中频率而不按权重重复计数），无健康后端的失败只计入 failed 与
    failures 中的 no_healthy_backend。explain、statistics 与健康标记
    变化均不累计任何事件；摘除或恢复后端保留全部累计值。统计只描述
    通过本实例发生的 select 调用，不影响共享同一后端池的其他调度器
    实例。
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
        # 累计统计按声明顺序与后端平行保存；统计只增不减，健康标记变化
        # 与只读查询都不触碰这些值，额外空间 O(n)。
        self._stats_attempts = 0
        self._stats_succeeded = 0
        self._stats_failed = 0
        self._stats_failures = {"no_healthy_backend": 0}
        self._stats_selected = [0] * len(pool._backends)

    def select(self):
        """选择并返回下一个健康后端。

        结果是键序固定为 id、address、port 的新字典；成功后游标推进到
        所选逻辑位置。每次调用（无论成功或失败）都先把 attempts 加一；
        成功时同时把 succeeded 与被选后端的 selected 加一，weight 只
        决定命中频率，单次调用不会按权重重复计数。空池或没有健康后端时
        抛出 NoAvailableBackendError，游标保持不变，只增加 failed 与
        failures 中的 no_healthy_backend，池状态与各后端 selected 均不
        改变。
        """
        backends = self._pool._backends
        size = len(backends)
        # 每次 select 调用都计入尝试，包括随后抛出失败异常的调用。
        self._stats_attempts += 1
        if self._total == 0:
            self._record_no_healthy()
        position = (self._cursor + 1) % self._total
        # 二分定位 position 所属的后端段，之后逐个后端检查；
        # 不健康则直接跳到下一个后端的段首，至多检查 n 个后端。
        index = bisect.bisect_right(self._offsets, position) - 1
        for _ in range(size):
            backend = backends[index]
            if backend["healthy"]:
                self._cursor = position
                self._stats_succeeded += 1
                self._stats_selected[index] += 1
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
        self._record_no_healthy()

    def _record_no_healthy(self):
        """记录一次无健康后端的失败并抛出对应异常。

        只增加 failed 与 failures 中的 no_healthy_backend，不改变游标、
        池状态或任何后端 selected。
        """
        self._stats_failed += 1
        self._stats_failures["no_healthy_backend"] += 1
        raise NoAvailableBackendError("no healthy backend available")

    def statistics(self):
        """返回本实例构造以来累计的选择统计的隔离副本。

        结果是键序固定为 policy、attempts、succeeded、failed、
        failures、backends 的新字典：policy 固定为 weighted_round_robin；
        attempts、succeeded、failed 为从零开始的非负整数累计计数；
        failures 是只含 no_healthy_backend 一个固定键的新字典；
        backends 按池声明顺序排列，每项键序固定为 backend_id、
        selected，即使后端从未被选中、当前不健康或随后恢复，也保留
        对应的零值或累计值。每次 select 调用增加 attempts：成功时
        同时增加 succeeded 与实际返回后端的 selected（一次调用只计
        一次，weight 只决定命中频率，不按权重重复计数），空池或无
        健康后端的失败只增加 failed 与 failures 中的
        no_healthy_backend。explain、statistics 与健康标记变化均不
        累计任何事件；摘除或恢复后端保留累计值，多个绑定同一池的
        调度器实例各自维护互不影响的统计。返回字典、failures 字典与
        backends 列表全部为新建对象，修改它们不污染内部状态或后续
        结果；池状态不变时重复调用逐字段相同。单次查询 O(n) 时间与
        O(n) 返回空间。
        """
        return {
            "policy": "weighted_round_robin",
            "attempts": self._stats_attempts,
            "succeeded": self._stats_succeeded,
            "failed": self._stats_failed,
            "failures": {
                "no_healthy_backend":
                    self._stats_failures["no_healthy_backend"],
            },
            "backends": [
                {
                    "backend_id": backend["id"],
                    "selected": self._stats_selected[index],
                }
                for index, backend in enumerate(self._pool._backends)
            ],
        }

    def explain(self):
        """返回一次加权轮询选择的可重放解释，不推进游标或改变任何状态。

        结果是键序固定为 policy、cursor、candidates、selected、outcome、
        reason、next_cursor 的新字典：policy 固定为 weighted_round_robin；
        cursor 为调用前的逻辑游标（上一次成功选择的位置，初始为 -1）；
        candidates 按后端声明顺序排列，每项键序固定为 backend_id、
        healthy、weight、segment_start、segment_end，其中 segment_start
        与 segment_end 是包含起点、不包含终点的整数边界
        （segment_end 为下一个后端段首，末段为权重循环总长），完整
        覆盖权重循环，不按权重展开重复项。解释从 cursor 的后继位置
        出发，沿用与 select 完全相同的整段跳过与回绕规则：存在健康
        后端时 selected 是键序固定为 id、address、port 的新字典，
        outcome 为 selected，reason 为 weighted_round_robin，
        next_cursor 为下次成功选择将写入的逻辑位置；池状态不变时
        紧随其后的 select 返回同一后端并把游标推进到 next_cursor。
        全部后端均不健康时不抛出 NoAvailableBackendError：返回全部
        候选，selected 为 None、outcome 为 failed、reason 为
        no_healthy_backend、next_cursor 等于 cursor；同一情形下
        select 仍抛出 NoAvailableBackendError。解释不修改游标、池或
        健康标记，修改返回对象不污染后续结果；池状态不变时重复调用
        逐字段一致，set_healthy 的变化立即反映在下一次解释中。
        单次查询 O(n) 时间，除返回的 O(n) 解释结果外只使用 O(1)
        额外空间。
        """
        backends = self._pool._backends
        size = len(backends)
        offsets = self._offsets
        total = self._total
        cursor = self._cursor
        candidates = []
        for index, backend in enumerate(backends):
            segment_end = offsets[index + 1] if index + 1 < size else total
            candidates.append(
                {
                    "backend_id": backend["id"],
                    "healthy": backend["healthy"],
                    "weight": backend["weight"],
                    "segment_start": offsets[index],
                    "segment_end": segment_end,
                }
            )
        if total == 0:
            return {
                "policy": "weighted_round_robin",
                "cursor": cursor,
                "candidates": candidates,
                "selected": None,
                "outcome": "failed",
                "reason": "no_healthy_backend",
                "next_cursor": cursor,
            }
        # 起点定位、整段跳过与回绕与 select 完全一致，只是不写回游标。
        position = (cursor + 1) % total
        index = bisect.bisect_right(offsets, position) - 1
        for _ in range(size):
            backend = backends[index]
            if backend["healthy"]:
                selected = {
                    "id": backend["id"],
                    "address": backend["address"],
                    "port": backend["port"],
                }
                return {
                    "policy": "weighted_round_robin",
                    "cursor": cursor,
                    "candidates": candidates,
                    "selected": selected,
                    "outcome": "selected",
                    "reason": "weighted_round_robin",
                    "next_cursor": position,
                }
            index += 1
            if index == size:
                index = 0
                position = 0
            else:
                position = offsets[index]
        return {
            "policy": "weighted_round_robin",
            "cursor": cursor,
            "candidates": candidates,
            "selected": None,
            "outcome": "failed",
            "reason": "no_healthy_backend",
            "next_cursor": cursor,
        }


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

    ``explain()`` 复用与 select 完全相同的筛选与决胜规则，但不抛出
    调度异常、不增加任何计数：返回包含全部候选的连接数、容量与
    eligible 标记、选中结果与失败原因的可重放解释字典，健康变化与
    release_connection 的结果立即体现在下一次解释中。

    ``statistics()`` 返回构造以来累计的选择与释放统计：每次 select
    调用（无论成功或抛出失败）都计入 attempts，成功时同时计入
    succeeded 与被选后端的 selected，无健康后端或全部满载的失败只
    计入 failed 与 failures 中对应的失败原因；成功的
    release_connection 计入 released 总数与目标后端的 released。
    explain、active_connections、statistics 与健康标记变化均不累计
    任何事件；摘除或恢复后端保留全部累计值。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        self._counts = [0] * len(pool._backends)
        # 累计统计与活动计数平行保存，均按声明顺序排列；统计只增不减，
        # 健康标记变化与只读查询都不触碰这些值，额外空间 O(n)。
        self._stats_attempts = 0
        self._stats_succeeded = 0
        self._stats_failed = 0
        self._stats_released = 0
        self._stats_failures = {
            "no_healthy_backend": 0,
            "all_healthy_backends_at_capacity": 0,
        }
        self._stats_selected = [0] * len(pool._backends)
        self._stats_released_by_backend = [0] * len(pool._backends)

    def select(self):
        """选择当前健康且未满容量、活动连接数最小的后端。

        结果是键序固定为 id、address、port 的新字典；每次调用（无论
        成功或失败）都先把 attempts 加一。成功后同时把 succeeded 与被
        选后端的 selected 加一，活动连接数也照常先加一再返回。没有健康
        后端时抛出 NoAvailableBackendError；存在健康后端但计数全部达到
        各自 max_connections 上限时抛出 BackendOverloadedError：两种
        失败只增加 failed 与 failures 中对应的失败原因，不改变任何活动
        连接数与后端 selected 计数。
        """
        backends = self._pool._backends
        counts = self._counts
        # 每次 select 调用都计入尝试，包括随后抛出失败异常的调用。
        self._stats_attempts += 1
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
            self._stats_failed += 1
            if not saw_healthy:
                self._stats_failures["no_healthy_backend"] += 1
                raise NoAvailableBackendError("no healthy backend available")
            self._stats_failures["all_healthy_backends_at_capacity"] += 1
            raise BackendOverloadedError(
                "all healthy backends are at capacity"
            )
        counts[chosen] += 1
        self._stats_succeeded += 1
        self._stats_selected[chosen] += 1
        backend = backends[chosen]
        return {
            "id": backend["id"],
            "address": backend["address"],
            "port": backend["port"],
        }

    def release_connection(self, backend_id):
        """按 id 释放一个活动连接，对应计数减一。

        成功时把 released 总数与目标后端的 released 各加一，活动连接
        数照常减一。不健康的后端同样允许释放。id 不是字符串时抛出
        TypeError，id 不存在时抛出 KeyError；计数已为零时抛出
        ConnectionStateError：三种失败都不改变任何活动连接数与累计
        统计。
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
        self._stats_released += 1
        self._stats_released_by_backend[index] += 1

    def active_connections(self):
        """返回全部后端的活动连接数。

        结果是与内部状态隔离的新字典，键按声明顺序排列，值为非负整数。
        """
        return {
            backend["id"]: self._counts[index]
            for index, backend in enumerate(self._pool._backends)
        }

    def statistics(self):
        """返回构造以来累计的选择与释放统计的隔离副本。

        结果是键序固定为 policy、attempts、succeeded、failed、
        released、failures、backends 的新字典：policy 固定为
        least_connections；attempts、succeeded、failed、released 为从零
        开始的非负整数累计计数；failures 是键序固定为
        no_healthy_backend、all_healthy_backends_at_capacity 的新字典；
        backends 按池声明顺序排列，每项键序固定为 backend_id、
        selected、released、active_connections，其中 selected 与
        released 是该后端累计被选与成功释放次数，active_connections
        沿用当前实时活动连接数。每次 select 调用增加 attempts：成功时
        同时增加 succeeded 与被选后端的 selected，无健康后端或全部满载
        的失败只增加 failed 与对应失败原因；成功的
        release_connection 增加 released 总数与目标后端的 released。
        explain、active_connections、statistics 与健康标记变化均不累计
        任何事件；摘除或恢复后端保留累计值，active_connections 始终
        反映实时计数。返回字典与列表全部为新建对象，修改它们不污染
        内部状态或后续结果；状态不变时重复调用逐字段相同。单次查询
        O(n) 时间与 O(n) 返回空间。
        """
        backends = self._pool._backends
        counts = self._counts
        return {
            "policy": "least_connections",
            "attempts": self._stats_attempts,
            "succeeded": self._stats_succeeded,
            "failed": self._stats_failed,
            "released": self._stats_released,
            "failures": {
                "no_healthy_backend":
                    self._stats_failures["no_healthy_backend"],
                "all_healthy_backends_at_capacity":
                    self._stats_failures["all_healthy_backends_at_capacity"],
            },
            "backends": [
                {
                    "backend_id": backend["id"],
                    "selected": self._stats_selected[index],
                    "released": self._stats_released_by_backend[index],
                    "active_connections": counts[index],
                }
                for index, backend in enumerate(backends)
            ],
        }

    def explain(self):
        """返回一次最少连接选择的可重放解释，不改变任何状态。

        结果是键序固定为 policy、candidates、selected、outcome、reason
        的新字典：policy 固定为 least_connections；candidates 按后端
        声明顺序排列，每项键序固定为 backend_id、healthy、
        active_connections、max_connections、eligible，未配置容量上限时
        max_connections 为 None，eligible 仅在后端健康且活动连接数低于
        自身上限时为 True（省略上限的健康后端恒为 eligible）。解释采用
        与紧随其后的 select 完全相同的规则：在 eligible 候选中取活动
        连接数最小者，计数相同取声明顺序最前者。可以调度时 selected 是
        键序固定为 id、address、port 的新字典，outcome 为 selected，
        reason 为 least_connections；没有健康后端时 selected 为 None、
        outcome 为 failed、reason 为 no_healthy_backend；存在健康后端但
        全部已满时 selected 为 None、outcome 为 failed、reason 为
        all_healthy_backends_at_capacity，两种失败都不抛出调度异常。
        解释不增加计数、不改变池状态或后续选择；池状态不变时重复调用
        返回逐字段一致的结果，修改返回值不污染内部状态。单次查询 O(n)
        时间，除返回的 O(n) 解释结果外只使用 O(1) 额外空间。
        """
        backends = self._pool._backends
        counts = self._counts
        candidates = []
        chosen = -1
        saw_healthy = False
        for index, backend in enumerate(backends):
            healthy = backend["healthy"]
            limit = backend["max_connections"]
            # eligible 与 select 的筛选条件严格一致：健康且计数低于上限，
            # 省略上限（None）时容量筛选始终通过。
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
            if limit is not None and counts[index] >= limit:
                continue
            if chosen < 0 or counts[index] < counts[chosen]:
                chosen = index
        if chosen < 0:
            selected = None
            outcome = "failed"
            if not saw_healthy:
                reason = "no_healthy_backend"
            else:
                reason = "all_healthy_backends_at_capacity"
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
            "policy": "least_connections",
            "candidates": candidates,
            "selected": selected,
            "outcome": outcome,
            "reason": reason,
        }


class QueuedLeastConnectionsScheduler:
    """带等待队列的最少连接调度器（库接口，不接入 schedule）。

    为每个后端维护从零开始的独立活动连接数，与绑定同一池的其他调度器
    及 ConnectionTable 的计数互不影响。submit 先按当前 now 清理截止时间
    不晚于 now 的等待请求，再沿用 LeastConnectionsScheduler 的规则在
    当前健康且计数低于自身 max_connections 上限的后端中取计数最小者
    （计数相同取声明顺序最前者），成功后把该后端计数加一。仅当存在
    健康后端但它们全部满载时，才在有界队列未满时把请求按 FIFO 入队。

    时间只由调用方显式传入的 now 驱动，全部入口共享同一单调时钟：now
    必须是非布尔的非负整数，回退抛出 ConnectionStateError。等待请求在
    queued_at + queue_timeout 时刻到期（截止时刻不晚于 now 即清理）；
    advance 只推进时间并按入队顺序返回本次到期的请求。

    选择与释放后分配最坏 O(n) 时间，入队与逐项过期摊销 O(1)，查询
    O(q)，状态空间 O(n+max_queue)，q 为当前队列长度。
    """

    def __init__(self, pool, max_queue, queue_timeout):
        # 先完成全部校验，再建立任何实例状态；校验失败时对象不会被
        # 部分构造。
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        # bool 是 int 的子类，上限与超时必须显式排除布尔值。
        for name, value in (
            ("max_queue", max_queue),
            ("queue_timeout", queue_timeout),
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
        self._max_queue = max_queue
        self._queue_timeout = queue_timeout
        self._counts = [0] * len(pool._backends)
        # FIFO 等待队列：每项为 (request_id, queued_at, expires_at)；
        # _waiting 平行保存仍在等待的 request_id，支持 O(1) 幂等查找。
        self._queue = deque()
        self._waiting = {}
        # 幂等提交结果，键为仍在等待的 request_id；离队（被分配或到期）
        # 时同步删除。
        self._results = {}
        self._now = None

    def _validate_now(self, now):
        """校验共享时间入口：非布尔非负整数且不得回退。"""
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be an integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        if self._now is not None and now < self._now:
            raise ConnectionStateError("now must not move backwards")

    def _purge_expired(self, now):
        """移除所有截止时间不晚于 now 的等待请求，摊销 O(1)/项。

        到期项按入队顺序从队首弹出；健康变化或分配造成的非队首到期不
        影响 FIFO 顺序，后续入口在更大或相同的 now 继续清理。
        """
        queue = self._queue
        while queue and queue[0][2] <= now:
            request_id, _queued_at, _expires_at = queue.popleft()
            del self._waiting[request_id]
            del self._results[request_id]

    def _choose_backend(self):
        """沿用最少连接规则选择健康且未满的后端声明位置。

        返回 (chosen, saw_healthy)：chosen 为 -1 时没有可选后端，
        saw_healthy 区分“没有任何健康后端”与“健康后端全部满载”。
        """
        chosen = -1
        saw_healthy = False
        for index, backend in enumerate(self._pool._backends):
            if not backend["healthy"]:
                continue
            saw_healthy = True
            limit = backend["max_connections"]
            if limit is not None and self._counts[index] >= limit:
                continue
            if chosen < 0 or self._counts[index] < self._counts[chosen]:
                chosen = index
        return chosen, saw_healthy

    def submit(self, request_id, now):
        """提交一个请求：立即选择后端，或在健康后端全部满载时入队。

        先清理截止时间不晚于 now 的等待请求，再按最少连接规则选择。
        request_id 必须是非空字符串（非字符串抛 TypeError，空字符串
        抛 ValueError），now 必须是非布尔非负整数且不得回退；全部校验
        完成后才修改状态，任何失败都不改变时钟、队列或计数。选中时
        对应计数加一，返回键序固定为 request_id、outcome、backend、
        queued_at、expires_at、reason 的新字典，backend 为被选后端 id、
        queued_at 与 expires_at 为 None、reason 为 least_connections。
        仅当存在健康后端但全部满载时才在队列未满时入队，返回 outcome
        为 queued、backend 为 None、queued_at 为 now、expires_at 为
        now + queue_timeout、reason 为 all_healthy_backends_at_capacity；
        同一 request_id 仍在等待时重复提交幂等返回原结果。没有健康
        后端抛出 NoAvailableBackendError；存在健康后端但全部满载且
        队列已满时抛出 BackendOverloadedError，两种失败都不改变状态。
        """
        if not isinstance(request_id, str):
            raise TypeError("request_id must be a string")
        if request_id == "":
            raise ValueError("request_id must be a non-empty string")
        self._validate_now(now)

        self._now = now
        self._purge_expired(now)

        # 幂等：同一标识仍在等待时原样返回其入队结果，不推进时钟以外的
        # 任何状态（时钟已校验为非回退，前进到 now 不改变等待语义）。
        if request_id in self._waiting:
            return dict(self._results[request_id])

        chosen, saw_healthy = self._choose_backend()
        if chosen >= 0:
            self._counts[chosen] += 1
            return {
                "request_id": request_id,
                "outcome": "selected",
                "backend": self._pool._backends[chosen]["id"],
                "queued_at": None,
                "expires_at": None,
                "reason": "least_connections",
            }
        if not saw_healthy:
            raise NoAvailableBackendError("no healthy backend available")
        if len(self._queue) >= self._max_queue:
            raise BackendOverloadedError(
                "all healthy backends are at capacity and queue is full"
            )
        expires_at = now + self._queue_timeout
        self._queue.append((request_id, now, expires_at))
        self._waiting[request_id] = None
        result = {
            "request_id": request_id,
            "outcome": "queued",
            "backend": None,
            "queued_at": now,
            "expires_at": expires_at,
            "reason": "all_healthy_backends_at_capacity",
        }
        self._results[request_id] = result
        return dict(result)

    def release_and_dispatch(self, backend_id, now):
        """释放一个连接，并把队首可分配请求立即分配给任一可用后端。

        backend_id 必须是池内字符串 id（非字符串抛 TypeError，未知 id
        抛 KeyError），该后端当前计数为零时抛 ConnectionStateError；
        now 沿用共享单调时钟校验。全部校验完成后才修改时钟、队列或
        计数。先释放指定后端的一个连接，再清理截止时间不晚于 now 的
        等待请求，随后沿 FIFO 队首检查：把第一个仍能按最少连接规则
        立即分配的请求分配给当前可用后端（计数加一）并离队；暂不可
        分配（无健康后端或健康后端全部满载）时保留请求。返回键序固定
        为 released_backend_id、dispatched 的新字典，released_backend_id
        为被释放后端 id，成功分配时 dispatched 为键序固定为 request_id、
        backend、queued_at、expires_at 的新字典，否则 dispatched 为 None。
        释放与释放后分配合计最坏 O(n) 时间。
        """
        if not isinstance(backend_id, str):
            raise TypeError("backend id must be a string")
        index = self._pool._index.get(backend_id)
        if index is None:
            raise KeyError(backend_id)
        self._validate_now(now)
        if self._counts[index] == 0:
            raise ConnectionStateError(
                f"backend {backend_id!r} has no active connection to release"
            )

        # 全部校验完成：先释放并推进时钟，再做到期清理与队首分配。
        self._counts[index] -= 1
        self._now = now
        self._purge_expired(now)

        dispatched = None
        if self._queue:
            chosen, _saw_healthy = self._choose_backend()
            if chosen >= 0:
                request_id, queued_at, expires_at = self._queue.popleft()
                del self._waiting[request_id]
                del self._results[request_id]
                self._counts[chosen] += 1
                dispatched = {
                    "request_id": request_id,
                    "backend": self._pool._backends[chosen]["id"],
                    "queued_at": queued_at,
                    "expires_at": expires_at,
                }
        return {
            "released_backend_id": backend_id,
            "dispatched": dispatched,
        }

    def advance(self, now):
        """只推进时间，按入队顺序返回本次到期的等待请求。

        now 沿用共享单调时钟校验：非布尔非负整数，回退抛出
        ConnectionStateError；校验失败不改变时钟、队列或计数。截止时间
        不晚于 now 的等待请求按 FIFO 顺序离队并返回，每项为键序固定为
        request_id、queued_at、expires_at 的新字典；同一时刻重复推进是
        幂等的（返回空列表）。逐项弹出摊销 O(1)，除返回列表外只使用
        O(1) 额外空间。
        """
        self._validate_now(now)
        self._now = now
        expired = []
        queue = self._queue
        while queue and queue[0][2] <= now:
            request_id, queued_at, expires_at = queue.popleft()
            del self._waiting[request_id]
            del self._results[request_id]
            expired.append(
                {
                    "request_id": request_id,
                    "queued_at": queued_at,
                    "expires_at": expires_at,
                }
            )
        return expired

    def pending(self):
        """按队列顺序（最旧到最新）返回等待请求的隔离副本。

        每项是键序固定为 request_id、queued_at、expires_at 的新字典；
        返回的列表与字典都与内部状态隔离，修改它们不影响后续提交、
        分配或到期。O(q) 时间与 O(q) 返回空间。
        """
        return [
            {
                "request_id": request_id,
                "queued_at": queued_at,
                "expires_at": expires_at,
            }
            for request_id, queued_at, expires_at in self._queue
        ]


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

    ``statistics()`` 返回本实例构造以来累计的选择统计：每次通过 key
    校验的 select 调用（无论成功或抛出失败）都计入 attempts，成功时
    同时计入 succeeded 与实际返回后端的 selected（一次调用只计一次），
    无健康后端的失败只计入 failed 与 failures 中的 no_healthy_backend。
    key 校验失败（TypeError、ValueError）、explain、statistics 与健康
    标记变化均不累计任何事件；摘除或恢复后端保留全部累计值。统计只描述
    通过本实例发生的 select 调用，不影响共享同一后端池的其他调度器
    实例。
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        self._pool = pool
        # 累计统计按声明顺序与后端平行保存；统计只增不减，健康标记变化
        # 与只读查询都不触碰这些值，额外空间 O(n)。
        self._stats_attempts = 0
        self._stats_succeeded = 0
        self._stats_failed = 0
        self._stats_failures = {"no_healthy_backend": 0}
        self._stats_selected = [0] * len(pool._backends)

    def select(self, key):
        """按会话键选择一个当前健康的后端。

        结果是键序固定为 id、address、port 的新字典；同一键在后端集合
        与健康状态不变时，无论调用次数以及与其他键的调用顺序如何，都
        返回逐字段相同的结果。key 不是字符串时抛出 TypeError，为空
        字符串时抛出 ValueError，两种失败均不产生状态变化，全部统计
        保持不变。每次通过校验的调用（无论成功或失败）都先把 attempts
        加一；成功时同时把 succeeded 与实际返回后端的 selected 加一。
        没有健康后端时抛出 NoAvailableBackendError，只增加 failed 与
        failures 中的 no_healthy_backend，池状态与各后端命中数均不改变。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")
        backends = self._pool._backends
        # 每次通过校验的 select 调用都计入尝试，包括随后抛出失败异常的调用。
        self._stats_attempts += 1
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
            self._stats_failed += 1
            self._stats_failures["no_healthy_backend"] += 1
            raise NoAvailableBackendError("no healthy backend available")
        self._stats_succeeded += 1
        self._stats_selected[chosen] += 1
        backend = backends[chosen]
        return {
            "id": backend["id"],
            "address": backend["address"],
            "port": backend["port"],
        }

    def statistics(self):
        """返回本实例构造以来累计的选择统计的隔离副本。

        结果是键序固定为 policy、attempts、succeeded、failed、
        failures、backends 的新字典：policy 固定为 consistent_hash；
        attempts、succeeded、failed 为从零开始的非负整数累计计数；
        failures 是只含 no_healthy_backend 一个固定键的新字典；
        backends 按池声明顺序排列，每项键序固定为 backend_id、
        selected，即使后端从未被选中、当前不健康或随后恢复，也保留
        对应的零值或累计值。每次通过 key 校验的 select 调用增加
        attempts：成功时同时增加 succeeded 与实际返回后端的 selected
        （一次调用只计一次），无健康后端的失败只增加 failed 与
        failures 中的 no_healthy_backend。key 校验失败、explain、
        statistics 与健康标记变化均不累计任何事件；摘除或恢复后端
        保留累计值，多个绑定同一池的调度器实例各自维护互不影响的
        统计。返回字典、failures 字典与 backends 列表全部为新建对象，
        修改它们不污染内部状态或后续结果；池状态不变时重复调用逐字段
        相同。单次查询 O(n) 时间与 O(n) 返回空间。
        """
        return {
            "policy": "consistent_hash",
            "attempts": self._stats_attempts,
            "succeeded": self._stats_succeeded,
            "failed": self._stats_failed,
            "failures": {
                "no_healthy_backend":
                    self._stats_failures["no_healthy_backend"],
            },
            "backends": [
                {
                    "backend_id": backend["id"],
                    "selected": self._stats_selected[index],
                }
                for index, backend in enumerate(self._pool._backends)
            ],
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


class RetryChainScheduler:
    """跨后端有界重试链调度器（库接口，不接入 schedule）。

    调度器自身无状态、不维护游标或任何按链累计的数据：每次调用都由
    调用方显式传入本链已经失败的后端 id 列表，并依据共享池当前的健康
    标记重新评分。max_attempts 是包含首次选择在内的尝试总次数：
    failed_backend_ids 为空时给出首次选择，每失败一个后端后携带其 id
    再次调用即可取得下一次尝试。候选必须当前健康且不在失败链中；评分
    沿用 ConsistentHashScheduler 的规则（``[key, backend_id]`` 的紧凑
    JSON 经 SHA-256 取无符号大端整数，最高分胜出，同分取声明顺序
    最前者），address、port、weight 不参与评分。已在本链失败的后端
    即使恢复健康也不会在同一链被重选。健康变化在下一次调用立即生效，
    调用不修改池或调度器的任何状态，也不改写调用方传入的失败链。

    池中没有任何健康后端时抛出 NoAvailableBackendError；失败链长度
    已达到 max_attempts（首次选择在内的尝试次数已用完），或仍有健康
    后端但它们全部已在本链失败时，抛出 RetryExhaustedError。失败链
    长度超过 max_attempts 属于调用方错误，抛出 ValueError。
    """

    def __init__(self, pool, max_attempts):
        # 先完成 pool 与 max_attempts 的全部校验，再建立任何实例状态；
        # 校验失败时对象不会被部分构造。
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        # bool 是 int 的子类，上限必须显式排除布尔值。
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise ConfigurationError(
                "max_attempts must be an integer between 1 and 10000"
            )
        if not 1 <= max_attempts <= 10000:
            raise ConfigurationError(
                "max_attempts must be an integer between 1 and 10000"
            )
        self._pool = pool
        self._max_attempts = max_attempts

    def _validate_chain(self, key, failed_backend_ids):
        """校验 key 与失败链，返回与调用方列表隔离的失败 id 集合。

        key 沿用一致性哈希校验；失败链必须是列表，元素须为池内字符串
        id，重复项抛 ValueError，未知 id 抛 KeyError，长度超过
        max_attempts 抛 ValueError。全部输入校验完成后才返回，任何
        一项非法都不产生状态变化。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")
        if not isinstance(failed_backend_ids, list):
            raise TypeError("failed_backend_ids must be a list")
        failed = set()
        for backend_id in failed_backend_ids:
            if not isinstance(backend_id, str):
                raise TypeError("failed backend id must be a string")
            if backend_id in failed:
                raise ValueError(
                    f"duplicate failed backend id: {backend_id!r}"
                )
            if backend_id not in self._pool._index:
                raise KeyError(backend_id)
            failed.add(backend_id)
        if len(failed_backend_ids) > self._max_attempts:
            raise ValueError(
                "failed_backend_ids length must not exceed max_attempts"
            )
        return failed

    def next_backend(self, key, failed_backend_ids):
        """返回重试链上的下一个当前健康且未在本链失败的后端。

        结果是键序固定为 id、address、port 的新字典；池状态不变且
        失败链相同时，重复调用返回逐字段相同的结果，选择过程不修改
        池、调度器或调用方列表。key 校验与
        ConsistentHashScheduler.select 相同：非字符串抛 TypeError，
        空字符串抛 ValueError。failed_backend_ids 必须是列表：非列表
        抛 TypeError，元素非字符串抛 TypeError，重复元素抛
        ValueError，含池中未知 id 抛 KeyError，长度超过 max_attempts
        抛 ValueError；全部输入校验完成后才做选择判定。池中没有任何
        健康后端时抛 NoAvailableBackendError；失败链已含
        max_attempts 个 id（首次选择在内的尝试次数已用完），或健康
        后端均已在本链失败时，抛 RetryExhaustedError。单次查询
        O(n+m) 时间、O(m) 额外空间，m 为失败链长度且不超过
        max_attempts。
        """
        failed = self._validate_chain(key, failed_backend_ids)
        backends = self._pool._backends
        chosen = -1
        best_score = -1
        saw_healthy = False
        # 预算用尽时仍需扫描以区分“没有任何健康后端”，但不再记录候选：
        # 已失败后端即使恢复也不在同一链重选。
        budget_left = len(failed_backend_ids) < self._max_attempts
        for index, backend in enumerate(backends):
            if not backend["healthy"]:
                continue
            saw_healthy = True
            if not budget_left or backend["id"] in failed:
                continue
            # 评分与 ConsistentHashScheduler.select 完全一致：仅键与
            # 后端 id 参与评分，摘要按无符号大端整数比较。
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
        if not saw_healthy:
            raise NoAvailableBackendError("no healthy backend available")
        if not budget_left or chosen < 0:
            raise RetryExhaustedError("retry chain is exhausted")
        backend = backends[chosen]
        return {
            "id": backend["id"],
            "address": backend["address"],
            "port": backend["port"],
        }

    def explain(self, key, failed_backend_ids):
        """返回一次重试链选择的可重放解释，不改变任何状态。

        与 next_backend 采用完全相同的输入校验与选择规则，校验失败
        抛出同样的异常，但没有可选后端时不抛出选择异常。结果是键序
        固定为 policy、key、failed_backend_ids、candidates、selected、
        outcome、reason 的新字典：policy 固定为 retry_chain；
        failed_backend_ids 是与入参隔离、保持原顺序的新列表；
        candidates 按后端声明顺序排列，每项键序固定为 backend_id、
        healthy、failed、score，健康后端的 score 为保留前导零的 64 位
        小写十六进制 SHA-256 摘要（与一致性哈希 explain 一致），不
        健康后端不参与比较且 score 为 None，failed 表示该 id 是否在
        失败链中。存在可选后端时 selected 是键序固定为 id、address、
        port 的新字典，与相同入参下 next_backend 的返回逐字段一致，
        outcome 为 selected；失败链为空时 reason 为 initial_selection，
        否则为 retry_after_failure。没有可选后端时 selected 为 None、
        outcome 为 failed：池中没有任何健康后端时 reason 为
        no_healthy_backend，其余情形（尝试上限已用完或健康后端均已
        失败）为 retries_exhausted。返回结果与内部状态隔离，池状态与
        入参不变时重复查询逐字段相同。单次查询 O(n+m) 时间，除返回
        的 O(n) 解释结果外只使用 O(m) 额外空间。
        """
        failed = self._validate_chain(key, failed_backend_ids)
        backends = self._pool._backends
        candidates = []
        chosen = -1
        best_score = -1
        saw_healthy = False
        budget_left = len(failed_backend_ids) < self._max_attempts
        for index, backend in enumerate(backends):
            healthy = backend["healthy"]
            is_failed = backend["id"] in failed
            if not healthy:
                candidates.append(
                    {
                        "backend_id": backend["id"],
                        "healthy": False,
                        "failed": is_failed,
                        "score": None,
                    }
                )
                continue
            saw_healthy = True
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
                    "healthy": True,
                    "failed": is_failed,
                    "score": digest.hex(),
                }
            )
            # 预算已用尽或已在本链失败的后端不参与选择；只在严格更大
            # 时替换，摘要相同保留声明顺序最前者。
            if budget_left and not is_failed and score > best_score:
                best_score = score
                chosen = index
        if not saw_healthy:
            selected = None
            outcome = "failed"
            reason = "no_healthy_backend"
        elif not budget_left or chosen < 0:
            selected = None
            outcome = "failed"
            reason = "retries_exhausted"
        else:
            backend = backends[chosen]
            selected = {
                "id": backend["id"],
                "address": backend["address"],
                "port": backend["port"],
            }
            outcome = "selected"
            reason = (
                "initial_selection"
                if not failed_backend_ids
                else "retry_after_failure"
            )
        return {
            "policy": "retry_chain",
            "key": key,
            "failed_backend_ids": list(failed_backend_ids),
            "candidates": candidates,
            "selected": selected,
            "outcome": outcome,
            "reason": reason,
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
_EVENT_FIELDS = ("sequence", "operation", "now", "flow", "backend_id")
_EVENT_OPERATIONS = ("open", "activity", "close", "advance")


def _validate_log_event(event, position):
    """校验单条日志事件的结构，返回规范化后的执行参数元组。

    任何结构问题（事件不是对象、未知或缺失字段、类型错误、序号不连续、
    操作与可空字段组合不符）都统一抛出 ConfigurationError；不会修改
    调用方对象。返回 (operation, now, flow, backend_id)，其中 flow 为
    规范化五元组或 None（advance），backend_id 为字符串或 None。
    """
    location = f"event at position {position}"
    if not isinstance(event, dict):
        raise ConfigurationError(f"{location}: expected an object")
    unknown = [key for key in event if key not in _EVENT_FIELDS]
    if unknown:
        raise ConfigurationError(f"{location}: unknown field {unknown[0]!r}")
    missing = [key for key in _EVENT_FIELDS if key not in event]
    if missing:
        raise ConfigurationError(f"{location}: missing field {missing[0]!r}")

    sequence = event["sequence"]
    # bool 是 int 的子类，序号必须显式排除布尔值。
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        raise ConfigurationError(f"{location}: sequence must be an integer")
    if sequence != position:
        raise ConfigurationError(
            f"{location}: sequence must increase contiguously from zero"
        )

    operation = event["operation"]
    if not isinstance(operation, str) or operation not in _EVENT_OPERATIONS:
        raise ConfigurationError(
            f"{location}: operation must be one of {_EVENT_OPERATIONS!r}"
        )

    now = event["now"]
    # bool 是 int 的子类，时间戳必须显式排除布尔值。
    if isinstance(now, bool) or not isinstance(now, int):
        raise ConfigurationError(f"{location}: now must be an integer")
    if now < 0:
        raise ConfigurationError(
            f"{location}: now must be a non-negative integer"
        )

    flow = event["flow"]
    backend_id = event["backend_id"]
    if operation == "advance":
        if flow is not None or backend_id is not None:
            raise ConfigurationError(
                f"{location}: advance requires null flow and backend_id"
            )
        prepared = None
    else:
        try:
            prepared = _validate_flow(flow)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{location}: {exc}") from None
    if operation == "open":
        if not isinstance(backend_id, str):
            raise ConfigurationError(
                f"{location}: open requires a string backend_id"
            )
    elif backend_id is not None:
        raise ConfigurationError(
            f"{location}: {operation} requires a null backend_id"
        )
    return operation, now, prepared, backend_id


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

    每次 open_connection、record_activity、close_connection 或
    advance 成功返回后，按调用顺序向内存事件日志追加一个事件（幂等的
    重复建立与重复关闭也各记录一次；抛出异常的调用不新增或改写日志）。
    日志不记录查询与隐式到期项：到期由重放相同 now 时按既有规则重新
    计算。``export_log()`` 把日志导出为无末尾换行的紧凑 JSON 数组；
    类级入口 ``replay_log(pool, idle_timeout, hard_timeout, text)``
    先完成解析与结构校验，再按 sequence 在新表上重放并返回该表。
    单次追加 O(1) 时间与空间，导出 O(e)，重放状态空间 O(e+n)，
    e 为事件数、n 为连接记录数。
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
        # 确定性内存事件日志：只在四个生命周期操作成功返回后追加，
        # 元素为键序固定的新字典，sequence 从零连续递增。
        self._events = []

    def _append_event(self, operation, now, flow, backend_id):
        """把一次成功操作追加到内存事件日志，O(1) 时间与空间。"""
        self._events.append(
            {
                "sequence": len(self._events),
                "operation": operation,
                "now": now,
                "flow": flow,
                "backend_id": backend_id,
            }
        )

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
                # 幂等的重复建立也记录一次事件。
                self._append_event("open", now, dict(prepared), backend_id)
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
        self._append_event("open", now, dict(prepared), backend_id)
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
        self._append_event("activity", now, dict(prepared), None)
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
        # 幂等的重复关闭也记录一次事件。
        self._append_event("close", now, dict(prepared), None)
        return _copy_record(record)

    def advance(self, now):
        """把时间推进到 now，返回本次到期的记录副本（按建立顺序）。"""
        self._validate_now(now)
        self._now = now
        expired = self._process_expirations(now)
        self._append_event("advance", now, None, None)
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

    def export_log(self):
        """把内存事件日志导出为无末尾换行的紧凑 JSON 数组字符串。

        数组按调用顺序排列，每个事件对象的键固定为 sequence、
        operation、now、flow、backend_id：sequence 从零连续递增；
        operation 为 open、activity、close、advance 之一；open 事件
        记录规范化五元组、后端 id 与传入时间，activity 与 close 事件
        的 backend_id 为 null，advance 事件的 flow 与 backend_id 均为
        null；五元组保持 src_address、src_port、dst_address、dst_port、
        protocol 的既有字段顺序。日志只记录成功返回的调用（含幂等的
        重复建立与重复关闭），不记录查询与隐式到期项。状态不变时重复
        导出逐字节一致；导出结果为新建字符串，调用方对象与返回内容都
        不能污染内部日志。O(e) 时间，e 为事件数。
        """
        return json.dumps(
            self._events, separators=(",", ":"), ensure_ascii=False
        )

    @classmethod
    def replay_log(cls, pool, idle_timeout, hard_timeout, text):
        """按事件日志在新 ConnectionTable 上重放并返回该表。

        先完成 text 的解析与全部事件的结构校验，再按 sequence 在绑定
        pool 的新表上依次执行；日志不记录查询与隐式到期项，到期由重放
        相同 now 时按既有规则重新计算。text 非字符串抛出 TypeError；
        非法 JSON、顶层非数组、未知或缺失字段、类型错误、序号不连续、
        操作与可空字段组合不符，以及按给定池和超时无法合法执行的事件，
        统一抛出 ConfigurationError。任何失败都不修改传入池，也不暴露
        部分结果。成功时返回表的 connections、active_connections 与
        再次导出的日志与原表逐字段、逐字节一致。idle_timeout 与
        hard_timeout 沿用构造函数的既有校验。重放的状态空间为 O(e+n)，
        e 为事件数、n 为连接记录数。
        """
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        try:
            events = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                f"invalid JSON: {exc}"
            ) from None
        if not isinstance(events, list):
            raise ConfigurationError("event log must be a JSON array")
        # 先完成全部事件的结构校验，再建立任何可见状态。
        prepared = [
            _validate_log_event(event, position)
            for position, event in enumerate(events)
        ]

        table = cls(pool, idle_timeout, hard_timeout)
        for position, (operation, now, flow, backend_id) in enumerate(
            prepared
        ):
            try:
                if operation == "open":
                    table.open_connection(flow, backend_id, now)
                elif operation == "activity":
                    table.record_activity(flow, now)
                elif operation == "close":
                    table.close_connection(flow, now)
                else:
                    table.advance(now)
            except (TypeError, ValueError, KeyError,
                    ConnectionStateError) as exc:
                raise ConfigurationError(
                    f"event at position {position} cannot be replayed: "
                    f"{exc}"
                ) from None
        return table


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

    可选的 stale_timeout 为检查失联超时（省略或 None 表示不启用）。
    ``advance(now)`` 沿与 record_result 共享的全局单调时间线推进事件
    时钟：启用超时后，当前健康、已有合法检查结果且
    checked_at + stale_timeout <= now 的后端在本次推进中于共享池内
    变为不健康，连续成功与失败计数清零、checked_at 保留；从未检查或
    已经不健康的后端不产生变化。后续 record_result 从零累计成功，仍按
    recovery_threshold 回切。

    单次记录平均 O(1) 时间，advance 与 statuses 完整查询 O(n) 时间，
    空间 O(n)；``explain`` 单次为 O(1) 时间与 O(1) 额外空间。
    ``record_batch`` 在同一事件时刻原子提交一批结果：先完成整批校验
    与判定再一次性提交，任一失败不留下部分修改，k 个条目使用 O(k)
    时间与 O(k) 暂存及返回空间。
    """

    def __init__(self, pool, failure_threshold, recovery_threshold,
                 stale_timeout=None):
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
        # stale_timeout 省略或为 None 时不启用检查失联超时；
        # bool 是 int 的子类，显式值必须显式排除布尔值。
        if stale_timeout is not None and (
            isinstance(stale_timeout, bool)
            or not isinstance(stale_timeout, int)
            or stale_timeout <= 0
        ):
            raise ConfigurationError(
                "stale_timeout must be a positive integer or None"
            )
        self._pool = pool
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold
        self._stale_timeout = stale_timeout
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

    def _validate_event(self, backend_id, success, now):
        """校验一次检查事件，返回后端在声明顺序中的位置。

        校验顺序与 record_result 完全一致：backend_id 非字符串、success
        非布尔或 now 类型错误抛出 TypeError，负 now 抛出 ValueError，
        未知 id 抛出 KeyError，全局时间回退抛出 ConnectionStateError。
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
        return index

    def _project(self, state, actual, success):
        """按记录规则纯计算事件后的计数、健康状态与原因，不修改任何状态。

        以池中实际健康标记 actual 为准：与跟踪器已知状态不一致（外部
        set_healthy 改写）时先按真实记录规则把连续计数重置为零，再累计
        本次事件。返回 (consecutive_failures, consecutive_successes,
        healthy, changed, reason)：actual 为健康且连续失败达到
        failure_threshold 时摘除并给出 failure_threshold_reached；
        actual 为不健康且连续成功达到 recovery_threshold 时回切并给出
        recovery_threshold_reached；其余按 success 给出
        success_recorded 或 failure_recorded。
        """
        failures = state["consecutive_failures"]
        successes = state["consecutive_successes"]
        if actual != state["known_healthy"]:
            failures = 0
            successes = 0
        if success:
            successes += 1
            failures = 0
        else:
            failures += 1
            successes = 0
        healthy = actual
        changed = False
        if actual and failures >= self._failure_threshold:
            healthy = False
            changed = True
            reason = "failure_threshold_reached"
        elif not actual and successes >= self._recovery_threshold:
            healthy = True
            changed = True
            reason = "recovery_threshold_reached"
        else:
            reason = "success_recorded" if success else "failure_recorded"
        return failures, successes, healthy, changed, reason

    def _apply_event(self, index, backend_id, success, now, actual,
                     projection):
        """把已判定的事件写入跟踪状态与共享池，返回对外结果字典。

        只在全部校验与判定完成后调用：外部 set_healthy 造成的状态偏移
        在此以池中实际状态为准落盘，达到阈值时经 BackendPool.set_healthy
        摘除或回切；同时更新 checked_at、changed 与幂等记录。返回与
        内部状态隔离的新字典，键序固定为 backend_id、healthy、changed、
        consecutive_failures、consecutive_successes、checked_at。
        """
        failures, successes, healthy, changed, _reason = projection
        state = self._states[index]
        if actual != state["known_healthy"]:
            state["known_healthy"] = actual
        state["consecutive_failures"] = failures
        state["consecutive_successes"] = successes
        if changed:
            self._pool.set_healthy(backend_id, healthy)
            state["known_healthy"] = healthy

        state["checked_at"] = now
        state["changed"] = changed
        result = {
            "backend_id": backend_id,
            "healthy": healthy,
            "changed": changed,
            "consecutive_failures": failures,
            "consecutive_successes": successes,
            "checked_at": now,
        }
        state["last_now"] = now
        state["last_success"] = success
        state["last_result"] = result
        return dict(result)

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
        index = self._validate_event(backend_id, success, now)

        state = self._states[index]
        if state["last_now"] is not None and now == state["last_now"]:
            if success != state["last_success"]:
                raise ConnectionStateError(
                    f"conflicting result for backend {backend_id!r} "
                    f"at now={now}"
                )
            return dict(state["last_result"])

        # 外部 set_healthy 造成的状态偏移在下一次记录时被发现：
        # 以池中实际状态为准，并重置该后端的连续计数。判定（含阈值
        # 摘除/回切与原因）与 explain 共用同一纯计算，保持顺序一致。
        actual = self._pool._backends[index]["healthy"]
        projection = self._project(state, actual, success)
        result = self._apply_event(
            index, backend_id, success, now, actual, projection
        )
        self._now = now
        return result

    def record_batch(self, results, now):
        """在同一事件时刻原子提交一批健康检查结果。

        results 是按调用方顺序排列的列表，每项为仅含 backend_id 与
        success 的字典，批次中的 backend_id 不得重复。调用先完成整批
        校验，再产生任何修改：results 不是列表、条目不是字典、字段
        缺失或多余、backend_id 非字符串或 success 非布尔时抛出
        TypeError；空列表或重复 id 抛出 ValueError；未知 id 抛出
        KeyError；now 沿用 record_result 的非布尔非负整数与全局单调
        时间线规则，类型错误抛出 TypeError，负值抛出 ValueError，时间
        回退或某后端在同一 now 已记录相反 success 时抛出
        ConnectionStateError。任一条目失败时，事件时钟、全部计数、
        最近结果与 BackendPool 健康标记均保持不变。

        整批判定以调用开始时的池健康快照与跟踪状态为准，输入顺序不
        改变其他条目的阈值结论；外部 set_healthy 造成的状态偏移仍按
        单条规则先重置对应计数再累计。成功时按输入顺序返回独立的新
        列表，每项是键序固定为 backend_id、healthy、changed、
        consecutive_failures、consecutive_successes、checked_at 的
        新字典，字段语义与 record_result 一致，并一次性提交全部结果
        及必要的健康变更。同一后端在同一 now 以相同 success 再次提交
        时幂等返回既有结果副本，不重复累计；修改返回对象不影响后续
        查询。单次调用对 k 个条目使用 O(k) 时间与 O(k) 暂存及返回
        空间。
        """
        if not isinstance(results, list):
            raise TypeError("results must be a list")
        if not results:
            raise ValueError("results must not be empty")
        prepared = []
        seen_ids = set()
        for position, entry in enumerate(results):
            location = f"result at position {position}"
            if not isinstance(entry, dict):
                raise TypeError(f"{location}: expected an object")
            unknown = [
                key for key in entry if key not in ("backend_id", "success")
            ]
            if unknown:
                raise TypeError(f"{location}: unknown field {unknown[0]!r}")
            missing = [
                key
                for key in ("backend_id", "success")
                if key not in entry
            ]
            if missing:
                raise TypeError(f"{location}: missing field {missing[0]!r}")
            backend_id = entry["backend_id"]
            if not isinstance(backend_id, str):
                raise TypeError("backend id must be a string")
            success = entry["success"]
            if not isinstance(success, bool):
                raise TypeError("success must be a boolean")
            if backend_id in seen_ids:
                raise ValueError(f"duplicate backend id: {backend_id!r}")
            seen_ids.add(backend_id)
            prepared.append((backend_id, success))
        # bool 是 int 的子类，时间戳必须显式排除布尔值。
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be an integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        indices = []
        for backend_id, _success in prepared:
            index = self._pool._index.get(backend_id)
            if index is None:
                raise KeyError(backend_id)
            indices.append(index)
        if self._now is not None and now < self._now:
            raise ConnectionStateError("now must not move backwards")

        # 整批只读判定：以调用开始时的池健康快照与跟踪状态为准，
        # 任何同刻冲突都在产生修改之前抛出。批次内 id 互不相同，
        # 各条目的投影互不影响。
        backends = self._pool._backends
        plans = []
        for (backend_id, success), index in zip(prepared, indices):
            state = self._states[index]
            if state["last_now"] is not None and now == state["last_now"]:
                if success != state["last_success"]:
                    raise ConnectionStateError(
                        f"conflicting result for backend {backend_id!r} "
                        f"at now={now}"
                    )
                plans.append((index, backend_id, success, None, None))
                continue
            actual = backends[index]["healthy"]
            projection = self._project(state, actual, success)
            plans.append((index, backend_id, success, actual, projection))

        # 全部校验与判定完成后才一次性提交：写跟踪状态、必要的池健康
        # 变更与幂等记录，最后推进事件时钟。
        committed = []
        for index, backend_id, success, actual, projection in plans:
            state = self._states[index]
            if projection is None:
                committed.append(dict(state["last_result"]))
                continue
            committed.append(
                self._apply_event(
                    index, backend_id, success, now, actual, projection
                )
            )
        self._now = now
        return committed

    def explain(self, backend_id, success, now):
        """只读预览下一次检查会保持、摘除还是回切后端，不修改任何状态。

        与 record_result 采用完全相同的校验与判定顺序：backend_id 非
        字符串、success 非布尔或 now 类型错误抛出 TypeError，负 now
        抛出 ValueError，未知 id 抛出 KeyError，全局时间回退或同一后端
        同一 now 的冲突结果抛出 ConnectionStateError；任何失败都不改变
        状态。结果是键序固定为 backend_id、success、checked_at、
        healthy、changed、consecutive_failures、consecutive_successes、
        reason 的独立新字典：普通事件的健康状态、变化标记、连续计数与
        检查时间与当前状态下随后记录同一事件的对应字段逐字段一致，
        reason 在达到失败/恢复阈值时为 failure_threshold_reached/
        recovery_threshold_reached，其余为 success_recorded/
        failure_recorded。外部 set_healthy 改写健康标记后，按池中实际
        状态重置该后端计数再预测。同一后端在同一 now 重复相同 success
        时 reason 为 idempotent_repeat，其余字段预测原幂等结果。解释不
        修改时间、计数、最近结果或池；池状态不变时重复解释逐字段一致，
        解释后立即记录同一事件也与解释一致；修改返回对象不影响后续
        行为。单次查询 O(1) 时间与 O(1) 额外空间。
        """
        index = self._validate_event(backend_id, success, now)

        state = self._states[index]
        if state["last_now"] is not None and now == state["last_now"]:
            if success != state["last_success"]:
                raise ConnectionStateError(
                    f"conflicting result for backend {backend_id!r} "
                    f"at now={now}"
                )
            recorded = state["last_result"]
            return {
                "backend_id": backend_id,
                "success": success,
                "checked_at": recorded["checked_at"],
                "healthy": recorded["healthy"],
                "changed": recorded["changed"],
                "consecutive_failures": recorded["consecutive_failures"],
                "consecutive_successes": recorded["consecutive_successes"],
                "reason": "idempotent_repeat",
            }

        actual = self._pool._backends[index]["healthy"]
        failures, successes, healthy, changed, reason = self._project(
            state, actual, success
        )
        return {
            "backend_id": backend_id,
            "success": success,
            "checked_at": now,
            "healthy": healthy,
            "changed": changed,
            "consecutive_failures": failures,
            "consecutive_successes": successes,
            "reason": reason,
        }

    def advance(self, now):
        """把事件时钟推进到 now，摘除检查失联超时的健康后端。

        now 与 record_result 共享同一全局单调时间线：非布尔整数类型
        错误抛出 TypeError，负值抛出 ValueError，早于已推进到的时刻
        抛出 ConnectionStateError；先完成全部校验再推进时间和修改
        状态，校验失败不留下任何部分变更。同一时刻重复推进是幂等的。

        启用 stale_timeout 时，按池声明顺序扫描：池中当前健康、已有
        合法检查结果（checked_at 非 None）且
        checked_at + stale_timeout <= now 的后端在本次推进中经
        BackendPool.set_healthy 标为不健康，连续成功与失败计数清零、
        checked_at 保留，绑定同一池的调度器在下一次选择时即可看到；
        从未检查或已经不健康的后端不产生变化。返回全部新摘除项组成
        的新列表（按池声明顺序），每项键序固定为 backend_id、
        healthy、changed、checked_at、expired_at、reason，其中
        healthy 为 False、changed 为 True、checked_at 为最后检查
        时间、expired_at 等于本次 now、reason 为
        health_check_timeout；没有变化或未启用超时时返回空列表。
        返回的列表与字典都与内部状态隔离，修改它们不影响后续行为。
        单次推进最坏 O(n) 时间，除返回列表外只使用 O(1) 额外空间，
        不保存事件历史。
        """
        # bool 是 int 的子类，时间戳必须显式排除布尔值。
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be an integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        if self._now is not None and now < self._now:
            raise ConnectionStateError("now must not move backwards")
        self._now = now

        if self._stale_timeout is None:
            return []
        removed = []
        for index, backend in enumerate(self._pool._backends):
            if not backend["healthy"]:
                continue
            state = self._states[index]
            checked_at = state["checked_at"]
            if checked_at is None:
                continue
            if checked_at + self._stale_timeout > now:
                continue
            backend_id = backend["id"]
            self._pool.set_healthy(backend_id, False)
            state["known_healthy"] = False
            state["consecutive_failures"] = 0
            state["consecutive_successes"] = 0
            state["changed"] = True
            removed.append(
                {
                    "backend_id": backend_id,
                    "healthy": False,
                    "changed": True,
                    "checked_at": checked_at,
                    "expired_at": now,
                    "reason": "health_check_timeout",
                }
            )
        return removed

    def statuses(self):
        """按后端声明顺序返回全部后端的状态（新字典组成的新列表）。

        healthy 反映共享池中的当前标记；从未检查的后端 checked_at
        为 None、changed 为 False、连续计数为零。超时摘除的后端立即
        反映为不健康，checked_at 保留最后检查时间，连续计数为零。
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


class CircuitBreakerScheduler:
    """按后端维护熔断状态的一致性哈希调度器（库接口，不接入 schedule）。

    为池中每个后端独立维护 closed、open、half_open 三种状态与连续失败
    计数：closed 下成功清零计数，连续失败达到 failure_threshold 时转为
    open 并记录 open_until = now + reset_timeout；open 后端不参与选择，
    到期（open_until <= now）后允许一次 half_open 探测，探测成功则关闭
    并清零，探测失败则以当前 now 重新打开（新 open_until 为
    now + reset_timeout）。熔断状态只存在于本调度器，不写入共享池的
    健康标记，也不影响绑定同一池的其他调度器。

    时间只由调用方显式传入的 now 驱动，record_result、select 与 explain
    共享同一单调时间线：now 必须是非布尔的非负整数，回退抛出
    ConnectionStateError。open→half_open 的到期转换是惰性的：只由携带
    now 的调用观察，不提前改写状态，因此 explain 不推进时间或占用探测。
    select 选中期后端后立即占用唯一的半开探测：在对应探测结果经
    record_result 记录之前，该后端不再参与后续选择。

    选择沿用一致性哈希的 key 校验与评分（``[key, backend_id]`` 的紧凑
    JSON 经 SHA-256 取无符号大端整数，最高分胜出，同分取声明顺序
    最前者）：候选为池中当前健康且熔断状态为 closed 的后端，加上至多
    一个当前健康、已到期且探测未被占用的 half_open 后端；half_open
    候选与 closed 候选一同按分数比较。没有任何健康后端时抛出
    NoAvailableBackendError；存在健康后端但全部处于 open 或其唯一探测
    已被占用时抛出 CircuitOpenError。

    ``statistics()`` 返回本实例构造以来累计的选择统计：每次通过 key 与
    now 校验的 select 调用（无论成功或抛出失败）都计入 attempts，成功
    时同时计入 succeeded 与实际返回后端的 selected（选中 closed 或取得
    half_open 探测资格都只计一次），没有任何健康后端的失败只计入 failed
    与 failures 中的 no_healthy_backend，存在健康后端但全部熔断或探测
    占用的失败只计入 failed 与 all_circuits_open。key 或 now 校验失败、
    时间回退、record_result、explain、statistics 与健康标记变化均不累计
    任何事件，也不清零统计；摘除或恢复后端与熔断状态转换保留全部累计值。
    统计只描述通过本实例发生的 select 调用，不影响共享同一后端池的其他
    调度器实例。记录为 O(1)，选择与解释最坏 O(n)，状态空间 O(n)。
    """

    _CLOSED = "closed"
    _OPEN = "open"
    _HALF_OPEN = "half_open"

    def __init__(self, pool, failure_threshold, reset_timeout):
        # 先完成 pool 与两个配置值的全部校验，再建立任何实例状态；
        # 校验失败时对象不会被部分构造。
        if not isinstance(pool, BackendPool):
            raise TypeError("pool must be a BackendPool instance")
        # bool 是 int 的子类，阈值与超时必须显式排除布尔值。
        for name, value in (
            ("failure_threshold", failure_threshold),
            ("reset_timeout", reset_timeout),
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
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._now = None
        # 每个后端的熔断跟踪状态，按声明顺序平行于 pool._backends。
        self._states = [
            {
                "state": self._CLOSED,
                "failures": 0,
                "open_until": None,
                "probe_in_flight": False,
                # 幂等记录：上一次 record_result 的时刻、结果与返回字典。
                "last_now": None,
                "last_success": None,
                "last_result": None,
            }
            for _backend in pool._backends
        ]
        # 累计统计按声明顺序与后端平行保存；统计只增不减，健康标记变化、
        # record_result、explain 与只读查询都不触碰这些值，额外空间 O(n)。
        self._stats_attempts = 0
        self._stats_succeeded = 0
        self._stats_failed = 0
        self._stats_failures = {
            "no_healthy_backend": 0,
            "all_circuits_open": 0,
        }
        self._stats_selected = [0] * len(pool._backends)

    def _validate_time(self, now):
        """校验共享时间入口：非布尔非负整数且不得回退。"""
        # bool 是 int 的子类，时间戳必须显式排除布尔值。
        if isinstance(now, bool) or not isinstance(now, int):
            raise TypeError("now must be an integer")
        if now < 0:
            raise ValueError("now must be a non-negative integer")
        if self._now is not None and now < self._now:
            raise ConnectionStateError("now must not move backwards")

    @staticmethod
    def _effective_state(state, now):
        """返回 now 时刻观察到的熔断状态，惰性到期但不修改存储状态。

        open 后端的 open_until 不晚于 now 时观察为 half_open；其余情况
        与存储状态一致。
        """
        if (
            state["state"] == CircuitBreakerScheduler._OPEN
            and state["open_until"] is not None
            and state["open_until"] <= now
        ):
            return CircuitBreakerScheduler._HALF_OPEN
        return state["state"]

    def record_result(self, backend_id, success, now):
        """记录一次发往后端的请求结果，返回键序固定的状态副本。

        backend_id 非字符串抛出 TypeError，success 非布尔抛出
        TypeError，now 类型错误抛出 TypeError，负 now 抛出
        ValueError，未知 id 抛出 KeyError，时间回退或同一后端同一 now
        的相反结果抛出 ConnectionStateError；任何失败都不改变时钟或
        熔断状态。closed 下成功把连续失败计数清零；连续失败达到
        failure_threshold 时转为 open，open_until 为
        now + reset_timeout。open 到期后的第一次记录视为 half_open
        探测结果：成功转为 closed 并清零，失败以当前 now 重新打开。
        同一后端同一 now 的相同结果幂等返回上次结果的副本，不重复
        累计；探测结果同时清除探测占用。返回新字典的键序固定为
        backend_id、state、changed、failures、open_until、reason，
        open_until 在非 open 状态下为 None。单次记录 O(1) 时间与
        O(1) 额外空间。
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

        observed = self._effective_state(state, now)
        if observed == self._OPEN:
            # open 未到期前不接受请求结果：状态机只定义了 closed 累计与
            # half_open 探测两种记录入口，提前记录与当前熔断状态冲突。
            raise ConnectionStateError(
                f"circuit for backend {backend_id!r} is open until "
                f"{state['open_until']}"
            )
        failures = state["failures"]
        changed = False
        probe_finished = observed == self._HALF_OPEN
        if observed == self._HALF_OPEN:
            # half_open 探测：成功关闭并清零，失败重新打开；无论成败，
            # 探测占用都随结果清除。
            if success:
                stored = self._CLOSED
                failures = 0
                open_until = None
                changed = True
                reason = "probe_succeeded_closed"
            else:
                stored = self._OPEN
                open_until = now + self._reset_timeout
                changed = True
                reason = "probe_failed_reopened"
        elif success:
            stored = self._CLOSED
            open_until = None
            if failures != 0:
                failures = 0
                changed = True
            reason = "success_recorded"
        else:
            failures += 1
            if failures >= self._failure_threshold:
                stored = self._OPEN
                open_until = now + self._reset_timeout
                changed = True
                reason = "failure_threshold_reached"
            else:
                stored = self._CLOSED
                open_until = None
                reason = "failure_recorded"

        # 全部校验与判定完成后才一次性提交状态并推进时钟。
        if probe_finished:
            state["probe_in_flight"] = False
        state["state"] = stored
        state["failures"] = failures
        state["open_until"] = open_until
        self._now = now
        result = {
            "backend_id": backend_id,
            "state": stored,
            "changed": changed,
            "failures": failures,
            "open_until": open_until,
            "reason": reason,
        }
        state["last_now"] = now
        state["last_success"] = success
        state["last_result"] = result
        return dict(result)

    def _plan(self, key, now):
        """按选择规则扫描，返回候选与选择判定，不修改任何状态。

        返回 (candidates, chosen, saw_healthy, eligible)：candidates 为
        解释候选项列表，chosen 为选中位置（-1 表示无候选），saw_healthy
        区分没有任何健康后端，eligible 为存在可参与比较的候选。
        """
        backends = self._pool._backends
        candidates = []
        chosen = -1
        best_score = -1
        saw_healthy = False
        has_eligible = False
        for index, backend in enumerate(backends):
            healthy = backend["healthy"]
            state = self._states[index]
            observed = self._effective_state(state, now)
            open_until = state["open_until"]
            probe_in_flight = state["probe_in_flight"]
            if not healthy:
                candidates.append(
                    {
                        "backend_id": backend["id"],
                        "healthy": False,
                        "state": observed,
                        "open_until": open_until,
                        "probe_in_flight": probe_in_flight,
                        "score": None,
                    }
                )
                continue
            saw_healthy = True
            # 只比较 closed 后端，加上至多一个未占用探测的到期 half_open
            # 后端；未到期 open、half_open 探测已占用都不参与比较。
            eligible = observed == self._CLOSED or (
                observed == self._HALF_OPEN and not probe_in_flight
            )
            if not eligible:
                candidates.append(
                    {
                        "backend_id": backend["id"],
                        "healthy": True,
                        "state": observed,
                        "open_until": open_until,
                        "probe_in_flight": probe_in_flight,
                        "score": None,
                    }
                )
                continue
            has_eligible = True
            digest = hashlib.sha256(
                json.dumps(
                    [key, backend["id"]],
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
            ).digest()
            candidates.append(
                {
                    "backend_id": backend["id"],
                    "healthy": True,
                    "state": observed,
                    "open_until": open_until,
                    "probe_in_flight": probe_in_flight,
                    "score": digest.hex(),
                }
            )
            score = int.from_bytes(digest, "big", signed=False)
            # 只在严格更大时替换，摘要相同保留声明顺序最前者。
            if score > best_score:
                best_score = score
                chosen = index
        return candidates, chosen, saw_healthy, has_eligible

    def select(self, key, now):
        """在健康且未熔断的后端中按一致性哈希选择，允许一次半开探测。

        key 沿用一致性哈希校验：非字符串抛出 TypeError，空字符串抛出
        ValueError；now 必须是非布尔非负整数且不得回退，类型错误抛出
        TypeError，负值抛出 ValueError，回退抛出
        ConnectionStateError，三种校验失败均不产生任何统计变化。成功时
        返回键序固定为 id、address、port 的新字典：在健康且 closed 的
        后端与至多一个健康、已到期且探测未被占用的 half_open 后端中按
        SHA-256 评分取最高者，同分取声明顺序最前者。通过全部校验后先把
        attempts 加一；成功时把 succeeded 与实际返回后端的 selected 各
        加一（选中 closed 或取得 half_open 探测资格都只计一次），命中
        计数与探测占用、时钟推进在同一提交点生效。选中 half_open 后端时
        立即占用其唯一探测，记录探测结果前该后端不再参与选择。没有任何
        健康后端时抛出 NoAvailableBackendError，只增加 failed 与
        failures 中的 no_healthy_backend；存在健康后端但全部处于 open
        或探测已占用时抛出 CircuitOpenError，只增加 failed 与
        all_circuits_open：两种失败都不留下后端命中、不推进时钟或改变
        熔断状态。最坏 O(n) 时间、除固定大小数据外额外 O(n) 用于候选
        判定（不向外返回），统计更新只增加 O(1) 开销。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")
        self._validate_time(now)

        # 通过 key 与 now 的全部校验后才计入尝试，包括随后抛出失败异常的调用。
        self._stats_attempts += 1
        _candidates, chosen, saw_healthy, has_eligible = self._plan(key, now)
        if not saw_healthy:
            self._stats_failed += 1
            self._stats_failures["no_healthy_backend"] += 1
            raise NoAvailableBackendError("no healthy backend available")
        if not has_eligible or chosen < 0:
            self._stats_failed += 1
            self._stats_failures["all_circuits_open"] += 1
            raise CircuitOpenError(
                "all healthy backends have open circuits or probes in flight"
            )

        state = self._states[chosen]
        # 全部判定完成后一次性提交：选中到期后端即占用唯一的半开探测，
        # 并把观察到的 half_open 落盘为存储状态；open_until 保留原值直到
        # 探测结果记录。探测占用、时钟推进与命中计数同时生效。
        if self._effective_state(state, now) == self._HALF_OPEN:
            state["state"] = self._HALF_OPEN
            state["probe_in_flight"] = True
        self._now = now
        self._stats_succeeded += 1
        self._stats_selected[chosen] += 1
        backend = self._pool._backends[chosen]
        return {
            "id": backend["id"],
            "address": backend["address"],
            "port": backend["port"],
        }

    def statistics(self):
        """返回本实例构造以来累计的选择统计的隔离副本。

        结果是键序固定为 policy、attempts、succeeded、failed、
        failures、backends 的新字典：policy 固定为 circuit_breaker；
        attempts、succeeded、failed 为从零开始的非负整数累计计数；
        failures 是键序固定为 no_healthy_backend、all_circuits_open 的
        新字典；backends 按池声明顺序排列，每项键序固定为 backend_id、
        selected，即使后端从未被选中、当前不健康、摘除后恢复或经历熔断
        转换，也保留对应的零值或累计值。每次通过 key 与 now 校验的
        select 调用增加 attempts：成功时同时增加 succeeded 与实际返回
        后端的 selected（选中 closed 或取得 half_open 探测资格都只计
        一次），没有任何健康后端的失败只增加 failed 与
        no_healthy_backend，存在健康后端但全部熔断或探测占用的失败只
        增加 failed 与 all_circuits_open。key 类型或空值错误、now 类型
        或取值错误与时间回退均不产生统计变化；record_result、explain、
        statistics、健康标记变化与配置热加载均不累计或清零统计；摘除或
        恢复后端与熔断转换保留累计值，多个绑定同一池的调度器实例各自
        维护互不影响的统计。返回字典、failures 字典与 backends 列表
        全部为新建对象，修改它们不污染内部状态或后续结果；状态不变时
        重复调用逐字段相同。单次查询 O(n) 时间与 O(n) 返回空间。
        """
        backends = self._pool._backends
        return {
            "policy": "circuit_breaker",
            "attempts": self._stats_attempts,
            "succeeded": self._stats_succeeded,
            "failed": self._stats_failed,
            "failures": {
                "no_healthy_backend":
                    self._stats_failures["no_healthy_backend"],
                "all_circuits_open":
                    self._stats_failures["all_circuits_open"],
            },
            "backends": [
                {
                    "backend_id": backend["id"],
                    "selected": self._stats_selected[index],
                }
                for index, backend in enumerate(backends)
            ],
        }

    def explain(self, key, now):
        """返回一次熔断选择的可重放解释，不推进时间或占用探测。

        与 select 采用完全相同的 key 与 now 校验：非字符串 key 抛
        TypeError，空字符串抛 ValueError；now 类型错误抛 TypeError，
        负值抛 ValueError，时间回退抛 ConnectionStateError；校验失败
        不改变任何状态。结果是键序固定为 policy、key、candidates、
        selected、outcome、reason 的新字典：policy 固定为
        circuit_breaker；candidates 按后端声明顺序排列，每项键序固定
        为 backend_id、healthy、state、open_until、probe_in_flight、
        score，state 为当前 now 惰性观察到的 closed、open 或
        half_open（open 到期即显示 half_open，但不提前落盘），
        open_until 为打开时刻加 reset_timeout（非 open 派生状态时为
        None），可参与比较的候选 score 为保留前导零的 64 位小写十六
        进制 SHA-256 摘要，其余候选 score 为 None。存在可选后端时
        selected 是键序固定为 id、address、port 的新字典，与池状态不
        变时紧随其后的 select 逐字段一致，outcome 为 selected，选中
        closed 后端时 reason 为 highest_score，选中 half_open 后端时
        为 half_open_probe；没有任何健康后端时 selected 为 None、
        outcome 为 failed、reason 为 no_healthy_backend，存在健康
        后端但全部熔断或探测占用时 reason 为 all_circuits_open。
        解释不推进时钟、不占用探测或改变任何熔断状态，修改返回对象
        不污染后续结果；相同输入序列下结果确定。最坏 O(n) 时间与
        O(n) 返回空间。
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key == "":
            raise ValueError("key must be a non-empty string")
        self._validate_time(now)

        candidates, chosen, saw_healthy, has_eligible = self._plan(key, now)
        if not saw_healthy:
            selected = None
            outcome = "failed"
            reason = "no_healthy_backend"
        elif not has_eligible or chosen < 0:
            selected = None
            outcome = "failed"
            reason = "all_circuits_open"
        else:
            backend = self._pool._backends[chosen]
            selected = {
                "id": backend["id"],
                "address": backend["address"],
                "port": backend["port"],
            }
            outcome = "selected"
            observed = self._effective_state(self._states[chosen], now)
            if observed == self._HALF_OPEN:
                reason = "half_open_probe"
            else:
                reason = "highest_score"
        return {
            "policy": "circuit_breaker",
            "key": key,
            "candidates": candidates,
            "selected": selected,
            "outcome": outcome,
            "reason": reason,
        }


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
