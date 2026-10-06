# load-balancer

负载均衡的后端选择、一致性哈希与可解释调度。

## 约束

* 仅使用 Python 标准库，不联网，不依赖第三方包。
* 行为必须确定：相同输入多次运行产生逐字节一致的输出；时间相关行为由显式注入的时钟驱动，不读墙上时钟。
* 所有结论可由公开接口与落盘产物独立验收。

## 公开入口

* 入口文件：`load_balancer.py` 
* 命令行：`python load_balancer.py --help` 
* 使用说明与行为契约以本文件为准；入口的签名、键序与既有语义在迭代中保持兼容。

## 状态

仓库初始为空，功能按增量需求持续构建。

## 调度策略

`schedule` 命令支持可选的 `--policy` 参数，省略时为 `round_robin`（输出与既有行为逐字节一致）；`--policy weighted_round_robin` 使用加权轮询。

* 后端配置可声明可选的 `weight` 字段：缺省按 1 处理，显式值必须是 1 至 10000 的整数；布尔、浮点、字符串、零、负数或越界值均属于 `ConfigurationError`（退出码 3）。
* 加权轮询按配置声明顺序构造逻辑循环，每个后端连续占有 `weight` 个位置。例如健康后端 A、B、C 权重为 2、1、3 时，选择顺序为 A、A、B、C、C、C 后重复。
* 调度器从上次成功位置的下一个逻辑位置开始，整段跳过不健康后端占有的位置；`set_healthy` 立即影响下一次选择，恢复健康的后端从游标后方下一次遇到的自身位置重新参与，不补发停用期间错过的次数。
* 实现不按权重展开保存重复后端项：调度器额外空间 O(n)，单次选择最坏 O(n)。

`--policy least_connections` 使用最少连接策略：

* 后端配置可声明可选的 `max_connections` 字段作为该后端的并发上限：缺省表示不限量（旧配置与原选择序列保持不变）；显式值必须是 1 至 1000000 的整数，布尔、浮点、字符串、零、负数或越界值均属于 `ConfigurationError`（退出码 3），全部配置校验成功后才建立池。普通轮询、加权轮询、一致性哈希与 `ConnectionTable` 不接入该限制，返回的后端字典不增加字段。
* 调度器为池中每个后端维护从零开始的活动连接数；每次选择只考察当前健康且活动连接数低于自身上限的后端（不限量后端始终通过容量筛选），取活动连接数最小者，计数相同时按声明顺序取最前者。选择成功后只把被选后端的计数加一，再返回与轮询调度器相同键序的后端字典。`weight` 仍按既有规则校验，但不参与比较。
* `schedule` 命令中的 count 次选择视为依次建立且未释放的连接；没有健康后端时仍以退出码 4 结束，存在健康后端但全部达到上限时在 stderr 输出键序为 type、message 的紧凑 JSON（类型 `BackendOverloadedError`、消息 `all healthy backends are at capacity`）并以退出码 5 结束；两种失败都不在 stdout 留下部分结果。`BackendOverloadedError` 可从 `load_balancer` 直接导入。
* 库接口提供 `release_connection(backend_id)` 与 `active_connections()`：释放成功只把对应计数减一，释放出的空位立即可用于下一次选择；重复释放到零以下抛出 `ConnectionStateError` 且任何计数不变；id 非字符串抛出 `TypeError`，id 不存在抛出 `KeyError`。查询返回与内部状态隔离、按声明顺序排列的新字典。
* 健康状态变化立即影响后续选择，但不清除或改写已有计数；不健康的后端仍允许释放既有连接，恢复健康后按保留的计数与上限重新参与比较。
* 构造为 O(n) 时间与 O(n) 额外空间，单次选择最坏 O(n)、额外空间 O(1)，单次释放 O(1)，计数查询 O(n)，不按容量展开存储。

## 一致性哈希（库接口）

库接口提供 `ConsistentHashScheduler(pool)`，可从 `load_balancer` 直接导入；`schedule` 命令不新增该策略，参数、输出、异常类型与退出码保持不变。

* `select(key)` 根据调用方给出的会话键选择一个当前健康后端。key 必须是非空字符串：非字符串抛出 `TypeError`，空字符串抛出 `ValueError`，两种失败都不产生状态变化；构造参数不是 `BackendPool` 时与其他调度器一样抛出 `TypeError`；没有健康后端时抛出 `NoAvailableBackendError`。
* 评分输入为由 key 和后端 id 组成的 JSON 数组，按 `ensure_ascii=False` 与紧凑分隔符序列化为 UTF-8 后计算 SHA-256，摘要按无符号大端整数解释，分数最大的后端胜出；摘要相同时按配置声明顺序选择最前者。后端的 address、port、weight 与声明位置均不参与评分（`weight` 仍由 `BackendPool` 按既有规则校验）。
* 调度器不维护轮询游标、连接计数或按键增长的缓存：同一键在后端集合与健康状态不变时，无论调用次数及与其他键的调用顺序如何，都返回键序固定为 id、address、port 的逐字段相同新字典。
* 每次选择只比较当前健康后端：某后端被标记为不健康后，原本未选择它的键保持原选择，原本选择它的键在其余健康后端中重新映射；该后端恢复健康后，相同键按原评分规则重新选择，此前属于它的键确定性地回到它。健康变化在下一次 `select` 立即生效，选择过程不改写池或调度器状态。
* 单次选择最坏 O(n) 时间，除摘要计算所需的固定大小数据外额外空间 O(1)。
* `explain(key)` 为一次一致性哈希选择给出可重放的解释：对 key 采用与 `select` 相同的类型和值校验（非字符串抛 `TypeError`、空字符串抛 `ValueError`，校验失败不改变状态），成功时返回键序固定为 `policy`、`key`、`candidates`、`selected`、`outcome`、`reason` 的新字典。`policy` 固定为 `consistent_hash`；`candidates` 按后端声明顺序排列，每项键序固定为 `backend_id`、`healthy`、`score`，健康后端的 `score` 为保留前导零的 64 位小写十六进制 SHA-256 摘要，不健康后端不参与比较且 `score` 为 `null`；`selected` 保持 `id`、`address`、`port` 的既有键序，`outcome` 为 `selected`、`reason` 为 `highest_score`，最高分相同时仍选择声明顺序最前的健康后端。
* 池状态不变时，`explain` 返回的 `selected` 与 `select` 对同一 key 的结果逐字段一致。没有健康后端时 `explain` 不抛 `NoAvailableBackendError`（`select` 仍抛出），而是返回全部候选、`selected` 为 `null`、`outcome` 为 `failed`、`reason` 为 `no_healthy_backend`。返回结果与内部状态隔离，相同 key 与相同池状态下多次查询逐字段相同；查询不修改池或调度器、不留下按键累计的状态，单次查询 O(n) 时间，除返回的 O(n) 解释结果外只使用 O(1) 额外空间。

## 连接生命周期表（库接口）

库接口提供 `ConnectionTable(pool, idle_timeout, hard_timeout)`，可从 `load_balancer` 直接导入；它不接入命令行，`schedule` 命令的参数、输出、异常类型与退出码保持不变，也不改变 `LeastConnectionsScheduler` 各自独立的连接计数。

* 连接以五元组标识：`src_address`、`src_port`、`dst_address`、`dst_port`、`protocol`。两个地址必须是非空字符串；两个端口必须是 1 至 65535 的整数（布尔值被拒绝）；协议必须是小写的 `"tcp"` 或 `"udp"`。`idle_timeout` 与 `hard_timeout` 必须是正整数，非法时抛出 `ConfigurationError`。
* 时间只由调用方显式传入的 `now` 驱动，不读墙上时钟。`now` 必须是非布尔的非负整数：类型错误抛出 `TypeError`，负值抛出 `ValueError`，`now` 回退抛出 `ConnectionStateError`。
* `open_connection(flow, backend_id, now)` 创建 active 记录：后端必须存在（否则 `KeyError`）且当前健康（否则 `ConnectionStateError`）。同一五元组已有 active 连接时，后端相同则返回原记录且不产生状态变化，后端不同则抛出 `ConnectionStateError`；五元组最新记录已 closed 或 expired 时建立新记录。
* `record_activity(flow, now)`、`close_connection(flow, now)` 与 `advance(now)` 管理生命周期。每个操作都先按 `now` 处理到期再执行：空闲期限（最后活动时间加 `idle_timeout`）或硬期限（建立时间加 `hard_timeout`）不晚于 `now` 的 active 连接被置为 expired，两个期限同时命中时 `end_reason` 为 `hard_timeout`，因此截止时刻的活动不能挽救连接。`advance` 按建立顺序返回本次到期的记录。
* `record_activity` 只更新 active 连接的最后活动时间，对不存在的或非 active 的连接抛出 `ConnectionStateError`；`close_connection` 把 active 连接置为 closed（`end_reason` 为 `closed`），重复关闭幂等，五元组从未建立连接时抛出 `ConnectionStateError`。
* 记录是键序固定为 `flow`、`backend_id`、`state`、`created_at`、`last_activity_at`、`ended_at`、`end_reason` 的字典，`flow` 内保持五元组顺序；active 记录的 `ended_at` 与 `end_reason` 为 `null`。`connections()` 按建立顺序返回全部记录的隔离副本，`active_connections()` 按后端声明顺序返回每个后端的 active 连接数（新字典）。
* 五元组类型错误抛出 `TypeError`、值错误抛出 `ValueError`，非法超时抛出 `ConfigurationError`，未知后端抛出 `KeyError`；任何失败都不改变状态。
* 按五元组定位、记录活动与关闭平均 O(1) 时间，`advance` 与完整查询 O(n) 时间，空间 O(n)。
