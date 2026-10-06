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

* 调度器为池中每个后端维护从零开始的活动连接数；每次选择只考察当前健康的后端，取活动连接数最小者，计数相同时按声明顺序取最前者。选择成功后先把被选后端的计数加一，再返回与轮询调度器相同键序的后端字典。`weight` 仍按既有规则校验，但不参与比较。
* `schedule` 命令中的 count 次选择视为依次建立且未释放的连接；没有可用后端时仍以退出码 4 结束，且不在 stdout 留下部分结果。
* 库接口提供 `release_connection(backend_id)` 与 `active_connections()`：释放成功只把对应计数减一，重复释放到零以下抛出 `ConnectionStateError` 且任何计数不变；id 非字符串抛出 `TypeError`，id 不存在抛出 `KeyError`。查询返回与内部状态隔离、按声明顺序排列的新字典。
* 健康状态变化立即影响后续选择，但不清除或改写已有计数；不健康的后端仍允许释放既有连接，恢复健康后以保留的计数继续参与比较。
* 构造为 O(n) 时间与 O(n) 额外空间，单次选择最坏 O(n)、额外空间 O(1)，单次释放 O(1)，计数查询 O(n)。

## 一致性哈希（库接口）

库接口提供 `ConsistentHashScheduler(pool)`，可从 `load_balancer` 直接导入；`schedule` 命令不新增该策略，参数、输出、异常类型与退出码保持不变。

* `select(key)` 根据调用方给出的会话键选择一个当前健康后端。key 必须是非空字符串：非字符串抛出 `TypeError`，空字符串抛出 `ValueError`，两种失败都不产生状态变化；构造参数不是 `BackendPool` 时与其他调度器一样抛出 `TypeError`；没有健康后端时抛出 `NoAvailableBackendError`。
* 评分输入为由 key 和后端 id 组成的 JSON 数组，按 `ensure_ascii=False` 与紧凑分隔符序列化为 UTF-8 后计算 SHA-256，摘要按无符号大端整数解释，分数最大的后端胜出；摘要相同时按配置声明顺序选择最前者。后端的 address、port、weight 与声明位置均不参与评分（`weight` 仍由 `BackendPool` 按既有规则校验）。
* 调度器不维护轮询游标、连接计数或按键增长的缓存：同一键在后端集合与健康状态不变时，无论调用次数及与其他键的调用顺序如何，都返回键序固定为 id、address、port 的逐字段相同新字典。
* 每次选择只比较当前健康后端：某后端被标记为不健康后，原本未选择它的键保持原选择，原本选择它的键在其余健康后端中重新映射；该后端恢复健康后，相同键按原评分规则重新选择，此前属于它的键确定性地回到它。健康变化在下一次 `select` 立即生效，选择过程不改写池或调度器状态。
* 单次选择最坏 O(n) 时间，除摘要计算所需的固定大小数据外额外空间 O(1)。

## 连接生命周期表（库接口）

库接口提供 `ConnectionTable(pool, idle_timeout, hard_timeout)`，可从 `load_balancer` 直接导入；`schedule` 命令不接入连接表，参数、输出、异常类型与退出码保持不变，也不改变 `LeastConnectionsScheduler` 独立维护的连接计数。

* 连接以五元组标识：`src_addr`、`src_port`、`dst_addr`、`dst_port`、`protocol`。源/目的地址必须是非空字符串；端口必须是 1 至 65535 的整数且拒绝布尔值；协议只接受小写 `"tcp"` 或 `"udp"`。类型错误抛出 `TypeError`，值错误抛出 `ValueError`。
* `idle_timeout` 与 `hard_timeout` 必须为正整数，否则抛出 `ConfigurationError`。时间只由显式注入的 `now` 驱动（非布尔的非负整数），不读取墙上时钟；`now` 回退抛出 `ConnectionStateError`。
* `open_connection(flow, backend_id, now)` 创建 active 记录：后端不存在抛出 `KeyError`，不健康抛出 `ConnectionStateError`；同一五元组已 active 时，相同后端返回原记录，不同后端抛出 `ConnectionStateError`；已 closed 或 expired 的五元组可重新建立并排到建立顺序末尾。
* `record_activity(flow, now)` 只更新 active 连接的最后活动时间，对非 active 连接抛出 `ConnectionStateError`；`close_connection(flow, now)` 把 active 连接置为 closed，重复关闭幂等。每个操作先按 `now` 处理到期再执行：空闲或硬期限不晚于 `now` 时 active 连接转为 expired（同时命中时 `end_reason` 为 `hard_timeout`），截止时刻的活动不能挽救连接。`advance(now)` 按建立顺序返回本次到期的记录。
* 查询 `connections()` 按建立顺序返回与内部状态隔离的记录副本，记录键序固定为 flow、backend_id、state、created_at、last_activity_at、ended_at、end_reason，flow 保持五元组顺序；`active_connections()` 按后端声明顺序返回每个后端的 active 连接数。任何失败调用都不改变表状态。
* 按五元组定位、记录活动和关闭平均 O(1)，`advance` 与完整查询 O(n)，空间 O(n)。
