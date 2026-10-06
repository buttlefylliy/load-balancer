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
"""

import argparse
import json
import sys

__all__ = [
    "ConfigurationError",
    "NoAvailableBackendError",
    "BackendPool",
    "RoundRobinScheduler",
]

_POLICY = "round_robin"
_FIELDS = ("id", "address", "port", "healthy")
_COMPLEXITY = (
    "复杂度：建池为 O(n) 时间与 O(n) 空间；"
    "单次选择最坏 O(n) 时间且额外空间 O(1)。"
)


class ConfigurationError(ValueError):
    """后端配置不合法（字段缺失、类型错误、端口越界或 id 重复等）。"""


class NoAvailableBackendError(Exception):
    """一次有界扫描内没有找到任何健康后端。"""


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

        unknown = [key for key in entry if key not in _FIELDS]
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

    scheduler = RoundRobinScheduler(pool)
    selections = []
    try:
        for _ in range(args.count):
            selections.append(scheduler.select())
    except NoAvailableBackendError as exc:
        _emit_error("NoAvailableBackendError", exc, 4)

    result = {"policy": _POLICY, "selections": selections}
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
