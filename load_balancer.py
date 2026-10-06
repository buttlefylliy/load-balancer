"""Deterministic load balancer core.

This module provides a static backend pool and a plain round-robin scheduler:

* ``BackendPool`` validates and stores backend configurations in declaration
  order. Construction is O(n) time and space and is all-or-nothing: if any
  backend is invalid, no partially constructed pool becomes visible and the
  caller's input objects are never mutated or retained.
* ``RoundRobinScheduler`` selects healthy backends one at a time. Each scan is
  bounded by the pool size (worst-case O(n) time, O(1) extra space), starts at
  the successor of the last successful position, and leaves the cursor
  untouched when no healthy backend exists.
* Health is updated by backend id through the pool; setting the same state
  repeatedly is idempotent and never moves the cursor.

The module performs no network I/O, reads no wall clock, persists nothing and
uses only the Python standard library, so identical inputs always yield
identical results.
"""

import argparse
import json
import sys

__all__ = [
    "ConfigurationError",
    "NoAvailableBackendError",
    "Backend",
    "BackendPool",
    "RoundRobinScheduler",
    "main",
]

_POLICY = "round_robin"
_SELECTION_KEYS = ("id", "address", "port")
_BACKEND_KEYS = frozenset(_SELECTION_KEYS + ("healthy",))


class ConfigurationError(ValueError):
    """Raised when backend configuration fails validation."""


class NoAvailableBackendError(Exception):
    """Raised when a selection is attempted but no backend is healthy."""


class Backend:
    """An immutable-identity backend record; only ``healthy`` may change."""

    __slots__ = ("id", "address", "port", "healthy")

    def __init__(self, backend_id, address, port, healthy):
        self.id = backend_id
        self.address = address
        self.port = port
        self.healthy = healthy

    def to_dict(self):
        """Return a fresh selection dict with the fixed key order."""
        return {
            "id": self.id,
            "address": self.address,
            "port": self.port,
        }


def _is_strict_bool(value):
    # ``bool`` is a subclass of ``int``; reject it for boolean fields by
    # accident and require an exact bool here.
    return type(value) is bool


def _validate_backends(raw):
    """Validate a list of backend config dicts.

    Returns a fully built list of ``Backend`` objects. Validation of every
    entry completes before the list is handed back, so callers never observe
    partial state. Input containers and dicts are never mutated or retained.
    """
    if not isinstance(raw, list):
        raise ConfigurationError("'backends' must be a list")

    backends = []
    seen_ids = set()
    for index, item in enumerate(raw):
        where = "backend at index {}".format(index)
        if not isinstance(item, dict):
            raise ConfigurationError("{} must be an object".format(where))

        keys = set(item.keys())
        missing = _BACKEND_KEYS - keys
        if missing:
            raise ConfigurationError(
                "{} is missing field(s): {}".format(
                    where, ", ".join(sorted(missing))
                )
            )
        extra = keys - _BACKEND_KEYS
        if extra:
            raise ConfigurationError(
                "{} has unknown field(s): {}".format(
                    where, ", ".join(sorted(extra))
                )
            )

        backend_id = item["id"]
        if not isinstance(backend_id, str) or len(backend_id) == 0:
            raise ConfigurationError(
                "{}: 'id' must be a non-empty string".format(where)
            )
        if backend_id in seen_ids:
            raise ConfigurationError(
                "{}: duplicate backend id {!r}".format(where, backend_id)
            )

        address = item["address"]
        if not isinstance(address, str) or len(address) == 0:
            raise ConfigurationError(
                "{}: 'address' must be a non-empty string".format(where)
            )

        port = item["port"]
        # Exact type check: booleans are integers in Python and must not be
        # accepted as ports.
        if type(port) is not int or not (1 <= port <= 65535):
            raise ConfigurationError(
                "{}: 'port' must be an integer in the range 1..65535".format(
                    where
                )
            )

        healthy = item["healthy"]
        if not _is_strict_bool(healthy):
            raise ConfigurationError(
                "{}: 'healthy' must be a boolean".format(where)
            )

        seen_ids.add(backend_id)
        backends.append(Backend(backend_id, address, port, healthy))

    return backends


class BackendPool:
    """A static, declaration-ordered pool of backends.

    ``Backends`` are stored in the order given at construction; that order is
    the round-robin order. Construction runs in O(n) time and space.
    """

    def __init__(self, backends):
        # Validate everything into a temporary list first; only assign visible
        # state once validation has fully succeeded.
        validated = _validate_backends(backends)
        self._backends = validated
        self._index = {backend.id: i for i, backend in enumerate(validated)}

    def __len__(self):
        return len(self._backends)

    def __contains__(self, backend_id):
        return backend_id in self._index

    def is_healthy(self, backend_id):
        """Return the health flag of the backend with the given id."""
        return self._backends[self._index[backend_id]].healthy

    def set_healthy(self, backend_id, healthy):
        """Atomically set health by id.

        Setting the same value repeatedly is a no-op. The round-robin cursor
        lives in the scheduler and is therefore never affected. Raises
        ``KeyError`` for an unknown id and ``TypeError`` for a non-boolean
        value.
        """
        if not _is_strict_bool(healthy):
            raise TypeError("'healthy' must be a boolean")
        if backend_id not in self._index:
            raise KeyError(backend_id)
        self._backends[self._index[backend_id]].healthy = healthy

    def _backend_at(self, position):
        return self._backends[position]


class RoundRobinScheduler:
    """Plain round-robin scheduler over a :class:`BackendPool`.

    The cursor is the position of the last successful selection and starts
    before the first backend, so the first selection starts at index 0. A
    selection scans at most ``len(pool)`` positions beginning immediately
    after the cursor, skips unhealthy backends, advances the cursor to the
    chosen position and returns that backend as a dict with the fixed key
    order ``id``, ``address``, ``port``. If no backend is healthy the cursor
    is left unchanged and ``NoAvailableBackendError`` is raised.
    """

    def __init__(self, pool):
        if not isinstance(pool, BackendPool):
            raise TypeError("'pool' must be a BackendPool")
        self._pool = pool
        self._cursor = -1

    def select(self):
        """Select the next healthy backend; see class docstring."""
        size = len(self._pool)
        for offset in range(1, size + 1):
            position = (self._cursor + offset) % size
            backend = self._pool._backend_at(position)
            if backend.healthy:
                self._cursor = position
                return backend.to_dict()
        raise NoAvailableBackendError("no healthy backend available")


# --------------------------------------------------------------------------- #
# Command line interface
# --------------------------------------------------------------------------- #

_HELP_EPILOG = """\
complexity:
  building a pool is O(n) time and O(n) space for n backends;
  each selection is worst-case O(n) time with O(1) extra space.
output is deterministic: no random values and no time fields are emitted.
"""


def _positive_int(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            "{!r} is not a positive integer".format(value)
        )
    if number <= 0:
        raise argparse.ArgumentTypeError("count must be a positive integer")
    return number


class _JSONArgumentParser(argparse.ArgumentParser):
    """Argument parser whose usage errors are JSON on stderr (exit code 2)."""

    def error(self, message):
        emit_error("ArgumentError", message, 2)


def _build_parser():
    parser = _JSONArgumentParser(
        prog="load_balancer.py",
        description=(
            "Deterministic static backend pool with round-robin scheduling."
        ),
        epilog=_HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    schedule = subparsers.add_parser(
        "schedule",
        help="run round-robin selections from a JSON config file",
        description="Run round-robin selections from a JSON config file.",
        epilog=_HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    schedule.add_argument(
        "--config",
        required=True,
        metavar="PATH",
        help="path to a UTF-8 JSON config file with a 'backends' list",
    )
    schedule.add_argument(
        "--count",
        required=True,
        type=_positive_int,
        metavar="N",
        help="positive number of selections to perform",
    )
    return parser


def emit_error(error_type, message, exit_code):
    """Write a compact ``{"type", "message"}`` JSON error to stderr and exit."""
    payload = json.dumps(
        {"type": error_type, "message": message},
        separators=(",", ":"),
    )
    sys.stderr.write(payload + "\n")
    raise SystemExit(exit_code)


def _load_config(path):
    """Read and parse the config file.

    Unreadable files / invalid UTF-8 and JSON syntax errors are distinct
    failure classes from semantic configuration errors.
    """
    try:
        with open(path, "rb") as handle:
            raw_bytes = handle.read()
    except OSError as exc:
        emit_error("IOError", "cannot read config file: {}".format(exc), 2)

    try:
        raw_text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        emit_error("IOError", "config file is not valid UTF-8", 2)

    try:
        config = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        emit_error(
            "JSONDecodeError",
            "invalid JSON at line {} column {}: {}".format(
                exc.lineno, exc.colno, exc.msg
            ),
            2,
        )

    return config


def _backends_from_config(config):
    if not isinstance(config, dict):
        raise ConfigurationError("config root must be an object")
    if "backends" not in config:
        raise ConfigurationError("config must contain a 'backends' list")
    return config["backends"]


def _cmd_schedule(args):
    config = _load_config(args.config)

    try:
        raw_backends = _backends_from_config(config)
        pool = BackendPool(raw_backends)
    except ConfigurationError as exc:
        emit_error("ConfigurationError", str(exc), 3)

    scheduler = RoundRobinScheduler(pool)
    selections = []
    for _ in range(args.count):
        try:
            selections.append(scheduler.select())
        except NoAvailableBackendError:
            emit_error(
                "NoAvailableBackendError",
                "no healthy backend available",
                4,
            )

    result = {"policy": _POLICY, "selections": selections}
    sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
    return 0


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.error("a subcommand is required")
    if args.command == "schedule":
        return _cmd_schedule(args)
    parser.error("unknown subcommand {!r}".format(args.command))


if __name__ == "__main__":
    raise SystemExit(main())
