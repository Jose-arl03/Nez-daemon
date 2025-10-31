from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, TypeVar, overload
try:
    # Python 3.11+
    from typing import ParamSpec
except ImportError:  # Python 3.9/3.10
    from typing_extensions import ParamSpec

import asyncio
import inspect
import random

from option import Result, Ok, Err
from mictlanx.errors import MictlanXError


# -------- Typing --------

P = ParamSpec("P")
T = TypeVar("T")                  # success type
E = TypeVar("E", bound=Exception) # original error type (only for input functions)


# -------- Policy --------
@dataclass
class RetryPolicy:
    retries: int = 3
    initial_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 30.0
    jitter: bool = True
    retry_on: Callable[[Exception], bool] | None = None

    def __post_init__(self) -> None:
        if self.retry_on is None:
            def _default_retry_on(err: Exception) -> bool:
                # Ejemplo: return isinstance(err, MictlanXError) and err.status_code in {429,500,502,503,504}
                return True
            self.retry_on = _default_retry_on

def _apply_backoff(delay: float, factor: float, max_delay: float, jitter: bool) -> float:
    next_delay = min(max_delay, delay * factor)
    return random.uniform(0, next_delay) if jitter else next_delay

def _is_result(v: Any) -> bool:
    # duck-typing; evita dependencia fuerte a la clase concreta Result
    return hasattr(v, "is_ok") and hasattr(v, "unwrap") and hasattr(v, "unwrap_err")

# ---------- Overloads (ORDEN IMPORTA: primero los que retornan Result) ----------

# (1) Async que retorna Result[T, E]
@overload
async def raf(
    func: Callable[P, Awaitable[Result[T, E]]],
    *args: P.args,
    policy: RetryPolicy = ...,
    on_attempt: Optional[Callable[[int], None]] = ...,
    on_error: Optional[Callable[[int, MictlanXError], None]] = ...,
    **kwargs: P.kwargs,
) -> Result[T, MictlanXError]: ...

# (2) Sync que retorna Result[T, E]
@overload
async def raf(
    func: Callable[P, Result[T, E]],
    *args: P.args,
    policy: RetryPolicy = ...,
    on_attempt: Optional[Callable[[int], None]] = ...,
    on_error: Optional[Callable[[int, MictlanXError], None]] = ...,
    **kwargs: P.kwargs,
) -> Result[T, MictlanXError]: ...

# (3) Async que retorna T
@overload
async def raf(
    func: Callable[P, Awaitable[T]],
    *args: P.args,
    policy: RetryPolicy = ...,
    on_attempt: Optional[Callable[[int], None]] = ...,
    on_error: Optional[Callable[[int, MictlanXError], None]] = ...,
    **kwargs: P.kwargs,
) -> Result[T, MictlanXError]: ...

# (4) Sync que retorna T
@overload
async def raf(
    func: Callable[P, T],
    *args: P.args,
    policy: RetryPolicy = ...,
    on_attempt: Optional[Callable[[int], None]] = ...,
    on_error: Optional[Callable[[int, MictlanXError], None]] = ...,
    **kwargs: P.kwargs,
) -> Result[T, MictlanXError]: ...

# ---------- Implementación ----------

async def raf(
    func: Callable[P, Any],
    *args: P.args,
    policy: RetryPolicy = RetryPolicy(),
    on_attempt: Optional[Callable[[int], None]] = None,
    on_error: Optional[Callable[[int, MictlanXError], None]] = None,
    **kwargs: P.kwargs,
) -> Result[Any, MictlanXError]:
    delay = policy.initial_delay
    attempt = 0

    while attempt < policy.retries:
        if on_attempt:
            on_attempt(attempt + 1)

        try:
            ret = func(*args, **kwargs)
            value = await ret if inspect.isawaitable(ret) else ret

            if _is_result(value):
                # APLANA: Ok(v) -> Ok(v); Err(e) -> Err(MictlanXError(...))
                if value.is_ok:
                    return Ok(value.unwrap())
                raw_err = value.unwrap_err()
                mx_err = raw_err if isinstance(raw_err, MictlanXError) else MictlanXError.from_exception(raw_err)
            else:
                # Valor “T” -> Ok(T)
                return Ok(value)

        except Exception as exc:
            mx_err = exc if isinstance(exc, MictlanXError) else MictlanXError.from_exception(exc)

        # Error path
        if on_error:
            on_error(attempt + 1, mx_err)

        attempt += 1
        if attempt < policy.retries and policy.retry_on and policy.retry_on(mx_err):
            retry_after_secs: float | None = None
            if hasattr(mx_err, "_retry_after"):
                try:
                    retry_after_secs = float(getattr(mx_err, "_retry_after"))
                except Exception:
                    retry_after_secs = None

            sleep_for = retry_after_secs if retry_after_secs is not None else delay
            await asyncio.sleep(max(0.0, sleep_for))
            delay = _apply_backoff(delay, policy.backoff_factor, policy.max_delay, policy.jitter)
            continue

        return Err(mx_err)

    return Err(MictlanXError("Retry loop exited unexpectedly"))