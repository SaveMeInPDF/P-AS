"""Повторы с экспоненциальной задержкой; отдельная пауза при ограничении частоты запросов."""
import time

RATE_LIMIT_MARKERS = ("too many requests", "rate limit", "ratelimit", "429")


class RetryExhausted(Exception):
    def __init__(self, last_exc: Exception, attempts: int):
        super().__init__(f"{attempts} attempts failed: {last_exc!r}")
        self.last_exc = last_exc
        self.attempts = attempts


def is_rate_limit(exc: Exception) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return any(m in text for m in RATE_LIMIT_MARKERS)


def call_with_retry(fn, retry_cfg: dict, sleep=time.sleep):
    """Возвращает (результат, число_попыток). После исчерпания попыток — RetryExhausted."""
    attempts = int(retry_cfg["attempts"])
    last = None
    for n in range(1, attempts + 1):
        try:
            return fn(), n
        except Exception as exc:  # сетевые ошибки, таймауты, ошибки парсинга ответа
            last = exc
            if n == attempts:
                break
            delay = (retry_cfg["rate_limit_delay_sec"] if is_rate_limit(exc)
                     else retry_cfg["base_delay_sec"] * 2 ** (n - 1))
            sleep(delay)
    raise RetryExhausted(last, attempts)
