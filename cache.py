"""
cache.py — Redis caching layer.
Degrades gracefully if REDIS_URL is not set or Redis is unavailable.
"""
import os, json, logging
log = logging.getLogger(__name__)

_redis_client = None
_redis_url_logged = False


def get_redis():
    global _redis_client, _redis_url_logged
    if _redis_client is not None:
        return _redis_client
    url = os.environ.get("REDIS_URL")
    if not url:
        if not _redis_url_logged:
            log.info("REDIS_URL not set; caching disabled")
            _redis_url_logged = True
        return None
    try:
        import redis
        client = redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
        client.ping()
        _redis_client = client
        log.info("Redis connected")
    except Exception as e:
        log.warning(f"Redis unavailable: {e}")
    return _redis_client


def cache_set(key: str, data, ttl: int = 300):
    r = get_redis()
    if r is None:
        return
    try:
        r.set(key, json.dumps(data, default=str), ex=ttl)
    except Exception as e:
        log.warning(f"cache_set failed: {e}")


def cache_get(key: str):
    r = get_redis()
    if r is None:
        return None
    try:
        raw = r.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as e:
        log.warning(f"cache_get failed: {e}")
        return None


def cache_key(instrument: str, interval: str, days: int) -> str:
    return f"leaderboard:{instrument}:{interval}:{days}"


def invalidate(instrument: str, interval: str):
    r = get_redis()
    if r is None:
        return
    try:
        pattern = f"leaderboard:{instrument}:{interval}:*"
        keys = r.keys(pattern)
        if keys:
            r.delete(*keys)
    except Exception as e:
        log.warning(f"invalidate failed: {e}")
