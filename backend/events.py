import os
import json
import asyncio
import contextlib
from typing import AsyncIterator, Dict, Optional, Set

# redis-py has asyncio in the same package
try:
    import redis.asyncio as aioredis
    import redis as redis_sync
except Exception:  # redis not installed or very old
    aioredis = None
    redis_sync = None


class EventHub:
    """
    Cross-process event hub.
    - If REDIS_URL is set and redis is available: use Redis pub/sub (workers publish; API subscribes).
    - Else: in-process fallback (works only when parser runs in same process).
    """

    def __init__(self, url: Optional[str]):
        self.url = url
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self._local_queues: Dict[str, Set[asyncio.Queue]] = {}
        self._async_redis = None
        self._sync_redis = None
        self.redis_ok = False

        if self.url and aioredis and redis_sync:
            try:
                # decode_responses=True -> str payloads
                self._async_redis = aioredis.from_url(self.url, decode_responses=True)
                self._sync_redis = redis_sync.Redis.from_url(self.url, decode_responses=True)
                # ping to verify
                self._sync_redis.ping()
                self.redis_ok = True
            except Exception:
                self._async_redis = None
                self._sync_redis = None
                self.redis_ok = False

    def bind_loop(self, loop: asyncio.AbstractEventLoop):
        """Bind API server loop for in-process fallback."""
        self.loop = loop

    # -------- publishers (workers / API threads) --------
    def publish(self, job_id: str, payload: Dict):
        """
        Publish a JSON-serializable dict to a job channel.
        - With Redis: cross-process broadcast.
        - Without Redis: push to local queues (same process only).
        """
        if self.redis_ok and self._sync_redis is not None:
            try:
                self._sync_redis.publish(f"jobs:{job_id}", json.dumps(payload))
                return
            except Exception:
                # fall through to local fallback
                pass

        # in-process fallback
        if self.loop and job_id in self._local_queues:
            data = payload

            def _push():
                for q in list(self._local_queues.get(job_id, set())):
                    try:
                        q.put_nowait(data)
                    except Exception:
                        pass

            try:
                self.loop.call_soon_threadsafe(_push)
            except Exception:
                pass

    # -------- subscribers (API WS endpoint) --------
    async def stream(self, job_id: str):
        """
        Async generator yielding payload dicts for a job_id.
        Uses Redis pubsub if available; else local queues.
        """
        channel = f"jobs:{job_id}"

        # Redis path
        if self.redis_ok and self._async_redis is not None:
            pubsub = self._async_redis.pubsub()
            await pubsub.subscribe(channel)
            try:
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        raw = msg.get("data")
                        try:
                            yield json.loads(raw)
                        except Exception:
                            # ignore malformed
                            pass
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.unsubscribe(channel)
                    await pubsub.close()
            return

        # Local fallback path
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._local_queues.setdefault(job_id, set()).add(q)
        try:
            while True:
                item = await q.get()
                yield item
        finally:
            s = self._local_queues.get(job_id)
            if s is not None:
                s.discard(q)
                if not s:
                    self._local_queues.pop(job_id, None)


# instantiate hub
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("CELERY_BROKER_URL") or os.getenv("CELERY_RESULT_BACKEND")
events = EventHub(REDIS_URL)
