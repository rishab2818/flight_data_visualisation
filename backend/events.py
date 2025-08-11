import asyncio
from collections import defaultdict, deque
from typing import Dict, List

class JobEventManager:
    def __init__(self, history_size: int = 500) -> None:
        self._queues: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
        self._lock = asyncio.Lock()

    async def subscribe(self, job_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        async with self._lock:
            self._queues[job_id].append(q)
            # push recent history so the UI sees logs even if the job finished fast
            for item in list(self._history[job_id]):
                try:
                    q.put_nowait(item)
                except asyncio.QueueFull:
                    break
        return q

    async def unsubscribe(self, job_id: str, q: asyncio.Queue) -> None:
        async with self._lock:
            qs = self._queues.get(job_id)
            if qs and q in qs:
                qs.remove(q)
            if not qs:
                self._queues.pop(job_id, None)
                self._history.pop(job_id, None)

    def publish(self, job_id: str, payload: dict) -> None:
        # store in history and fan-out to subscribers (thread-safe)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return

        def _put():
            self._history[job_id].append(payload)
            for q in list(self._queues.get(job_id, [])):
                if q.full():
                    try:
                        _ = q.get_nowait()
                    except Exception:
                        pass
                try:
                    q.put_nowait(payload)
                except Exception:
                    pass

        loop.call_soon_threadsafe(_put)

    # OPTIONAL: for compatibility polling from the UI
    def dump(self, job_id: str) -> list[dict]:
        return list(self._history.get(job_id, []))

events = JobEventManager()
