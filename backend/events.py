import asyncio
from collections import defaultdict, deque
from typing import Dict, List

class JobEventManager:
    def __init__(self, history_size: int = 500) -> None:
        self._queues: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None  # bound on startup

    # call this once on app startup (from the main loop)
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def subscribe(self, job_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        async with self._lock:
            self._queues[job_id].append(q)
            # replay recent events so late subscribers see something immediately
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
        # always record to history
        self._history[job_id].append(payload)

        def _put():
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

        # use the bound main loop so this works from any thread
        loop = self._loop
        if loop is None:
            # last resort: try the running loop (in-async contexts)
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return  # no loop available; history is still saved for later subscribers
        loop.call_soon_threadsafe(_put)

    # optional: for debugging / polling
    def dump(self, job_id: str) -> list[dict]:
        return list(self._history.get(job_id, []))

events = JobEventManager()
