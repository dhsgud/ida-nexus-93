"""Main-thread dispatch for Nexus callbacks on IDA 9.3 headless workers."""

import threading
from collections.abc import Callable
from concurrent.futures import Future
from queue import Empty, Queue


class MainThreadLoop:
    """Replace the missing idalib serve loop, without patching IDAPython APIs."""

    def __init__(self) -> None:
        self._owner = threading.get_ident()
        self._queue: Queue[tuple[Callable[[], int], Future[int]] | None] = Queue()
        self._lock = threading.RLock()
        self._stopped = False

    def execute_sync(self, callback: Callable[[], int], flags: int) -> int:
        # ponytail: dispatch Nexus's blocking callbacks only; native UI request
        # IDs/cancellation require IDA 9.4's own execute_sync + serve implementation.
        if flags & 8:  # MFF_NOWAIT is only used by Nexus's native GUI path.
            raise ValueError("IDA 9.3 headless dispatcher requires blocking requests")
        if threading.get_ident() == self._owner:
            # Teardown still runs on the owner after stop_serving().
            return callback()
        future: Future[int] = Future()
        with self._lock:
            if self._stopped:
                raise RuntimeError("IDA main-thread dispatcher is stopped")
            self._queue.put((callback, future))
        return future.result()

    def serve(self) -> None:
        if threading.get_ident() != self._owner:
            raise RuntimeError("IDA dispatch loop must run on its owning thread")
        while True:
            request = self._queue.get()
            if request is None:
                return
            callback, future = request
            try:
                future.set_result(callback())
            except BaseException as exc:  # noqa: BLE001 -- forward to waiting caller
                # Marshal failures to the caller; never strand a waiting thread.
                future.set_exception(exc)

    def stop_serving(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            while True:
                try:
                    request = self._queue.get_nowait()
                except Empty:
                    break
                if request is not None:
                    request[1].set_exception(
                        RuntimeError("IDA main-thread dispatcher is stopped")
                    )
            self._queue.put(None)
