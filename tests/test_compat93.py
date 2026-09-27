import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from ida_nexus._compat93 import MainThreadLoop


def test_dispatch_runs_on_owner_propagates_errors_and_allows_teardown():
    loop = MainThreadLoop()
    owner = threading.get_ident()

    def fail():
        raise ValueError("callback failed")

    def client():
        try:
            assert loop.execute_sync(threading.get_ident, 2) == owner
            with pytest.raises(ValueError, match="callback failed"):
                loop.execute_sync(fail, 2)
            assert loop.execute_sync(lambda: loop.execute_sync(lambda: 42, 0), 2) == 42
        finally:
            loop.stop_serving()

    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(client)
        loop.serve()
        task.result(timeout=5)
    assert loop.execute_sync(lambda: 7, 0) == 7


def test_stop_releases_queued_and_future_callers():
    loop = MainThreadLoop()
    queued = threading.Event()
    original_put = loop._queue.put

    def put(item):
        original_put(item)
        queued.set()

    loop._queue.put = put
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(loop.execute_sync, lambda: pytest.fail("ran after stop"), 2)
        assert queued.wait(5)
        loop.stop_serving()
        loop.stop_serving()
        with pytest.raises(RuntimeError, match="stopped"):
            task.result(timeout=5)
        with pytest.raises(RuntimeError, match="stopped"):
            pool.submit(loop.execute_sync, lambda: 1, 2).result(timeout=5)
    loop.serve()


def test_rejects_wrong_owner_and_nonblocking_requests():
    loop = MainThreadLoop()
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pytest.raises(RuntimeError, match="owning thread"),
    ):
        pool.submit(loop.serve).result(timeout=5)
    with pytest.raises(ValueError, match="blocking"):
        loop.execute_sync(lambda: 1, 8)
