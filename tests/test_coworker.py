import asyncio
from time import time

import pytest

from coworker import Coworker


class SquareWorker(Coworker):
    async def do_task(self, task):
        return task * task


class ConcurrentWorker(Coworker):
    async def do_task(self, task):
        print("Performing task", task)
        await asyncio.sleep(0.1 * task / 5)
        print("Finished task", task)


class SleepyWorker(Coworker):
    sleeping = None

    async def do_task(self, task):
        SleepyWorker.sleeping = True
        try:
            await asyncio.sleep(1000)

        except BaseException as e:
            SleepyWorker.sleeping = False
            print("Woked up by", repr(e))
            raise


class OnWorker(Coworker):
    on_start_called = 0
    on_finish_called = 0
    on_start_task_called = 0
    on_finish_task_called = 0
    do_task_called = 0

    async def do_task(self, task):
        self.do_task_called += 1

    async def on_start(self):
        self.on_start_called += 1

    async def on_finish(self):
        self.on_finish_called += 1

    async def on_start_task(self, task):
        self.on_start_task_called += 1

    async def on_finish_task(self, task, result):
        self.on_finish_task_called += 1


async def test_do_tasks():
    worker = SquareWorker()

    # One task
    result = await worker.do(2)
    assert result == 4

    # Mulitiple tasks
    results = await worker.do([1, 2, 3])
    assert results == [1, 4, 9]

    # Mulitiple tasks as iterator
    for first in worker.do([1, 2, 3], as_iterator=True):
        result = await first
        assert result in [1, 4, 9]

    assert worker._is_finished


async def test_do_task_param():
    # Sync
    worker = Coworker(do_task=lambda x: x * x)

    result = await worker.do(2)
    assert result == 4

    # Async
    async def do_task(task):
        return task * task
    worker = Coworker(do_task=do_task)

    result = await worker.do(2)
    assert result == 4


async def test_cancel():
    worker = SleepyWorker()

    result = worker.do(1)

    await asyncio.sleep(0.1)

    assert SleepyWorker.sleeping is True

    worker.cancel_task(1)

    await asyncio.sleep(0.1)

    assert SleepyWorker.sleeping is False
    with pytest.raises(asyncio.exceptions.CancelledError):
        await result

    await worker.stop()
    assert worker._is_finished


async def test_should_exit():
    # No tasks
    worker = Coworker()
    assert worker.should_exit

    # Has tasks
    worker.do([1, 2, 3])
    assert not worker.should_exit

    # Tasks done
    await worker.start()
    assert worker.should_exit
    assert worker._is_finished


async def test_concurrency_with_window():
    worker = ConcurrentWorker()

    results = worker.do(list(range(30)))
    worker.debug = True
    assert worker._concurrency == 0
    assert worker._task_queue.qsize() == 30
    assert len(worker._task_futures) == 30

    await asyncio.sleep(0)         # 1st batch: Wait for tasks to start

    assert worker._concurrency == 10
    assert worker._task_queue.qsize() == 20
    assert len(worker._task_futures) == 30

    await asyncio.sleep(0.2)      # 2nd batch: Wait for tasks to finish

    assert worker._concurrency == 10
    assert worker._task_queue.qsize() == 10
    assert len(worker._task_futures) == 20

    await results                 # Last batch: Wait for all to be done

    assert worker._concurrency == 0
    assert worker._task_queue.qsize() == 0
    assert len(worker._task_futures) == 0

    assert worker._is_finished


async def test_concurrency_without_window():
    worker = ConcurrentWorker(sliding_window=False)
    worker.exit_when_idle = True

    results = worker.do(list(range(30)))

    assert worker._concurrency == 0
    assert worker._task_queue.qsize() == 30
    assert len(worker._task_futures) == 30

    await asyncio.sleep(0)      # 1st batch: Wait for tasks to start

    assert worker._concurrency == 10
    assert worker._task_queue.qsize() == 20
    assert len(worker._task_futures) == 30

    await asyncio.sleep(0.1)    # 2nd batch: Wait for tasks to finish

    assert worker._concurrency == 5
    assert worker._task_queue.qsize() == 20
    assert len(worker._task_futures) == 25

    await asyncio.sleep(0.1)    # 3rd batch

    assert worker._concurrency == 10
    assert worker._task_queue.qsize() == 10
    assert len(worker._task_futures) == 20

    await results        # Wait for everything to be done

    assert worker._concurrency == 0
    assert worker._task_queue.qsize() == 0
    assert len(worker._task_futures) == 0
    assert worker._is_finished


async def test_on_events():
    worker = OnWorker()
    await worker.do(list(range(5)))

    assert worker.on_start_called == 1
    assert worker.on_finish_called == 1
    assert worker.on_start_task_called == 5
    assert worker.on_finish_task_called == 5
    assert worker.do_task_called == 5
    assert worker._is_finished


@pytest.mark.parametrize('sliding_window', [True, False])
async def test_performance(sliding_window):
    worker = SquareWorker(sliding_window=sliding_window)
    # worker.debug = True  # Uncomment to see debug info

    start_time = time()

    await worker.do(list(range(40000)))

    total_time = time() - start_time

    assert 0.5 < total_time < 1.5
    assert worker._is_finished
