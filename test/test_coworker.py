import asyncio
from time import time

from coworker import Coworker

import pytest
from utils import async_test


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

        except Exception as e:
            SleepyWorker.sleeping = False
            print("Woked up by", repr(e))
            raise


class OnWorker(Coworker):
    on_start_called = 0
    on_finish_called = 0
    on_start_task_called = 0
    on_finish_task_called = 0

    async def on_start(self):
        self.on_start_called += 1

    async def on_finish(self):
        self.on_finish_called += 1

    async def on_start_task(self, task):
        self.on_start_task_called += 1

    async def on_finish_task(self, task, result):
        self.on_finish_task_called += 1


@async_test
async def test_cancel():
    worker = SleepyWorker()
    asyncio.ensure_future(worker.start())

    future = worker.add_tasks(1)

    await asyncio.sleep(0.1)

    assert SleepyWorker.sleeping is True

    worker.cancel_task(1)

    await asyncio.sleep(0.1)

    assert future.cancelled
    assert SleepyWorker.sleeping is False

    await worker.stop()


@async_test
async def test_should_exit():
    # No tasks
    worker = Coworker()
    assert not worker.should_exit

    # Set exit
    worker.exit_when_idle = True
    assert worker.should_exit

    # Has tasks
    worker.add_tasks([1, 2, 3])
    assert not worker.should_exit

    # Tasks done
    await worker.start()
    assert worker.should_exit


@async_test
async def test_start_tasks():
    task_futures = await SquareWorker().start([1, 2, 3])
    responses = [f.result() for f in task_futures]

    assert responses == [1, 4, 9]


@async_test
async def test_add_tasks():
    worker = SquareWorker()
    asyncio.ensure_future(worker.start())  # Generally, add_tasks is used with a worker in the background.

    # One task
    result = await worker.add_tasks(2)
    assert result == 4

    # Mulitiple tasks
    results = await asyncio.gather(*worker.add_tasks([1, 2, 3]))
    assert results == [1, 4, 9]

    # Stop worker
    await worker.stop()
    assert worker.is_finished


@async_test
async def test_concurrency_with_window():
    worker = ConcurrentWorker(sliding_window=True)
    worker.exit_when_idle = True

    worker.add_tasks(list(range(30)))

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 30
    assert len(worker.task_futures) == 30

    await worker._do_tasks()

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 30

    await asyncio.sleep(0)      # 1st batch: Wait for tasks to start

    assert worker.concurrency == 10
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 30

    await asyncio.sleep(0.1)    # 1st batch: Wait for tasks to finish

    assert worker.concurrency == 4
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 24

    await worker._do_tasks()    # 2nd batch: Starts 6 to bring concurrency to 10
    await asyncio.sleep(0)

    assert worker.concurrency == 10
    assert worker.task_queue.qsize() == 14
    assert len(worker.task_futures) == 24

    await worker.start()        # Wait for everything to be done

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 0
    assert len(worker.task_futures) == 0


@async_test
async def test_concurrency_without_window():
    worker = ConcurrentWorker(sliding_window=False)
    worker.exit_when_idle = True

    worker.add_tasks(list(range(30)))

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 30
    assert len(worker.task_futures) == 30

    await worker._do_tasks()

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 30

    await asyncio.sleep(0)      # 1st batch: Wait for tasks to start

    assert worker.concurrency == 10
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 30

    await asyncio.sleep(0.1)    # 1st batch: Wait for tasks to finish

    assert worker.concurrency == 4
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 24

    await worker._do_tasks()   # 2nd batch: As 1st batch isn't finished, no additional tasks started.
    await asyncio.sleep(0)

    assert worker.concurrency == 4
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 24

    await asyncio.sleep(0.1)

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 20
    assert len(worker.task_futures) == 20

    await worker._do_tasks()   # 2nd batch: As 1st batch is finished, starts next batch.
    await asyncio.sleep(0)

    assert worker.concurrency == 10
    assert worker.task_queue.qsize() == 10
    assert len(worker.task_futures) == 20

    await worker.start()        # Wait for everything to be done

    assert worker.concurrency == 0
    assert worker.task_queue.qsize() == 0
    assert len(worker.task_futures) == 0


@async_test
async def test_on_events():
    worker = OnWorker()
    await worker.start(list(range(5)))

    assert worker.on_start_called == 1
    assert worker.on_finish_called == 1
    assert worker.on_start_task_called == 5
    assert worker.on_finish_task_called == 5


@async_test
async def test_start_tasks():
    worker = SquareWorker()
    task_futures = await worker.start([1, 2, 3])
    results = await asyncio.gather(*task_futures)

    assert results == [1, 4, 9]


@async_test
@pytest.mark.parametrize('sliding_window', [True, False])
async def test_performance(sliding_window):
    worker = SquareWorker(sliding_window=sliding_window)
    # worker.debug = True  # Uncomment to see debug info

    start_time = time()

    await worker.start(list(range(35000)))

    total_time = time() - start_time

    assert 0.5 < total_time < 1
