coworker
==============

Generic worker that performs concurrent tasks using coroutine.

Quick Start Tutorial
====================

Define how a task is performed and create the worker:

.. code-block:: python

    from coworker import Coworker

    class SquareWorker(Coworker):
        async def do_task(self, task):
            return task * task

    worker = SquareWorker(max_concurrency=5)    # Only 5 tasks will run concurrently
                                                # As do_task is fast, 35,000 tasks can be done in 1 second.

To run in the background forever and add tasks:

.. code-block:: python

    import asyncio

    async def background_worker_example():
        # Start worker / Run in background
        asyncio.ensure_future(worker.start())

        # Mulitiple tasks
        tasks = list(range(100))
        results = await asyncio.gather(*worker.add_tasks(tasks))
        print(results)  # results = [0, 1, 4, 9, ...]

        # Single task
        result = await worker.add_tasks(2)
        print(result)   # result = 4

        # Stop worker
        await worker.stop()

    # Run async usage example
    asyncio.get_event_loop().run_until_complete(background_worker_example())


To run for a list of tasks and stop worker when finished:

.. code-block:: python

    task_futures = asyncio.get_event_loop().run_until_complete(worker.start([1, 2, 3]))
    print([t.result() for t in task_futures])   # [1, 4, 9]


Links & Contact Info
====================

| Documentation: http://coworker.readthedocs.org
|
| PyPI Package: https://pypi.python.org/pypi/coworker
| GitHub Source: https://github.com/maxzheng/coworker
| Report Issues/Bugs: https://github.com/maxzheng/coworker/issues
|
| Connect: https://www.linkedin.com/in/maxzheng
| Contact: maxzheng.os @t gmail.com
