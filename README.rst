coworker
==============

Generic worker that performs concurrent tasks using coroutine.

Quick Start Tutorial
====================

Define how a task is performed -- takes a single input and optionally returns a result:

.. code-block:: python

    from coworker import Coworker

    class SquareWorker(Coworker):
        async def do_task(self, task):  # Task can be anything, such as a tuple with a callable and args.
            return task * task

To run in the background forever and add tasks:

.. code-block:: python

    import asyncio

    async def background_worker_example():
        worker = SquareWorker(max_concurrency=5,    # Only 5 tasks will run concurrently
                              auto_start=True)      # Automatically start worker in the background after adding tasks
                                                    # and exits too when done.

        # Mulitiple tasks -- get all results after everything is done
        tasks = list(range(100))
        results = await asyncio.gather(*worker.add_tasks(tasks))
        print(results)  # results = [0, 1, 4, 9, ...]

        # Mulitiple tasks -- get first completed
        tasks = list(range(10))
        for task in asyncio.as_completed(worker.add_tasks(tasks)):
            print(await task)  # results = 0, 1, 4, 9, ...

        # Single task
        result = await worker.add_tasks(2)
        print(result)   # result = 4

    # Run async usage example
    asyncio.run(background_worker_example())

To run for a list of tasks and stop worker when finished:

.. code-block:: python

    task_futures = asyncio.run(SquareWorker().start([1, 2, 3]))
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
