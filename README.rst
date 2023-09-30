coworker
==============

Generic worker that performs concurrent tasks using coroutine.

Quick Start Tutorial
====================

Define how a task is performed -- `do_task` takes a single input and optionally returns a result:

.. code-block:: python

    from coworker import Coworker

    class SquareWorker(Coworker):
        async def do_task(self, task):  # Task can be anything, such as a tuple with a callable and args.
            return task * task

Now let's do some work:

.. code-block:: python

    import asyncio

    async def background_worker_example():
        worker = SquareWorker(max_concurrency=5)    # Only 5 tasks will run concurrently. Defalts to 10

        # Single task
        result = await worker.do(2)
        print(result)   # result = 4

        # Mulitiple tasks -- get all results after everything is done
        tasks = list(range(100))
        results = await worker.do(tasks)
        print(results)  # results = [0, 1, 4, 9, ...]

        # Mulitiple tasks -- get first completed
        tasks = list(range(10))
        for result in worker.do(tasks, as_iterator=True):
            print(await result)  # results = 0, 1, 4, 9, ...

    # Run async usage example
    asyncio.run(background_worker_example())

Besides `max_concurrency`, you can also pass the following params to Coworker:

* `sliding_window` to use sliding window or tumbling window when processing the tasks concurrently. Defaults to True.
* `do_task` for a callable to do the task instead of having to sub-class. E.g. Coworker(do_task=lamdba x: x * x)

Links & Contact Info
====================

| PyPI Package: https://pypi.python.org/pypi/coworker
| GitHub Source: https://github.com/maxzheng/coworker
| Report Issues/Bugs: https://github.com/maxzheng/coworker/issues
|
| Connect: https://www.linkedin.com/in/maxzheng
| Contact: maxzheng.os @t gmail.com
