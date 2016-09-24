coworker
==============

Generic worker that performs concurrent tasks using coroutine.

Quick Start Tutorial
====================

Define how a task is performed and create the worker:

.. code-block:: python

    from coworker import Coworker

    class SquareWorker(Coworker)
        async def do_task(self, task)
            return task * task

    worker = SquareWorker()

To run in the background forever and add tasks:

.. code-block:: python

    # Run in background
    asyncio.ensure_future(worker.start())

    # Add tasks
    tasks = list(range(100))
    task_futures = await worker.add_tasks(tasks)

    # Wait / get results
    asyncio.wait(task_futures)
    results = [f.result() for f in task_futures]

    # Do a single task
    task = 2
    task_future = worker.add_tasks(task)
    result = await task_future  # result = 4

    # Stop worker (cancels outstanding tasks)
    worker.stop()

To run for a list of tasks and exit when finished:

.. code-block:: python

    task_futures = await worker.start(tasks)
    results = [f.result() for f in task_futures]


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
