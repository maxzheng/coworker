import asyncio


class Coworker(object):
    """
    Generic worker to perform concurrent tasks using coroutine loop.

    Define how a task is performed and create the worker:

    .. code-block:: python

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

    """

    def __init__(self, max_concurrency=10, sliding_window=True):
        """
        Initialize worker

        :param int max_concurrency: How many tasks can be done at the same time. Defaults to 10.
        :param bool sliding_window: Start a task as soon as there is an available slot based on concurrency instead of
                                    waiting for all concurrent tasks to be completed first.
        """

        #: Queue for tasks to be performed
        self.task_queue = asyncio.Queue()

        #: Futures for each task: task => {'result': future, 'do': future}
        self.task_futures = {}

        #: Exit the worker when idle (all tasks are done / no more tasks queued)
        self.exit_when_idle = False

        #: Current number of concurrent tasks being performed
        self.concurrency = 0

        #: Maxium number of concurrent tasks to perform
        self.max_concurrency = max_concurrency

        #: Number of seconds to wait before checking for new tasks to do
        self.check_tasks_interval = 0.1

        #: Is this worker finished?
        self.is_finished = False

        #: Start a task as soon as there is an available slot based on concurrency instead of waiting until
        #: all concurrent tasks are done.
        self.sliding_window = sliding_window

    @property
    def idle(self):
        """ Worker has nothing to do and is doing nothing """
        return self.task_queue.empty() and not self.task_futures

    @property
    def should_exit(self):
        return self.idle and self.exit_when_idle

    async def start(self, tasks=None):
        """
        Start the worker.

        :param list tasks: List of tasks to do. If provided, worker will exit immediately after all tasks
                           are done. If that's not desired, use :meth:`self.add_task` instead.
        :return: List of futures for each task in the same order.
        """
        self.is_finished = False

        task_futures = []

        if tasks:
            task_futures = await self.add_tasks(tasks)
            self.exit_when_idle = True

        await self.on_start()

        while not self.should_exit:
            await self._do_tasks()

            await asyncio.sleep(self.check_tasks_interval)

        await self.on_finish()

        self.is_finished = True

        return task_futures

    @property
    def available_slots(self):
        """ Number of available slots to do tasks based on concurrency and window settings """

        if self.sliding_window:
            return self.max_concurrency - self.concurrency

        else:
            return self.max_concurrency if not self.concurrency else 0

    async def _do_tasks(self):
        """ Perform the tasks based on available concurrency slots """
        if self.available_slots:
            for _ in range(self.available_slots):
                try:
                    task = self.task_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                if task in self.task_futures:  # If it is not in here, it was cancelled. See :meth:`self.cancel_task`
                    task_future = self._do_task(task)
                    self.task_futures[task]['do'] = asyncio.ensure_future(task_future)

    async def _do_task(self, task):
        """ Perform the task and call :meth:`self.on_start_task` and :meth:`self.on_finish_task` """
        self.concurrency += 1

        try:
            try:
                await self.on_start_task(task)

                response = await self.do_task(task)

                await self.on_finish_task(task)

            except Exception as e:
                self.task_futures[task]['result'].set_exception(e)

            else:
                self.task_futures[task]['result'].set_result(response)

            del self.task_futures[task]

        except KeyError:  # Task was cancelled / removed from self.task_futures by :meth:`self.cancel_task`
            pass

        self.concurrency -= 1

    async def do_task(self, task):
        """ Perform the task. Sub-class should override this to do something more meaningful. """
        print('Performing task', task)

    async def add_tasks(self, tasks):
        """
        Add task(s) to queue

        :param object|list tasks: A single or list of task(s) to add to the queue.
        :return: If a single task is given, then returns a single task future that will contain result from :meth:`self.do_task`
                 If a list of tasks is given, then a list of task futures, one for each task.

                 Note that if hash(task) is the same as another/existing task,
                 the same future will be returned, and the task is only performed once.
                 If it is desired to perform the same task multiple times / distinctly, then the task
                 will need to be wrapped in another object that has a unique hash.
        """
        if not tasks:
            raise ValueError('Please provide tasks to add')

        is_list = isinstance(tasks, list)
        tasks = tasks if is_list else [tasks]
        task_futures = []

        for task in tasks:
            if task in self.task_futures:
                task_futures.append(self.task_futures[task]['result'])

            else:
                task_future = asyncio.Future()
                task_futures.append(task_future)

                self.task_futures[task] = {}  # Do not use defaultdict as that creates the task key from other places.
                self.task_futures[task]['result'] = task_future

                await self.task_queue.put(task)

        if is_list:
            return task_futures

        else:
            return task_future

    async def stop(self):
        """ Stop the worker by canceling all tasks and then wait for worker to finish. """
        for task in self.task_futures:
            self.cancel_task(task)

        self.exit_when_idle = True

        while not self.is_finished:
            await asyncio.sleep(0.1)

    def cancel_task(self, task):
        """ Cancel a task """
        if task in self.task_futures:
            for task_future in self.task_futures[task].values():
                task_future.cancel()

            # No way to remove task from self.task_queue so using existence in self.task_futures to indicate cancel.
            del self.task_futures[task]

    async def on_start(self):
        """ Invoked before worker starts """

    async def on_finish(self):
        """ Invoked after worker completes all tasks before exiting worker """

    async def on_start_task(self, task):
        """ Invoked before starting the task """

    async def on_finish_task(self, task):
        """" Invoked after the task is completed """
