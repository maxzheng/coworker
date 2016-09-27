import asyncio
import logging


log = logging.getLogger(__name__)


class Coworker(object):
    """ Generic worker to perform concurrent tasks using coroutine IO loop. """

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

        #: A future to indicate when new tasks need to be checked / can be worked on.
        self._check_task_future = asyncio.Future()

        #: Is this worker finished?
        self.is_finished = False

        #: Start a task as soon as there is an available slot based on concurrency instead of waiting until
        #: all concurrent tasks are done.
        self.sliding_window = sliding_window

        #: Log debug. By having this distinct flag allows for debugging when needed without polluting upstream debug logs.
        self.debug = False

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
            task_futures = self.add_tasks(tasks)
            self.exit_when_idle = True

        self._log('on_start')
        await self.on_start()

        while not self.should_exit:
            self._log('_do_tasks', '(Check for new tasks and start them)')
            await self._do_tasks()

            self._log('_wait_for_tasks', '(Wait for new tasks or existing tasks)')
            await self._wait_for_tasks()

        self._log('on_finish')
        await self.on_finish()

        self.is_finished = True

        return task_futures

    def _check_tasks(self):
        """" Signal that there are new tasks or tasks were completed. """
        if not self._check_task_future.done():
            self._check_task_future.set_result(True)

    async def _wait_for_tasks(self):
        """" Wait for new tasks or tasks to be completed. """
        await self._check_task_future
        self._check_task_future = asyncio.Future()

    @property
    def available_slots(self):
        """ Number of available slots to do tasks based on concurrency and window settings """

        if self.sliding_window:
            return self.max_concurrency - self.concurrency

        else:
            return self.max_concurrency if not self.concurrency else 0

    async def _do_tasks(self):
        """ Perform the tasks based on available concurrency slots """
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
                self._log('on_start_task', task)
                await self.on_start_task(task)

                self._log('do_task', task)
                result = await self.do_task(task)

                self._log('on_finish_task', task)
                await self.on_finish_task(task, result)

            except Exception as e:
                self.task_futures[task]['result'].set_exception(e)

            else:
                self.task_futures[task]['result'].set_result(result)

            del self.task_futures[task]

        except KeyError:  # Task was cancelled / removed from self.task_futures by :meth:`self.cancel_task`
            pass

        self._check_tasks()

        self.concurrency -= 1

    async def do_task(self, task):
        """ Perform the task. Sub-class should override this to do something more meaningful. """
        print('Performing task', task)

    def add_tasks(self, tasks):
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

                self.task_queue.put_nowait(task)

        self._check_tasks()

        if is_list:
            return task_futures

        else:
            return task_future

    async def stop(self):
        """ Stop the worker by canceling all tasks and then wait for worker to finish. """
        for task in self.task_futures:
            self.cancel_task(task)

        self.exit_when_idle = True
        self._check_tasks()

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
        """ Invoked before worker starts. Subclass should override if needed. """

    async def on_finish(self):
        """ Invoked after worker completes all tasks before exiting worker. Subclass should override if needed. """

    async def on_start_task(self, task):
        """
        Invoked before starting the task. Subclass should override if needed.

        :param task: Task that will start
        """

    async def on_finish_task(self, task, result):
        """"
        Invoked after the task is completed. Subclass should override if needed.

        :param task: Task that was finished
        :param result: Return value from :meth:`self.do_task(task)`
        """

    def _log(self, action, detail=None):
        """" Log action with optional detail."""
        if self.debug:
            log.debug('%s %s %s', self.__class__.__name__, action, '' if detail is None else detail)
