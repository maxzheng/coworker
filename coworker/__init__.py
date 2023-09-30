import asyncio
import inspect
import logging


log = logging.getLogger(__name__)


class Coworker(object):
    """ Generic worker to perform concurrent tasks using coroutine IO loop. """

    def __init__(self, max_concurrency=10, sliding_window=True, do_task=None):
        """
        Initialize worker

        :param int max_concurrency: How many tasks can be done at the same time. Defaults to 10.
        :param bool sliding_window: Start a task as soon as there is an available slot based on concurrency instead of
                                    waiting for all concurrent tasks to be completed first.
        """
        #: Maxium number of concurrent tasks to perform
        self.max_concurrency = max_concurrency

        #: Start a task as soon as there is an available slot based on concurrency instead of waiting until
        #: all concurrent tasks are done.
        self.sliding_window = sliding_window

        #: Callable for doing the actual task
        self._do = do_task

        #: Queue for tasks to be performed
        self._task_queue = asyncio.Queue()

        #: Futures for each task: task => {'result': future, 'do': future}
        self._task_futures = {}

        #: Current number of concurrent tasks being performed
        self._concurrency = 0

        #: A future to indicate when new tasks need to be checked / can be worked on.
        self._check_task_future = asyncio.Future()

        #: Is this worker finished?
        self._is_finished = True

        #: Log debug. This distinct flag allows for debugging when needed without polluting upstream
        #: debug logs.
        self.debug = False

    @property
    def idle(self):
        """ Worker has nothing to do and is doing nothing """
        return self._task_queue.empty() and not self._task_futures

    @property
    def should_exit(self):
        return self.idle

    async def start(self):
        """
        Start the worker.
        """
        if not self._is_finished:
            return
        self._is_finished = False

        self._log('on_start')
        await self.on_start()

        while not self.should_exit:
            self._log('_do_tasks', f'(Check for new tasks and start them). Available slots: {self.available_slots}')
            await self._do_tasks()

            self._log('_wait_for_tasks', '(Wait for new tasks or existing tasks)')
            await self._wait_for_tasks()

        self._log('on_finish')
        await self.on_finish()

        self._is_finished = True

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
            return self.max_concurrency - self._concurrency

        else:
            return self.max_concurrency if not self._concurrency else 0

    async def _do_tasks(self):
        """ Perform the tasks based on available concurrency slots """
        for _ in range(self.available_slots):
            try:
                task = self._task_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if id(task) in self._task_futures:  # If it is not in here, it was cancelled. See :meth:`self.cancel_task`
                self._concurrency += 1
                task_future = self._do_task(task)
                self._task_futures[id(task)]['do'] = asyncio.ensure_future(task_future)

    async def _do_task(self, task):
        """ Perform the task and call :meth:`self.on_start_task` and :meth:`self.on_finish_task` """
        try:
            try:
                self._log('on_start_task', task)
                await self.on_start_task(task)

                self._log('do_task', task)
                result = await self.do_task(task)

                self._log('on_finish_task', task)
                await self.on_finish_task(task, result)

            except Exception as e:
                self._task_futures[id(task)]['result'].set_exception(e)

            else:
                self._task_futures[id(task)]['result'].set_result(result)

            del self._task_futures[id(task)]

        except KeyError:  # Task was cancelled / removed from self._task_futures by :meth:`self.cancel_task`
            pass

        self._check_tasks()

        self._concurrency -= 1

    async def do_task(self, task):
        """ Perform the task. Sub-class should override this to do something more meaningful. """
        if self._do:
            if inspect.iscoroutinefunction(self._do):
                return await self._do(task)
            else:
                return self._do(task)
        else:
            raise NotImplementedError('Please either instantiate constructor with do_task param or '
                                      'sub-class and override do_task function')

    def do(self, tasks, as_iterator=False):
        """
        Add task(s) to queue and return the results

        Note that if id(task) is the same as another/existing task, the same future will be returned, and the task is
        only performed once. If it is desired to perform the same task multiple times / distinctly, then the task
        will need to be wrapped in another object that has a unique id.

        :param object|list tasks: A single or list of task(s) to add to the queue.
        :param bool as_iterator: Return result as an iterator where the first result can be awaited on as soon as the
                                 task is done.
        :return: If a single task is provided, then returns an instance of Future is returned that can be awaited on
                 to get the result.

                 If a list of task is provied, then return a list of results when all tasks are done or
                 an iterator to get the first result as soon as a task is done when `as_iterator` is set to True
        """
        if not tasks:
            raise ValueError('Please provide tasks to do')

        is_list = isinstance(tasks, (list, tuple))
        tasks = tasks if is_list else [tasks]
        task_futures = []

        for task in tasks:
            if id(task) in self._task_futures:
                task_futures.append(self._task_futures[id(task)]['result'])

            else:
                task_future = asyncio.Future()
                task_futures.append(task_future)

                self._task_futures[id(task)] = {}    # Do not use defaultdict as that creates the task key
                                                    # from other places.
                self._task_futures[id(task)]['result'] = task_future

                self._task_queue.put_nowait(task)

        self._check_tasks()

        if self._is_finished:
            asyncio.ensure_future(self.start())

        if is_list:
            return asyncio.as_completed(task_futures) if as_iterator else asyncio.gather(*task_futures)

        else:
            return task_future

    async def stop(self):
        """ Stop the worker by canceling all tasks and then wait for worker to finish. """
        for task in self._task_futures:
            self.cancel_task(task)

        self._check_tasks()

        while not self._is_finished:
            await asyncio.sleep(0.1)

    def cancel_task(self, task):
        """ Cancel a task """
        task_ids = [id(task)]
        if isinstance(task, int):
            task_ids.append(task)  # Allows tasks to be cancelled by ID

        for task_id in task_ids:
            if task_id in self._task_futures:
                for task_future in self._task_futures[task_id].values():
                    task_future.cancel()

                # No way to remove task from self._task_queue so using existence in self._task_futures to
                # indicate cancel.
                del self._task_futures[task_id]

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
            print(self.__class__.__name__, action, '' if detail is None else detail)
