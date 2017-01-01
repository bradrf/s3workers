import logging
import threading
import queue

_logger = logging.getLogger(__name__)


class Worker(threading.Thread):
    '''Simple thread to continuously pull jobs off a work queue until told to stop or that no more
    jobs will be submitted.

    :param work_queue: the job queue to query for work
    '''
    def __init__(self, work_queue):
        super(self.__class__, self).__init__()
        self._work_queue = work_queue
        self._current_lock = threading.Lock()
        self._current_job = None
        self._stop_requested = threading.Event()

    def __str__(self):
        statestr = 'alive' if self.is_alive() else 'dead'
        jobstr = ' current=' + str(self._current_job) if self._current_job else ''
        return '%s(%s%s)' % (self.__class__.__name__, statestr, jobstr)

    def run(self):
        '''Run until a stop is requested or there is no more work expected.'''
        while True:
            if self._stop_requested.is_set():
                break
            if self._work_queue.is_done():
                break
            try:
                with self._current_lock:
                    self._current_job = self._work_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                _logger.debug('starting: %s', self._current_job)
                self._current_job.run()
            finally:
                self._work_queue.task_done()
                with self._current_lock:
                    self._current_job = None

    def stop(self):
        '''Finish any job currently in progress and then terminate.'''
        self._stop_requested.set()
        with self._current_lock:
            if self._current_job:
                self._current_job.stop()
