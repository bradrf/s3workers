import logging
import threading
import queue

_logger = logging.getLogger(__name__)


class Worker(threading.Thread):
    _all_jobs_submitted = threading.Event()

    @classmethod
    def all_jobs_submitted(self):
        self._all_jobs_submitted.set()

    def __init__(self, work):
        super(self.__class__, self).__init__()
        self._work = work
        self._current_lock = threading.Lock()
        self._current_job = None
        self._stop_requested = threading.Event()

    def __str__(self):
        statestr = 'alive' if self.is_alive() else 'dead'
        jobstr = ' current=' + str(self._current_job) if self._current_job else ''
        return '%s(%s%s)' % (self.__class__.__name__, statestr, jobstr)

    def run(self):
        while True:
            if self._stop_requested.isSet():
                break
            if Worker._all_jobs_submitted.isSet() and self._work.empty():
                break
            try:
                with self._current_lock:
                    self._current_job = self._work.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                _logger.debug('starting: %s', self._current_job)
                self._current_job.run()
            finally:
                self._work.task_done()
                with self._current_lock:
                    self._current_job = None

    def stop(self):
        self._stop_requested.set()
        with self._current_lock:
            if self._current_job:
                self._current_job.stop()
