import sys
import signal
import logging
import threading

from queue import Queue
from time import sleep

from .worker import Worker

_logger = logging.getLogger(__name__)


class Manager(object):
    def __init__(self, worker_count,
                 stop_signals=[signal.SIGINT, signal.SIGTERM, signal.SIGPIPE],
                 listen_for_unhandled_exceptions=True):
        self.worker_count = worker_count
        self._stop_requested = threading.Event()

        self._work_queue = Queue(worker_count * 3)
        self._workers = [ Worker(self._work_queue) for i in range(worker_count) ]

        for sgnl in stop_signals:
            signal.signal(sgnl, self.stop_workers)

        if listen_for_unhandled_exceptions:
            sys.excepthook = self._process_unhandled_exception

    def start_workers(self):
        for worker in self._workers:
            worker.start()

    def add_work(self, job):
        if self._stop_requested.is_set():
            return  # ignore work requests if a stop was requested (often if app was interrupted)
        _logger.debug('Submitting %s', job)
        self._work_queue.put(job)

    def wait_for_workers(self, join_timeout=1):
        Worker.all_jobs_submitted()
        _logger.debug('All jobs submitted (%d outstanding)', self._work_queue.qsize())

        while threading.active_count() > 1:
            sleep(0.1)

        for worker in self._workers:
            worker.join(join_timeout)

    def stop_workers(self, *_ignored):
        if self._stop_requested.is_set():
            return
        self._stop_requested.set()
        _logger.info('Stopping with %d jobs outstanding', self._work_queue.qsize())
        for worker in self._workers:
            if worker.is_alive():
                _logger.debug(worker)
                worker.stop()

    ######################################################################
    # private

    def _process_unhandled_exception(self, type, value, traceback):
        '''Ensure application does not hang waiting on the workers for unhandled exceptions.'''
        sys.__excepthook__(type, value, traceback)
        self.stop_workers()
