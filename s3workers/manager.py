import sys
import signal
import logging
import threading

from time import sleep

from .work_queue import WorkQueue
from .worker import Worker

_logger = logging.getLogger(__name__)


class Manager(object):
    '''Manage several worker threads and their shared job work queue.

    :param worker_count: number of worker threads to use (i.e. concurrency)
    :param stop_signals: list of signals to act on for automatically stopping workers
    :param listen_For_unhandled_exceptions: when enabled, automatically stop workers when an
           exception is thrown and not processed (will call orignal handler)
    '''
    def __init__(self, worker_count,
                 stop_signals=[signal.SIGINT, signal.SIGTERM, signal.SIGPIPE],
                 listen_for_unhandled_exceptions=True):
        self.worker_count = worker_count
        self._stop_requested = threading.Event()

        self._work_queue = WorkQueue(worker_count * 3)
        self._workers = [Worker(self._work_queue) for i in range(worker_count)]

        for sgnl in stop_signals:
            signal.signal(sgnl, self.stop_workers)

        if listen_for_unhandled_exceptions:
            sys.excepthook = self._process_unhandled_exception

    def start_workers(self):
        '''Tell workers to start listening and handling jobs posted to the shared work queue.'''
        for worker in self._workers:
            worker.start()

    def add_work(self, job):
        '''Add a new job into the shared work queue.'''
        if self._stop_requested.is_set():
            return  # ignore work requests if a stop was requested (often if app was interrupted)
        _logger.debug('Submitting %s', job)
        self._work_queue.put(job)

    def wait_for_workers(self, join_timeout=1):
        '''Wait for workers to finish all outstanding jobs in the shared work queue.

        Should be called after all work has been submitted and the caller is ready to wait for all
        workers to gracefully stop.
        '''
        self._work_queue.all_jobs_submitted()

        _logger.debug('All jobs submitted (%d outstanding)', self._work_queue.qsize())

        while threading.active_count() > 1:
            sleep(0.1)

        for worker in self._workers:
            worker.join(join_timeout)

    def stop_workers(self, *_ignored):
        '''Immediately request that all workers stop pulling jobs off the shared work queue and stop
        themselves.

        Workers will finish jobs in progress but will stop accepting new ones and with terminate
        themselves.
        '''
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

    def _process_unhandled_exception(self, *args):
        '''Ensure application does not hang waiting on the workers for unhandled exceptions.'''
        sys.__excepthook__(*args)
        self.stop_workers()
