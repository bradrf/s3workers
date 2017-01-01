import threading

from queue import Queue


class WorkQueue(Queue):
    '''Simple wrapper to also provide ability to indicate when no more work is expected.
    '''
    def __init__(self, *args):
        Queue.__init__(self, *args)  # Queue does not inherit from object (is an old-style class)
        self._all_jobs_submitted = threading.Event()

    def all_jobs_submitted(self):
        '''Indicate that no more work is expected on this queue.'''
        self._all_jobs_submitted.set()

    def is_done(self):
        '''Determine if there is more work expected on this queue.'''
        return self._all_jobs_submitted.is_set() and self.empty()
