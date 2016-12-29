import sys
import threading

# TODO: http://click.pocoo.org/5/utils/#showing-progress-bars


class SimpleProgress(object):
    def __init__(self):
        self._lock = threading.RLock()

    def report(self, msg, *args):
        with self._lock:
            sys.stdout.write("\r" + (msg % args))
            sys.stdout.flush()

    def write(self, msg, *args):
        with self._lock:
            sys.stdout.write("\r" + (msg % args) + "\n")
            sys.stdout.flush()

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()


class S3KeyProgress(SimpleProgress):
    def __init__(self):
        super(self.__class__, self).__init__()
        self._counter = 0
        self._selected = 0

    def report(self):
        with self._lock:
            self._counter += 1
            super(self.__class__, self).report('Selected %d of %d keys',
                                               self._selected, self._counter)

    def write(self, msg, *args):
        with self._lock:
            self._selected += 1
            super(self.__class__, self).write(msg, *args)
