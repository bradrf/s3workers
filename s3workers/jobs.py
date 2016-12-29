import threading


class Job(object):
    def __init__(self):
        self._state = 'ready'
        self._stop_requested = threading.Event()

    def run(self, *args, **kwargs):
        try:
            if not self._stop_requested.isSet():
                self._state = 'running'
                self._runner(*args, **kwargs)
        finally:
            self._state = 'stopped'

    def stop(self):
        self._stop_requested.set()

    def __str__(self, details=''):
        return '%s(%s) is %s' % (self.__class__.__name__, details, self._state)

    def _runner(self):
        pass


class S3ListJob(Job):
    def __init__(self, bucket, prefix, selector, key_handler, progress):
        super(self.__class__, self).__init__()
        self._bucket = bucket
        self._prefix = prefix
        self._selector = selector
        self._key_handler = key_handler
        self._progress = progress

    def __str__(self):
        return super(self.__class__, self).__str__(self._bucket.name + '/' + self._prefix)

    def _runner(self):
        for key in self._bucket.list(prefix=self._prefix):
            if self._stop_requested.isSet():
                break
            self._progress()
            if not key.md5:
                key.md5 = key.etag[1:-1]  # GROSS. HACK. Likely break if multipart-uploaded...
            if self._is_selected(key):
                self._key_handler(key)

    def _is_selected(self, key):
        if not self._selector:
            return True
        size = key.size  # noqa: 841
        name = key.name  # noqa: 841
        md5 = key.md5    # noqa: 841
        return eval(self._selector)
