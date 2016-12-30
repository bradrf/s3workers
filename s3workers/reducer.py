import threading


class Reducer(object):
    '''Execute reduction logic against an accumulator value.

    This abstraction allows for a caller to perform a reduction of many values into one or a smaller
    set. For example, to provide summation of values, collect interesting values into an array, or
    produce grouped summations in a dictionary.

    :param reduction_string: exec'd to perform accumulation logic
                             (must set the accumulator during each call or nothing will aggregate)
    :param accumulation_string: eval'd to an initial value to accumulate the reduction results
    '''

    def __init__(self, reduction_string, accumulation_string='0'):
        self.accumulator = eval(accumulation_string)
        self._accumulator_lock = threading.Lock()
        reducer_code = compile('def reduce(accumulator, name, size, md5, last_modified): ' +
                               reduction_string +
                               '; return accumulator',
                               '<reduce>',
                               'exec')
        rlocals = {}
        exec(reducer_code, {}, rlocals)
        self._reduce = rlocals['reduce']

    def reduce(self, name, size, md5, last_modified):
        with self._accumulator_lock:
            self.accumulator = self._reduce(self.accumulator, name, size, md5, last_modified)
        return self.accumulator
