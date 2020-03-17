import functools


class persistent(object):

    def __init__(self, method):
        self.method = method

    def __call__(self, obj):
        attr = f'_{self.method.__name__}'
        if not hasattr(obj, attr) or getattr(obj, attr) is None:
            setattr(obj, attr, self.method(obj))
        return getattr(obj, attr)

    def __get__(self, instance, instancetype):
        return functools.partial(self.__call__, instance)
