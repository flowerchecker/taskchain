import functools


class Meta(dict):

    def __init__(self, cls):
        super().__init__()

        if not hasattr(cls, 'Meta'):
            return

        for attr in dir(cls.Meta):
            if attr.startswith('__'):
                continue
            self[attr] = getattr(cls.Meta, attr)

    def __getattr__(self, attr):
        if attr in self:
            return self[attr]
        return super().__getattribute__(attr)


class persistent:

    def __init__(self, method):
        self.method = method

    def __call__(self, obj):
        attr = f'_{self.method.__name__}'
        if not hasattr(obj, attr) or getattr(obj, attr) is None:
            setattr(obj, attr, self.method(obj))
        return getattr(obj, attr)

    def __get__(self, instance, instancetype):
        return functools.partial(self.__call__, instance)


def inheritors(cls, include_self=True):
    subclasses = {cls} if include_self else set()
    work = [cls]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            subclasses.add(child)
            work.append(child)
    return subclasses
