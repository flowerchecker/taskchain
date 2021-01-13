import functools
import importlib
import inspect
import re
from types import ModuleType
from typing import Union, List, Type


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


def fullname(clazz):
    """
    Return fully qualified name of the given class.

    >>> from collections import Counter
    >>> fullname(Counter)
    'collections.Counter'
    """
    return f'{clazz.__module__}.{clazz.__name__}'


def issubclass(clazz, superclazz):
    """
    Check whether one class is subclass of the other.

    >>> class SuperClass:
    ...     pass
    >>> class SubClass(SuperClass):
    ...     pass
    >>> class SubSubClass(SubClass):
    ...     pass
    >>> import collections

    >>> issubclass(SuperClass, SubClass)
    False
    >>> issubclass(SubClass, SuperClass)
    True
    >>> issubclass(SubSubClass, SuperClass)
    True
    >>> issubclass(dict, collections.defaultdict)
    False
    >>> issubclass(collections.defaultdict, dict)
    True
    >>> issubclass(collections.defaultdict, 'builtins.dict')
    True
    """
    # HACK: fix autoreload in jupyter notebooks
    superclazz_name = superclazz if type(superclazz) == str else fullname(superclazz)
    return any(superclazz_name == fullname(c) for c in clazz.__mro__)


def isinstance(obj, clazz):
    # HACK: fix autoreload in jupyter notebooks
    return issubclass(obj.__class__, clazz)


def import_by_string(string: str) -> Union[ModuleType, List[type], type]:
    parts = string.split('.')
    try:
        return importlib.import_module('.'.join(parts))
    except ModuleNotFoundError:
        pass

    has_wiled_card = '*' in parts[-1]
    module = importlib.import_module('.'.join(parts[:-1]))
    pattern = re.compile(re.sub(r'((?<=([^.]))|^)\*', '.*', parts[-1]))

    members = []
    for name, member in module.__dict__.items():
        if pattern.match(name) and not name.startswith('__'):
            if not has_wiled_card:
                return member
            if inspect.getmodule(member) == module:
                members.append(member)
    return members


def get_classes_by_import_string(string: str, cls: Type[object] = object):
    members = import_by_string(string)
    if type(members) is not list:
        members = [members]
    classes = []
    for members in members:
        if inspect.isclass(members) and issubclass(members, cls):
            classes.append(members)
    return classes


def instancelize_clazz(clazz, args, kwargs):
    cls = import_by_string(clazz)
    obj = cls(*args, **kwargs)
    return obj


def find_and_instancelize_clazz(obj):
    if isinstance(obj, dict) and 'class' in obj:
        return instancelize_clazz(
            obj['class'],
            find_and_instancelize_clazz(obj.get('args', [])),
            find_and_instancelize_clazz(obj.get('kwargs', {})),
        )

    if type(obj) is list:
        return [find_and_instancelize_clazz(i) for i in obj]

    if type(obj) is dict:
        return {k: find_and_instancelize_clazz(v) for k, v in obj.items()}

    return obj
