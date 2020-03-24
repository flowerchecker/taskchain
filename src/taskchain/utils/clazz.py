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


def import_by_string(string: str) -> Union[ModuleType, List[type], type]:
    parts = string.split('.')
    has_wiled_card = '*' in parts[-1]
    is_class = parts[-1][0].upper() == parts[-1][0]
    if has_wiled_card or is_class:
        module = importlib.import_module('.'.join(parts[:-1]))
        pattern = re.compile(re.sub(r'((?<=([^.]))|^)\*', '.*', parts[-1]))
    else:
        return importlib.import_module('.'.join(parts))

    members = []
    for name, member in module.__dict__.items():
        if pattern.match(name) and not name.startswith('__') and inspect.getmodule(member) == module:
            if not has_wiled_card:
                return member
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
