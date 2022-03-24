import functools
import importlib
import inspect
import re
from time import sleep
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
    """
    Method decorator.
    Has to be used on decorator without arguments.
    Saves result in `self.__method_name`
    and next time does not call decorated method and only return saved value.
    """

    def __init__(self, method):
        self.method = method

    def __call__(self, obj):
        attr = f'__{self.method.__name__}'
        if not hasattr(obj, attr) or getattr(obj, attr) is None:
            setattr(obj, attr, self.method(obj))
        return getattr(obj, attr)

    def __get__(self, instance, instancetype):
        return functools.partial(self.__call__, instance)


class repeat_on_error:
    """ Method decorator which calls method again on error. """

    def __init__(self, retries: int = 10, waiting_time: int = 1, wait_extension: float = 1.):
        """
        Args:
            retries: how many times try to call again
            waiting_time: how many seconds wait before first retry
            wait_extension: how many times increase waiting time after each retry
        """
        if callable(retries):
            self.method = retries
            retries = 10
        self.retries = retries
        self.waiting_time = waiting_time
        self.wait_extension = wait_extension

    def __call__(self, method):
        def decorated(*args, **kwargs):
            waiting_time = self.waiting_time
            for i in range(self.retries):
                try:
                    return method(*args, **kwargs)
                except Exception as error:
                    if i + 1 == self.retries:
                        raise error
                    sleep(waiting_time)
                    waiting_time *= self.wait_extension
            assert False
        return decorated

    def __get__(self, instance, instancetype):
        return functools.partial(self(self.method), instance)


def inheritors(cls, include_self=True):
    """ Get all classes inheriting of given class. """
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
    clazz_name = clazz.__name__ if hasattr(clazz, '__name__') else clazz._name
    return f'{clazz.__module__}.{clazz_name}'


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
    """
    Get member (class, function or module) by import string.
    String can contain `*` as last part then list of all members in the module is return.
    """
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
    if len(members) == 0:
        raise ImportError(f'Cannot import "{string}".')
    return members


def get_classes_by_import_string(string: str, cls: Type[object] = object):
    """
    Get all classes inheriting given class and are described by given import string.
    Wildcard `*` can be used to import all classes in described module.
    """
    members = import_by_string(string)
    if type(members) is not list:
        members = [members]
    classes = []
    for member in members:
        if inspect.isclass(member) and issubclass(member, cls):
            classes.append(member)
    return classes


def instantiate_clazz(clazz, args, kwargs):
    """ Instantiate class from import string and arguments. """
    cls = import_by_string(clazz)
    obj = cls(*args, **kwargs)
    return obj


def find_and_instantiate_clazz(obj, instancelize_clazz_fce=None):
    """
    Got through json-like object and find all classes definitions and instantiate them.
    Class definition is dict with `class` key and optionally `args` and `kwargs` keys.
    """

    if instancelize_clazz_fce is None:
        instancelize_clazz_fce = instantiate_clazz
    if isinstance(obj, dict) and 'class' in obj:
        return instancelize_clazz_fce(
            obj['class'],
            find_and_instantiate_clazz(obj.get('args', [])),
            find_and_instantiate_clazz(obj.get('kwargs', {})),
        )

    if type(obj) is list:
        for i, value in enumerate(obj):
            obj[i] = find_and_instantiate_clazz(value)

    if type(obj) is dict:
        for key, value in obj.items():
            obj[key] = find_and_instantiate_clazz(value)

    return obj


def object_to_definition(obj):
    """ Get config definition from class instance. Kind of reverse of `find_and_instantiate_clazz`. """
    result = {
        'class': fullname(obj.__class__),
    }
    result['kwargs'] = kwargs = {}
    parameters = inspect.signature(obj.__init__).parameters
    for name, parameter in parameters.items():
        if hasattr(obj, '_' + name):
            value = getattr(obj, '_' + name)
        elif hasattr(obj, name):
            value = getattr(obj, name)
        else:
            raise AttributeError(f'Value of __init__ argument `{name}` not found for class `{fullname(obj.__class__)}`, '
                                 f'make sure that value is saved in `self.{name}` or `self._{name}`')
        if value is not None and all(not isinstance(value, type_) for type_ in {int, float, bool, str, list, set, dict}):
            value = object_to_definition(value)
        kwargs[name] = value
    return result
