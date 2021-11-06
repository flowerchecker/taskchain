import abc
from inspect import signature
from pathlib import Path

import taskchain.config
from taskchain.utils.clazz import fullname
from typing import Union, Any, Iterable, List


class NO_DEFAULT: pass
class NO_VALUE: pass


class AbstractParameter(abc.ABC):

    NO_DEFAULT = NO_DEFAULT
    NO_VALUE = NO_VALUE

    def __init__(self,
                 default: Any = NO_DEFAULT,
                 ignore_persistence: bool = False,
                 dont_persist_default_value: bool = True,
                 ):
        """
        Args:
            default: value used if not provided in config, default to NO_DEFAULT meaning that param is required
            ignore_persistence: do not use this parameter in persistence, useful params without influence on output
            dont_persist_default_value: if value of parameter is same as default, do not use it in persistence,
                useful for adding new parameters without recomputation of data
        """
        self.default = default
        self.ignore_persistence = ignore_persistence
        self.dont_persist_default_value = dont_persist_default_value

    @property
    def required(self) -> bool:
        return self.default == self.NO_DEFAULT

    @property
    @abc.abstractmethod
    def value(self) -> Any:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    def value_repr(self):
        if isinstance(self.value, ParameterObject):
            return self.value.repr()
        if isinstance(self.value, Path):
            return repr(self._value)
        return repr(self.value)

    @property
    def repr(self) -> Union[str, None]:
        if self.ignore_persistence:
            return None

        if self.dont_persist_default_value and self.value == self.default:
            return None

        return f'{self.name}={self.value_repr()}'


class Parameter(AbstractParameter):

    NO_DEFAULT = NO_DEFAULT
    NO_VALUE = NO_VALUE

    def __init__(self,
                 name: str,
                 dtype: Union[type] = None,
                 default: Any = NO_DEFAULT,
                 name_in_config: str = None,
                 ignore_persistence: bool = False,
                 dont_persist_default_value: bool = False,
                 ):
        """
        Args:
            name: name for referencing from task
            dtype: expected datatype
            default: value used if not provided in config, default to NO_DEFAULT meaning that param is required
            name_in_config: name used for search in config, defaults to `name` argument
            ignore_persistence: do not use this parameter in persistence, useful params without influence on output
            dont_persist_default_value: if value of parameter is same as default, do not use it in persistence,
                useful for adding new parameters without recomputation of data
        """
        super().__init__(
            default=default,
            ignore_persistence=ignore_persistence,
            dont_persist_default_value=dont_persist_default_value,
        )
        assert name not in taskchain.config.Config.RESERVED_PARAMETER_NAMES
        self._name = name
        self.dtype = dtype
        self.name_in_config = name if name_in_config is None else name_in_config
        self._value = self.NO_VALUE

    def __str__(self):
        return self.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> Any:
        if self._value == self.NO_VALUE:
            raise ValueError(f'Value not set for parameter `{self}`')

        if self.dtype is Path and self._value is not None:
            return Path(self._value)
        return self._value

    def set_value(self, config) -> Any:
        if self.name_in_config in config:
            value = config[self.name_in_config]
        else:
            if self.required:
                raise ValueError(f'Value for parameter `{self}` not found in config `{config}`')
            value = self.default

        if self.dtype is not None:
            if value is not None and not isinstance(value, self.dtype) and not(self.dtype is Path and isinstance(value, str)):
                raise ValueError(f'Value `{value}` of parameter `{self}` has type {type(value)} instead of `{self.dtype}`')

        self._value = value
        return value


class InputTaskParameter(AbstractParameter):

    def __init__(self,
                 task_identifier: Union[str, type],
                 default: Any = NO_DEFAULT,
                 ):
        super().__init__(
            default=default,
            dont_persist_default_value=True,
        )
        self.task_identifier = task_identifier
        self.task = None

    @property
    def name(self) -> str:
        if isinstance(self.task_identifier, str):
            return self.task_identifier
        return self.task_identifier.slugname

    @property
    def value(self) -> Any:
        if not self.required and self.task is None:
            return self.default
        assert self.task is not None, f'The input task parameter ({self.task_identifier}) has to be initialized before its value is accessed.'
        return self.task.value


class ParameterRegistry:

    def __init__(self, parameters: Iterable[Parameter] = None):
        super().__init__()
        self._parameters = {}
        for parameter in parameters if parameters is not None else []:
            if parameter.name in self._parameters:
                raise ValueError(f'Multiple parameters with same name `{parameter.name}`')
            self._parameters[parameter.name] = parameter

    def set_values(self, config):
        for parameter in self._parameters.values():
            parameter.set_value(config)

    def get(self, item: str):
        return self._parameters[item].value

    def __getitem__(self, item, default=None):
        if default is not None:
            raise ValueError('Default argument is not allowed')
        return self.get(item)

    def __getattr__(self, item: str):
        if item in self:
            return self.get(item)
        return self.__getattribute__(item)

    def __str__(self):
        return str(self._parameters)

    def __contains__(self, item):
        return item in self._parameters

    def __bool__(self):
        return True

    def items(self):
        return self._parameters.items()

    def keys(self):
        return self._parameters.keys()

    def values(self):
        return self._parameters.values()

    @property
    def repr(self):
        reprs = []
        for name, parameter in sorted(self._parameters.items()):
            repr = parameter.repr
            if repr is not None:
                reprs.append(repr)
        if reprs:
            return '###'.join(reprs)
        return None


class ParameterObject(abc.ABC):
    """
    Every class used in configs has to be inherit from this class.
    """

    @abc.abstractmethod
    def repr(self) -> str:
        """
        Representation which should uniquely describe object,
        i.e. be based on all arguments of __init__.
        """
        raise NotImplemented

    def __repr__(self):
        return self.repr()


class AutoParameterObject(ParameterObject):
    """
    ParameterObject with automatic `repr` method based on arguments of __init__ method.

    For correct functionality, is necessary store all __init__ argument values as self.arg_name or self._arg_name
    """

    def repr(self) -> str:
        parameters = signature(self.__init__).parameters

        ignore_persistence_args = self.ignore_persistence_args()
        dont_persist_default_value_args = self.dont_persist_default_value_args()
        args = {}
        for i, (arg, parameter) in enumerate(parameters.items()):
            if arg in ignore_persistence_args:
                continue
            if hasattr(self, '_' + arg):
                value = getattr(self, '_' + arg)
            elif hasattr(self, arg):
                value = getattr(self, arg)
            else:
                raise AttributeError(f'Value of __init__ argument `{arg}` not found for class `{fullname(self.__class__)}`, '
                                     f'make sure that value is saved in `self.{arg}` or `self._{arg}`')
            args[arg] = value
            if arg in dont_persist_default_value_args and args[arg] == parameter.default:
                del args[arg]
        args_repr = ', '.join(f'{k}={repr(v)}' for k, v in sorted(args.items()))
        assert 'object at 0x' not in args_repr, f'repr for arguments is fragile: {args_repr}'
        return f'{self.__class__.__name__}({args_repr})'

    @staticmethod
    def ignore_persistence_args() -> List[str]:
        """ List of __init__ argument names which are ignored in persistence. """
        return ['verbose', 'debug']

    @staticmethod
    def dont_persist_default_value_args() -> List[str]:
        """
        List of __init__ argument names which are ignored in persistence
        when they take default value.
        """
        return []
