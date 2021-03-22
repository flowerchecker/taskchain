import abc
from inspect import signature
from pathlib import Path
from taskchain.utils.clazz import fullname
from typing import Union, Any, Iterable, List


class NO_DEFAULT: pass
class NO_VALUE: pass


class Parameter:

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
        :param name: name for referencing from task
        :param dtype:
        :param default: value used if not provided in config, default to NO_DEFAULT meaning that param is required
        :param name_in_config: name used for search in config, defaults to parameter name
        :param ignore_persistence: do not use this parameter in persistence, useful params without influence on output
        :param dont_persist_default_value: if value of parameter is same as default, do not use it in persistence
            useful for adding new parameters without recomputation of data
        """
        self.name = name
        self.dtype = dtype
        self.default = default
        self.name_in_config = name if name_in_config is None else name_in_config
        self.ignore_persistence = ignore_persistence
        self.dont_persist_default_value = dont_persist_default_value

        self._value = self.NO_VALUE

    def __str__(self):
        return self.name

    @property
    def required(self) -> bool:
        return self.default == self.NO_DEFAULT

    @property
    def value(self) -> Any:
        if self._value == self.NO_VALUE:
            raise ValueError(f'Value not set for parameter `{self}`')

        return self._value

    def set_value(self, config) -> Any:
        if self.name_in_config in config:
            value = config[self.name_in_config]
        else:
            if self.required:
                raise ValueError(f'Value for parameter `{self}` not found in config `{config}`')
            value = self.default

        if self.dtype is not None:
            if self.dtype is Path:
                value = Path(value)
            if value is not None and not isinstance(value, self.dtype):
                raise ValueError(f'Value `{value}` of parameter `{self}` has type {type(value)} instead of `{self.dtype}`')

        self._value = value
        return value

    def value_repr(self):
        if isinstance(self.value, ParameterObject):
            return self.value.repr()
        return repr(self.value)

    @property
    def repr(self) -> Union[str, None]:
        if self.ignore_persistence:
            return None

        if self.dont_persist_default_value and self.value == self.default:
            return None

        return f'{self.name}={self.value_repr()}'


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
        return self.get(item)

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

    @abc.abstractmethod
    def repr(self) -> str:
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
            if hasattr(self, arg):
                value = getattr(self, arg)
            elif hasattr(self, '_' + arg):
                value = getattr(self, '_' + arg)
            else:
                raise AttributeError(f'Value of __init__ argument `{arg}` not found for class `{fullname(self.__class__)}`, '
                                     f'make sure that value is saved in `self.{arg}` or `self._{arg}`')
            args[arg] = value
            if arg in dont_persist_default_value_args and args[arg] == parameter.default:
                del args[arg]
        args_repr = ', '.join(f'{k}={repr(v)}' for k, v in sorted(args.items()))
        return f'{self.__class__.__name__}({args_repr})'

    @staticmethod
    def ignore_persistence_args() -> List[str]:
        return ['verbose', 'debug']

    @staticmethod
    def dont_persist_default_value_args() -> List[str]:
        return []
