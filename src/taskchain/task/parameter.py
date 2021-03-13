import abc
from inspect import signature, Parameter as SignatureParameter
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


class ParameterObject:

    @abc.abstractmethod
    def repr(self) -> str:
        raise NotImplemented

    def __repr__(self):
        return self.repr()


class AutoParameterObject(ParameterObject):

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._init(*args, *kwargs)

    @abc.abstractmethod
    def _init(self, *args, **kwargs):
        pass

    def repr(self) -> str:
        parameters = signature(self._init).parameters

        if self._args is None \
                or self._kwargs is None \
                or len(self._args) != len([p for p in parameters.values() if p.default == SignatureParameter.empty]):
            raise AttributeError(f'Object `{self.__class__.__name__}`, args or kwargs not saved correctly, '
                                 f'did you use _init instead of __init__?')

        ignore_persistence_args = self.ignore_persistence_args()
        dont_persist_default_value_args = self.dont_persist_default_value_args()
        kwargs = dict(self._kwargs)
        for i, (arg, parameter) in enumerate(parameters.items()):
            if i < len(self._args):
                kwargs[arg] = self._args[i]
            if parameter.default != SignatureParameter.empty and arg not in kwargs:
                kwargs[arg] = parameter.default
            if arg in dont_persist_default_value_args and kwargs[arg] == parameter.default:
                del kwargs[arg]
        args_repr = ', '.join(f'{k}={repr(v)}' for k, v in sorted(kwargs.items()) if k not in ignore_persistence_args)
        return f'{self.__class__.__name__}({args_repr})'

    @staticmethod
    def ignore_persistence_args() -> List[str]:
        return []

    @staticmethod
    def dont_persist_default_value_args() -> List[str]:
        return []
