import abc
from typing import Union, Any, Iterable


NO_DEFAULT = '>>NO_DEFAULT<<'
NO_VALUE = '>>NO_VALUE<<'


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

        self._value = NO_VALUE

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

    def value_hash(self):
        if isinstance(self.value, ParameterObject):
            return self.value.hash()
        return repr(self.value)

    @property
    def hash(self) -> Union[str, None]:
        if self.ignore_persistence:
            return None

        if self.dont_persist_default_value and self.value == self.default:
            return None

        return f'{self.name}={self.value_hash()}'


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
    def hash(self):
        hashes = []
        for name, parameter in sorted(self._parameters.items()):
            hsh = parameter.hash
            if hsh is not None:
                hashes.append(hsh)
        if hashes:
            return '###'.join(hashes)
        return None


class ParameterObject:

    @abc.abstractmethod
    def hash(self) -> str:
        raise NotImplemented
