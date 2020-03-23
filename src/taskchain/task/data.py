import abc
import json
from pathlib import Path
from typing import Any, List, Dict, Generator, Union


class Data:
    DATA_TYPES = []

    @classmethod
    def is_data_type_accepted(cls, data_type):
        return data_type in cls.DATA_TYPES

    def __init__(self):
        self._persisting = False
        self._base_dir = None
        self._name = None
        self._value = None

    def init_persistence(self, base_dir: Path, name: str):
        self._persisting = True
        self._base_dir = base_dir
        self._name = name

    @property
    def is_persisting(self):
        return self._persisting

    @abc.abstractmethod
    def exists(self) -> bool:
        pass

    @abc.abstractmethod
    def save(self):
        pass

    @abc.abstractmethod
    def load(self) -> Any:
        pass

    @property
    def path(self) -> Path:
        if not self.is_persisting:
            raise AttributeError(f'Data {self} is not in persisting mode, call `init_persistence` first')
        return self._path

    @property
    @abc.abstractmethod
    def _path(self) -> Path:
        pass

    def set_value(self, value: Any = None):
        self._value = value

    @property
    def value(self):
        if not hasattr(self, '_value') or self._value is None:
            raise ValueError(f'Value of {self} is not set')

        return self._value

    def __str__(self):
        if self.is_persisting:
            return f'{self._base_dir}:{self._name}'
        return f'{self.__class__.__name__}'


class FileData(Data, abc.ABC):

    @property
    def _path(self):
        if self.extension is None:
            return self._base_dir / self._name
        return self._base_dir / f'{self._name}.{self.extension}'

    @property
    def extension(self) -> Union[str, None]:
        return None


class JSONData(FileData):

    DATA_TYPES = [str, int, float, bool, Dict, List]

    @property
    def extension(self) -> Union[str, None]:
        return 'json'

    def exists(self) -> bool:
        return self.path.exists()

    def save(self):
        json.dump(self.value, self.path.open('w'), indent=2, sort_keys=True)

    def load(self) -> Any:
        self._value = json.load(self.path.open())
        return self._value


class GeneratedData(Data, abc.ABC):

    DATA_TYPES = [Generator]
