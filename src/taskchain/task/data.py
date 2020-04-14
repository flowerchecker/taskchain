import abc
import json
import pickle
import shutil
from pathlib import Path
from typing import Any, List, Dict, Generator, Union

import pylab
import numpy as np
import pandas as pd


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
        base_dir.mkdir(parents=True, exist_ok=True)

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

    def on_run_error(self):
        pass

    @property
    def path(self) -> Union[Path, None]:
        if not self.is_persisting:
            raise AttributeError(f'Data {self} is not in persisting mode, call `init_persistence` first')
        return self._path

    @property
    @abc.abstractmethod
    def _path(self) -> Union[Path, None]:
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


class InMemoryData(Data):

    def __init__(self):
        super().__init__()
        self._value = self

    def init_persistence(self, base_dir: Path, name: str):
        pass

    @property
    def _path(self) -> Union[Path, None]:
        return None

    def exists(self) -> bool:
        return False

    def save(self):
        pass

    def load(self) -> Any:
        return self


class FileData(Data, abc.ABC):

    @property
    def _path(self) -> Path:
        if self.extension is None:
            return self._base_dir / self._name
        return self._base_dir / f'{self._name}.{self.extension}'

    @property
    def extension(self) -> Union[str, None]:
        return None

    def exists(self) -> bool:
        return self.path.exists()


class JSONData(FileData):

    DATA_TYPES = [str, int, float, bool, Dict, List]

    @property
    def extension(self) -> Union[str, None]:
        return 'json'

    def save(self):
        json.dump(self.value, self.path.open('w'), indent=2, sort_keys=True)

    def load(self) -> Any:
        self._value = json.load(self.path.open())
        return self._value


class NumpyData(FileData):

    DATA_TYPES = [np.ndarray]

    @property
    def extension(self) -> Union[str, None]:
        return 'npy'

    def save(self):
        np.save(str(self.path), self.value)

    def load(self) -> Any:
        self._value = np.load(str(self.path))
        return self._value


class PandasData(FileData):

    DATA_TYPES = [pd.DataFrame, pd.Series]

    @property
    def extension(self) -> Union[str, None]:
        return 'pd'

    def save(self):
        self.value.to_pickle(self.path)

    def load(self) -> Any:
        self._value = pd.read_pickle(self.path)
        return self._value


class FigureData(FileData):

    DATA_TYPES = [pylab.Figure]

    @property
    def extension(self) -> Union[str, None]:
        return 'pickle'

    def save(self):
        pickle.dump(self.value, self.path.open('wb'))
        self.value.savefig(self._base_dir / f'{self._name}.png')
        self.value.savefig(self._base_dir / f'{self._name}.svg')

    def load(self) -> Any:
        self._value = pickle.load(self.path.open('rb'))
        return self._value


class GeneratedData(Data, abc.ABC):

    DATA_TYPES = [Generator]


class DirData(Data):

    def __init__(self):
        super().__init__()
        self._dir = None

    def init_persistence(self, base_dir: Path, name: str):
        super().init_persistence(base_dir, name)
        if self.tmp_path.exists():
            shutil.rmtree(self.tmp_path)
        self.tmp_path.mkdir()
        self._dir = self.tmp_path

    def on_run_error(self):
        if self.error_path.exists():
            shutil.rmtree(self.error_path)
        shutil.move(str(self.tmp_path), str(self.error_path))

    @property
    def dir(self) -> Path:
        return self._dir

    @property
    def _path(self) -> Union[Path, None]:
        return self._base_dir / self._name

    @property
    def tmp_path(self) -> Path:
        return self._base_dir / f'{self._name}_tmp'

    @property
    def error_path(self) -> Path:
        return self._base_dir / f'{self._name}_error'

    def exists(self) -> bool:
        return self.path.exists()

    def save(self):
        if self.path.exists():
            shutil.rmtree(self.path)
        shutil.move(str(self.tmp_path), str(self.path))
        self._value = self._dir = self.path

    def load(self) -> Path:
        self._dir = self._value = self.path
        return self._value
