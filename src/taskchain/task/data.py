import abc
import json
import logging
import pickle
import shutil
from collections.abc import Generator
from pathlib import Path
from typing import Any, Dict, Union

import h5py
import numpy as np
import pandas as pd
import pylab
import yaml
from matplotlib import pyplot as plt

from taskchain.utils.io import NumpyEncoder, iter_json_file, write_jsons


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

    @abc.abstractmethod
    def delete(self):
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

    @property
    def run_info_path(self) -> Path:
        path = self._path
        return path.parent / f'{path.stem}.run_info.yaml'

    def save_run_info(self, info: Dict):
        yaml.dump(info, self.run_info_path.open('w'))

    def load_run_info(self) -> Union[Dict, None]:
        if not self.run_info_path.exists():
            return None
        return yaml.load(self.run_info_path.open(), yaml.Loader)

    @property
    def log_path(self) -> Path:
        path = self._path
        return path.parent / f'{path.stem}.log'

    def get_log_handler(self):
        return logging.FileHandler(self.log_path, mode='w')

    @property
    def log(self):
        if not self.log_path.exists():
            return None
        return [l.strip() for l in self.log_path.open().readlines()]

    @property
    def is_logging(self):
        return self.is_persisting


class InMemoryData(Data):

    def __init__(self):
        super().__init__()
        self._value = self
        self._log = None

    def init_persistence(self, base_dir: Path, name: str):
        self._base_dir = base_dir
        self._name = name
        base_dir.mkdir(parents=True, exist_ok=True)

    @property
    def _path(self) -> Union[Path, None]:
        return self._base_dir / self._name

    def exists(self) -> bool:
        return False

    def save(self):
        pass

    def delete(self):
        pass

    def load(self) -> Any:
        return self

    @property
    def is_logging(self):
        return True


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

    def delete(self):
        self.path.unlink()


class JSONData(FileData):

    DATA_TYPES = [str, int, float, bool, dict, list]

    @property
    def extension(self) -> Union[str, None]:
        return 'json'

    def save(self):
        json.dump(self.value, self.path.open('w'), indent=2, sort_keys=True, cls=NumpyEncoder, ensure_ascii=False)

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


class ListOfNumpyData(Data):

    @property
    def _path(self) -> Path:
            return self._base_dir / self._name

    def save(self):
        if self.path.exists():
            shutil.rmtree(self.path)
        self.path.mkdir()

        for i, v in enumerate(self.value):
            np.save(str(self.path / f'{i}.npy'), v)

    def load(self) -> Any:
        self._value = []
        for file in sorted(self.path.glob('*.npy'), key=lambda f: int(f.name.split('.')[0])):
            self._value.append(np.load(str(file)))
        return self._value

    def delete(self):
        shutil.rmtree(str(self.path))

    def exists(self) -> bool:
        return self.path.exists()


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
        plt.close(self.value)

    def load(self) -> Any:
        self._value = pickle.load(self.path.open('rb'))
        return self._value


class GeneratedData(FileData):

    DATA_TYPES = [Generator]

    @property
    def extension(self) -> Union[str, None]:
        return 'jsonl'

    def save(self):
        write_jsons(self.value, self.path)

    def load(self) -> Any:
        self._value = list(iter_json_file(self.path))
        return self._value

    def set_value(self, value: Any = None):
        value = list(value)
        super().set_value(value)


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

    def delete(self):
        shutil.rmtree(str(self.path))


class ContinuesData(Data):

    def __init__(self):
        super().__init__()
        self._dir = None

    def init_persistence(self, base_dir: Path, name: str):
        super().init_persistence(base_dir, name)
        if not self.tmp_path.exists():
            self.tmp_path.mkdir()
        self._dir = self.tmp_path

    @property
    def _path(self) -> Union[Path, None]:
        return self._base_dir / self._name

    @property
    def tmp_path(self) -> Path:
        return self._base_dir / f'{self._name}_tmp'

    @property
    def dir(self) -> Path:
        return self._dir

    def exists(self) -> bool:
        return self.path.exists()

    def save(self):
        self._value = self._dir

    def load(self) -> Any:
        self._dir = self._value = self.path
        return self._value

    def delete(self):
        shutil.rmtree(str(self.path))
        shutil.rmtree(str(self.tmp_path))

    def finished(self):
        shutil.rmtree(str(self.path), ignore_errors=True)
        shutil.move(str(self.tmp_path), str(self.path))
        self._value = self._dir = self.path


class H5Data(ContinuesData):

    def append_data(self, dataset, data: np.ndarray, dataset_len=None):
        len_before = dataset.len() if dataset_len is None else dataset_len
        dataset.resize(len_before + data.shape[0], axis=0)
        dataset[len_before:] = data

    def data_file(self, mode=None):
        return h5py.File(self.dir / 'data.h5', 'a' if mode is None else 'r')

    def dataset(self, name, data_file=None, maxshape=None, dtype=None):
        if data_file is None:
            data_file = self.data_file()
        try:
            return data_file[name]
        except:
            shape = tuple([0] + list(maxshape[1:]))
            return data_file.create_dataset(name, shape=shape, maxshape=maxshape)
