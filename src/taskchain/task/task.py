import abc
import inspect
import re
from pathlib import Path
from types import GeneratorType
from typing import Generator, Union, Any, get_type_hints, Dict, List

from taskchain.task.config import Config
from taskchain.task.data import Data, BasicData, GeneratedData
from taskchain.utils.clazz import persistent, Meta


class Task:

    def __init__(self, config: Config = None):
        self.config = config
        self._data: Union[None, Data] = None

        self.meta = Meta(self)
        _ = self.data_type

    @abc.abstractmethod
    def run(self) -> Union[Data, Generator, str, int, float, bool, dict, list]:
        pass

    @property
    def data(self) -> Data:
        if hasattr(self, '_data') and self._data is not None:
            return self._data

        self._data = self.process_run_result(self.run())
        return self._data

    @property
    def value(self) -> Any:
        return self.data.value

    def process_run_result(self, run_result: Any) -> Data:
        if isinstance(run_result, Data) and isinstance(run_result, self.data_type):
            return run_result
        if self.data_type in BasicData.TYPES and type(run_result) is self.data_type:
            return BasicData(run_result)
        if isinstance(run_result, GeneratorType) and issubclass(self.data_type, GeneratedData):
            return GeneratedData(run_result)

        raise ValueError(f'Invalid result data type: {type(run_result)} instead of {self.data_type}')

    @property
    @persistent
    def slugname(self) -> str:
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()
        if name.endswith('_task'):
            return name[:-5]
        return name

    @property
    @persistent
    def path(self) -> Path:
        path = self.config.base_dir / self.slugname
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    @persistent
    def data_type(self):
        return_data_type = get_type_hints(self.run).get('return')
        if return_data_type == Dict:
            return_data_type = dict
        if return_data_type == List:
            return_data_type = list
        if return_data_type == Generator:
            return_data_type = GeneratedData

        meta_data_type = Meta(self).get('data_type')

        if return_data_type is None and meta_data_type is None:
            raise AttributeError(f'Missing data_type for task {self.slugname}')

        if return_data_type is None:
            data_type = meta_data_type
        elif meta_data_type is None:
            data_type = return_data_type
        elif meta_data_type != return_data_type:
            raise AttributeError(f'Data type {meta_data_type} and return data type {return_data_type} does not match for task {self.slugname}')
        else:
            data_type = return_data_type

        if data_type not in BasicData.TYPES and not issubclass(data_type, Data):
            raise AttributeError(f'Invalid data type {data_type} for task {self.slugname}')

        return data_type
