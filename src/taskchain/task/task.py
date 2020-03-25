import abc
import re
from inspect import isclass
from pathlib import Path
from typing import Union, Any, get_type_hints, Type, Dict

from taskchain.task.config import Config
from taskchain.task.data import Data
from taskchain.utils.clazz import persistent, Meta, inheritors


class Task:

    def __init__(self, config: Config = None):
        self.config: Config = config
        self._data: Union[None, Data] = None
        self._input_tasks: Union[None, Dict[str, 'Task']] = None
        self._forced = False

        self.meta = Meta(self)
        _ = self.data_class

    @abc.abstractmethod
    def run(self):
        pass

    @property
    def data(self) -> Data:
        if hasattr(self, '_data') and self._data is not None:
            return self._data

        self._data = self.data_class()
        self._init_persistence()

        if self._data.is_persisting and self._data.exists() and not self._forced:
            self._data.load()
        else:
            self.process_run_result(self.run())
        return self._data

    @property
    def value(self) -> Any:
        return self.data.value

    @property
    @persistent
    def slugname(self) -> str:
        if 'name' in self.meta:
            name = self.meta.name
        else:
            name = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()
            if name.endswith('_task'):
                name = name[:-5]
        if self.group:
            return f'{self.group}:{name}'
        return name

    def __str__(self):
        return self.slugname

    def __repr__(self):
        return f'<task: {self}>'

    @property
    @persistent
    def group(self) -> str:
        return self.meta.get('task_group', '')

    @property
    @persistent
    def path(self) -> Path:
        path = self.config.base_dir / self.slugname.replace(':', '/')
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    @persistent
    def data_type(self) -> Type[Union[Data, Any]]:
        return_data_type = get_type_hints(self.run).get('return')
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

        return data_type

    def force(self):
        self._forced = True
        self._data = None

    def stop_forcing(self):
        self._forced = False

    @property
    def is_forced(self):
        return self._forced

    @property
    @persistent
    def data_class(self) -> Type[Data]:
        if isclass(self.data_type) and issubclass(self.data_type, Data):
            return self.data_type

        cls = None
        for c in inheritors(Data):
            if c.is_data_type_accepted(self.data_type):
                if cls is None:
                    cls = c
                else:
                    raise AttributeError(f'Multiple data handlers for type {self.data_type}: {cls} and {c}')

        if cls is None:
            raise AttributeError(f'Missing data handler for type {self.data_type}')

        return cls

    @property
    def input_tasks(self) -> Dict[str, 'Task']:
        if self._input_tasks is None:
            raise ValueError(f'Input tasks for task `{self}` not initialized')
        return self._input_tasks

    def set_input_tasks(self, task_map: Dict[str, 'Task']):
        self._input_tasks = task_map

    def process_run_result(self, run_result: Any):
        if isclass(self.data_type) and issubclass(self.data_type, Data) and isinstance(run_result, self.data_type):
            self._data = run_result
            self._init_persistence()
        elif isinstance(run_result, self.data_type):
            self._data.set_value(run_result)
        else:
            raise ValueError(f'Invalid result data type: {type(run_result)} instead of {self.data_type}')

        if self._data.is_persisting:
            self._data.save()

    def _init_persistence(self):
        if self.config is not None:
            self._data.init_persistence(self.path, self.config.name)
