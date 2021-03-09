import abc
import inspect
import logging
import re
import sys
from inspect import isclass
from pathlib import Path
from typing import Union, Any, get_type_hints, Type, Dict, Iterable

from taskchain.task.config import Config
from taskchain.task.data import Data, DirData
from taskchain.utils.clazz import persistent, Meta, inheritors, isinstance as custom_isinstance, fullname


logger = logging.getLogger('tasks_chain')
logger.addHandler(logging.StreamHandler(sys.stdout))


class MetaTask(type):

    @property
    def meta(cls):
        return Meta(cls)

    @property
    def group(cls) -> str:
        return cls.meta.get('task_group', '')

    @property
    def slugname(cls) -> str:
        if 'name' in cls.meta:
            name = cls.meta.name
        else:
            name = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()
            if name.endswith('_task'):
                name = name[:-5]
        if cls.group:
            return f'{cls.group}:{name}'
        return name

    def fullname(cls, config) -> str:
        if config is None or config.namespace is None:
            return cls.slugname
        return f'{config.namespace}::{cls.slugname}'

    @property
    def data_type(cls) -> Type[Union[Data, Any]]:
        return_data_type = get_type_hints(cls.run).get('return')
        meta_data_type = cls.meta.get('data_type')

        if return_data_type is None and meta_data_type is None:
            raise AttributeError(f'Missing data_type for task {cls.slugname}')

        if return_data_type is None:
            data_type = meta_data_type
        elif meta_data_type is None:
            data_type = return_data_type
        elif meta_data_type != return_data_type:
            raise AttributeError(
                f'Data type {meta_data_type} and return data type {return_data_type} does not match for task {cls.slugname}')
        else:
            data_type = return_data_type

        return data_type

    @property
    def data_class(self) -> Type[Data]:
        if 'data_class' in self.meta:
            return self.meta['data_class']

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


class MetaModuleTask(MetaTask):

    @property
    def group(cls) -> str:
        return inspect.getmodule(cls).__name__.split('.')[-1]


class MetaDoubleModuleTask(MetaTask):

    @property
    def group(cls) -> str:
        return cls.meta.get('task_group', ':'.join(inspect.getmodule(cls).__name__.split('.')[-2:]))


class Task(object, metaclass=MetaTask):

    logger = logger

    def __init__(self, config: Config = None):
        self.config: Config = config
        self._data: Union[None, Data, DirData] = None
        self._input_tasks: Union[None, Dict[str, 'Task']] = None
        self._forced = False

        self.meta = self.__class__.meta
        self.group = self.__class__.group
        self.slugname = self.__class__.slugname
        self.fullname = self.__class__.fullname(config)
        self.data_class = self.__class__.data_class
        self.data_type = self.__class__.data_type

    @abc.abstractmethod
    def run(self):
        pass

    @property
    def data(self) -> Data:
        if hasattr(self, '_data') and self._data is not None:
            return self._data

        if len(inspect.signature(self.data_class).parameters) == 0:
            # data class is not meant to be created out of run method -> data cannot be loaded
            self._data = self.data_class()
            self._init_persistence()

        if self._data and self._data.is_persisting and self._data.exists() and not self._forced:
            self._data.load()
        else:
            try:
                logger.info(f'{self} - run started')
                run_result = self.run()
                logger.info(f'{self} - run ended')
            except Exception as error:
                if self._data:
                    self._data.on_run_error()
                    self._data = None
                raise error
            self.process_run_result(run_result)
        return self._data

    # to run from task itself
    def get_data_object(self):
        if not hasattr(self, '_data'):
            raise ValueError('Data object is not initialized, run this only from task')
        return self._data

    @property
    def value(self) -> Any:
        return self.data.value

    def __str__(self):
        return self.fullname

    def __repr__(self):
        return f'<task: {self}>'

    @property
    @persistent
    def path(self) -> Path:
        path = self.config.base_dir / self.slugname.replace(':', '/')
        return path

    def reset_data(self):
        self._data = None
        return self

    def force(self):
        self._forced = True
        self._data = None
        return self

    def stop_forcing(self):
        self._forced = False

    @property
    def is_forced(self):
        return self._forced

    @property
    def input_tasks(self) -> Dict[str, 'Task']:
        if self._input_tasks is None:
            raise ValueError(f'Input tasks for task `{self}` not initialized')
        return self._input_tasks

    def set_input_tasks(self, task_map: 'InputTasks'):
        self._input_tasks = task_map

    def process_run_result(self, run_result: Any):
        if isclass(self.data_type) and issubclass(self.data_type, Data) and isinstance(run_result, self.data_type):
            self._data = run_result
            self._init_persistence()
        elif isinstance(run_result, self.data_type) or custom_isinstance(run_result, self.data_type) or fullname(self.data_type) == 'typing.Generator':
            self._data.set_value(run_result)
        else:
            raise ValueError(f'Invalid result data type: {type(run_result)} instead of {self.data_type}')

        if self._data.is_persisting:
            self._data.save()

    def _init_persistence(self):
        if self.config is not None and not self._data.is_persisting:
            self._data.init_persistence(self.path, self.config.name)


class ModuleTask(Task, metaclass=MetaModuleTask):

    @abc.abstractmethod
    def run(self):
        pass


class DoubleModuleTask(Task, metaclass=MetaDoubleModuleTask):

    @abc.abstractmethod
    def run(self):
        pass


class InputTasks(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_list = []

    def __setitem__(self, key, value):
        if not super().__contains__(key):
            self.task_list.append(value)
        super().__setitem__(key, value)

    def __getitem__(self, item):
        return self.get(item)

    def get(self, item, default=None):
        if type(item) is int:
            return self.task_list[item]
        if default is not None:
            raise ValueError('Default task is not allowed')
        return super().get(find_task_full_name(item, self.keys()))

    def __contains__(self, item):
        return super().__contains__(find_task_full_name(item, self.keys()))


def find_task_full_name(task_name: str, tasks: Iterable[str], determine_namespace: bool = True) -> str:
    def _task_name_match(name, fullname):
        # remove and check namespaces
        namespace = '::'.join(name.split('::')[:-1])
        fullnamespace = '::'.join(fullname.split('::')[:-1])
        if (namespace or not determine_namespace) and fullnamespace != namespace:
            return False
        name = name.split('::')[-1]
        fullname = fullname.split('::')[-1]

        # direct check
        if fullname == name:
            return True

        # check without group
        if ':' in fullname and ':' not in name:
            return fullname.split(':')[-1] == name

        return False

    matching_tasks = [t for t in tasks if _task_name_match(task_name, t)]
    if len(matching_tasks) > 1:
        raise KeyError(f'Ambiguous task name `{task_name}`. Possible matches: {matching_tasks}')
    if len(matching_tasks) == 0:
        raise KeyError(f'Task `{task_name}` not found')
    return matching_tasks[0]
