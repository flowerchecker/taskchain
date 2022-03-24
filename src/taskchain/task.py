import abc
import getpass
import inspect
import logging
import re
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from inspect import isclass
from pathlib import Path
from typing import Union, Any, get_type_hints, Type, Dict, Iterable, get_origin, List

import taskchain
from .config import Config
from .data import Data, DirData, InMemoryData
from .parameter import Parameter, ParameterRegistry, NO_VALUE
from taskchain.utils.clazz import persistent, Meta, inheritors, isinstance as custom_isinstance, fullname


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

        return get_origin(data_type) if get_origin(data_type) else data_type

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
            raise AttributeError(f'{fullname(self)}: Missing data handler for type {self.data_type}')

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
    """
    Object representing one computation step in chains.
    """

    def __init__(self, config: Config = None):
        """

        Args:
            config: config with parameters for this task
        """
        self._config: Config = config
        self._data: Union[None, Data, DirData] = None
        self._input_tasks: Union[None, InputTasks] = None
        self._forced = False

        self.meta = self.__class__.meta
        self.group = self.__class__.group
        self.slugname = self.__class__.slugname
        self.fullname = self.__class__.fullname(config)
        self.data_class = self.__class__.data_class
        self.data_type = self.__class__.data_type

        self.logger = logging.getLogger(f'task_{self.fullname}')
        self.logger.setLevel(logging.DEBUG)

        self._prepare_parameters()

    def _prepare_parameters(self):
        """ Create task's parameter registry and load their values from config """
        parameters = self.meta.get('parameters')
        if parameters is not None:
            parameters = [
                p
                for p in deepcopy(parameters)
                if isinstance(p, Parameter)
            ]
        self.params = self.parameters = ParameterRegistry(parameters)
        self.parameters.set_values(self._config)

    @abc.abstractmethod
    def run(self, *args):
        """
        Abstract method which is called by a chain when data are needed.
        This method represents computation.

        Args:
            *args: can be names of parameters and input tasks.
            Their values are then provided by the chain.
        """
        pass

    @property
    def data(self) -> Data:
        """
        Get data object of this tasks.
        This also triggers loading or computation of data same as `.value`
        """
        if hasattr(self, '_data') and self._data is not None:
            return self._data

        if len(inspect.signature(self.data_class).parameters) == 0 and not inspect.isabstract(self.data_class):
            # data class is not meant to be created out of run method -> data cannot be loaded
            self._data = self.data_class()
            self._init_persistence(self._data)

        if self._data and self._data.is_persisting and self._data.exists() and not self._forced:
            self._data.load()
        else:
            try:
                self._init_run_info()
                if self._data and self._data.is_logging:
                    data_log_handler = self._data.get_log_handler()
                    self.logger.addHandler(data_log_handler)
                else:
                    data_log_handler = None
                self.logger.info(f'{self} - run started with params: {self.params.repr}')
                run_result = self.run(*self._get_run_arguments())
                self.logger.info(f'{self} - run ended')
                self.logger.removeHandler(data_log_handler)
                self._process_run_result(run_result)
            except Exception as error:
                if self._data:
                    self._data.on_run_error()
                    self._data = None
                raise error
            self._finish_run_info()
        return self._data

    def _get_run_arguments(self):
        """
        Looks on arguments of `run` method and get values for them.
        It looks to task's parameters and input tasks.
        """
        args = []
        for arg, parameter in inspect.signature(self.run).parameters.items():
            if parameter.default != inspect.Parameter.empty:
                raise AttributeError('Kwargs arguments in run method not allowed')

            input_tasks_arg = self.input_tasks[arg] if arg in self.input_tasks else NO_VALUE
            if isinstance(input_tasks_arg, Task):
                input_tasks_arg = input_tasks_arg.value
            parameter_arg = self.parameters[arg] if arg in self.parameters else NO_VALUE

            if input_tasks_arg is NO_VALUE and arg not in self.parameters:
                raise KeyError(f'Argument `{arg}` of run method of {self} not found in input_tasks nor parameters')

            if input_tasks_arg is not NO_VALUE and arg in self.parameters:
                raise KeyError(f'Argument `{arg}` of run method of {self} found in both input_tasks and parameters')
            args.append(input_tasks_arg if input_tasks_arg is not NO_VALUE else parameter_arg)
        return args

    def get_data_object(self):
        """
        This is meant to be run only from `run` method.
        Needed when task return Data object directly, e.g. DirData or ContinuesData.

        Returns:
            Data: object handling data persistence of this task.
        """
        if not hasattr(self, '_data'):
            raise ValueError('Data object is not initialized, run this only from task')
        return self._data

    @property
    def value(self) -> Any:
        """ Return result of computation. Load persisted data or compute them by `run` method. """
        return self.data.value

    def __str__(self):
        return self.fullname

    def __repr__(self):
        return f'<task: {self}>'

    def _repr_markdown_(self):
        repr = f'**{self.slugname.split(":")[-1]}** \n' \
               f' - fullname: `{self.fullname}` \n' \
               f' - group: `{self.group}` \n' \
               f' - config: `{self.get_config()}` \n'

        if self.has_data:
            repr += f' - data: `{self.data_path}` \n' \

        return repr

    def get_config(self):
        """ Return config used to configure this task. """
        return self._config

    @property
    def path(self) -> Path:
        """ Path where all data of this task are persisted. """
        if self._config.base_dir is None:
            raise ValueError(f'Config `{self._config}` has not base dir set')
        path = self._config.base_dir / self.slugname.replace(':', '/')
        return path

    @property
    def _data_without_value(self) -> Data:
        """ Get data object but avoid loading or computation of data. """
        if hasattr(self, '_data') and self._data is not None:
            return self._data
        data = self.data_class()
        self._init_persistence(data)
        return data

    @property
    def has_data(self) -> bool:
        """ Check if this task has data already computed and persisted. """
        if issubclass(self.data_class, InMemoryData):
            return False
        return self._data_without_value.exists()

    @property
    def data_path(self) -> Path:
        """
        Path to data.
        Path can be not existent if data are not yet computed.
        Returns None if task does not persisting.
        """
        if issubclass(self.data_class, InMemoryData):
            return None
        return self._data_without_value.path

    def reset_data(self):
        self._data = None
        return self

    def force(self, delete_data=False):
        """
        Switch task to forced state to allow data recomputation.
        Next time value is requested persisted data are ignored and computation is triggered.

        Args:
            delete_data (bool): whether persisted data should be immediately deleted from disk.
        Returns:
            Task: allows chaining `task.force().value`
        """
        if delete_data:
            data = self._data_without_value
            if data.exists():
                data.delete()

        self._forced = True
        self._data = None
        return self

    @property
    def is_forced(self):
        return self._forced

    @property
    def input_tasks(self) -> 'InputTasks':
        """
        Get task's registry which allows access input tasks by name or index.
        """
        if self._input_tasks is None:
            raise ValueError(f'Input tasks for task `{self}` not initialized')
        return self._input_tasks

    def set_input_tasks(self, task_map: 'InputTasks'):
        self._input_tasks = task_map

    def _process_run_result(self, run_result: Any):
        """
        Handle result of computation by processing then by data object.

        Args:
            run_result: return value of run method
        """
        if isclass(self.data_type) and issubclass(self.data_type, Data) and isinstance(run_result, self.data_type):
            self._data = run_result
            self._init_persistence(self._data)
        elif isinstance(run_result, self.data_type) or custom_isinstance(run_result, self.data_type) or fullname(self.data_type) == 'typing.Generator':
            assert self._data is not None, f'{fullname(self.__class__)}: attribute "_data" cannot be None'
            self._data.set_value(run_result)
        else:
            raise ValueError(f'{fullname(self.__class__)}: Invalid result data type: {type(run_result)} instead of {self.data_type}')

        if self._data.is_persisting:
            self._data.save()

    def _init_persistence(self, data):
        if self._config is not None and not data.is_persisting:
            data.init_persistence(self.path, self.name_for_persistence)

    @property
    def name_for_persistence(self):
        """
        Get unique string representation of this object used in persistence.
        This value is provided by config.

        Returns:
            str: hash based on input tasks and parameters in parameter mode, name of config otherwise
        """
        return self._config.get_name_for_persistence(self)

    @property
    def run_info(self) -> Dict:
        """ Info about last call of `run` method. """
        data = self._data_without_value
        return data.load_run_info()

    def _init_run_info(self):
        self._run_info = {
            'task': {
                'name': self.slugname,
                'class': self.__class__.__name__,
                'module': self.__class__.__module__,
            },
            'parameters': {p.name: p.value_repr() for p in self.parameters.values()},
            'user': {
                'name': getpass.getuser(),
                'taskchain_version': taskchain.__version__,
            },
            'log': [],
        }
        if self._config is not None:
            self._run_info['config'] = {
                'name': self._config.name,
                'namespace': self._config.namespace,
                'context': self._config.context.name if self._config.context is not None else None,
            }

            from .chain import TaskParameterConfig
            if isinstance(self._config, TaskParameterConfig):
                self._run_info['input_tasks'] = self._config.input_tasks
        self._run_info['started'] = datetime.timestamp(datetime.now())

    def save_to_run_info(self, record):
        """
        Save information to run info. Should be called from `run` method.

        Args:
            record: any json-like object
        """
        if isinstance(record, defaultdict):
            record = dict(record)
        self._run_info['log'].append(record)

    def _finish_run_info(self):
        now = datetime.now()
        self._run_info['ended'] = str(now)
        self._run_info['time'] = datetime.timestamp(now) - self._run_info['started']
        self._run_info['started'] = str(datetime.fromtimestamp(self._run_info['started']))

        if self._data and self._data.is_logging:
            self._data.save_run_info(self._run_info)

    @property
    def log(self) -> Union[None, List[str]]:
        """ Log (from `self.logger`) from last run as list of rows. """
        data = self._data_without_value
        if data:
            return data.log
        return None


class ModuleTask(Task, metaclass=MetaModuleTask):
    """
    Task which group is based on python module name (file with the task)
    """


class DoubleModuleTask(Task, metaclass=MetaDoubleModuleTask):
    """
    Task which groups are based on python module name (file with the task) and package (dir with that file).
    Full name of the task: `package_name:module_name:task_name`
    """


class InputTasks(dict):
    """
    Registry of input tasks.
    Main feature of this class is that it allow access task in multiple ways:

    - by full name of the task (including namespace and groups)
    - by shorter name without namespace or groups as long as it is unambiguous
    - by index, order is given by order in `Meta`
    """

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
        """"""
        if type(item) is int:
            return self.task_list[item]
        if default is not None:
            raise ValueError('Default task is not allowed')
        return super().get(_find_task_full_name(item, self.keys()))

    def __contains__(self, item):
        try:
            return super().__contains__(_find_task_full_name(item, self.keys()))
        except KeyError:
            return False


def _find_task_full_name(task_name: str, tasks: Iterable[str], determine_namespace: bool = True) -> str:
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
        # if any task name is suffix of all others, it has priority
        for cand in matching_tasks:
            if all(t.endswith(cand) for t in matching_tasks):
                return cand
    if len(matching_tasks) > 1:
        raise KeyError(f'Ambiguous task name `{task_name}`. Possible matches: {matching_tasks}')
    if len(matching_tasks) == 0:
        raise KeyError(f'Task `{task_name}` not found')
    return matching_tasks[0]
