from __future__ import annotations

import json
from pathlib import Path
from typing import Union, Dict, Iterable, Any

import yaml

from taskchain.task.parameter import ParameterObject
from taskchain.utils.clazz import find_and_instancelize_clazz
from taskchain.utils.data import search_and_replace_placeholders


class Config(dict):
    """
    Object carrying parameters needed for task execution.
    Config also describe which tasks configures (`tasks` field) and
    and on which other configs depends (`uses` field).
    Thus, config carry all information needed to assemble task chain.

    Typical usage:
    ```python
    chain = Config(task_data_dir, 'config.yaml').chain()
    ```
    """

    def __init__(self,
                 base_dir: Union[Path, str, None] = None,
                 filepath: Union[Path, str] = None,
                 global_vars: Union[Any, None] = None,
                 context: Union[None, dict, str, Path, Context, Iterable] = None,
                 name: str = None,
                 namespace: str = None,
                 data: Dict = None,
                 ):
        """
        :param base_dir: dir with task data, required for task data persistence
        :param filepath: json or yaml with config data
        :param global_vars: data to fill placeholders inf config data such as `{DATA_DIR}`
        :param context: config which amend or overwrite data of this config
        :param name: specify name of config directly, required when not using filepath
        :param namespace: used by chains, allow work with same tasks with multiple configs in one chain
        :param data: alternative for `filepath`, inject data directly
        """
        super().__init__()

        self.base_dir = base_dir
        self._name = None
        self.namespace = namespace
        self._data = None
        self.context = Context.prepare_context(context)
        self.global_vars = global_vars
        self._filepath = filepath

        if filepath is not None:
            filepath = Path(filepath)
            name_parts = filepath.name.split('.')
            extension = name_parts[-1]
            self._name = '.'.join(name_parts[:-1])
            if extension == 'json':
                self._data = json.load(filepath.open())
            elif extension == 'yaml':
                self._data = yaml.load(filepath.open(), Loader=yaml.Loader)

        if data is not None:
            self._data = data
        if name is not None:
            self._name = name

        self._prepare()

    def _prepare(self):
        if self.context is not None:
            self.apply_context(self.context)
        self._validate_data()
        if self.global_vars is not None:
            self.apply_global_vars(self.global_vars)
        self.prepare_objects()

    @property
    def name(self) -> str:
        if self._name is None:
            raise ValueError(f'Missing config name')
        return self._name

    def get_name_for_persistence(self, *args, **kwargs) -> str:
        """ Used for creating filename in task data persistence, should uniquely define config """
        return self.name

    @property
    def fullname(self):
        """ Name with namespace """
        if self.namespace is None:
            return f'{self.name}'
        return f'{self.namespace}::{self.name}'

    @property
    def repr_name(self):
        """ Should be unique representation of this config"""
        if self._filepath:
            return str(self._filepath)
        return self.name

    def __str__(self):
        return self.fullname

    def __repr__(self):
        return f'<config: {self}>'

    @property
    def data(self):
        if self._data is None:
            raise ValueError(f'Data of config `{self}` not initialized')
        return self._data

    def __getitem__(self, item):
        return self.data[item]

    def __getattr__(self, item):
        return self.data[item]

    def get(self, item, default=None):
        return self.data.get(item, default)

    def __contains__(self, item):
        return item in self.data

    def apply_context(self, context: Context):
        """ Amend or rewrite data of config by data from context"""
        self._data.update(context.data)

    def _validate_data(self):
        """ Check correct format of data """
        if self._data is None:
            return

        data = self._data
        uses = data.get('uses', [])
        if not isinstance(uses, Iterable) or isinstance(uses, str):
            raise ValueError(f'`uses` of config `{self}` have to be list or str')

        tasks = data.get('tasks', [])
        if not isinstance(tasks, Iterable) or isinstance(tasks, str):
            raise ValueError(f'`tasks` of config `{self}` have to list or str')

    def apply_global_vars(self, global_vars):
        search_and_replace_placeholders(self._data, global_vars)

    def prepare_objects(self):
        """ Instantiate objects described in config """
        if self._data is None:
            return
        for key, value in self._data.items():
            if isinstance(value, dict) and 'class' in value:
                obj = find_and_instancelize_clazz(value)
                if not isinstance(obj, ParameterObject):
                    raise ValueError(f'Object `{obj}` in config `{self}` is not instance of ParameterObject')
                self._data[key] = obj

    def chain(self, parameter_mode=True, **kwargs):
        """ Create chain from this config """
        from taskchain.task import Chain
        return Chain(self, parameter_mode=parameter_mode, **kwargs)


class Context(Config):
    """
    Config intended for amend or rewrite other configs
    """

    @staticmethod
    def prepare_context(context_config: Union[None, dict, str, Path, Context, Iterable]) -> Union[Context, None]:
        """ Helper function for instantiating Context from various sources"""
        if context_config is None:
            return
        if type(context_config) is str or isinstance(context_config, Path):
            return Context(filepath=context_config)
        if type(context_config) is dict:
            return Context(data=context_config, name=f'dict_context({",".join(sorted(context_config))})')
        if isinstance(context_config, Context):
            return context_config
        if isinstance(context_config, Iterable):
            contexts = map(Context.prepare_context, context_config)
            return Context.merge_contexts(contexts)

        raise ValueError(f'Unknown context type `{type(context_config)}`')

    @staticmethod
    def merge_contexts(contexts: Iterable[Context]) -> Context:
        """
        Helper function for merging multiple Context to one

        Later contexts have higher priority and rewrite data of earlier contexts if there is conflict in data.
        """
        data = {}
        names = []
        for context in contexts:
            data.update(context.data)
            names.append(context.name)
        return Context(data=data, name=';'.join(names))

