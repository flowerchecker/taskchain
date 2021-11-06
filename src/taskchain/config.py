from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Union, Dict, Iterable, Any

import yaml

from .parameter import ParameterObject
from .utils.clazz import find_and_instantiate_clazz, instantiate_clazz
from .utils.data import search_and_replace_placeholders
from .utils.iter import list_or_str_to_list

LOGGER = logging.getLogger()


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

    RESERVED_PARAMETER_NAMES = ['tasks', 'excluded_tasks', 'uses', 'human_readable_data_name',
                                'configs', 'for_namespaces', 'main_part']

    def __init__(self,
                 base_dir: Union[Path, str, None] = None,
                 filepath: Union[Path, str] = None,
                 global_vars: Union[Any, None] = None,
                 context: Union[None, dict, str, Path, Context, Iterable] = None,
                 name: str = None,
                 namespace: str = None,
                 data: Dict = None,
                 part: str = None,
                 ):
        """

        Args:
            base_dir: dir with task data, required for task data persistence
            filepath: json or yaml with config data
            global_vars: data to fill placeholders inf config data such as `{DATA_DIR}`
            context: config which amend or overwrite data of this config
            name: specify name of config directly, required when not using filepath
            namespace: used by chains, allow work with same tasks with multiple configs in one chain
            data: alternative for `filepath`, inject data directly
            part: for multi config files, name of file part
        """
        super().__init__()

        self.base_dir = base_dir
        self._name = None
        self.namespace = namespace
        self._data = None
        self.context = Context.prepare_context(context, global_vars=global_vars)
        self.global_vars = global_vars
        self._filepath = filepath
        self._part = part

        if filepath is not None:
            if '#' in str(self._filepath):
                self._filepath, self._part = str(self._filepath).split('#')
            filepath = Path(self._filepath)
            name_parts = filepath.name.split('.')
            extension = name_parts[-1]
            self._name = '.'.join(name_parts[:-1])
            if extension == 'json':
                self._data = json.load(filepath.open())
            elif extension == 'yaml':
                self._data = yaml.load(filepath.open(), Loader=yaml.Loader)
            else:
                raise ValueError(f'Unknown file extension for config file `{filepath}`')

        if data is not None:
            self._data = data
        if name is not None:
            self._name = name

        self._prepare()

    def _prepare(self, create_objects=True):
        if self._data and 'configs' in self._data:
            self._get_part()
            self._update_uses()
        if self.context is not None:
            self.apply_context(self.context)
        self._validate_data()
        if self.global_vars is not None:
            self.apply_global_vars(self.global_vars)
        if create_objects:
            self.prepare_objects()

    def _get_part(self):
        assert len(self._data) == 1, 'Multipart configs should contain only field `configs`'

        if self._part:
            try:
                self._data = self._data['configs'][self._part]
            except KeyError:
                raise KeyError(f'Part `{self._part}` not found in config `{self}`')
            return

        assert len([c for c in self._data['configs'] if 'main_part' in c]) < 2, \
            f'More then one part of config `self` are marked as main'
        for part_name, part in self._data['configs'].items():
            if part.get('main_part', False):
                self._data = part
                self._part = part_name
                return

        raise KeyError(f'No part specified for multi config `{self}`')

    def _update_uses(self):
        if self._filepath is None or 'uses' not in self._data:
            return
        for i, use in enumerate(self._data['uses']):
            if isinstance(use, str) and use.startswith('#'):
                self._data['uses'][i] = str(self._filepath) + use

    @property
    def name(self) -> str:
        if self._name is None:
            raise ValueError(f'Missing config name')
        if self._part:
            return f'{self._name}#{self._part}'
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
    def repr_name(self) -> str:
        """ Should be unique representation of this config"""
        if self._filepath:
            if self.namespace is None:
                name = str(self._filepath)
            else:
                name = f'{self.namespace}::{self._filepath}'
            if self._part:
                return f'{name}#{self._part}'
            return name
        return self.fullname

    @property
    def repr_name_without_namespace(self) -> str:
        """ Unique representation of this config without namespace"""
        return self.repr_name.split('::')[-1]

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
        if item in self:
            return self.data[item]
        return self.__getattribute__(item)

    def get(self, item, default=None):
        return self.data.get(item, default)

    def __contains__(self, item):
        return item in self.data

    def apply_context(self, context: Context):
        """ Amend or rewrite data of config by data from context"""
        self._data.update(deepcopy(context.data))
        if self.namespace:
            for namespace, data in context.for_namespaces.items():
                if self.namespace == namespace:
                    self._data.update(deepcopy(data))

    def _validate_data(self):
        """ Check correct format of data """
        if self._data is None:
            return

        data = self._data
        uses = data.get('uses', [])
        if not (isinstance(uses, Iterable) or isinstance(uses, str)):
            raise ValueError(f'`uses` of config `{self}` have to be list or str')

        tasks = data.get('tasks', [])
        if not (isinstance(tasks, Iterable) or isinstance(tasks, str)):
            raise ValueError(f'`tasks` of config `{self}` have to list or str')

    def apply_global_vars(self, global_vars):
        search_and_replace_placeholders(self._data, global_vars)

    def prepare_objects(self):
        """ Instantiate objects described in config """
        if self._data is None:
            return

        def _instancelize_clazz(clazz, args, kwargs):
            obj = instantiate_clazz(clazz, args, kwargs)
            if not isinstance(obj, ParameterObject):
                LOGGER.warning(f'Object `{obj}` in config `{self}` is not instance of ParameterObject')
            if not hasattr(obj, 'repr'):
                raise Exception(f'Object `{obj}` does not implement `repr` property')
            return obj

        for key, value in self._data.items():
            self._data[key] = find_and_instantiate_clazz(value, instancelize_clazz_fce=_instancelize_clazz)

    def chain(self, parameter_mode=True, **kwargs):
        """ Create chain from this config """
        from .chain import Chain
        return Chain(self, parameter_mode=parameter_mode, **kwargs)

    def get_original_config(self):
        """ Get self of config from which this one is derived """
        return self


class Context(Config):
    """
    Config intended for amend or rewrite other configs
    """

    def __repr__(self):
        return f'<context: {self}>'

    @staticmethod
    def prepare_context(context_config: Union[None, dict, str, Path, Context, Iterable],
                        namespace=None, global_vars=None) -> Union[Context, None]:
        """ Helper function for instantiating Context from various sources"""
        context = None
        if context_config is None:
            return
        elif type(context_config) is str or isinstance(context_config, Path):
            context = Context(filepath=context_config, namespace=namespace)
        elif type(context_config) is dict:
            value_reprs = [f'{k}:{v}' for k, v in sorted(context_config.items())]
            context = Context(data=context_config, name=f'dict_context({",".join(value_reprs)})', namespace=namespace)
        elif isinstance(context_config, Context):
            context = context_config
        elif isinstance(context_config, Iterable):
            contexts = map(partial(Context.prepare_context, namespace=namespace, global_vars=global_vars), context_config)
            context = Context.merge_contexts(contexts)

        if context is None:
            raise ValueError(f'Unknown context type `{type(context_config)}`')

        current_context_data = context.for_namespaces[namespace] if namespace else context
        if 'uses' not in current_context_data:
            return context

        if global_vars is not None:
            search_and_replace_placeholders(current_context_data['uses'], global_vars)

        contexts = [context]
        for use in list_or_str_to_list(current_context_data['uses']):
            if matched := re.match(r'(.*) as (.*)', use):
                # uses context with namespace
                filepath = matched[1]
                sub_namespace = f'{context.namespace}::{matched[2]}' if context.namespace else matched[2]
            else:
                filepath = use
                sub_namespace = context.namespace if context.namespace else None
            contexts.append(Context.prepare_context(filepath, sub_namespace, global_vars=global_vars))
        if namespace:
            del context.for_namespaces[namespace]['uses']
        else:
            del context._data['uses']
        return Context.prepare_context(contexts)

    @staticmethod
    def merge_contexts(contexts: Iterable[Context]) -> Context:
        """
        Helper function for merging multiple Context to one

        Later contexts have higher priority and rewrite data of earlier contexts if there is conflict in data.
        """
        data = {}
        names = []
        for_namespaces = defaultdict(dict)
        for context in contexts:
            data.update(context.data)
            names.append(context.name)
            for namespace, values in context.for_namespaces.items():
                for_namespaces[namespace].update(values)
        data['for_namespaces'] = for_namespaces
        return Context(data=data, name=';'.join(names))

    def _prepare(self):
        if 'for_namespaces' in self._data:
            self.for_namespaces = self._data['for_namespaces']
        else:
            self.for_namespaces = {}

        if self.namespace is not None:
            self.for_namespaces = {f'{self.namespace}::{k}': v for k, v in self.for_namespaces.items()}
            self.for_namespaces[self.namespace] = {
                k: v for k, v in self._data.items()
                if k not in Context.RESERVED_PARAMETER_NAMES or k == 'uses'
            }
            self._data = {}
        super()._prepare(create_objects=False)
