import abc
import logging
import re
from collections import defaultdict
from hashlib import sha256
from itertools import chain
from pathlib import Path
from typing import Dict, Type, Union, Set, Iterable, Sequence, Tuple

import networkx as nx
import pandas as pd

from .data import InMemoryData
from .config import Config
from .task import Task, _find_task_full_name, InputTasks
from .parameter import AbstractParameter, InputTaskParameter
from taskchain.utils.clazz import get_classes_by_import_string
from taskchain.utils.iter import list_or_str_to_list

log_handler = logging.StreamHandler()
log_handler.setLevel(logging.WARNING)


class ChainObject:
    """
    If ParameterObject inherits this class, chain call `init_chain` on initialization and allow
    object to access whole chain.
    """

    @abc.abstractmethod
    def init_chain(self, chain):
        pass


class Chain(dict):
    """
    Chain takes a config, recursively load prerequisite configs, initialize tasks connect them to DAG vie input tasks.
    """

    log_handler = log_handler

    @classmethod
    def set_log_level(cls, level):
        """ Set log level to log handler responsible for console output of task loggers. """
        Chain.log_handler.setLevel(level)

    def __init__(self, config: Config,
                 shared_tasks: Dict[Tuple[str, str], Task] = None,
                 parameter_mode: bool = True
                 ):
        super().__init__()
        self.tasks: Dict[str, Task] = {}
        self._configs: Dict[str, Config] = {}

        self._parameter_mode = parameter_mode
        self._base_config = config
        self._task_registry = shared_tasks if shared_tasks is not None else {}
        self.graph: Union[None, nx.DiGraph] = None

        if not parameter_mode and config.context is not None:
            logging.warning('Using context without parameter mode can break persistence!')

        self._prepare()

    def __str__(self):
        a = max(len(n.split(":")[-1]) for n in self.tasks)
        b = max(len(n) for n in self.tasks)
        return '\n'.join(f'{n.split(":")[-1]:<{a}}  {n:<{b}}  {t.get_config()}' for n, t in self.tasks.items())

    def __repr__(self):
        return f'<chain for config `{self._base_config}`>'

    def _repr_markdown_(self):
        """ Nice display in jupyter notebooks. """
        return self.tasks_df[['name', 'group', 'namespace', 'computed']].to_markdown()

    @property
    def tasks_df(self) -> pd.DataFrame:
        """ Dataframe with rows ass all tasks in chain. """
        rows = {}
        for name, task in self.tasks.items():
            rows[name] = {
                'name': task.slugname.split(':')[-1],
                'group': task.group,
                'namespace': task.get_config().namespace,
                'computed': task.has_data if task.data_path else None,
                'data_path': task.data_path,
                'parameters': list(task.parameters.keys()),
                'input_tasks': list(task.input_tasks.keys()),
                'config': str(task.get_config()).split("/")[0],
            }

        return pd.DataFrame.from_dict(rows, orient='index').sort_values(['namespace', 'group'], na_position='first')

    def __getitem__(self, item):
        """ Get task by name in dict-like fashion. """
        return self.get(item)

    def __getattr__(self, item):
        """ Get task by name as atribute. """
        if item in self:
            return self.get(item)
        return self.__getattribute__(item)

    def get(self, item, default=None):
        """ Get task by name. """
        if default is not None:
            raise ValueError('Default task is not allowed')
        return self.tasks.get(_find_task_full_name(item, self.tasks.keys()))

    def __contains__(self, item):
        try:
            return _find_task_full_name(item, self.tasks.keys()) in self.tasks
        except KeyError:
            return False

    def _prepare(self):
        """ Initialize chain. """
        self._process_config(self._base_config)
        tasks = self._create_tasks(task_registry={} if self._parameter_mode else self._task_registry)
        self._process_dependencies(tasks)

        if self._parameter_mode:
            self.tasks = self._recreate_tasks_with_parameter_config(tasks, self._task_registry)
            self._process_dependencies(self.tasks)
        else:
            self.tasks = tasks

        self._build_graph()
        self._init_objects()

    def _process_config(self, config: Config):
        """ Look for prerequisite configs, instantiate them and process them recursively. """
        if config.repr_name in self._configs:
            return
        self._configs[config.repr_name] = config
        for use in list_or_str_to_list(config.get('uses', [])):
            if isinstance(use, str):
                pattern = r'(.*) as (.*)'
                if matched := re.match(pattern, use):
                    # uses config with namespace
                    used_config = Config(
                        config.base_dir,
                        filepath=matched[1],
                        namespace=f'{config.namespace}::{matched[2]}' if config.namespace else matched[2],
                        global_vars=config.global_vars,
                        context=config.context,
                    )
                else:
                    # uses config without namespace
                    used_config = Config(
                        config.base_dir,
                        use,
                        namespace=config.namespace if config.namespace else None,
                        global_vars=config.global_vars,
                        context=config.context,
                    )
            else:
                # mainly for testing
                assert isinstance(use, Config)
                assert config.base_dir == use.base_dir, f'Base dirs of configs `{config}` and `{use}` do not match'
                if config.namespace:
                    if use.namespace:
                        use.namespace = f'{config.namespace}::{use.namespace}'
                    else:
                        use.namespace = config.namespace
                use.context = config.context
                use._prepare()
                used_config = use
            self._process_config(used_config)

    def _create_tasks(self, task_registry=None) -> Dict[str, Task]:
        """ Look to configs and instantiate their tasks. """
        tasks = {}

        def _register_task(_task: Task, task_name: str):
            if task_name in tasks and tasks[task_name].get_config() != _task.get_config():
                raise ValueError(f'Conflict of task name `{task_name}` '
                                 f'with configs `{tasks[task_name].get_config()}` and `{_task.get_config()}`')
            tasks[task_name] = _task

        for config in self._configs.values():
            # first find excluded tasks, then register all other tasks
            excluded_tasks = set()
            for field_name, exclude in [('excluded_tasks', True), ('tasks', False)]:

                def _process_task(_task_class):
                    if exclude:
                        excluded_tasks.add(_task_class)
                        return
                    if _task_class in excluded_tasks:
                        return
                    task = self._create_task(_task_class, config, task_registry)
                    _register_task(task, _task_class.fullname(config))

                for task_description in list_or_str_to_list(config.get(field_name, [])):
                    if type(task_description) is str:
                        for task_class in get_classes_by_import_string(task_description, Task):
                            if task_class.meta.get('abstract', False):
                                continue
                            _process_task(task_class)
                    elif issubclass(task_description, Task):
                        # mainly for testing
                        _process_task(task_description)
                    else:
                        raise ValueError(f'Unknown task description `{task_description}` in config `{config}`')
        return tasks

    def _recreate_tasks_with_parameter_config(self, tasks: Dict[str, Task], task_registry: Dict) -> Dict[str, Task]:
        """
        Helper function for parameter mode.
        Take tasks and instantiate them again but with TaskParameterConfig
        which contain only parameters needed for the tasks and knows all input tasks (new ones)
        so it can create hash for persistence of task data.
        """
        new_tasks: Dict[str, Task] = {}

        def _get_task(_task_name, _task):
            if _task.fullname in new_tasks:
                if _task_name not in _task.fullname:
                    # this is for support of multiple names (through different namespaces paths) of one task
                    new_tasks[_task_name] = new_tasks[_task.fullname]
                return new_tasks[_task.fullname]
            # this triggers recursion
            input_tasks = {n: _get_task(n, t) for n, t in _task.input_tasks.items() if isinstance(t, Task)}
            config = TaskParameterConfig(_task, input_tasks)
            new_task = self._create_task(_task.__class__, config, task_registry)
            new_tasks[_task.fullname] = new_task
            return new_task

        for task_name, task in tasks.items():
            _get_task(task_name, task)

        return new_tasks

    @staticmethod
    def _create_task(task_class: Type[Task], config: Config, task_registry: Dict = None):
        """
        Instantiate task object and save to task registry.
        If task is in registry already, return it.
        """
        task = task_class(config)
        task.logger.addHandler(Chain.log_handler)

        if isinstance(config, TaskParameterConfig):
            key = task.slugname, task.name_for_persistence
        else:
            key = task.slugname, config.repr_name_without_namespace
        if task_registry and key in task_registry:
            del task
            return task_registry[key]
        if task_registry is not None:
            task_registry[key] = task
        return task

    @staticmethod
    def _process_dependencies(tasks: Dict[str, Task]):
        """ Process input tasks and inject input task object to tasks. """
        for task_name, task in tasks.items():
            input_tasks = InputTasks()
            for input_task in chain(task.meta.get('input_tasks', []), task.meta.get('parameters', [])):
                if isinstance(input_task, AbstractParameter):
                    if not isinstance(input_task, InputTaskParameter):
                        continue
                    assert input_task.dont_persist_default_value
                    assert not input_task.ignore_persistence
                    required, default = input_task.required, input_task.default
                    input_task = input_task.task_identifier
                else:
                    required, default = True, InputTaskParameter.NO_DEFAULT
                if type(input_task) is str:
                    input_task_name = input_task
                else:  # for reference by class
                    input_task_name = input_task.slugname
                if type(input_task_name) is str and task.get_config().namespace and not input_task_name.startswith(task.get_config().namespace):
                    input_task_name = f'{task.get_config().namespace}::{input_task_name}'     # add current config to reference
                if input_task_name in input_tasks.keys():
                    raise ValueError(f'Multiple input tasks with same name `{input_task_name}`')
                try:
                    found_name = _find_task_full_name(input_task_name, tasks, determine_namespace=False)
                    if type(input_task) is str:
                        input_task_name = found_name
                except KeyError:
                    if not required:
                        input_tasks[input_task_name] = default
                        continue
                    raise ValueError(f'Input task `{input_task_name}` of task `{task}` not found')
                input_tasks[input_task_name] = tasks[input_task_name]
            task.set_input_tasks(input_tasks)

    def _build_graph(self):
        """ Go through task and their input tasks and build nx.DiGraph"""
        self.graph = G = nx.DiGraph()
        G.add_nodes_from(self.tasks.values())

        for task in self.tasks.values():
            for input_task in task.input_tasks.values():
                if not isinstance(input_task, Task):
                    continue
                G.add_edge(input_task, task)

        if not nx.is_directed_acyclic_graph(G):
            raise ValueError('Chain is not acyclic')

    def _init_objects(self):
        for config in self._configs.values():
            for obj in config.data.values():
                if isinstance(obj, ChainObject):
                    obj.init_chain(self)

    def get_task(self, task: Union[str, Task]) -> Task:
        if isinstance(task, Task):
            return task
        if task not in self:
            raise ValueError(f'Task `{task}` not found')
        return self.get(task)

    def is_task_dependent_on(self, task: Union[str, Task], dependency_task: Union[str, Task]) -> bool:
        """ Check whether a task is dependant on dependency task. """
        task = self.get_task(task)
        dependency_task = self.get_task(dependency_task)

        return nx.has_path(self.graph, dependency_task, task)

    def dependent_tasks(self, task: Union[str, Task], include_self: bool = False) -> Set[Task]:
        """ Get all tasks which depend ald given task. """
        task = self.get_task(task)
        descendants = nx.descendants(self.graph, task)
        if include_self:
            descendants.add(task)
        return descendants

    def required_tasks(self, task: Union[str, Task], include_self: bool = False) -> Set[Task]:
        """ Get all task which are required fot given task. """
        task = self.get_task(task)
        ancestors = nx.ancestors(self.graph, task)
        if include_self:
            ancestors.add(task)
        return ancestors

    def force(self, tasks: Union[str, Task, Iterable[Union[str, Task]]], recompute=False, delete_data=False):
        """
        Force recomputation of of given tasks and all dependant tasks.
        If either additional argument is used, recomputation must be done manually,
        e.g. by calling `chain.my_task.value` for each task.

        Args:
            tasks: task as objects or names or list of them
            recompute: automatically recompute all forced tasks
            delete_data: also delete persisted data of forced tasks
        """
        if type(tasks) is str or isinstance(tasks, Task):
            tasks = [tasks]
        forced_tasks = set()
        for task in tasks:
            forced_tasks |= self.dependent_tasks(task, include_self=True)

        for task in forced_tasks:
            task.force(delete_data=delete_data)

        if recompute:
            for task in list(forced_tasks)[::-1]:
                _ = task.value

    @property
    def fullname(self):
        return self._base_config.name

    def draw(self, groups_to_show=None):
        """
        Draw graph of tasks. Color is based on tasks' group.
        Border is based on data state:

        - **none** - is not persisting data (`InMemoryData`)
        - **dashed** - data not computed
        - **solid** - data computed

        Args:
            groups_to_show (str or list[str]): limit drawn tasks to given groups and their neighbours
        """
        import graphviz as gv
        import seaborn as sns

        groups_to_show = list_or_str_to_list(groups_to_show)

        node_attr = {'shape': 'box', 'width': '2'}
        graph_attr = {'splines': 'ortho'}
        edge_attr = {}

        groups = list({(n.get_config().namespace, n.group) for n in self.graph.nodes})
        colors = sns.color_palette('pastel', len(groups)).as_hex()

        G = gv.Digraph(
            format='png',
            engine='dot',
            graph_attr=graph_attr,
            node_attr=node_attr,
            edge_attr=edge_attr
        )

        def _is_node_in_groups(node):
            if not groups_to_show:
                return True
            return node.group in groups_to_show

        nodes = set()
        for edge in self.graph.edges:
            if _is_node_in_groups(edge[0]) or _is_node_in_groups(edge[1]):
                nodes.add(edge[0])
                nodes.add(edge[1])

        def _get_slugname(task: Task):
            return f'{task.slugname.split(":")[-1]}#{task.get_config().get_name_for_persistence(task)}'

        for node in nodes:
            color = colors[groups.index((node.get_config().namespace, node.group))]
            style = ['filled']
            if not (node.has_data or issubclass(node.data_class, InMemoryData)):
                style.append('dashed')
            attrs = {
                'label': f"<<FONT POINT-SIZE='10'>{':'.join(node.fullname.split(':')[:-1])}</FONT> <BR/> {node.fullname.split(':')[-1]}>",
                'fillcolor': color,
                'color': color if issubclass(node.data_class, InMemoryData) else 'black' ,
                'style': ','.join(style),
            }
            if not _is_node_in_groups(node):
                attrs['shape'] = 'note'

            G.node(
                _get_slugname(node),
                **attrs,
            )

        for edge in self.graph.edges:
            if edge[0] in nodes and edge[1] in nodes:
                G.edge(_get_slugname(edge[0]), _get_slugname(edge[1]))
        return G

    def create_readable_filenames(self, groups=None, name=None, verbose=False, keep_existing=False):
        """
        Create human readable symlink to data of tasks in the chain.
        Symlink is in same directory as data, i.e. in directory with all task's data.
        Name of link is taken from first available in order:

        - this method's parameter
        - task's config, parameter `human_readable_data_name`
        - name of task's config

        Args:
            groups (optional): name of group or list of names of groups, for which should be symlinks created
            name (optional): name of links
            verbose:
            keep_existing: rewrite link if link already exists
        """
        symlink_actions = defaultdict(list)
        groups = list_or_str_to_list(groups)

        for task_name, task in self.tasks.items():
            if groups is not None and task.group not in groups:
                continue
            if not task.has_data:
                continue
            an, n, sp = self._create_softlink_to_task_data(task, name, keep_existing=keep_existing)
            symlink_actions[n].append((an, sp))

        if verbose:
            for name, actions in symlink_actions.items():
                print(f'{name}')
                for action_name, symlink_path in actions:
                    print(f'   {action_name} link to {symlink_path}')

    def _create_softlink_to_task_data(self, task, name=None, keep_existing=False):
        if name is None:
            config = task.get_config().get_original_config()
            if 'human_readable_data_name' in config:
                name = config['human_readable_data_name']
            else:
                name = config.name

        symlink_path = task.path / f'{name}{task.data_path.suffix}'

        action_name = 'keep existing'
        if symlink_path.exists() and not keep_existing:
            symlink_path.unlink()
            action_name = 'rewriting'
        if not symlink_path.exists():
            symlink_path.symlink_to(task.data_path.relative_to(symlink_path.parent), task.data_path.is_dir())
            if action_name != 'rewriting':
                action_name = 'creating'
        return action_name, name, symlink_path


class TaskParameterConfig(Config):
    """
    Helper config used in parameter mode.

    It takes

     - instance of Task and create config with only parameters which are used by this task
     - input tasks (already with TaskParameterConfig)

    From this data creates unique hash used in persistence

    Note: These configs creates a kind of blockchain. Each config is block,
    parameters are content of blocks and input_tasks are dependencies between blocks.
    Change in one config invalidates all dependant configs, which is property desired
    and required for correct functionality of TaskChains data persistence.
    """
    def __init__(self, original_task: Task, input_tasks: Dict[str, Task]):
        super(Config, self).__init__()

        self.original_config = original_config = original_task.get_config()
        self.base_dir = original_config.base_dir
        self.namespace = original_config.namespace
        self.global_vars = original_config.global_vars
        self.context = original_config.context
        self._name = f'{original_config.name}/{original_task}'
        self._part = None

        self._data = {}
        for parameter in original_task.parameters.values():
            if parameter.name_in_config in original_config:
                self._data[parameter.name_in_config] = original_config[parameter.name_in_config]

        self.input_tasks = {name: task.get_config().get_name_for_persistence(task) for name, task in input_tasks.items() if isinstance(task, Task)}

    def get_name_for_persistence(self, task: Task) -> str:
        def _get_input_task_repr(_name, _task):
            if outer_namespace := task.get_config().namespace:
                # remove namespace of this task from task name
                assert _name.startswith(outer_namespace)
                _name = _name[len(outer_namespace) + 2:]
            return f'{_name}={_task}'

        parameter_repr = task.parameters.repr
        input_tasks_repr = '###'.join(_get_input_task_repr(n, it) for n, it in sorted(self.input_tasks.items()))
        return sha256(f'{parameter_repr}$$${input_tasks_repr}'.encode()).hexdigest()[:32]

    @property
    def repr_name(self) -> str:
        return self.name

    @property
    def repr_name_without_namespace(self):
        return self.repr_name

    def get_original_config(self):
        return self.original_config


class MultiChain:
    """
    Hold multiple chains which share task object,
    i.e. it can be more memory efficient then dict of chains.
    Otherwise behaves as dict of chains.
    """

    logger = logging.getLogger('tasks_chain')

    @classmethod
    def from_dir(cls, data_dir: Path, dir_path: Path, **kwargs) -> 'MultiChain':
        """
        Create MultiConfig from directory of configs.

        Args:
            data_dir: tasks data persistence path
            dir_path: directory with configs
            **kwargs: other arguments passed to Config, e.g. global_vars

        Returns:
            MultiChain based on all configs in dir
        """
        configs = []
        for config_file in dir_path.iterdir():
            configs.append(
                Config(data_dir, config_file, **kwargs)
            )
        return MultiChain(configs)

    def __init__(self, configs: Sequence[Config], parameter_mode: bool = True):
        """
        Args:
            configs: list of Config objects from which Chains are created.
            parameter_mode:
        """
        self._tasks: Dict[Tuple[str, str], Task] = {}
        self.chains: Dict[str, Chain] = {}
        self._base_configs = configs
        self.parameter_mode = parameter_mode

        self._prepare()

    def _prepare(self):
        for config in self._base_configs:
            assert config.name not in self.chains, f'Multiple configs with same name `{config.name}`'
            self.chains[config.name] = Chain(config, self._tasks, parameter_mode=self.parameter_mode)

    def __getitem__(self, chain_name: str):
        if chain_name not in self.chains:
            raise ValueError(f'Unknown chain name `{chain_name}`')
        return self.chains[chain_name]

    def force(self, tasks: Union[str, Iterable[Union[str, Task]]], **kwargs):
        """ Pass force to all chains. """
        for chain in self.chains.values():
            chain.force(tasks, **kwargs)

    def latest(self, chain_name: str=None):
        """ Get latest chain based on name (alphabetically last)

        Args:
            chain_name: return latest chain from chain with name containing `chain_name`
        """
        for fullname, chain in sorted(self.chains.items(), reverse=True):
            if chain_name is None or chain_name in fullname:
                return chain

    @classmethod
    def set_log_level(cls, level):
        """ Pass log level to all chains. """
        Chain.log_handler.setLevel(level)

    def items(self):
        yield from sorted(self.chains.items())

    def keys(self):
        yield from sorted(self.chains.keys())

    def values(self):
        for _, val in sorted(self.chains.items()):
            yield val

    def __len__(self):
        return len(self.chains)

    def __repr__(self):
        return 'multichain:\n - ' + '\n - '.join(map(repr, self.values()))

    def __str__(self):
        return '\n'.join(fullname for fullname in self.keys())
