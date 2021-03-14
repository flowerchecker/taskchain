import abc
import logging
import re
from hashlib import sha256
from typing import Dict, Type, Union, Set, Iterable, Sequence, Tuple

import networkx as nx

from taskchain.task.config import Config
from taskchain.task.task import Task, find_task_full_name, InputTasks
from taskchain.utils.clazz import get_classes_by_import_string


class ChainObject:

    @abc.abstractmethod
    def init_chain(self, chain):
        pass


class Chain(dict):

    logger = logging.getLogger('tasks_chain')

    def __init__(self, config: Config,
                 shared_tasks: Dict[Tuple[str, str], Task] = None,
                 parameter_mode: bool = True
                 ):
        super().__init__()
        self.tasks: Dict[str, Task] = {}
        self.configs: Dict[str, Config] = {}

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

    def __getitem__(self, item):
        return self.get(item)

    def __getattr__(self, item):
        return self.get(item)

    def get(self, item, default=None):
        if default is not None:
            raise ValueError('Default task is not allowed')
        return self.tasks.get(find_task_full_name(item, self.tasks.keys()))

    def __contains__(self, item):
        return find_task_full_name(item, self.tasks.keys()) in self.tasks

    def _prepare(self):
        self._process_config(self._base_config)
        tasks = self._create_tasks(task_registry=None if self._parameter_mode else self._task_registry)
        self._process_dependencies(tasks)

        if self._parameter_mode:
            self.tasks = self._recreate_tasks_with_parameter_config(tasks, self._task_registry)
            self._process_dependencies(self.tasks)
        else:
            self.tasks = tasks

        self._build_graph()
        self._init_objects()

    def _process_config(self, config: Config):
        self.configs[config.name] = config
        for use in config.get('uses', []):
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
        tasks = {}

        def _register_task(_task: Task):
            task_name = _task.fullname
            if task_name in tasks:
                raise ValueError(f'Conflict of task name `{task_name}` '
                                 f'with configs `{tasks[task_name].get_config()}` and `{task.get_config()}`')
            tasks[task_name] = _task

        for config in self.configs.values():
            for task_description in config.get('tasks', []):
                if type(task_description) is str:
                    for task_class in get_classes_by_import_string(task_description, Task):
                        if task_class.meta.get('abstract', False):
                            continue
                        task = self._create_task(task_class, config, task_registry)
                        _register_task(task)
                elif issubclass(task_description, Task):
                    task = self._create_task(task_description, config, task_registry)
                    _register_task(task)
                else:
                    raise ValueError(f'Unknown task description `{task_description}` in config `{config}`')
        return tasks

    def _recreate_tasks_with_parameter_config(self, tasks: Dict[str, Task], task_registry: Dict) -> Dict[str, Task]:
        new_tasks: Dict[str, Task] = {}

        def _get_task(_task):
            if _task.fullname in new_tasks:
                return new_tasks[_task.fullname]
            input_tasks = {n: _get_task(t) for n, t in _task.input_tasks.items()}
            config = TaskParameterConfig(_task, input_tasks)
            new_task = self._create_task(_task.__class__, config, task_registry)
            assert new_task.fullname == _task.fullname
            new_tasks[new_task.fullname] = new_task
            return new_task

        for task in tasks.values():
            _get_task(task)

        return new_tasks

    @staticmethod
    def _create_task(task_class: Type[Task], config: Config, task_registry: Dict = None):
        task_name = task_class.fullname(config)
        if task_registry and (task_name, config.name) in task_registry:
            return task_registry[task_name, config.name]

        task = task_class(config)
        if task_registry is not None:
            task_registry[task_name, config.name] = task
        return task

    @staticmethod
    def _process_dependencies(tasks: Dict[str, Task]):
        for task_name, task in tasks.items():
            input_tasks = InputTasks()
            for input_task in task.meta.get('input_tasks', []):
                if type(input_task) is str:
                    input_task_name = input_task
                else:  # for reference by class
                    input_task_name = input_task.slugname
                if type(input_task_name) is str and task.get_config().namespace and not input_task_name.startswith(task.get_config().namespace):
                    input_task_name = f'{task.get_config().namespace}::{input_task_name}'     # add current config to reference
                if input_task_name in input_tasks.keys():
                    raise ValueError(f'Multiple input tasks with same name `{input_task_name}`')
                try:
                    found_name = find_task_full_name(input_task_name, tasks, determine_namespace=False)
                    if type(input_task) is str:
                        input_task_name = found_name
                except KeyError:
                    raise ValueError(f'Input task `{input_task_name}` of task `{task}` not found')
                input_tasks[input_task_name] = tasks[input_task_name]
            task.set_input_tasks(input_tasks)

    def _build_graph(self):
        self.graph = G = nx.DiGraph()
        G.add_nodes_from(self.tasks.values())

        for task in self.tasks.values():
            for input_task in task.input_tasks.values():
                G.add_edge(input_task, task)

        if not nx.is_directed_acyclic_graph(G):
            raise ValueError('Chain is not acyclic')

    def _init_objects(self):
        for config in self.configs.values():
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
        task = self.get_task(task)
        dependency_task = self.get_task(dependency_task)

        return nx.has_path(self.graph, dependency_task, task)

    def dependent_tasks(self, task: Union[str, Task], include_self: bool = False) -> Set[Task]:
        task = self.get_task(task)
        descendants = nx.descendants(self.graph, task)
        if include_self:
            descendants.add(task)
        return descendants

    def required_tasks(self, task: Union[str, Task], include_self: bool = False) -> Set[Task]:
        task = self.get_task(task)
        ancestors = nx.ancestors(self.graph, task)
        if include_self:
            ancestors.add(task)
        return ancestors

    def force(self, tasks: Union[str, Task, Iterable[Union[str, Task]]], recompute=False):
        if type(tasks) is str or isinstance(tasks, Task):
            tasks = [tasks]
        forced_tasks = set()
        for task in tasks:
            forced_tasks |= self.dependent_tasks(task, include_self=True)

        for task in forced_tasks:
            task.force()

        if recompute:
            for task in list(forced_tasks)[::-1]:
                _ = task.value

    @property
    def fullname(self):
        return self._base_config.name

    def draw(self, node_attrs=None, edge_attrs=None, graph_attrs=None, split_by_namespaces=False):
        import graphviz as gv
        import seaborn as sns

        node_attr = {'shape': 'plain_text', 'style': 'filled', 'width': '2'}
        node_attr.update((node_attrs if node_attrs else {}))
        graph_attr = {'splines': 'ortho'}
        graph_attr.update(graph_attrs if graph_attrs else {})
        edge_attr = {}
        edge_attr.update(edge_attrs if edge_attrs else {})

        groups = list({(n.config.namespace, n.group) for n in self.graph.nodes})
        colors = sns.color_palette('pastel', len(groups)).as_hex()

        G = gv.Digraph(
            engine='dot',
            graph_attr=graph_attr,
            node_attr=node_attr,
            edge_attr=edge_attr
        )

        def _get_slugname(task: Task):
            if split_by_namespaces:
                return node.fullname.replace(':', '/')
            return f'{task.slugname.split(":")[-1]}#{task.get_config()._filepath}'

        for node in self.graph.nodes:
            G.node(
                _get_slugname(node),
                label=node.fullname.split(':')[-1],
                color=colors[groups.index((node.config.namespace, node.group))]
            )

        for edge in self.graph.edges:
            G.edge(_get_slugname(edge[0]), _get_slugname(edge[1]))
        return G


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

        original_config = original_task.get_config()
        self.base_dir = original_config.base_dir
        self.namespace = original_config.namespace
        self.global_vars = original_config.global_vars
        self.context = original_config.context
        self._name = f'{original_config.name}/{original_task}'

        self._data = {}
        for parameter in original_task.parameters.values():
            if parameter.name_in_config in original_config:
                self._data[parameter.name_in_config] = original_config[parameter.name_in_config]

        self.input_tasks = {name: task.get_config().get_name_for_persistence(task) for name, task in input_tasks.items()}

    def get_name_for_persistence(self, task: Task) -> str:
        parameter_repr = task.parameters.repr
        input_tasks_repr = '###'.join(f'{n}={it}' for n, it in sorted(self.input_tasks.items()))
        return sha256(f'{parameter_repr}$$${input_tasks_repr}'.encode()).hexdigest()[:32]


class MultiChain:

    logger = logging.getLogger('tasks_chain')

    def __init__(self, configs: Sequence[Config], parameter_mode: bool = True):
        self._tasks: Dict[Tuple[str, str], Task] = {}
        self.chains: Dict[str, Chain] = {}
        self._base_configs = configs
        self.parameter_mode = parameter_mode

        self._prepare()

    def _prepare(self):
        for config in self._base_configs:
            self.chains[config.name] = Chain(config, self._tasks, parameter_mode=self.parameter_mode)

    def __getitem__(self, chain_name: str):
        if chain_name not in self.chains:
            raise ValueError(f'Unknown chain name `{chain_name}`')
        return self.chains[chain_name]

    def force(self, tasks: Union[str, Iterable[Union[str, Task]]]):
        for chain in self.chains.values():
            chain.force(tasks)

    def latest(self, chain_name: str=None):
        for fullname, chain in sorted(self.chains.items(), reverse=True):
            if chain_name is None or chain_name in fullname:
                return chain

    def items(self):
        yield from sorted(self.chains.items())

    def keys(self):
        yield from sorted(self.chains.keys())

    def values(self):
        for _, val in sorted(self.chains.items()):
            yield val

    def __repr__(self):
        return 'multichain:\n - ' + '\n - '.join(map(repr, self.values()))

    def __str__(self):
        return '\n'.join(fullname for fullname in self.keys())
