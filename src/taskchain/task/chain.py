import abc
import logging
import re
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

    def __init__(self, config: Config, shared_tasks: Dict[Tuple[str, str], Task] = None):
        super().__init__()
        self.tasks: Dict[str, Task] = {}
        self.configs: Dict[str, Config] = {}

        self._base_config = config
        self._task_registry = shared_tasks if shared_tasks is not None else {}
        self.graph: Union[None, nx.DiGraph] = None

        self._prepare()

    def __str__(self):
        a = max(len(n.split(":")[-1]) for n in self.tasks)
        b = max(len(n) for n in self.tasks)
        return '\n'.join(f'{n.split(":")[-1]:<{a}}  {n:<{b}}  {t.config}' for n, t in self.tasks.items())

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
        self._process_dependencies()
        self._build_graph()
        self._init_objects()

    def _process_config(self, config: Config):
        self.configs[config.name] = config
        for use in config.get('uses', []):
            if isinstance(use, Config):
                assert config.base_dir == use.base_dir, f'Base dirs of configs `{config}` and `{use}` do not match'
                if config.namespace:
                    if use.namespace:
                        use.namespace = f'{config.namespace}::{use.namespace}'
                    else:
                        use.namespace = config.namespace
                used_config = use
            else:
                pattern = r'(.*) as (.*)'
                if matched := re.match(pattern, use):
                    used_config = Config(
                        config.base_dir,
                        filepath=matched[1],
                        namespace=f'{config.namespace}::{matched[2]}' if config.namespace else matched[2],
                        context=config.context
                    )
                else:
                    used_config = Config(
                        config.base_dir,
                        use,
                        namespace=config.namespace if config.namespace else None,
                        context=config.context,
                    )
            self._process_config(used_config)

        for task_description in config.get('tasks', []):
            if type(task_description) is str:
                for task_class in get_classes_by_import_string(task_description, Task):
                    if task_class.meta.get('abstract', False):
                        continue
                    self._create_task(task_class, config)
            elif issubclass(task_description, Task):
                self._create_task(task_description, config)
            else:
                raise ValueError(f'Unknown task description `{task_description}` in config `{config}`')

    def _create_task(self, task_class: Type[Task], config: Config):
        task_name = task_class.fullname(config)
        if self._task_registry and (task_name, config.fullname) in self._task_registry:
            self.tasks[task_name] = self._task_registry[task_name, config.fullname]
            return self._task_registry[task_name, config.fullname]

        task = task_class(config)
        for input_param in task.meta.get('input_params', []):
            if input_param not in config:
                raise ValueError(f'Input parameter `{input_param}` required by task `{task}` is not in its config `{config}`')
        if task_name in self.tasks:
            raise ValueError(f'Conflict of task name `{task_name}` with configs `{self.tasks[task_name].config}` and `{task.config}`')
        self.tasks[task_name] = task
        self._task_registry[task_name, config.fullname] = task
        return task

    def _process_dependencies(self):
        for task_name, task in self.tasks.items():
            input_tasks = InputTasks()
            for input_task in task.meta.get('input_tasks', []):
                if type(input_task) is not str:     # for reference by class
                    input_task = input_task.slugname
                if type(input_task) is str and task.config.namespace and not input_task.startswith(task.config.namespace):
                    input_task = f'{task.config.namespace}::{input_task}'     # add current config to reference
                try:
                    input_task = find_task_full_name(input_task, self.tasks, determine_namespace=False)
                except KeyError:
                    raise ValueError(f'Input task `{input_task}` of task `{task}` not found')
                input_tasks[input_task] = self.tasks[input_task]
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
            for obj in config.objects.values():
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
        return self._base_config.fullname

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
            return f'{task.slugname.split(":")[-1]}#{task.config._filepath}'

        for node in self.graph.nodes:
            G.node(
                _get_slugname(node),
                label=node.fullname.split(':')[-1],
                color=colors[groups.index((node.config.namespace, node.group))]
            )

        for edge in self.graph.edges:
            G.edge(_get_slugname(edge[0]), _get_slugname(edge[1]))
        return G


class MultiChain:

    logger = logging.getLogger('tasks_chain')

    def __init__(self, configs: Sequence[Config]):
        self._tasks: Dict[Tuple[str, str], Task] = {}
        self.chains: Dict[str, Chain] = {}
        self._base_configs = configs

        self._prepare()

    def _prepare(self):
        for config in self._base_configs:
            self.chains[config.fullname] = Chain(config, self._tasks)

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
