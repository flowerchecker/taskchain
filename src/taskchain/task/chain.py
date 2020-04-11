from typing import Dict, Type, Union, Set, Iterable, Sequence, Tuple

import networkx as nx

from taskchain.task.config import Config
from taskchain.task.task import Task
from taskchain.utils.clazz import get_classes_by_import_string


class Chain(dict):

    def __init__(self, config: Config, shared_tasks: Dict[Tuple[str, str], Task] = None):
        super().__init__()
        self.tasks: Dict[str, Task] = {}
        self.configs: Dict[str, Config] = {}

        self._base_config = config
        self._task_registry = shared_tasks if shared_tasks is not None else {}
        self.graph: Union[None, nx.DiGraph] = None

        self._prepare()

    def __str__(self):
        return f'<chain for config `{self._base_config}`>'

    def __getitem__(self, item):
        return self.tasks[item]

    def __getattr__(self, item):
        return self.tasks[item]

    def get(self, item, default=None):
        return self.tasks.get(item, default)

    def __contains__(self, item):
        return item in self.tasks

    def _prepare(self):
        self._process_config(self._base_config)
        self._process_dependencies()
        self._build_graph()

    def _process_config(self, config: Config):
        self.configs[config.name] = config
        for used in config.get('uses', []):
            if isinstance(used, Config):
                assert config.base_dir == used.base_dir, f'Base dirs of configs `{config}` and `{used}` do not match'
                used_config = used
            else:
                used_config = Config(config.base_dir, used)
            self._process_config(used_config)

        for task_description in config.get('tasks', []):
            if type(task_description) is str:
                for task_class in get_classes_by_import_string(task_description, Task):
                    self._create_task(task_class, config)
            elif issubclass(task_description, Task):
                self._create_task(task_description, config)
            else:
                raise ValueError(f'Unknown task description `{task_description}` in config `{config}`')

    def _create_task(self, task_class: Type[Task], config: Config):
        if self._task_registry and (task_class.slugname, config.name) in self._task_registry:
            self.tasks[task_class.slugname] = self._task_registry[task_class.slugname, config.name]
            return self._task_registry[task_class.slugname, config.name]

        task = task_class(config)
        for input_param in task.meta.get('input_params', []):
            if input_param not in config:
                raise ValueError(f'Input parameter `{input_param}` required by task `{task}` is not in its config `{config}`')
        self.tasks[task.slugname] = task
        self._task_registry[task_class.slugname, config.name] = task
        return task

    def _process_dependencies(self):
        for task_name, task in self.tasks.items():
            input_tasks = {}
            for input_task in task.meta.get('input_tasks', []):
                if type(input_task) is not str:
                    for n, t in self.tasks.items():
                        if t.__class__ == input_task:
                            input_task = t.slugname
                            break
                if input_task not in self.tasks:
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

    def get_task(self, task: Union[str, Task]) -> Task:
        if isinstance(task, Task):
            return task
        if task not in self.tasks:
            raise ValueError(f'Task `{task}` not found')
        return self.tasks[task]

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

    def force(self, tasks: Union[str, Task, Iterable[Union[str, Task]]]):
        if type(tasks) is str or isinstance(tasks, Task):
            tasks = [tasks]
        forced_tasks = set()
        for task in tasks:
            forced_tasks |= self.dependent_tasks(task, include_self=True)

        for task in forced_tasks:
            task.force()


class MultiChain:

    def __init__(self, configs: Sequence[Config]):
        self._tasks: Dict[Tuple[str, str], Task] = {}
        self.chains: Dict[str, Chain] = {}
        self._base_configs = configs

        self._prepare()

    def _prepare(self):
        for config in self._base_configs:
            self.chains[config.name] = Chain(config, self._tasks)

    def __getitem__(self, chain_name: str):
        if chain_name not in self.chains:
            raise ValueError(f'Unknown chain name `{chain_name}`')
        return self.chains[chain_name]

    def force(self, tasks: Union[str, Iterable[Union[str, Task]]]):
        for chain in self.chains.values():
            chain.force(tasks)
