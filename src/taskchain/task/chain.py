from typing import Dict, Type

from taskchain.task.config import Config
from taskchain.task.task import Task
from taskchain.utils.clazz import get_classes_by_import_string


class Chain:

    def __init__(self, config: Config):
        self.tasks: Dict[str, Task] = {}
        self.configs: Dict[str, Config] = {}

        self._process_config(config)

    def _process_config(self, config: Config):
        self.configs[config.name] = config
        for used in config.get('uses', []):
            if isinstance(used, Config):
                assert config.base_dir == used.base_dir, f'Base dirs of configs `{config}` and `{used}` do not match'
                used_config = used
            else:
                used_config = Config(config.base_dir, used)
            self._process_config(used_config)

        for task_string in config.get('tasks', []):
            for task_class in get_classes_by_import_string(task_string, Task):
                self._create_task(task_class, config)

    def _create_task(self, task_class: Type[Task], config: Config):
        task = task_class(config)
        if task.slugname in self.tasks:
            existing_task = self.tasks[task.slugname]
            if existing_task.config.name == task.config.name:
                del task
                return existing_task
            else:
                raise ValueError(f'Multiple tasks of name `{task.slugname}` with different configs `{task.config}` and `{existing_task.config}`')

        for input_param in task.meta.get('input_params', []):
            if input_param not in config:
                raise ValueError(f'Input parameter `{input_param}` required by task `{task}` is not in its config `{config}`')
        self.tasks[task.slugname] = task
        return task


class MultiChain:

    pass
