from typing import Dict, Tuple

from taskchain.task.config import Config
from taskchain.task.task import Task


class Chain:

    def __init__(self, config: Config):
        self.tasks: Dict[str, Tuple[Task, Config]] = {}
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
