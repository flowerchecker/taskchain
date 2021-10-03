import tempfile
from pathlib import Path
from typing import Any, Dict, List, Type, Union

from taskchain import Chain, Config, Task, InMemoryData


class MockTask(Task):

    class Meta:
        data_type = Any
        data_class = InMemoryData

    def __init__(self, value: Any):
        super().__init__()
        self._value = value

    @property
    def value(self) -> Any:
        return self._value


class TestChain(Chain):

    def __init__(self,
                 tasks: List[Type[Task]],
                 mock_tasks: Dict[Union[str, Type[Task]], Any] = None,
                 parameters: Dict[str, Any] = None,
                 base_dir: Path = None,
                 ):
        """
        Helper class for testing part of a chain. Some tasks are present fully, some are mocked.
        Config is not needed, parameters are provided directly.

        Args:
            tasks: list of task classes which should be part of the chain
            mock_tasks: other task which are needed (as input_tasks) in chain but their output is only mocked
                (by values of this dict)
            parameters: parameter names and their values
            base_dir: path for data persistence, if None tmp dir is created
        """
        self._tasks = tasks
        self._mock_tasks = mock_tasks or {}

        if base_dir is None:
            base_dir = Path(tempfile.TemporaryDirectory().name)

        if parameters is None:
            parameters = {}
        self.config = Config(base_dir, name='test', data=parameters)

        super().__init__(self.config)

    def _prepare(self):
        self._process_config(self._base_config)
        self.tasks = self._create_tasks()
        self._process_dependencies(self.tasks)

        self._build_graph()
        self._init_objects()

    def _create_tasks(self) -> Dict[str, Task]:
        tasks = {}
        for task_class in self._tasks:
            task = self._create_task(task_class, self.config)
            tasks[task_class.fullname(self.config)] = task

        for mock_task, value in self._mock_tasks.items():
            name = mock_task if isinstance(mock_task, str) else mock_task.fullname(self.config)
            tasks[name] = MockTask(value)

        return tasks


def create_test_task(
        task: Type[Task],
        input_tasks: Dict[Union[str, Type[Task]], Any] = None,
        parameters: Dict[str, Any] = None,
        base_dir: Path = None,
        ) -> Task:
    """
    Helper function which instantiate task in such way, that parameters and input_tasks are given with arguments
    of this function.

    Args:
        tasks class of tested tasks
        input_tasks: mocked values of input_tasks of tested class
        parameters: parameter names and their values
        base_dir: path for data persistence, if None tmp dir is created
    """
    test_chain = TestChain([task],  parameters=parameters, mock_tasks=input_tasks, base_dir=base_dir)
    return test_chain[task.fullname(test_chain.config)]
