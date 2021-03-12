from taskchain.task import Task
from taskchain.task.parameter import Parameter


class ATask(Task):

    def run(self) -> bool:
        return False


class BTask(Task):

    class Meta:
        parameters = [Parameter('a_number')]

    def run(self) -> bool:
        return False
