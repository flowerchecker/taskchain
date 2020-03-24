from taskchain.task import Task


class ATask(Task):

    def run(self) -> bool:
        return False


class BTask(Task):

    class Meta:
        input_params = ['a_number']

    def run(self) -> bool:
        return False
