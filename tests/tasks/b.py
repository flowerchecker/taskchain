from taskchain import Task


class CTask(Task):

    def run(self) -> bool:
        return False


class DTask(Task):

    class Meta:
        input_tasks = ['c']

    def run(self) -> bool:
        return False


class ETask(Task):

    class Meta:
        input_tasks = [CTask]

    def run(self) -> bool:
        return False
