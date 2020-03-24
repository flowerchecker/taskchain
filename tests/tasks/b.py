from taskchain.task import Task


class CTask(Task):

    def run(self) -> bool:
        return False


class DTask(Task):

    def run(self) -> bool:
        return False
