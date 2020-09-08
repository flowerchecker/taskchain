from taskchain.task import Task


class ATask(Task):

    def run(self) -> str:
        return self.get_value()

    def get_value(self):
        return 'a'


class BTask(ATask):

    def get_value(self):
        return 'b'


class CTask(ATask):

    def get_value(self):
        return 'c'
