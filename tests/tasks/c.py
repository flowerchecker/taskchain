from taskchain import Task


# X    N
#      |
#      v
# M -> O -> P
#

class MTask(Task):

    class Meta:
        input_tasks = []

    def run(self) -> bool:
        return False


class NTask(Task):

    class Meta:
        input_tasks = []

    def run(self) -> bool:
        return False


class OTask(Task):

    class Meta:
        input_tasks = [NTask, MTask]

    def run(self) -> bool:
        return False


class PTask(Task):

    class Meta:
        input_tasks = [OTask]

    def run(self) -> bool:
        return False


class XTask(Task):

    class Meta:
        input_tasks = []

    def run(self) -> bool:
        return False

