from types import ModuleType

from taskchain.task import Task
from taskchain.utils.clazz import persistent, import_by_string


class Clazz:
    def __init__(self):
        self.calls = 0

    @persistent
    def method(self):
        self.calls += 1
        return 7

    @property
    @persistent
    def property(self):
        return 5


def test_persistent():

    clz = Clazz()
    assert clz.calls == 0

    assert clz.method() == 7
    assert clz.calls == 1
    assert clz._method == 7

    assert clz.method() == 7
    assert clz.calls == 1

    assert clz.property == 5
    assert clz._property == 5
    assert clz.property == 5


def test_import_by_string():
    module = import_by_string('tests.test_task')
    assert isinstance(module, ModuleType)

    A = import_by_string('tests.test_task.A')
    assert issubclass(A, Task)
    assert A.__name__ == 'A'

    member = import_by_string('tests.test_task.*')
    assert type(member) == list
    assert any(hasattr(m, '__name__') and m.__name__ == 'A' for m in member)
    assert all(not(hasattr(m, '__name__') and m.__name__ == 'Task') for m in member)
