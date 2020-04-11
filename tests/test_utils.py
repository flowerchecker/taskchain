from types import ModuleType

from taskchain.task import Task
from taskchain.utils.clazz import persistent, import_by_string
from taskchain.utils.data import traverse, search_and_apply


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


def test_traverse():
    assert len(list(traverse([]))) == 0
    assert len(list(traverse({}))) == 0
    assert len(list(traverse('a'))) == 1
    assert len(list(traverse(['a', 'b']))) == 2
    assert len(list(traverse(['a', {1, 2, 3}]))) == 4
    assert len(list(traverse({1: 2, 3: [4, 5, 6, (1, 2, 3)]}))) == 7


def test_search_and_apply():
    s = [
        {'a': 1, 'b': False, 'c': 'a'},
        10,
        20
    ]
    search_and_apply(s, fce=lambda x: True, allowed_types=(bool, ))
    assert s[0]['b'] is True

    search_and_apply(s, fce=lambda x: x + 'x', allowed_types=(str,))
    assert s[0]['c'] == 'ax'

    search_and_apply(s, fce=lambda x: x * 2, allowed_types=(int,), filter=lambda x: x < 5)
    assert s[0]['a'] == 2
    assert s[1] == 10
    assert s[2] == 20

    search_and_apply(s, fce=lambda x: x * 2, allowed_types=(int,), filter=lambda x: x > 15)
    assert s[0]['a'] == 2
    assert s[1] == 10
    assert s[2] == 40
