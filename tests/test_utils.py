from copy import deepcopy
from types import ModuleType

import pytest

from taskchain import Task
from taskchain.utils.clazz import persistent, import_by_string, find_and_instantiate_clazz, repeat_on_error
from taskchain.utils.data import traverse, search_and_apply, ReprStr, search_and_replace_placeholders


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
    assert clz.__method == 7

    assert clz.method() == 7
    assert clz.calls == 1

    assert clz.property == 5
    assert clz.__property == 5
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

    dd = import_by_string('collections.defaultdict')
    assert dd(int)['666'] == 0
    assert dd(dict)['666'] == {}

    T = import_by_string('tests.test_task.Task')
    assert issubclass(T, Task)
    assert T.__name__ == 'Task'


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


class TestObject:
    __test__ = False

    def __init__(self, a, kwa=None):
        self.a = a
        self.kwa = kwa


def test_find_and_instancelize_clazz():
    class_def = {'class': 'tests.test_utils.TestObject', 'args': [1], 'kwargs': {'kwa': 2}}
    r = find_and_instantiate_clazz(class_def)
    assert r.a == 1
    assert r.kwa == 2

    obj = [class_def, class_def]
    r = find_and_instantiate_clazz(obj)
    assert r[0].a == 1
    assert r[1].a == 1
    assert r[0].kwa == 2
    assert r[1].kwa == 2
    assert id(obj) == id(r)

    obj = {'a': class_def, 'b': class_def}
    r = find_and_instantiate_clazz(obj)
    assert r['a'].a == 1
    assert r['b'].a == 1
    assert r['a'].kwa == 2
    assert r['b'].kwa == 2
    assert id(obj) == id(r)

    class_def2 = {
        'class': 'tests.test_utils.TestObject',
        'args': [class_def],
        'kwargs': {'kwa': class_def}
    }
    r = find_and_instantiate_clazz(class_def2)
    assert r.a.a == 1
    assert r.kwa.a == 1
    assert r.a.kwa == 2
    assert r.kwa.kwa == 2


def test_repeat_call():

    class Clazz:

        def __init__(self):
            self.calls = 0

        @repeat_on_error(waiting_time=0)
        def method(self, errors, kw=7):
            self.calls += 1
            if self.calls > errors:
                return kw
            raise Exception

    c = Clazz()
    assert c.method(0, 8) == 8
    assert c.calls == 1
    c.calls = 0
    assert c.method(5) == 7
    assert c.calls == 6
    c.calls = 0
    assert c.method(9) == 7
    assert c.calls == 10
    c.calls = 0
    with pytest.raises(Exception):
        c.method(10)
    assert c.calls == 10
    c.calls = 0
    with pytest.raises(Exception):
        print(c.method(11))
    assert c.calls == 10


def test_repr_str():
    s = ReprStr('a', 'b')
    assert isinstance(s, str)
    assert type(s) is ReprStr
    assert str(s) == 'a'
    assert repr(s) == "'b'"
    assert deepcopy(s) == s
    assert deepcopy(s) is not s


def test_search_and_replace_placeholders():
    r = search_and_replace_placeholders('{A}c{B}c{A}{A}', {'A': 'a', 'B': 'b'})
    for _ in range(2):
        assert r == 'acbcaa'
        assert repr(r) == "'{A}c{B}c{A}{A}'"
        assert type(r) is ReprStr
        r = search_and_replace_placeholders(r, {})


    r = search_and_replace_placeholders({
        'string': '{A}.b',
        'list': ['{A}.b', ['{A}{A}']],
    }, {'A': 'a'})
    assert r['string'] == 'a.b'
    assert r['list'][0] == 'a.b'
    assert r['list'][1][0] == 'aa'
