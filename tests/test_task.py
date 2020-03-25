from typing import Generator, Dict

import pytest

from taskchain.task import Task, Config
from taskchain.task.data import JSONData, GeneratedData


class ThisIsSomethingTask(Task):

    class Meta:
        int_value = 1
        str_value = 'value'
        bool_value = True

    def run(self) -> str:
        return 'something'


class ThisIsSomething(Task):

    def run(self) -> int:
        return 1


class X(Task):

    class Meta:
        task_group = 'a:b'
        name = 'y'

    def run(self) -> bool:
        pass


def test_slugname():
    assert ThisIsSomethingTask().slugname == 'this_is_something'
    assert ThisIsSomething().slugname == 'this_is_something'
    assert X().slugname == 'a:b:y'


def test_meta():
    task = ThisIsSomethingTask()
    assert task.meta['int_value'] == 1
    assert task.meta.int_value == 1
    assert task.meta.get('int_value') == 1
    assert task.meta.get('not_defined_value', 123) == 123


class A(Task):
    class Meta:
        data_type = JSONData

    def run(self):
        return set()


class B(Task):
    class Meta:
        data_type = JSONData

    def run(self) -> int:
        return 1


class C(Task):
    def run(self) -> Dict:
        return {}


class D(Task):
    def run(self) -> Generator:
        yield 1


class E(Task):
    class Meta:
        data_type = JSONData

    def run(self):
        data = JSONData()
        data.set_value([1, 2, 3])
        return data


def test_result_type():
    assert ThisIsSomethingTask().data_type == str
    assert ThisIsSomethingTask().data_class == JSONData
    assert ThisIsSomething().data_type == int
    assert ThisIsSomething().data_class == JSONData

    a = A()
    assert a.data_type == JSONData
    with pytest.raises(ValueError):
        _ = a.data

    with pytest.raises(AttributeError):
        B()

    c = C()
    assert c.data_type == Dict
    assert c.data_class == JSONData
    assert type(c.data) == JSONData

    d = D()
    assert d.data_type == Generator
    assert d.data_class == GeneratedData
    assert type(d.data) == GeneratedData

    e = E()
    assert e.data_type == JSONData
    assert e.data_class == JSONData
    assert type(e.data) == JSONData


def test_forcing(tmp_path):

    class A(Task):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> int:
            self.run_called += 1
            return 1

    config = Config(tmp_path, name='config')

    a = A(config)
    _ = a.value
    assert a.run_called == 1
    _ = a.value
    assert a.run_called == 1

    a = A(config)
    _ = a.value
    assert a.run_called == 0
    a.force()
    _ = a.value
    assert a.run_called == 1
    _ = a.value
    assert a.run_called == 1

    a = A(config)
    a.force()
    _ = a.value
    assert a.run_called == 1
    _ = a.value
    assert a.run_called == 1

