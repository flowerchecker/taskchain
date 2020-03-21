from typing import Union, Generator, Dict

import pytest

from taskchain.task import Task, Data
from taskchain.task.data import JSONData, GeneratedData, BasicData


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


def test_slugname():
    assert ThisIsSomethingTask().slugname == 'this_is_something'
    assert ThisIsSomething().slugname == 'this_is_something'


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
        return {}


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


def test_result_type():
    assert ThisIsSomethingTask().data_type == str
    assert ThisIsSomething().data_type == int

    a = A()
    assert a.data_type == JSONData
    with pytest.raises(ValueError):
        data = a.data

    with pytest.raises(AttributeError):
        B()

    c = C()
    assert c.data_type == dict
    assert type(c.data) == BasicData

    d = D()
    assert d.data_type == GeneratedData
    assert type(d.data) == GeneratedData
