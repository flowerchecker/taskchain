import pytest

from taskchain import Task, Parameter
from taskchain.parameter import AutoParameterObject
from taskchain.utils.testing import TestChain, MockTask, create_test_task


class MyParameterObject(AutoParameterObject):

    def __init__(self, value):
        self.value = value


class A(Task):

    class Meta:
        parameters = [Parameter('pa')]

    def run(self, pa) -> int:
        return pa.value


class B(Task):

    class Meta:
        parameters = [Parameter('pb', default=7)]

    def run(self, pb) -> int:
        return pb

class C(Task):

    class Meta:
        input_tasks = [A]
        parameters = [Parameter('pc')]

    def run(self, a, pc) -> int:
        return a + pc


class D(Task):

    class Meta:
        input_tasks = [A, C]
        parameters = [Parameter('pd', default=7000)]

    def run(self, a, c, pd) -> int:
        self.save_to_run_info(f'log {a} {c} {pd}')
        return a + c + pd


def test_mock_task():
    mt = MockTask(7)
    assert mt.value == 7


def test_test_chain(tmp_path):
    tc = TestChain(
        tasks = [D],
        mock_tasks = {
            'a': 10,
            C: 1,
        },
        parameters={
            'pd': 1000,
        },
        base_dir=tmp_path,
    )
    assert tc.a.value == 10
    assert tc.c.value == 1
    assert tc.d.value == 1011
    assert 'b' not in tc.tasks
    assert not (tmp_path / 'a').exists()
    assert (tmp_path / 'd' / 'test.json').exists()
    assert tc.d.run_info['log'] == ['log 10 1 1000']


def test_multiple_tasks():
    tc = TestChain(
        tasks=[A, D],
        mock_tasks={
            C: 1,
        },
        parameters={
            'pa': MyParameterObject(value=10),
            'pd': 1000,
        },
    )
    assert tc.a.value == 10
    assert tc.d.value == 1011


def test_test_chain_with_default_parameter():
    tc = TestChain(
        tasks=[D],
        mock_tasks={
            A: 10,
            C: 1,
        },
    )
    assert tc.d.value == 7011


def test_missing_input_task():
    with pytest.raises(ValueError):
        TestChain(
            tasks=[D],
            mock_tasks={
                C: 1,
            },
        )

def test_missing_parameter():
    with pytest.raises(ValueError):
        TestChain(
            tasks=[C],
        )


def test_parameter_objects():
    tc = TestChain(
        tasks=[A],
        parameters={
            'pa': {'class': 'tests.test_testing.MyParameterObject', 'kwargs': {'value': 77}}
        }
    )
    assert tc.a.value == 77


def test_parameter_objects_as_instance():
    tc = TestChain(
        tasks=[A],
        parameters={
            'pa': MyParameterObject(33)
        }
    )
    assert tc.a.value == 33


def test_test_task():
    task = create_test_task(
        D,
        parameters={'pd': 3},
        input_tasks={'a': 1, C: 2},
    )
    assert task.value == 6
