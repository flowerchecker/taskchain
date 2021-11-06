from collections import defaultdict
from collections.abc import Generator
from typing import Dict, List

import pytest

from taskchain import Task, Config, ModuleTask
from taskchain.data import JSONData, GeneratedData, InMemoryData
from taskchain.parameter import Parameter, InputTaskParameter
from taskchain.task import _find_task_full_name


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


def test_module_task():
    class Mt(ModuleTask):

        def run(self) -> bool:
            pass

    assert Mt().slugname == 'test_task:mt'


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


class CustomData(JSONData):
    @classmethod
    def is_data_type_accepted(cls, data_type):
        return False


class F(Task):
    class Meta:
        data_class = CustomData

    def run(self) -> int:
        return 1


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
    assert c.data_type == dict
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

    f = F()
    assert f.data_type == int
    assert f.data_class == CustomData
    assert type(f.data) == CustomData
    assert f.value == 1


def test_advanced_return_types():
    class T(Task):
        def run(self) -> dict:
            return {'a': 1}

    task = T()
    assert task.data_type == dict
    assert task.data_class == JSONData
    assert type(task.data) == JSONData
    assert task.value == {'a': 1}

    class T2(Task):
        def run(self) -> Dict[str, int]:
            return {'a': 1}

    task = T2()
    assert task.data_type == dict
    assert task.data_class == JSONData
    assert type(task.data) == JSONData
    assert task.value == {'a': 1}


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
    assert not a.has_data
    _ = a.value
    assert a.has_data
    assert a.run_called == 1
    _ = a.value
    assert a.has_data
    assert a.run_called == 1

    a = A(config)
    _ = a.value
    assert a.has_data
    assert a.run_called == 0
    a.force()
    _ = a.value
    assert a.has_data
    assert a.run_called == 1
    _ = a.value
    assert a.run_called == 1

    a = A(config)
    a.force()
    _ = a.value
    assert a.run_called == 1
    _ = a.value
    assert a.run_called == 1

    a = A(config)
    assert (tmp_path / 'a' / 'config.json').exists()
    a.force(delete_data=True)
    a.force(delete_data=True)
    assert not (tmp_path / 'a' / 'config.json').exists()
    _ = a.value
    assert a.run_called == 1


def test_run_fail(tmp_path):

    class A(Task):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> int:
            self.run_called += 1
            raise ValueError()

    config = Config(tmp_path, name='config')
    a = A(config)

    with pytest.raises(ValueError):
        _ = a.value
    assert a.run_called == 1

    with pytest.raises(ValueError):
        _ = a.value
    assert a.run_called == 2


def test_namespace(tmp_path):
    class A(Task):
        def run(self) -> int:
            return 0

    config = Config(tmp_path, name='config', namespace='ns', data={'tasks': [A]})
    a = A(config)
    assert a.slugname == 'a'
    assert a.fullname == 'ns::a'


def test_run_info(tmp_path):
    class A(Task):
        class Meta:
            parameters = [Parameter('p'), Parameter('s')]

        def run(self) -> int:
            self.save_to_run_info('working')
            stats = defaultdict(int)
            stats['hit'] += 1
            self.save_to_run_info(stats)
            return 0

    a = A(config=Config(tmp_path, name='cfg', namespace='ns', data={'p': 1, 's': 'abc'}, context={}))
    _ = a.value

    a = A(config=Config(tmp_path, name='cfg', namespace='ns', data={'p': 1, 's': 'abc'}))
    info = a.run_info
    assert info['parameters'] == {'p': '1', 's': "'abc'"}
    assert info['config']['namespace'] == 'ns'
    assert info['config']['name'] == 'cfg'
    assert info['config']['context'] == 'dict_context()'
    assert len(info['log']) == 2
    assert info['log'][0] == 'working'
    assert info['log'][1] == {'hit': 1}

    assert 'started' in info
    assert 'ended' in info
    assert 0 < info['time'] < 0.01

    assert (tmp_path / 'a' / 'cfg.run_info.yaml').exists()

    for line in (tmp_path / 'a' / 'cfg.run_info.yaml').open().readlines():
        print(line[:-1])


def test_run_input_arguments(tmp_path):
    class A(Task):
        class Meta:
            task_group = 'group'

        def run(self) -> int:
            return 1

    class B(Task):
        def run(self) -> int:
            return 2

    class C1(Task):
        class Meta:
            input_tasks = [A, B]
            parameters = [Parameter('x'), Parameter('y', default=66)]

        def run(self, a, b, x, y) -> List:
            return [a, b, x, y]

    chain = Config(tmp_path, name='config1', data={'tasks': [A, B, C1], 'x': 77}).chain()
    assert chain.c1.value == [1, 2, 77, 66]

    class C2(Task):
        class Meta:
            input_tasks = [A, B]

        def run(self, b) -> List:
            return [self.input_tasks['a'].value, b]

    chain = Config(tmp_path, name='config1', data={'tasks': [A, B, C2]}).chain()
    assert chain.c2.value == [1, 2]

    class C3(Task):
        class Meta:
            input_tasks = [A, B]

        def run(self, b=7) -> List:
            return [self.input_tasks['a'].value, b]

    chain = Config(tmp_path, name='config1', data={'tasks': [A, B, C3]}).chain()
    with pytest.raises(AttributeError):
        _ = chain.c3.value

    class ATask(Task):
        class Meta:
            task_group = 'group2'

        def run(self) -> int:
            return 3

    class C4(Task):
        class Meta:
            input_tasks = [A, ATask]

        def run(self, a) -> List:
            return [self.input_tasks['a'].value, a]

    chain = Config(tmp_path, name='config1', data={'tasks': [A, ATask, C4]}).chain()
    with pytest.raises(KeyError):
        _ = chain.c4.value

    class C5(Task):
        class Meta:
            input_tasks = [A, B]
            parameters = [Parameter('a')]

        def run(self, a) -> List:
            return [a]

    chain = Config(tmp_path, name='config1', data={'tasks': [A, B, C5], 'a': 77}).chain()
    with pytest.raises(KeyError):
        _ = chain.c5.value


def test_optional_input_tasks(tmp_path):

    class A(Task):
        class Meta:
            parameters = [
                Parameter('a_value', default=1),
            ]

        def run(self, a_value) -> int:
            return a_value

    class B1(Task):
        class Meta:
            parameters = [
                InputTaskParameter(A, default=2),
            ]

        def run(self, a) -> int:
            return a

    class B2(Task):
        class Meta:
            input_tasks = [
                InputTaskParameter(A, default=2),
            ]

        def run(self, a) -> int:
            return a

    class B3(Task):
        class Meta:
            input_tasks = [
                InputTaskParameter('a', default=2),
            ]

        def run(self, a) -> int:
            return a

    class B4(Task):
        class Meta:
            input_tasks = [
                InputTaskParameter(A),
            ]

        def run(self, a) -> int:
            return a

    class B5(Task):
        class Meta:
            parameters = [
                InputTaskParameter(A),
            ]

        def run(self, a) -> int:
            return a

    class B6(Task):
        class Meta:
            name = 'b1'
            input_tasks = []

        def run(self) -> int:
            return 2

    chain = Config(tmp_path, name='config', data={'tasks': [A, B1], 'a_value': 666}).chain()
    assert chain.b1.value == 666
    assert list(chain.b1.run_info['input_tasks'].keys()) == ['a']
    chain = Config(tmp_path, name='config', data={'tasks': [A, B2], 'a_value': 666}).chain()
    assert chain.b2.value == 666
    assert list(chain.b2.run_info['input_tasks'].keys()) == ['a']
    chain = Config(tmp_path, name='config', data={'tasks': [A, B3], 'a_value': 666}).chain()
    assert chain.b3.value == 666
    assert list(chain.b3.run_info['input_tasks'].keys()) == ['a']

    chain = Config(tmp_path, name='config', data={'tasks': [B1], 'a_value': 666}).chain()
    assert chain.b1.value == 2
    assert chain.b1.run_info['input_tasks'] == {}
    chain = Config(tmp_path, name='config', data={'tasks': [B2], 'a_value': 666}).chain()
    assert chain.b2.value == 2
    assert chain.b2.run_info['input_tasks'] == {}
    chain = Config(tmp_path, name='config', data={'tasks': [B3], 'a_value': 666}).chain()
    assert chain.b3.value == 2
    assert chain.b3.run_info['input_tasks'] == {}

    with pytest.raises(ValueError):
        _ = Config(tmp_path, name='config', data={'tasks': [B4], 'a_value': 666}).chain()
    with pytest.raises(ValueError):
        _ = Config(tmp_path, name='config', data={'tasks': [B5], 'a_value': 666}).chain()

    chain1 = Config(tmp_path, name='config', data={'tasks': [B1], 'a_value': 666}).chain()
    chain2 = Config(tmp_path, name='config', data={'tasks': [B6], 'a_value': 666}).chain()
    assert chain1.b1.name_for_persistence == chain2.b1.name_for_persistence


def test_find_task_full_name():
    assert _find_task_full_name('a', ['n1::a', 'a']) == 'a'
    with pytest.raises(KeyError):
        _ = _find_task_full_name('a', ['n3::n1::a', 'n3::a'])
    assert _find_task_full_name('n3::a', ['n3::n1::a', 'n3::a']) == 'n3::a'


def test_in_memory_data(tmp_path):
    class A(Task):
        class Meta:
            data_class = InMemoryData

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> int:
            self.run_called += 1
            return 42

    config = Config(tmp_path, name='config')
    a = A(config)
    assert a.run_called == 0
    assert a.value == 42
    assert a.run_called == 1
    assert a.data_path is None
    assert not a.has_data

    a = A(config)
    assert a.run_called == 0
    assert a.value == 42
    assert a.run_called == 1
    assert a.force()
    assert a.value == 42
    assert a.run_called == 2
    assert a.force(delete_data=True)
    assert a.value == 42
    assert a.run_called == 3
