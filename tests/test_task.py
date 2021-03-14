from collections import defaultdict
from typing import Generator, Dict, List

import pytest

from taskchain.task import Task, Config, ModuleTask
from taskchain.task.data import JSONData, GeneratedData
from taskchain.task.parameter import Parameter


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

    f = F()
    assert f.data_type == int
    assert f.data_class == CustomData
    assert type(f.data) == CustomData
    assert f.value == 1


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
            self.log('working')
            stats = defaultdict(int)
            stats['hit'] += 1
            self.log(stats)
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

        def run(self, a, b) -> List:
            return [a, b]

    chain = Config(tmp_path, name='config1', data={'tasks': [A, B, C1]}).chain()
    assert chain.c1.value == [1, 2]

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

    print('---------')
    chain = Config(tmp_path, name='config1', data={'tasks': [A, ATask, C4]}).chain()
    with pytest.raises(KeyError):
        _ = chain.c4.value
