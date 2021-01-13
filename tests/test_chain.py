import json
from typing import List

import pytest

from taskchain.task import Config, Chain, Task, MultiChain
from taskchain.task.chain import ChainObject
from tests.tasks.a import ATask


def test_config(tmp_path):
    config_data = {
        'uses': []
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)
    assert len(chain.configs) == 1


def test_configs(tmp_path):
    config_data = {
        'uses': [
            Config(tmp_path, name='config2', data={'a': 2}),
            Config(tmp_path, name='config3', data={'a': 3}),
        ],
        'a': 1,
    }
    config = Config(tmp_path, name='config1', data=config_data)
    chain = Chain(config)
    assert len(chain.configs) == 3
    assert chain.configs['config1'].a == 1
    assert chain.configs['config2'].a == 2
    assert chain.configs['config3'].a == 3


def test_task_creation(tmp_path):
    config_data = {
        'tasks': ['tests.tasks.a.A']
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)

    assert len(chain.tasks) == 1


def test_task_creation_with_uses(tmp_path):
    config_data = {
        'uses': [
            Config(tmp_path, name='config', data={'tasks': ['tests.tasks.a.A']}),
        ],
        'tasks': ['tests.tasks.b.*'],
    }
    config2 = Config(tmp_path, name='config2', data=config_data)
    chain2 = Chain(config2)

    assert len(chain2.tasks) == 4


def test_missing_param(tmp_path):
    config_data = {
        'tasks': ['tests.tasks.a.*']
    }
    config = Config(tmp_path, name='config', data=config_data)
    with pytest.raises(ValueError):
        _ = Chain(config)


def test_params_in_bad_config(tmp_path):
    config_data = {
        'uses': [
            Config(tmp_path, name='config', data={'tasks': ['tests.tasks.a.*']}),
        ],
        'tasks': ['tests.tasks.b.*'],
        'a_number': 7,
    }
    config = Config(tmp_path, name='config2', data=config_data)
    with pytest.raises(ValueError):
        _ = Chain(config)


def test_params(tmp_path):
    config_data = {
        'uses': [
            Config(tmp_path, name='config', data={
                'tasks': ['tests.tasks.a.*'],
                'a_number': 7,
            }),
        ],
        'tasks': ['tests.tasks.b.*'],
    }
    config = Config(tmp_path, name='config2', data=config_data)
    chain = Chain(config)
    assert chain.tasks['b'].config.a_number == 7


class XTask(Task):

    class Meta:
        input_tasks = [ATask]

    def run(self) -> bool:
        return False


def test_missing_input_task(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.b.*',
            'tests.test_chain.XTask',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data)
    with pytest.raises(ValueError):
        _ = Chain(config)


class YTask(Task):

    class Meta:
        input_tasks = ['a']

    def run(self) -> bool:
        return False


def test_missing_input_name_task(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.b.*',
            'tests.test_chain.YTask',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data)
    with pytest.raises(ValueError):
        _ = Chain(config)


def test_input_name(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.b.*',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)

    assert len(chain.tasks['c'].input_tasks) == 0
    assert len(chain.tasks['d'].input_tasks) == 1
    assert len(chain.tasks['e'].input_tasks) == 1
    assert chain.tasks['d'].input_tasks['c'] == chain.tasks['c']
    assert chain.tasks['e'].input_tasks['c'] == chain.tasks['c']
    assert chain.tasks['e'].input_tasks[0] == chain.tasks['c']


def test_dependency_graph(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.b.*',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)

    assert len(chain.graph.nodes) == 3
    assert len(chain.graph.edges) == 2


def test_dependency(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.c.*',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)

    assert not chain.is_task_dependent_on('o', 'p')
    assert chain.is_task_dependent_on('p', 'o')
    assert chain.is_task_dependent_on('p', 'm')
    assert chain.is_task_dependent_on('p', 'n')

    assert not chain.is_task_dependent_on('o', 'x')
    assert not chain.is_task_dependent_on('x', 'o')

    assert chain.is_task_dependent_on(chain.tasks['p'], chain.tasks['o'])

    assert len(chain.dependent_tasks('x')) == 0
    assert len(chain.dependent_tasks('x', True)) == 1
    assert len(chain.dependent_tasks('m')) == 2
    assert len(chain.dependent_tasks('o')) == 1
    assert len(chain.dependent_tasks('p')) == 0

    assert len(chain.required_tasks('x')) == 0
    assert len(chain.required_tasks('x', True)) == 1
    assert len(chain.required_tasks('m')) == 0
    assert len(chain.required_tasks('o')) == 2
    assert len(chain.required_tasks('p')) == 3


def test_forcing(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.c.*',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)

    assert not chain.tasks['x'].is_forced
    chain.force('x')
    assert chain.tasks['x'].is_forced

    assert not chain.tasks['p'].is_forced
    chain.force('m')
    assert chain.tasks['m'].is_forced
    assert chain.tasks['o'].is_forced
    assert chain.tasks['p'].is_forced
    assert not chain.tasks['n'].is_forced


def test_multi_chain(tmp_path):
    from tests.tasks.c import PTask

    class ZTask(Task):

        class Meta:
            input_tasks = [PTask]

        def run(self) -> bool:
            return False

    config_data = {
        'tasks': [
            'tests.tasks.c.*',
        ]
    }
    common_config = Config(tmp_path, name='common_config', data=config_data)
    config1 = Config(tmp_path, name='config1', data={'tasks': [ZTask], 'uses': [common_config]})
    config2 = Config(tmp_path, name='config2', data={'tasks': [ZTask], 'uses': [common_config]})

    mc = MultiChain([config1, config2])
    assert len(mc.chains) == 2
    assert len(mc._tasks) == 7
    assert ('z', 'config1') in mc._tasks
    assert ('x', 'common_config') in mc._tasks
    assert ('p', 'common_config') in mc._tasks

    assert len(mc['config1'].tasks) == 6
    assert len(mc['config2'].tasks) == 6

    assert mc['config1'].tasks['p'] == mc['config2'].tasks['p']
    assert mc['config1'].tasks['z'] != mc['config2'].tasks['z']

    mc.force(['p'])
    assert mc['config1'].tasks['z'].is_forced
    assert mc['config2'].tasks['z'].is_forced
    assert mc['config1'].tasks['p'].is_forced
    assert mc['config2'].tasks['p'].is_forced
    assert not mc['config2'].tasks['o'].is_forced


class MyObject(ChainObject):

    def __init__(self):
        self.x = None

    def init_chain(self, chain):
        self.x = chain._base_config.x


def test_chain_objects(tmp_path):

    config_data = {
        'tasks': [],
        'x': 1,
        'my_object': {'class': 'tests.test_chain.MyObject'}
    }

    config = Config(tmp_path, name='config', data=config_data)
    _ = Chain(config)

    assert config['my_object'].x == 1


def test_task_short_names(tmp_path):
    class ABTask(Task):

        class Meta:
            task_group = 'a'
            name = 'b'

        def run(self) -> bool:
            return False

    class BBTask(Task):

        class Meta:
            task_group = 'b'
            name = 'b'

        def run(self) -> bool:
            return False

    config_data = {
        'tasks': [ABTask, BBTask]
    }
    chain = Config(tmp_path, name='config', data=config_data).chain()

    with pytest.raises(KeyError):
        _ = chain['c']

    with pytest.raises(KeyError):
        _ = chain['b']

    _ = chain['a:b']
    _ = chain['b:b']


def test_multiple_chain_instances(tmp_path):
    config_data = {
        'tasks': ['tests.tasks.a.*'],
        'a_number': 1,
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = config.chain()

    assert len(chain.tasks) == 2

    config2 = Config(tmp_path, name='config', data=config_data)
    chain2 = config2.chain()

    assert len(chain2.tasks) == 2
    assert id(chain) != id(chain2)


def test_task_inheritance(tmp_path):
    config_data = {
        'tasks': ['tests.tasks.inheritance.*'],
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = config.chain()

    assert len(chain.tasks) == 3
    assert chain.a.value == 'a'
    assert chain.b.value == 'b'
    assert chain.c.value == 'c'


def test_task_dependencies_in_namespace(tmp_path):
    config_data = {
        'tasks': [
            'tests.tasks.b.*',
        ]
    }
    config = Config(tmp_path, name='config', data=config_data, namespace='x')
    chain = Chain(config)

    assert len(chain.graph.nodes) == 3
    assert len(chain.graph.edges) == 2


def test_namespace_in_uses(tmp_path):
    json.dump(
        {'tasks': ['tests.tasks.a.A'], 'x': 1},
        (tmp_path / 'config1.json').open('w')
    )
    json.dump(
        {'uses': [f'{tmp_path}/config1.json as ns'], 'y': 2},
        (tmp_path / 'config2.json').open('w')
    )

    config = Config(tmp_path, str(tmp_path / 'config2.json'))
    chain = config.chain()
    assert chain['a'] == chain['ns::a']
    inner_config = chain['a'].config
    assert inner_config['x'] == 1
    assert inner_config.fullname == 'ns::config1'


def test_namespace_task_addressing(tmp_path):
    class A(Task):
        class Meta:
            task_group = 'x'
            input_parameters = ['value']

        def run(self) -> int:
            return self.config['value']

    config_a1 = Config(tmp_path, name='config_a1', namespace='nsa1', data={'tasks': [A], 'value': 1})
    config_a2 = Config(tmp_path, name='config_a2', namespace='nsa2', data={'tasks': [A], 'value': 2})

    config = Config(tmp_path, name='config_a1', data={'uses': [config_a1, config_a2]})

    chain = config.chain()
    assert len(chain.tasks) == 2
    assert chain['nsa1::a'].value == 1
    assert chain['nsa1::x:a'].value == 1
    assert chain['nsa2::a'].value == 2
    assert chain['nsa2::x:a'].value == 2
    with pytest.raises(KeyError):
        _ = chain['a']


def test_namespace_example(tmp_path):
    class Dataset(Task):
        class Meta:
            input_parameters = ['size']

        def run(self) -> List:
            return list(range(self.config['size']))

    class Train(Task):
        class Meta:
            input_tasks = ['train::dataset', 'valid::dataset']

        def run(self) -> List:
            return [sum(self.input_tasks['train::dataset'].value), sum(self.input_tasks['valid::dataset'].value)]

    train_config = Config(tmp_path, name='train_ds', namespace='train', data={'tasks': [Dataset], 'size': 100})
    valid_config = Config(tmp_path, name='valid_ds', namespace='valid', data={'tasks': [Dataset], 'size': 10})
    test_config = Config(tmp_path, name='test_ds', namespace='test', data={'tasks': [Dataset], 'size': 20})

    config = Config(tmp_path, name='config', data={'tasks': [Train], 'uses': [train_config, valid_config, test_config]})

    chain = config.chain()
    assert len(chain.tasks) == 4
    assert len(chain['train::dataset'].value) == 100
    assert len(chain['test::dataset'].value) == 20
    assert len(chain['valid::dataset'].value) == 10

    assert chain['train'].value == [99 * 50, 9 * 5]


def test_same_tasks_with_multiple_inputs(tmp_path):
    class A(Task):
        class Meta:
            input_parameters = ['value']

        def run(self) -> int:
            return self.config['value']

    class B(Task):
        class Meta:
            input_tasks = [A]

        def run(self) -> int:
            return self.input_tasks['a'].value

    class C(Task):
        class Meta:
            input_tasks = ['nsb::a', 'a2::a', 'nsb::b']

        def run(self) -> List:
            return [self.input_tasks['a2::a'].value, self.input_tasks['nsb::a'].value, self.input_tasks['b'].value]

    for namespace in [False, True]:
        config_a1 = Config(tmp_path, name='config_a1', data={'tasks': [A], 'value': 1})
        config_a2 = Config(tmp_path, name='config_a2', data={'tasks': [A], 'value': 2}, namespace='a2' if namespace else None)

        config_b = Config(tmp_path, name='config_b', data={'tasks': [B], 'uses': [config_a1]}, namespace='nsb' if namespace else None)
        config_c = Config(tmp_path, name='config_c', data={'tasks': [C], 'uses': [config_a2, config_b]})

        if namespace:
            chain = config_c.chain()
            assert chain.c.value == [2, 1, 1]
        else:
            with pytest.raises(ValueError):
                _ = config_c.chain()


def test_namespace_composition(tmp_path):
    json.dump(
        {'tasks': ['tests.tasks.a.A']},
        (tmp_path / 'config1.json').open('w')
    )
    json.dump(
        {'uses': [f'{tmp_path}/config1.json as ns1']},
        (tmp_path / 'config2.json').open('w')
    )
    json.dump(
        {'uses': [f'{tmp_path}/config2.json as ns2']},
        (tmp_path / 'config.json').open('w')
    )

    config = Config(tmp_path, str(tmp_path / 'config.json'))
    chain = config.chain()
    assert len(chain.tasks) == 1
    assert chain.a.config.namespace == 'ns2::ns1'
    assert chain['a'].value is False
    assert chain['ns2::ns1::a'].value is False


def test_namespaces_and_multiple_tasks(tmp_path):
    class XA(Task): 
        def run(self) -> int:
            return self.config['value']

    class XB(Task):
        class Meta:
            task_group = 'g'
            input_tasks = [XA]

        def run(self) -> int:
            return self.input_tasks['x_a'].value

    class Y(Task):
        class Meta:
            input_tasks = [XB]

        def run(self) -> int:
            return self.input_tasks['x_b'].value

    class Z(Task):
        class Meta:
            input_tasks = ['ns::g:x_b', Y]

        def run(self) -> List:
            return [self.input_tasks['ns::g:x_b'].value, self.input_tasks['y'].value]

    config_x1 = Config(tmp_path, name='config_x1', data={'tasks': [XA, XB], 'value': 1})
    config_y = Config(tmp_path, name='config_y', data={'tasks': [Y], 'uses': [config_x1]})

    config_x2 = Config(tmp_path, name='config_x2', data={'tasks': [XA, XB], 'value': 2}, namespace='ns')
    config_z1 = Config(tmp_path, name='config_z1', data={'tasks': [Z], 'uses': [config_x2, config_y]})
    config_z2 = Config(tmp_path, name='config_z2', data={'tasks': [Z], 'uses': [config_y, config_x2]})

    for config in [config_z1, config_z2]:
        chain = config.chain()
        assert chain.z.value == [2, 1]


def test_chained_namespaces(tmp_path):
    class XA(Task):
        def run(self) -> int:
            return self.config['value']

    class XB(Task):
        class Meta:
            task_group = 'g'
            input_tasks = [XA]

        def run(self) -> int:
            return self.input_tasks['x_a'].value

    class Y(Task):
        class Meta:
            input_tasks = ['x::g:x_b']

        def run(self) -> int:
            return self.input_tasks['x_b'].value

    class Z(Task):
        class Meta:
            input_tasks = ['y::y']

        def run(self) -> int:
            return self.input_tasks['y'].value

    config_x = Config(tmp_path, name='config_x', data={'tasks': [XA, XB], 'value': 1}, namespace='x')
    config_y = Config(tmp_path, name='config_y', data={'tasks': [Y], 'uses': [config_x]}, namespace='y')
    config_z = Config(tmp_path, name='config_z', data={'tasks': [Z], 'uses': [config_y]})

    chain = config_z.chain()
    assert chain.z.value == 1
    assert chain['x_a'].value == 1
    assert chain['y::x::x_a'].value == 1
    assert chain['y::x::x_b'].value == 1
    assert chain['y::x::g:x_b'].value == 1
    assert chain['x_b'].value == 1
    assert chain['g:x_b'].value == 1

