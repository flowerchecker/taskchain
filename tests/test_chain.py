import json
import logging
from typing import List

import pytest

from taskchain.task import Config, Chain, Task, MultiChain, InMemoryData
from taskchain.task.chain import ChainObject
from taskchain.task.parameter import Parameter, ParameterObject
from tests.tasks.a import ATask


def test_config(tmp_path):
    config_data = {
        'uses': []
    }
    config = Config(tmp_path, name='config', data=config_data)
    chain = Chain(config)
    assert len(chain._configs) == 1


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
    assert len(chain._configs) == 3
    assert chain._configs['config1'].a == 1
    assert chain._configs['config2'].a == 2
    assert chain._configs['config3'].a == 3


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
    assert chain.tasks['b'].params.a_number == 7
    assert chain.tasks['b'].parameters.a_number == 7


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


class MyObject(ChainObject, ParameterObject):

    def __init__(self):
        self.x = None

    def init_chain(self, chain):
        self.x = chain._base_config.x

    def repr(self):
        return ''


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
    inner_config = chain['a'].get_config()
    assert inner_config['x'] == 1
    assert inner_config.namespace == 'ns'


def test_namespace_task_addressing(tmp_path):
    class A(Task):
        class Meta:
            task_group = 'x'
            parameters = [Parameter('value')]

        def run(self) -> int:
            return self.params['value']

    config_a1 = Config(tmp_path, name='config_a1', namespace='nsa1', data={'tasks': [A], 'value': 1})
    config_a2 = Config(tmp_path, name='config_a2', namespace='nsa2', data={'tasks': [A], 'value': 2})

    config = Config(tmp_path, name='config', data={'uses': [config_a1, config_a2]})

    chain = config.chain()
    assert len(chain.tasks) == 2
    print(chain['nsa1::a'].get_config().value)
    assert chain['nsa1::a'].value == 1
    assert chain['nsa1::x:a'].value == 1
    assert chain['nsa2::a'].value == 2
    assert chain['nsa2::x:a'].value == 2
    with pytest.raises(KeyError):
        _ = chain['a']


def test_namespace_example(tmp_path):
    class Dataset(Task):
        class Meta:
            parameters = [Parameter('size')]

        def run(self) -> List:
            return list(range(self.params['size']))

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
            parameters = [Parameter('value')]

        def run(self) -> int:
            return self.params['value']

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
    assert chain.a.get_config().namespace == 'ns2::ns1'
    assert chain['a'].value is False
    assert chain['ns2::ns1::a'].value is False


def test_namespaces_and_multiple_tasks(tmp_path):
    class XA(Task):
        class Meta:
            parameters = [Parameter('value')]

        def run(self) -> int:
            return self.params['value']

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
        class Meta:
            parameters = [Parameter('value')]

        def run(self) -> int:
            return self.params['value']

    class XB(Task):
        class Meta:
            task_group = 'g'
            input_tasks = [XA]

        def run(self) -> int:
            return self.input_tasks['x_a'].value

    class Y(Task):
        class Meta:
            input_tasks = ['x::x_b']

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


@pytest.mark.parametrize('parameter_mode', [True, False])
def test_parameter_mode(tmp_path, parameter_mode):
    class A(Task):
        class Meta:
            parameters = [
                Parameter('a'),
                Parameter('x', default=0, ignore_persistence=True),
            ]

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> int:
            self.run_called += 1
            return self.params['a']

    class B(Task):
        class Meta:
            input_tasks = [A]
            parameters = [Parameter('b')]

        def run(self) -> int:
            return self.input_tasks['a'].value * self.parameters['b']

    config1 = Config(tmp_path, name='config1', data={'tasks': [A, B], 'a': 2, 'b': 2, 'x': 1})
    config2 = Config(tmp_path, name='config2', data={'tasks': [A, B], 'a': 2, 'b': 3, 'x': 2})
    config3 = Config(tmp_path, name='config3', data={'tasks': [A, B], 'a': 3, 'b': 3})

    chain1 = config1.chain(parameter_mode=parameter_mode)
    assert chain1.b.value == 4
    assert chain1.a.run_called == 1
    assert len(list((tmp_path / 'a').glob('*.json'))) == 1

    chain2 = config2.chain(parameter_mode=parameter_mode)
    assert chain2.b.value == 6
    assert chain1.a.run_called == 1
    assert len(list((tmp_path / 'a').glob('*.json'))) == 1 if parameter_mode else 2

    if parameter_mode:
        assert chain1.a.get_config().get_name_for_persistence(chain1.a) == chain2.a.get_config().get_name_for_persistence(chain2.a)
    else:
        assert chain1.a.get_config().get_name_for_persistence(chain1.a) != chain2.a.get_config().get_name_for_persistence(chain2.a)

    chain3 = config3.chain(parameter_mode=parameter_mode)
    assert chain3.b.value == 9
    assert chain1.a.run_called == 1
    assert len(list((tmp_path / 'a').glob('*.json'))) == 2 if parameter_mode else 3

    assert chain3.a.get_config().get_name_for_persistence(chain3.a) != chain2.a.get_config().get_name_for_persistence(chain2.a)

    chain = Config(tmp_path, name='config2', data={'tasks': [A, B], 'a': 2, 'b': 3}).chain(parameter_mode=parameter_mode)
    assert chain.a.run_called == 0

    for x in (tmp_path / 'a').glob('*'):
        print(x)


def test_context(tmp_path):
    config_data = {
        'uses': [
            Config(tmp_path, name='config1', data={'a': 2}),
        ],
        'b': 1,
    }
    chain = Config(tmp_path, name='config2', data=config_data).chain()
    assert chain._configs['config1'].a == 2
    assert chain._configs['config2'].b == 1

    chain = Config(tmp_path, name='config2', data=config_data, context={'a': 3, 'b': 2}).chain()
    assert chain._configs['config2'].b == 2
    assert chain._configs['config1'].a == 3


def test_context_with_files(tmp_path):
    json.dump({'a': 2}, (tmp_path / 'config1.json').open('w'))
    json.dump({'b': 1, 'uses': [str(tmp_path / 'config1.json')]}, (tmp_path / 'config2.json').open('w'))
    json.dump({'a': 3, 'b': 2}, (tmp_path / 'context.json').open('w'))

    chain = Config(tmp_path, tmp_path / 'config2.json').chain()
    assert chain._configs[str(tmp_path / 'config1.json')].a == 2
    assert chain._configs[str(tmp_path / 'config2.json')].b == 1

    chain = Config(tmp_path, tmp_path / 'config2.json', context=tmp_path / 'context.json').chain()
    assert chain._configs[str(tmp_path / 'config2.json')].b == 2
    assert chain._configs[str(tmp_path / 'config1.json')].a == 3


def test_multi_configs_uses(tmp_path):
    json.dump(
        {'configs': {
            'c1': {'tasks': ['tests.tasks.a.A'], 'x': 1},
            'c2': {'tasks': ['tests.tasks.a.A'], 'x': 2},
            'c': {'main_part': True, 'uses': ['#c1 as ns', '#c2 as ns2'], 'y': 2},
        }},
        (tmp_path / 'config.json').open('w')
    )

    c = Config(filepath=tmp_path / 'config.json')
    chain = c.chain()
    chain['ns::a'].params.x = 1
    chain['ns2::a'].params.x = 2
    assert len(chain._configs) == 3


class Abc(Task):
    class Meta:
        parameters = [
            Parameter('x'),
            Parameter('y'),
        ]

    def run(self) -> int:
        return 1000 * self.params.x + self.params.y


def test_context_for_namespaces(tmp_path):

    json.dump(
        {'configs': {
            'c1': {'tasks': ['tests.test_chain.Abc'], 'x': 1, 'y': 1},
            'c2': {'tasks': ['tests.test_chain.Abc'], 'x': 2, 'y': 2},
            'c': {'main_part': True, 'uses': ['#c1 as ns', '#c2 as ns2'], 'z': 2},
        }},
        (tmp_path / 'config.json').open('w')
    )

    config = Config(tmp_path, filepath=tmp_path / 'config.json', context={
        'for_namespaces': {
            'ns': {'x': 11},
            'ns2': {'x': 21},
            'nsX': {'x': 77},
        },
        'x': 666,
        'y': 33,
    })

    chain = config.chain()

    assert chain['ns::abc'].value == 11033
    assert chain['ns2::abc'].value == 21033


class Def(Task):
    class Meta:
        input_tasks = ['ns::abc', 'ns2::abc']

    def run(self) -> int:
        return self.input_tasks['ns::abc'].value + self.input_tasks['ns2::abc'].value


def test_namespaces_use_in_persistence(tmp_path):
    json.dump(
        {'configs': {
            'c1': {'tasks': ['tests.test_chain.Abc'], 'x': 1, 'y': 1},
            'c2': {'tasks': ['tests.test_chain.Abc'], 'x': 2, 'y': 2},
            'c': {'main_part': True, 'uses': ['#c1 as ns', '#c2 as ns2'], 'tasks': ['tests.test_chain.Def']},
        }},
        (tmp_path / 'config.json').open('w')
    )

    chain = Config(tmp_path, filepath=tmp_path / 'config.json').chain()
    assert chain['def'].value == 3003

    json.dump(
        {'configs': {
            'c1': {'tasks': ['tests.test_chain.Abc'], 'x': 1, 'y': 1},
            'c2': {'tasks': ['tests.test_chain.Abc'], 'x': 2, 'y': 2},
            'c': {'uses': ['#c1 as ns', '#c2 as ns2'], 'tasks': ['tests.test_chain.Def']},
            'd': {'main_part': True, 'uses': ['#c as nsc']},
        }},
        (tmp_path / 'config2.json').open('w')
    )
    chain = Config(tmp_path, filepath=tmp_path / 'config2.json').chain()
    assert chain['def'].has_data
    assert chain['def'].value == 3003


def test_logging(tmp_path, caplog):
    class T(Task):

        class Meta:
            parameters = [Parameter('debugs', default=1, ignore_persistence=True)]

        def run(self) -> int:
            for _ in range(self.params.debugs):
                self.logger.debug('debug')
            self.logger.info('info')
            self.logger.warning('warning')
            self.logger.error('error')

            return 0

    chain = Config(tmp_path, name='config', data={'tasks': [T]}).chain()
    assert chain.t.value == 0
    assert len(chain.t.log) == 4 + 2
    assert len(caplog.record_tuples) == 6

    chain = Config(tmp_path, name='config', data={'tasks': [T], 'debugs': 3}).chain()
    chain.set_log_level(logging.DEBUG)
    assert chain.t.value == 0
    assert len(chain.t.log) == 4 + 2
    assert len(caplog.record_tuples) == 6

    chain = Config(tmp_path, name='config', data={'tasks': [T], 'debugs': 3}).chain()
    chain.set_log_level(logging.ERROR)
    assert chain.t.force().value == 0
    assert len(chain.t.log) == 6 + 2
    assert len(caplog.record_tuples) == 6 + 8

    chain.set_log_level(logging.DEBUG)
    assert chain.t.force().value == 0
    assert len(chain.t.log) == 6 + 2
    assert len(caplog.record_tuples) == 6 + 8 + 8

    class R(Task):

        class Meta:
            parameters = [Parameter('debugs', default=1, ignore_persistence=True)]
            data_class = InMemoryData

        def run(self) -> int:
            for _ in range(self.params.debugs):
                self.logger.debug('debug')
            self.logger.info('info')
            self.logger.warning('warning')
            self.logger.error('error')

            return 0

    chain = Config(tmp_path, name='config', data={'tasks': [R]}).chain()
    assert chain.r.value == 0
    assert len(chain.r.log) == 4 + 2
    chain = Config(tmp_path, name='config', data={'tasks': [R]}).chain()
    assert chain.r.value == 0
    assert len(chain.r.log) == 4 + 2


def test_one_task_over_multiple_namespaces(tmp_path):
    json.dump(
        {'configs': {
            'a': {'tasks': ['tests.tasks.a.A']},
            'c1': {'tasks': [], 'uses': ['#a as a']},
            'c2': {'tasks': [], 'uses': ['#a as a']},
            'c': {'main_part': True, 'uses': ['#c1 as c1', '#c2 as c2']},
        }},
        (tmp_path / 'config.json').open('w')
    )

    chain = Config(filepath=tmp_path / 'config.json').chain()

    assert len(chain.tasks) == 2
    assert chain['c1::a::a'] == chain['c2::a::a']


def test_configs_with_same_names(tmp_path):
    json.dump(
        {'tasks': ['tests.test_chain.Abc'], 'x':1, 'y': 1},
        (tmp_path / 'config.json').open('w')
    )

    (tmp_path / 'a').mkdir()
    json.dump(
        {'tasks': ['tests.test_chain.Abc'], 'x': 2, 'y': 2},
        (tmp_path / 'a' / 'config.json').open('w')
    )


    json.dump(
        {'uses': [
            str(tmp_path / 'config.json') + ' as s',
            str(tmp_path / 'a' / 'config.json') + ' as a'
        ]},
        (tmp_path/ 'main.json').open('w')
    )

    chain = Config(tmp_path, tmp_path/ 'main.json').chain()
    assert len(chain.tasks) == 2
    assert chain['s::abc'] != chain['a::abc']
    assert chain['s::abc'].value == 1001
    assert chain['a::abc'].value == 2002


def test_create_readable_filenames(tmp_path):
    config_data = {
        'uses': [
            Config(tmp_path, name='config', data={'tasks': ['tests.tasks.a.A']}),
        ],
        'tasks': ['tests.tasks.b.*'],
    }
    config2 = Config(tmp_path, name='config2', data=config_data)
    chain = Chain(config2)

    for name, task in chain.tasks.items():
        if name == 'd':
            continue
        _ = task.value

    chain.create_readable_filenames('named', '', dry=True)
    assert not (tmp_path / 'c' / 'named.json').exists()

    chain.create_readable_filenames('named', '')
    assert (tmp_path / 'c' / 'named.json').exists()
    assert not (tmp_path / 'd' / 'named.json').exists()

    chain.create_readable_filenames('named', None)


def test_create_readable_filenames_base_od_config(tmp_path):
    config_data = {
        'save_as': 'named',
        'uses': [
            Config(tmp_path, name='config', data={'tasks': ['tests.tasks.a.A']}),
        ],
        'tasks': ['tests.tasks.b.*'],
    }
    config2 = Config(tmp_path, name='config2', data=config_data)
    chain = Chain(config2)

    for name, task in chain.tasks.items():
        if name == 'd':
            continue
        _ = task.value

    chain.create_readable_filenames()
    assert (tmp_path / 'c' / 'named.json').exists()
    assert not (tmp_path / 'd' / 'named.json').exists()
