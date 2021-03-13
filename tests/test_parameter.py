import pytest

from taskchain.task import Config
from taskchain.task.parameter import Parameter, ParameterRegistry, ParameterObject


def test_value():
    config = Config(name='config', data={
        'value0': None,
        'value1': 1,
        'value2': 'abc',
    })

    p = Parameter('value0')
    p.set_value(config)
    assert p.value is None

    p = Parameter('value1', default=2)
    p.set_value(config)
    assert p.value == 1

    p = Parameter('value2')
    p.set_value(config)
    assert p.value == 'abc'
    assert p.required

    p = Parameter('value3')
    with pytest.raises(ValueError):
        p.set_value(config)

    p = Parameter('value4', default=123)
    p.set_value(config)
    assert p.value == 123
    assert not p.required

    p = Parameter('value5', default=None)
    p.set_value(config)
    assert p.value is None


def test_type():
    config = Config(name='config', data={
        'value0': None,
        'value1': 1,
        'value2': 'abc',
        'value3': [1, 2],
        'value4': {},
    })

    p = Parameter('value0', dtype=str)
    p.set_value(config)

    p = Parameter('value1', dtype=int)
    p.set_value(config)

    p = Parameter('value2', dtype=str)
    p.set_value(config)

    p = Parameter('value3', dtype=list)
    p.set_value(config)

    p = Parameter('value4', dtype=dict)
    p.set_value(config)

    p = Parameter('value1', dtype=str)
    with pytest.raises(ValueError):
        p.set_value(config)


def test_hash():
    config = Config(name='config', data={
        'value0': None,
        'value1': 1,
        'value2': 'abc',
    })

    p = Parameter('value1', ignore_persistence=True)
    p.set_value(config)
    assert p.hash is None

    p = Parameter('value1')
    p.set_value(config)
    assert p.hash is not None

    p = Parameter('value1', default=1, dont_persist_default_value=True)
    p.set_value(config)
    assert p.hash is None

    p = Parameter('value1', default=2, dont_persist_default_value=True)
    p.set_value(config)
    assert p.hash is not None


def test_registry():
    config = Config(name='config', data={
        'value0': None,
        'value1': 1,
        'value2': 'abc',
    })

    with pytest.raises(ValueError):
        ParameterRegistry([Parameter('v1'), Parameter('v1')])

    ps = [
        Parameter('value1'),
        Parameter('value2')
    ]

    registry = ParameterRegistry(ps)
    registry.set_values(config)
    assert registry['value1'] == 1
    assert registry.value1 == 1
    assert registry['value2'] == registry.value2 == 'abc'

    registry2 = ParameterRegistry([
        Parameter('value2'), Parameter('value1'),
        Parameter('value0', ignore_persistence=True)
    ])
    registry2.set_values(config)
    assert registry.hash == registry2.hash


class Obj(ParameterObject):

    def __init__(self, arg):
        self.arg = arg

    def hash(self) -> str:
        return self.arg


def test_object_parameter():

    config = Config(name='config', data={
        'obj': {
            'class': 'tests.test_parameter.Obj',
            'args': ['abc']
        }
    })

    p = Parameter('obj')
    p.set_value(config)
    assert p.hash == 'obj=abc'
