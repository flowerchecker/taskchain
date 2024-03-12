import json

import pytest
import yaml

from copy import deepcopy
from taskchain import Config
from taskchain import Context
from taskchain.parameter import ParameterObject


def test_name(tmp_path):
    data = {}
    json.dump(data, (tmp_path / 'json_test.json').open('w'))
    yaml.dump(data, (tmp_path / 'yaml.test.yaml').open('w'))

    with pytest.raises(ValueError):
        _ = Config(tmp_path).name

    c = Config(tmp_path, name='test')
    assert c.name == 'test'

    c = Config(tmp_path, name='test2')
    assert c.name == 'test2'

    c = Config(tmp_path, tmp_path / 'json_test.json')
    assert c.name == 'json_test'

    c = Config(tmp_path, tmp_path / 'yaml.test.yaml')
    assert c.name == 'yaml.test'

    c = Config(tmp_path, tmp_path / 'yaml.test.yaml', name='name_test')
    assert c.name == 'name_test'


def test_data(tmp_path):
    with pytest.raises(ValueError):
        _ = Config(tmp_path, name='test').data

    data = """{"a": 1, "b": 2, "c": {"d": 3}}"""
    with (tmp_path / 'json_test.json').open('w') as f:
        f.write(data)

    data = """
    a: 1
    b: 2
    c: 
        d: 3
    """
    with (tmp_path / 'yaml.test.yaml').open('w') as f:
        f.write(data)

    json_c = Config(tmp_path, tmp_path / 'json_test.json')
    yaml_c = Config(tmp_path, tmp_path / 'yaml.test.yaml')
    data_c = Config(tmp_path, data={"a": 1, "b": 2, "c": {"d": 3}}, name='test')

    for c in [json_c, yaml_c, data_c]:
        assert c.data['a'] == 1
        assert c.data['b'] == 2
        assert c.data['c']['d'] == 3

        assert c['a'] == 1
        assert c['b'] == 2
        assert c['c']['d'] == 3

        assert c.a == 1
        assert c.b == 2
        assert c.c['d'] == 3

        assert c.get('a') == 1
        assert c.get('x', 9) == 9

        with pytest.raises(KeyError):
            _ = c['x']

        with pytest.raises(KeyError):
            _ = c.data['x']

        with pytest.raises(AttributeError):
            _ = c.x


class MyObject(ParameterObject):
    def __init__(self, a, b=1):
        self.a = a
        self.b = b

    def repr(self):
        return ''


class MyObject2(ParameterObject):
    def __init__(self, c, o=None):
        self.c = c
        self.o = o

    def repr(self):
        return ''


def test_config_objects(tmp_path):

    data = {
        'my_object': {
            'class': 'tests.test_config.MyObject',
            'args': [7],
            'kwargs': {'b': 13},
        }
    }

    config = Config(tmp_path, data=data, name='config')
    assert 'my_object' in config
    assert type(config['my_object']) == MyObject
    assert config['my_object'].a == 7
    assert config['my_object'].b == 13


def test_config_complex_objects(tmp_path):

    data = {
        'my_object': {
            'class': 'tests.test_config.MyObject2',
            'args': [5],
            'kwargs': {'o': {
                'class': 'tests.test_config.MyObject',
                'args': [7],
                'kwargs': {'b': 13},
            }},
        }
    }

    config = Config(tmp_path, data=data, name='config')
    assert 'my_object' in config
    assert type(config['my_object']) == MyObject2
    assert config['my_object'].c == 5

    assert type(config['my_object'].o) == MyObject
    inner_object = config['my_object'].o
    assert inner_object.a == 7
    assert inner_object.b == 13


def test_namespace(tmp_path):
    c = Config(tmp_path, name='test', namespace='ns')
    assert c.name == 'test'
    assert c.fullname == 'ns::test'


def test_context(tmp_path):
    json.dump({'b': 7}, (tmp_path / 'file_context.json').open('w'))

    config = Config(data={'a': 1, 'rw': 666}, context=[
        str(tmp_path / 'file_context.json'),
        tmp_path / 'file_context.json',
        {'c': 4, 'rw': 0},
        {'c': 6, 'rw': 1},
        Context(data={'d': 8}, name='my_context')
    ])

    assert len(config.context.name.split(';')) == 5
    assert config.a == 1
    assert config.b == 7
    assert config.c == 6
    assert config.rw == 1
    assert config.d == 8


def test_multi_configs():
    data = {'configs': {
        'config1': {
            'a': 1,
        },
        'config2': {
            'a': 2,
        },
        'config3': {
            'a': 3,
            'b': 2,
        },
    }}

    assert Config(data=data, part='config1', name='c').a == 1
    assert Config(data=data, part='config1', name='c').name == 'c#config1'
    assert Config(data=data, part='config2', name='c').a == 2
    assert Config(data=data, part='config3', name='c').a == 3
    with pytest.raises(AttributeError):
        assert Config(data=data, part='config1').b

    with pytest.raises(KeyError):
        assert Config(data=data, part='config', name='c').a

    with pytest.raises(KeyError):
        assert Config(data=data, name='c').a

    data = {'configs': {
        'config1': {
            'a': 1,
        },
        'config2': {
            'main_part': True,
            'a': 2,
        },
    }}
    assert Config(data=data, name='c').a == 2
    assert Config(data=data, name='c').name == 'c#config2'


def test_multi_configs_file(tmp_path):
    json.dump({'configs': {
        'config1': {
            'a': 1,
        },
        'config2': {
            'main_part': True,
            'a': 2,
        },
    }}, (tmp_path / 'file_context.json').open('w'))

    print(f'{tmp_path / "file_context.json"}#config1')
    c1 = Config(filepath=f'{tmp_path / "file_context.json"}#config1')
    assert c1.a == 1
    assert c1.name == 'file_context#config1'

    c1 = Config(filepath=f'{tmp_path / "file_context.json"}')
    assert c1.a == 2
    assert c1.name == 'file_context#config2'


def test_context_for_namespaces_merging():

    context = Context.prepare_context([
        {
            'for_namespaces': {
                'ns1': {
                    'a': 11,
                },
                'ns2': {
                    'a': 21,
                },
            },
            'a': 1,
        },
        {
            'for_namespaces': {
                'ns1': {
                    'a': 22,
                },
                'ns3': {
                    'a': 32,
                },
            },
            'a': 2,
        },
    ])

    assert context.for_namespaces['ns1']['a'] == 22
    assert context.for_namespaces['ns2']['a'] == 21
    assert context.for_namespaces['ns3']['a'] == 32
    assert context.a == 2


def test_deepcopy(tmp_path):
    data = {
        'my_object': {
            'class': 'tests.test_config.MyObject',
            'args': [7],
            'kwargs': {'b': 13},
        }
    }
    config = Config(tmp_path, data=data, name='config')
    assert deepcopy(config) == config
