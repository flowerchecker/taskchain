import pytest

from taskchain.task import Config, Chain, Task
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
