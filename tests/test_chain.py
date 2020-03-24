import pytest

from taskchain.task import Config, Chain


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

    assert len(chain2.tasks) == 3


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
