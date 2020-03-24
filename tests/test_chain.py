from taskchain.task import Config, Chain


def test_configs(tmp_path):
    config_data = {
        'uses': []
    }
    config = Config(tmp_path, name='test', data=config_data)
    chain = Chain(config)
    assert len(chain.configs) == 1

    config_data = {
        'uses': [
            Config(tmp_path, name='test2', data={'a': 2}),
            Config(tmp_path, name='test3', data={'a': 3}),
        ],
        'a': 1,
    }
    config = Config(tmp_path, name='test1', data=config_data)
    chain = Chain(config)
    assert len(chain.configs) == 3
    assert chain.configs['test1'].a == 1
    assert chain.configs['test2'].a == 2
    assert chain.configs['test3'].a == 3
