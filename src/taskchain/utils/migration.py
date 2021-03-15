from shutil import copyfile, copytree

from taskchain.task import Config, InMemoryData


def migrate_to_parameter_mode(config: Config, target_dir, dry: bool = True):
    assert config.base_dir != target_dir, 'target_dir has to be different from configs base_dir'
    old_chain = config.chain(parameter_mode=False)
    new_chain = Config(target_dir, config._filepath, global_vars=config.global_vars, context=config.context).chain()
    print(f'Set dry=False to make copies')
    for name, old_task in old_chain.tasks.items():
        print()
        new_task = new_chain[name]
        print(name)

        if issubclass(old_task.data_class, InMemoryData):
            print('   not persisting')
            continue

        if not old_task.has_data:
            print('   no data found')
            continue

        print(f'    original: `{old_task.data_path}`')
        print(f'      target: `{new_task.data_path}`')

        if new_task.has_data:
            assert new_task.data_path.stat().st_size == old_task.data_path.stat().st_size
            print(f'    target already exists')
            continue

        if dry:
            print('    to copy')
        else:
            print('    copying')
            if old_task.data_path.is_file():
                copyfile(old_task.data_path, new_task.data_path)
            else:
                copytree(old_task.data_path, new_task.data_path)
            print('    copied')
