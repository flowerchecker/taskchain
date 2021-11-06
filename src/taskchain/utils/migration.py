from math import isclose
from shutil import copyfile, copytree
from taskchain.task import Config, InMemoryData


def migrate_to_parameter_mode(config: Config, target_dir, dry: bool = True, verbose: bool = True):
    """
    Migrate a chain to parameter mode.

    Args:
        config: config defining the chain
        target_dir: dir to migrate data to
        dry: show only info, do not copy data
        verbose:
    """
    assert config.base_dir != target_dir, 'target_dir has to be different from configs base_dir'
    old_chain = {
        t.fullname: t
        for t in config.chain(parameter_mode=False).tasks.values()
    }
    new_chain = {
        t.fullname: t
        for t in Config(target_dir, config._filepath, global_vars=config.global_vars, context=config.context).chain().tasks.values()
    }
    print(f'Set dry=False to make copies')
    for name, old_task in old_chain.items():
        print()
        new_task = new_chain[name]
        print(f'{name}  -  {new_task.name_for_persistence}')
        if verbose:
            print(f'  parameters: `{new_task.params.repr}`')
            print(f' input tasks: `{"###".join(f"{n}={it}" for n, it in sorted(new_task.get_config().input_tasks.items()))}`')

        if issubclass(old_task.data_class, InMemoryData):
            print('   not persisting')
            continue

        if not old_task.has_data:
            print('   no data found')
            continue

        print(f'\n    original: `{old_task.data_path}`')
        print(f'      target: `{new_task.data_path}`')

        if new_task.has_data:
            # HACK: pd files do not have to have the same size with the same data
            if new_task.data_path.name.endswith('.pd'):
                assert isclose(new_task.data_path.stat().st_size, old_task.data_path.stat().st_size, rel_tol=2e-7, abs_tol=10), f'{new_task.data_path.stat().st_size} vs. {old_task.data_path.stat().st_size}'
            else:
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
