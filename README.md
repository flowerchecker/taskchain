# TaskChain

[Documentation](https://flowerchecker.gitlab.io/taskchain/)

## Docs
https://flowerchecker.gitlab.io/taskchain/


## Install

```bash
pip install taskchain
```

#### From source
```bash
git clone https://github.com/thran/taskchain
cd taskchain
python setup.py install
# or
python setup.py develop
```

## Chengelog

#### 1.1.1
- improve chain representation in jupyter
- add `tasks_df` parameter to chains
- add support for `uses` in contexts (same syntax as in configs)
- improve create_readable_filenames
  - use config name as default name
  - better verbose mode
- `force` method of both Chain and Task now supports `delete_data` parameter which delete persisted data 
  - it defaults to `False`
  - be careful with this
- add [Makefile](Makefile)

#### 1.1.0
- release to PIP

#### 1.0.3
- more types can be used for `run` method, e.g. `dict` or `Dict[str, int]`
- forbid some names of parameters with special meaning in configs (`uses`, `tasks`, ...)
- you should import from `taskchain` instead of `taskchain.taks`, later is deprecated and will be removed
  - use `from taskchain import Task, Config, Chain` or `import taskchain as tc; tc.Task`
- MultiChain are now more robust, you can use them with configs with context, and it will work correctly 

## Docs
https://flowerchecker.gitlab.io/taskchain/

#### Develop
```bash
mkdocs serve
```

#### Build
Automatically build by GitLab CI
```bash
mkdocs build --strict --verbose
```
