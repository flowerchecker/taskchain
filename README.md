# TaskChain

[Documentation](https://flowerchecker.github.io/taskchain/)


## Install

```bash
pip install taskchain
```

#### From source
```bash
git clone https://github.com/flowerchecker/taskchain
cd taskchain
poetry install
```

## Changelog

#### unpublished
- support for double ~ ignoring namespace

#### 1.4.0
- fix CI tests
- introduce function to chunk iterable
- use chunks for multithreading parallelism
- make subcaches of in-memory cache more intelligent
- be able get data from cache without computing it
- be able to manipualte with cache values in @cached operator
- use orjson instead of standard json module (except of cache keys)
- allow to define multiple input task using regexp
- use versions in @cached() decorator
- introduce data class to manipulate with generators lazily
- be able to use classes without repr method in definition configs
- fixes

#### 1.3.0

- migrate to poetry


#### 1.2.1
- fixes


#### 1.2.0
- remove redundant module `taskchain.task`
- add support for task exclusion, just use `exluded_tasks` in your config
- add tools for testing, check `taskchain.utils.testing`
- finish documentation
- remove some redundant methods

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

## Development

#### Release new version to PIP

```bash
make version-patch
# OR
make version-minor

make publish
```

#### Develop docs
run server which dynamically serves docs web.
```bash
make docs-develop
```

#### Build docs

Create documentation as static files. 
```bash
make docs-build
```


#### Build docs

Builds documentation and deploys it to GitHub Pages
```bash
make make docs-publish
```
