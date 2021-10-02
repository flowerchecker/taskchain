# Configs

Generally, configs defines all necessary parameter values, 
which are needed to run a pipeline and task in it.

In TaskChain, configs is also entry point for creating chains
and describes how pipelines are connected to chains.
Therefore, a config defines:
- description of task which should be part of a chain (e.g. pipeline)
- parameter values needed by these task
- dependencies on other configs

!!! Note "What is actually config?"
    Config in TaskChain has dual meaning. 
    First, config as YAML file containing information described above.
    Second, config as an instance of `taskchain.task.Config` which usually is based
    on the config file and adds other information necessary to computation. 


## Config definition

!!! Example "Simple example of config"
    === "config.yaml"
    
        ```yaml
        tasks: my_project.tasks.pipeline.*

        string_parameter: value
        int_parameter: 123
        complex_parameter:
            key1: value1
            key2: 
                - v1
                - v2
        ```

    === "code.py"
    
        ```python
        from taskchain import Config
        
        config = Config(
            '/path/to/task_data', 
            '/path/to/config.yaml', 
        )
        ```
    

Config is basically **map from strings to arbitrary values**. 
In addition to YAML files, you can define your config also in JSON file or directly in code
by passing dict like object to `Config()` in parameter `data`.

More examples of configs can be found in [example project]({{ config.code_url }}/example/configs).

You can access values of config object by attribute `config.string_parameter` 
or in dict like fashion `config['string_parameter']`. Actually, config is descendant of `dict`.
However, direct access to the values is rarely needed because parameters are handled by TaskChain.   


### Tasks

Each config should define witch tasks are configured. This is a way how a config defines a pipeline.
The special parameter `tasks` is **string or list of strings** describing tasks.
Task is described by a path to task's class: `'my_project.tasks.pipeline.ProcessTask'`.
This description corresponds exactly to python import system.

To import all tasks from pipeline (defined in single file) at once you can use wildcard `*` in last 
place of description: `'my_project.tasks.pipeline.*'`.


#### Task exclusion

For more flexibility, you can also exclude tasks with special parameter `excluded_tasks`
with same syntax as `tasks` parameter.


### Config dependencies

More complicated chains are split to multiple pipelines with corresponding configs.
Parameter `uses` defines how these pipelines are connected together.

For example project is split to *data preparation pipeline* and *model pipeline*. 
Config of data preparation have no prerequisites, and thus it doesn't need `uses`.
Some tasks of the model pipeline depends on tasks of data pipeline and therefore
model config has to depend on data config.

!!! Example
    === "model_config.yaml"
    
        ```yaml
        tasks: my_project.tasks.model.*
        uses: "/path/to/data_config.yaml"

        model: ...
        ```

    === "data_config.yaml"
    
        ```yaml
        tasks: my_project.tasks.data.*

        source_file: ...
        ```
    

`uses` is **string** or **list of strings** if there are multiple dependency configs. 

### Placeholders & Global vars

Placeholders and global variables is a mechanism which allows TaskChain projects work in multiple environments.
The same project can be moved to different directory or machine 
or can be run by multiple peoples with different setups.
This is especially useful for handle paths in configs.

To make configs independent on environment it is possible to use *placeholders* in them
which are later replaced by values provided in `Config` object in instantiation in parameter `global_vars`.

!!! Example "Basic usage of global vars"
    === "config.yaml"
    
        ```yaml
        tasks: my_project.tasks.pipeline.*
        uses: "{CONFIGS_DIR}/dependency_config.yaml"

        source_data: {DATA_DIR}/data.csv
        ```

    === "code.py"
    
        ```python
        from pathlib import Path
        from taskchain import Config
        
        CONFIGS_DIR = Path('/path/to/configs')

        config = Config(
            '/path/to/task_data', 
            CONFIGS_DIR / 'config.yaml',
            global_vars={
                'CONFIGS_DIR': CONFIGS_DIR,
                'DATA_DIR': '/path/to/data'
            }
        )
        ```

Parameter `global_var` can be a `dict` with placeholders as keys or an object with placeholders as attributes.
This allows following typical construction:

!!! Example "Typical usage of global vars"

    === "code.py"
    
        ```python
        from pathlib import Path
        from taskchain import Config
        
        from project import project_config
        
        config = Config(
            project_config.TASKS_DIR,
            project_config.CONFIGS_DIR / 'config.yaml',
            global_vars=project_config
        )
        ```

    === "project_config.py"
    
        ```python
        from pathlib import Path
        
        REPO_DIR = Path(__file__).resolve().parent.parent.parent
        
        DATA_DIR = Path('/path/to/project_data/source_data')
        TASKS_DIR = Path('/path/to/project_data/task_data')
        CONFIGS_DIR = REPO_DIR / 'configs'
        ```

    === "config.yaml"
    
        ```yaml
        tasks: my_project.tasks.pipeline.*
        uses: "{CONFIGS_DIR}/dependency_config.yaml"

        source_data: {DATA_DIR}/data.csv
        ```

## Parameter objects

#### `AutoParameterObject`

#### `ChainObject`


## Namespaces


## Advanced topics 


### Multi-config files


### Contexts

Context is mechanism witch allows rewrite parameter values in configs in the time of their instantiation.
Under the hood Context is special case of Config which is used in specific way. 

!!! Example


    ```python
    config = Config(
        '/path/to/task_data', 
        CONFIGS_DIR / 'config.yaml',
        context={'verbose': True, 'batch_size': 32}
    )
    ```

This can be useful for

- ad-hock experiment
- hyper-parameter optimization
- tuning parameters, which are not used in persistence, e.g. `batch_size` 
- consider long data processing chain consisting of multiple dependent pipelines each witch own config file. 
  When we get new input data, it usually leads to recreating all configs which are very similar 
  (only `input_data` parameter is changed and config paths in `uses`).
  Other approach is omit `input_data` parameter value in config provide it as context, 
  which allows run pipeline with same configuration on multiple inputs easily.


#### What can be context

- **dict** of parameters and their values
- **file path** with json on yaml file - this is analogous to context files
- Context object
- **list of previous** - in that case context are merged together. 
  In case of parameter conflict later has higher priority.

!!! Warning

    Parameters in context are applied globaly, i.e. on all configs in chain. 
    Be cearful with parameters of the same name in different pipelines.

#### Namespaces

In the case of more complicated chains which uses namespaces you can run to problems, 
when one pipeline is in chain multiple times with different configuration (under different namespaces).
For these cases, context can have `for_namespaces` field. It's valued should be dict witch namespaces as keys
and parameters to overwrite in this namespace.

!!! Example "Context YAML file using `for_namespace`"

    ```yaml
    for_namespaces:
        train:
            input_data: '/path/to/data1'
            other_param: 42
        test:
            input_data: '/path/to/data2'

    batch_size: 32
    ```

#### Uses

It is possible to join multiple context in one file with `uses` field. 
Syntax is the same as in configs, but meaning slight different. 
In contexts files in `uses` are just merged to the main context.
Is `... as namespace` format is used, loaded context is applied only for given namespace.
Following example is equivalent to the previous one.

!!! Example "Context YAML files using `uses`"

    === "context.yaml"

    ```yaml
    uses:
        - /path/to/train.context.yaml as train
        - /path/to/test.context.yaml as test

    batch_size: 32
    ```
    === "train.context.yaml"
    ```yaml
    input_data: '/path/to/data1'
    other_param: 42
    ```

    === "test.context.yaml"
    ```yaml
    input_data: '/path/to/data2'
    ```
