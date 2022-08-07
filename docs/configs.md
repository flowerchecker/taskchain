# Configs

Generally, configs defines all necessary parameter values, 
which are needed to run a pipeline and tasks in it.

In TaskChain, config is also entry point for creating a chain
and describes how pipelines are connected to a chain.
Therefore, a config has to define:

- description of tasks which should be part of a chain (e.g. pipeline)
- parameter values needed by these tasks
- dependencies on other configs (pipelines)

Usual setup is **one config in one file which defines one pipeline**. 
This allows effective reuse of the pipeline in multiple chains
without need of repeating parameter values.

!!! Note "What actually is config?"
    Config in TaskChain has dual meaning. 
    First, config as YAML file containing information described above.
    Second, config as an instance of `taskchain.Config` which is usually based
    on the config file and adds other information necessary for computation. 


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

Each config should define which tasks are configured. This is a way how a config defines a pipeline.
The special parameter `tasks` is **string or list of strings** describing tasks.
Task is described by a path to task's class: `'my_project.tasks.pipeline.ProcessTask'`.
This description corresponds exactly to python imports.

To import all tasks from pipeline (defined in single file) at once you can use wildcard `*` in last 
place of description: `'my_project.tasks.pipeline.*'`.


#### Task exclusion

For more flexibility, you can also exclude tasks with special parameter `excluded_tasks`
with same syntax as `tasks` parameter.


### Config dependencies

More complicated chains are split to multiple pipelines with corresponding configs.
Parameter `uses` defines how configs of these pipelines are connected together.

For example, project is split to *data preparation pipeline* and *model pipeline*. 
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

Sometimes configuration using only json-like parameter values is not enough, or it is not practical.
For these cases, you can include definition of a object instance as parameter value to your config. 

Object instance is defined by class and `args` and `kwargs` passed to constructor.
Class has to be derived class of `taskchain.paramater.ParameterObject`, i.e. has
to define `repr` method which should return unique string representation of object.
This value is then used by taskchain to keep track of changes in configs, 
and it is necessary for correct function of data persistence. 

Common pattern is that there is base class which defines interface which is used by tasks
and parameter objects are instances of child classes. 
Good example is `Model` class which define abstract methods
`train`, `predict`, `save` and`load`. Children of this base class 
(e.g. `NeuralModel`, `LinearRegressionModel`, ...) implement these methods, are configurable 
by their constructor and are used in configs. [Here]({{config.code_url}}/example/src/movie_ratings/models) is example of this pattern.

Definition of object instance in config is dict containing key `class` with fully-qualified name of class as value. 
Additionally, dict can contain `args` with a list and `kwargs` with a dict.


!!! Example "Definition of object in config"

    ```python
        model:
            class: my_project.models.LinearRegressionModel
            kwargs:
                normalize: True
                regularization: 1
    ```

In last example, config provide parameter `model` with value `LinearRegressionModel(normalize=True, regularization=1)`.

In config, objects can be defined inside other structures, such as list, dict or definitions of other objects.
I.e. you can define parameter which is list of objects.

#### `AutoParameterObject`

Writing of `repr` method for parameter object can repetitive and omitting
a argument can lead to mistakes.
Therefore, there is `AutoParameterObject` which defines `repr` for you
and it is based on class name and arguments of `__init__` method. 
To make it work, all arguments values of constructor has to be saved in object attributes.
For argument named `my_argument`, `AutoParameterObject` is looking for its value at
`self._my_argument` or `self.my_argument`.

To allow more flexibility and ease adding new arguments, you can also define
`ignore_persistence_args` or `dont_persist_default_value_args` which return 
list of string names of arguments and have similar meaning as 
[`Parameter` arguments]({{config.base_url}}/tasks#parameters-arguments).

#### `ChainObject`

In case that your parameter object need to access the chain directly
(e.g. take a task's data), you can inherit also from `taskchain.chain.ChainObject`
and implement `init_chain(self, chain)` method which is called after chain creation
and pass chain itself.

## Namespaces

If you need to use one pipeline with different configs in one chain, or
you just make your larger chains more structured, you can use namespaces.

You can put part of your chain to a namespace and all tasks in that part 
will be referenced not but their name `task_name` but by namespace and task name
`namespace_name::task_name`. 

Creating namespaces is really simple, in referencing other config in config definition
(`uses` clause) just add ` as namespace_name`.

!!! Example
    === "model_config.yaml"
    
        ```yaml
        tasks: my_project.tasks.model.*
        uses: 
            - "/path/to/data_configs/train_data.yaml as train"
            - "/path/to/data_configs/valid_data.yaml as valid"
            - "/path/to/data_configs/test_data.yaml as test"

        model: ...
        ```

    === "model.py"
    
        ```python
        ...

        class TrainModel(Task):
            class Meta:
                parameters = [Parameter('model')]
                input_tasks = ['train::features', 'valid::features']
        
            def run(self, model) -> ...:
                train_data = self.input_tasks['train::features'].value
                ...
        
        class EvalModel(Task):
            class Meta:
                parameters = [Parameter('model')]
                input_tasks = [TrainModel, 'test::features]
        
            def run(self, model, train_model, features) -> dict:
                ...
        ...
        ```

Notes
    
- if a config is in a namespace, also configs used by this config are in the same namespace
- namespaces can be nested, e.g. task `features` can be in nested namespace `main_model::train`
- you can still reference task without namespace as long as there is only one task of that name
    - this is the case of `input_tasks` of `Evalmodel` task but not `input_tasks` of `TrainModel`task in example above
    - this applies to referencing tasks in chain, in `input_tasks` and `run` method arguments.


## Advanced topics 


### Multi-config files

It is possible to save multiple configs to one file.
This can be useful, when chain has multiple pipelines, 
and you need one file configuration.

!!! Example "Example of multi-config file"

    ```yaml
    configs:
        data_config:
            tasks: ...
            input_data_file: ...
            ...
        model_config:
            main_part: True
            tasks: ...
            uses: "#data_config as data"
            model: ...
            ...
        other_config_name:
            ...
    ```

Single config can be taken from this file using `part` argument: 
```python
config = Config('/path/to/task_data', 'multi_config.yaml', part='data_config')
```
It is possible omit `part` argument if one of defined configs specify `main_part: True`.
To access a config from another config (in `uses`) use 
`/path/to/multi_config.yaml#data_config`
or if you refer to a part of the same multi-config, 
you can use only `#data_donfig` as shown in example.


### Contexts

Context is mechanism which allows rewrite parameter values in configs in the time of their instantiation.
Under the hood `Context` is special case of `Config` which is used in specific way. 

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
- consider long data processing chain consisting of multiple dependent pipelines each with own config file. 
  When we get new input data, it usually leads to recreating all configs which are very similar 
  (only `input_data` parameter is changed and config paths in `uses`).
  Other approach is omit `input_data` parameter value in config and provide it context, 
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
For these cases, context can have `for_namespaces` field. It's valued should be dict with namespaces as keys
and parameters to overwrite in this namespace as value.

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
If `... as namespace` format is used, loaded context is applied only for given namespace.
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
