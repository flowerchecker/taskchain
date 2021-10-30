# Tasks

Task defines computation step in pipeline and 
has to be class inheriting `taskchain.task.Task`.

## Basic structure

!!! example
    ```python
    from taskchain import Task, Parameter
    import pandas as pd

    class MyTask(Task):

        class Meta:
            input_tasks = [DependencyTask]
            parameters = [
                Parameter('param')
            ]
    
        def run(dependency, param) -> pd.DataFrame:
            # ... compitation ...
            return result

    ```


### `Meta` subclass

This class has to be defined inside every task and describe how should be the task handled by TaskChain.
The Meta class in not meant to define any methods, it should only define some attributes:

- **input_tasks** (`List[Task | str]`) - dependency tasks, more [below](#input-tasks)
- **parameters** (`List[Parameter]`) - parameters required by this task, which come from your configs, more [below](#parameters)
- **abstract** (`bool`) - if `True` this task isn't instantiated and is never part of the chain, useful for [inheritance](#task-inheritance)
- **name** (`str`) - if not defined name is derived from class name
- **group** (`str`)
- **data_class** (`Type[Data]`) - custom class to use for output data persistence
- **data_type** (`Type`) - output type of `run` method, alternative to typing notation 


### `run` method

This method is called by TaskChain when output of this task is requested and data are not already persisted.

Definition of the method has to contain **return type** (using typing `->` or `data_type` in `Meta` class).
Value returned by `run` method is checked by TaskChain if matches defined type.

!!! Tip
    The method can have **arguments**. TaskChain try to match and fill these arguments by values of parameters and input task.

!!! Warning
    Avoid expensive computation or loading data in `__init__`. 
    TaskChain can create task object multiple time and often task is not used at all.
    Put all expensive operation to `run` method. You can use [`@persistent` decorator](/utils/#persistent-decorator). 


### Task names and groups

Name of the task (`str`) is either defined by `name` attribute of `Meta` or it is derived from class name. Here are some examples:

- `DataTask` -> `data`
- `FilteredDataTask` -> `filtered_data`
- `FilteredData` -> `filtered_data`
- `LongNameOfTheTask` -> `long_name_of_the`

Group of task allows keep some order in larger projects and have mainly impact on structure of persisted data. 
Usually task with a same group defines pipeline.
The group can be defined in `Meta` class and if it is the **fullname of the task** is `group_name:task_name `. 
If you need more rich structure of groups, you can use `:` to separate multiple levels of groups, e.g. `group:sybgroup`. 


!!! Tip
    Usually all task of the pipeline are defined in one module (file).
    To avoid defining same group in all tasks,
    it is possible inherit from `ModuleTask` or `DoubleModuleTask` instead of `Task`.
    In that case group is set to module name.


## Parameters

Parameters are connection between task and configs. 
Parameters defined in `Meta` tell TaskChain which values should be extracted from configs 
and provided for `run` method.

Parameter can be accessed through arguments of `run` method 
or directly from class's `ParameterRegistry`: `self.params.my_param_name` or `self.params['my_param_name']`.

!!! example
    ```python

    class AllDataTask(Task):
        class Meta:
            parameters = [
                Parameter('input_file')
            ]
        
        def run(input_file) -> pd.DataFrame
            assert input_file == self.params.input_file
            return pd.read_csv(input_file)

    class FilteredDataTask(Tasks):
        class Meta:
            input_tasks = [AllDataTask]
            parameters = [
                Parameter('min_value', default=0)
                Parameter('max_value')
            ]
        
        def run(all_data, min_value, max_value) -> pd.DataFrame
            return all_data.query('{min_Value} <= value <= {max_value}')
    
    ```

#### Parameter's parameters :)

- **name** - name for referencing from task
- **default** - value used if not provided in config, default to NO_DEFAULT meaning that param is required
- **name_in_config** - name used for search in config, defaults to `name` argument
- **dtype** - expected datatype
- **ignore_persistence** - do not use this parameter in persistence, useful for params without influence on output, 
        e.g. verbose or debug
- **dont_persist_default_value** - if value of parameter is same as the default, do not use it in persistence.
        This is useful for adding new parameters without recomputation of data

!!! Tip
    You can use `pathlib.Path` as datatype. Expected value in config is `str`, however, 
    value provided by the parameter has type of `Path`.


#### Reserved config parameter names

Following names have special meaning in configs and cannot be used as parameters name

- `tasks`
- `uses`
- `human_readable_data_name`
- `configs`
- `for_namespaces`


## Input tasks

Input tasks are connection between tasks. 
This `Meta` argument tells TaskChain which other tasks are prerequisites of this task.

Values (data) of input tasks can be accessed through arguments of `run` method or
directly from class's `InputTasks`: `self.input_task['my_task'].value`.

It is also possible access input task by index: `self.input_task[0].value`. 
This can be useful if task inheritance is used. 
`run` method can stay unchanged and only `input_task` can be redefined. 

Input task can be defined in following ways:

- **by class**: `input_tasks = [MyDataTask]` - this way is preferred if possible
- **by name**: `input_tasks = ['my_data']`
- **by name and group**: `input_tasks = ['group:my_data']`
- **by name, group and namespace**: `input_tasks = ['namespace::group:my_data']`


## Data persistence

Persisting output of tasks is main feature of the TaskChain.

When `run` method produce value (data) TaskChain saves this value and
later when the value of the task is required again value is just loaded 
instead of calling `run` method again.

### `Data` class

Saving and loading of values is handled by inheritors of `taskchain.task.Data` class.
Witch class is used is determined automatically by return data type of `run` method 
or by `data_class` attribute of `Meta`.

These `Data` classes are determined automatically:

- **JSONData** persists `str`, `int`, `float`, `bool`, `dict`, `list` types into `.json` files
- **NumpyData** persists `np.ndarray` type into `.npy` file
- **PandasData** persists `pd.DataFrame` or `pd.Series` type into `.pd` file
- **FigureData** persists `plt.Figure` type into pickle but also saves plot as `.png` and `.svg` for easy sharing.
  Use `pylab` or `seaborn` as usual and just return `plt.gcf()`. 
- **GeneratedData** is used if return type is `Generator`. It is assumed that generated values are JSON-like.
    Values are saved to `.jsonl` file - JSON lines.

Other useful `Data` classes which have to be explicitly defined in `data_class` attribute.

- **InMemoryData** - this special type is not persisting and value is saved only in memory of process.
  
!!! Example
    ```python

    class MyTask(Task):

        class Meta:
            data_class = InMemoryData
    
        def run() -> WhatEver:
            # ...
            return whatever

    ```

- **DirData** - this class allows save arbitrary data to provided directory, but data have to be handled inside `run` method. 
        Value of the task is `Path` of this directory
  
!!! Example
    ```python
    
    class MyTask(Task):

        def run() -> DirData:
            # ...            
            data_object = self.get_data_object()
            self.save(my_data_1, data.dir / 'my_data_1.pickle')
            self.save(my_data_2, data.dir / 'my_data_2.pickle')
            return data_object

    ```
- **ContinuesData** - 
for task with large run time, e.g. training of large models, 
it is possible to make computation on multiple runs. 
This allows to save partial results and when computation is interrupted
next call of run method starts from last checkpoint.

!!! Example
    ```python
    class TrainModel(Task):
        class Meta:
            ...

        def run(self) -> ContinuesData:
            data: ContinuesData = self.get_data_object()
            working_dir = data.dir
    
            self.prepare_model()
            
            checkpoint_path = working_dir / 'checkpoint'
            if checkpoint_path.exists():
                self.load_checkpoint(checkpoint_path)
            
            self.train(
                save_path=working_dir / 'trained_model'
                checkpoint_path=checkpoint_path
            ) # training saves checkpoints periodically and trained model at the end
    
            data.finished()
            return data
    ```

- **H5Data** - special case of `ContinuesData` which allows to compute large data files.

!!! Example
    ```python
    class Embeddings(Task):
    
        class Meta:
            ...

        def run(self) -> H5Data:
            data: H5Data = self.get_data_object()
    
            with data.data_file() as data_file:
                h5_emb_dataset = data.dataset('embedding', data_file, maxshape=(None, embedding_size), dtype=np.float32)
                progress = h5_emb_dataset.len()

                for i, row in enumerate(my_dataset[progress:]):
                    if i % 1000 == 0:
                        gc.collect()
                    emb = self.get_embedding(row)
                    data.append_data(h5_emb_dataset, [emb], dataset_len=progress)
                    data_file.flush()
                    progress += batch_size
            data.finished()
            return data
    ```

You can define ad hoc `Data` classes to handle other data types.

### Returning `Data` object directly

In some cases it is convenient to return (by `run` method) `Data` object directly.
`DirData` is one example. 
Other use case is custom object which inherits from `InMemoryData`.
See `TrainedModel` task in [example project]({{ config.code_url }}/example/src/movie_ratings/tasks/rating_model.py)
which returns [`RatingModel`]({{ config.code_url }}/example/src/movie_ratings/models/core.py) directly.
This is the way to easily expose a important object to other tasks in the pipeline.


## Logging

TaskChain offer two ways to save addition information about computation mainly for debug purposes.

### Run info

After `run` method finishes computation and result value is saved to disk
`Data` object also save additional information about computation.
It is possible add any json-like information to this info.
```python
class MyTask(Task):
    ...
    def run(self):
        ...
        self.save_to_run_info('some important information')
        self.save_to_run_info({'records_processes': 42, 'errors': 0})
```

The run info is saved as YAML and is available under `task_object.run_info` in json-like form.

!!! Example "hash.run_info.yaml"
    ```yaml
    task:
      class: Movies
      module: movie_ratings.tasks.movies
      name: movies:movies
    config:
      context: null
      name: imdb.filtered/movies:movies
      namespace: null
    input_tasks:
      movies:all_movies: 436f7a5e06e540716b275a5f84499a78
    log: 
      - some important information
      - records_processes: 42
        errors': 0
    parameters:
      from_year: '1945'
      min_vote_count: '1000'
      to_year: None
    started: '2021-07-11 11:34:01.520866'
    ended: '2021-07-11 11:34:01.850913'
    time: 0.3300471305847168
    user:
      name: your_system_name
    ```

### logger

Each task has its own standard python logger, 
which can be used from `run` method.
```python
class MyTask(Task):
    ...
    def run(self):
        ...
        self.logger.debug('not so important information')
```

This logger has two handlers
    
- File handler managed by `Data` object which saves log along value produced by task. 
  Logging level of this handler is set to `DEBUG`.
- Other handler is managed by chain object and log to console. 
  Logging level of this handler is set to `WARNING` and can be changed from chain by `chain.set_log_level('DEBUG')`.

## Advanced topics

### Tasks inheritance

Tasks are classes and can be inherited. 
This simplifies cases when pipeline contains task with similar functionality.

You can inherit a task and change his behaviour by

- changing `Meta` class
  - you can change input tasks and then computation will be done with different input.
    In this case, it is not possible have input task in `run` arguments, 
    and they can access by `self.input_tasks[0].value`. 
    This way the task name, which is changing, is avoided. 
  - you can override some methods used by `run` method
  - you can add custom attribute to `Meta` class and access it by `self.meta.my_attribute`
    and make computation based on its value.

It is possible declare in `Meta` class `abstract = True`. 
In that case, task will be not recognized by `project.tasks.pipeline.*` in config 
and will not be part of your pipeline. 
This can be useful for tasks, which will be inherited from.

**Example** of task inheritance can be found in example project

- [movies pipeline]({{ config.code_url }}/example/src/movie_ratings/tasks/movies.py) - search for `ExtractFeatureTask`. 
- [model pipeline]({{ config.code_url }}/example/src/movie_ratings/tasks/rating_model.py) - search for `DataSelectionTask`. 
