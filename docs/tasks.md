# Tasks

Task defines computation step in pipeline and 
has to be class inheriting `taskchain.task.Task`.

## Basic structure

!!! example
    ```python
    from taskchain.task import Task, Parameter
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

These `Data` classes determined automatically:

- **JSONData** persists `str`, `int`, `float`, `bool`, `dict`, `list` types into `.json` files
- **NumpyData** persists `np.ndarray` type into `.npy` file
- **PandasData** persists `pd.DataFrame` or `pd.Series` type into `.pd` file
- **FigureData** persists `plt.Figure` type into pickle but also saves plot as `.png` and `.svg` for easy sharing
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
- **ContinuesData, H5Data** - TODO

  You can define ad hoc `Data` classes to handle other data types.

### Returning `Data` object directly

In some cases it is convenient to return (by `run` method) `Data` object directly.
`DirData` is one example. 
Other example is custom object which inherits from `InMemoryData`.
TODO example


## Advanced topics

### Tasks inheritance
