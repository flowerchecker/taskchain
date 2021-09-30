# Chains


## Chain instantiation


## Run tasks and data access


### Saved files

`/path/to/tasks/group_name/task_name/hash_`


### Forcing

Sometimes some persisted data become invalid, e.g. due to change in tasks' logic.
Both `Chain` and `Tasks` have `force` method which forces tasks to recompute their data
instead of loading from disk.

If you need recompute only one task, you can simply call 
(note that `force` return task itself, so you can use it in chain with other commands):

```python
chain.my_tasks.force().value
```

If data of a task turn out to be faulty, 
we usually want [force](/code/chain#taskchain.task.chain.Chain.force) recomputation also all dependant tasks in chain.

```python
chain.force('my_task')
```

Method takes name of a task or list of names of tasks which should be forced.
These and or dependant tasks are marked as forced and will be recomputed when their value is requested.
Note, that this "forced" status is lost when chain is recreated.
If you want to be sure, that all data are recomputed, 
use `recompute=True` argument which recompute all forced tasks
or `delete_data=True` which delete data of all forced tasks.



!!! warning 
    
    Be careful with forcing. 
    If you not handle recomputation correctly, your chain can end in incosistent state.
    Using `delete_data=True` solves this problem 
    but can lead to deletion of "expensive" data.


### Human readable files

Persisted data are nicely structured in directories based on tasks' names and groups, 
but names of files are unreadable hashes.
Data are mostly access through TaskChain, but sometimes it is useful access data directly, e.g. copy out final output. 
To simplify direct access chain offers method `create_readable_filenames` 
which creates human-readable symlinks for all tasks in chain. e.g.:

`/path/to/tasks/group_name/task_name/nice_name.pd -> /path/to/tasks/group_name/task_name/a4c5d45a6....pd`

[See more details](/code/chain#taskchain.task.chain.Chain.create_readable_filenames)


## Recomputing data


## Logging

### `run` log

### `run` info


## Working with chains

### `draw`

```python
chain.draw()
```

![Graph](images/graph.png)

Nodes are tasks, edges dependencies.

- **color** is based on tasks' group
- **border**
    - **none** - is not persisting data (`InMemoryData`) 
    - **dashed** - data not computed 
    - **solid** - data computed 

You can pass name of group or list of groups to show only selected task.
Note, that also neighbours are shown to give context for selected group.

## Advanced topics

### Mutlichains


