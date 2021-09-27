# Chains


## Chain instantiation


## Run tasks and data access


### Saved files

`/path/to/tasks/group_name/task_name/hash_`

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


