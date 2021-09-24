# Chains


## Chain instantiation


## Tun tasks and data access


## Recomputing data


## Logging

### `run` log

### `run` info


## Visualization

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


