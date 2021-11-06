# CheatSheet

## Basics

### Task

```python
class ExampleTask(Task):

    class Meta:
        input_tasks = [...]
        parameters = [
            Parameter('name', default=None),
        ]

    def run(self, ...) -> ...:
        ...
```

### Config

```yaml
tasks: ....*
uses: "{CONFIGS_DIR}/....yaml as ..."

parameter: value
...

parameter_object:
  class: path.to.class
  args:
    - arg1
    - arg2
  kwargs:
    kwarg1: value1
    kwarg2: value2
...
```

### Chain

```python
from taskchain import Config
from project import config

chain = Config(
    config.TASKS_DIR, 
    config.CONFIGS_DIR / 'config_name.yaml', 
    global_vars=config,
).chain()
chain.set_log_level('DEBUG')

chain.tasks_df
chain.draw()
chain.force('my_task', delete_data=True)
```

```python
chain.my_task.value
chain.my_task.force().value

chain.my_task.has_data
chain.my_task.data_path

chain.my_task.run_info
chain.my_task.log
```


## Other

### DirData

```python
def run(self) -> DirData:
    data_object = self.get_data_object()
    working_dir = data_object.dir
    ...
    return data_object
```
