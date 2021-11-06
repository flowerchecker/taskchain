# Testing

To help with testing of tasks and chains TaskChain offers some helper tools.


## Testing of a single task

A task usually exists in a context of chain with other tasks and configs.
Helper function `create_test_task` create tasks and mocks the context.

!!! Example "Testing of a task"

    ```python
    ...
    
    class MyTask(Task):
        class Meta:
            input_tasks = [ATask, BTask]
            parameters = [
                Parameter('p1'),
                Parameter('p2', default=3),
            ]
        
        def run(self, a, b, p1, p2) -> int:
            ...
            return a + b + p1 + p2

    from taskchain.utils.testing import create_test_task
    
    task = create_test_task(
        MyTask,             # class of tested task
        input_tasks={       # mocked input task values
            'a': 7,         #   task can be referenced by name
            BTask: 6,       #   or by class
        },
        parameters={
            'p1': 42,       # parameters provided to tested task
        }
    )
    assert task.value == 7 + 6 + 42 + 3

    ```

## Testing of a part of a chain

You can also test more tasks together. 
Class `TestChain` creates chain where some tasks are mocked, 
i.e. their values are not computed but provided on creation of test chain.

!!! Example "Testing of a part of chain"

    ```python
    ...
    
    class KTask(Task):
        class Meta:
            input_tasks = [JTask, BTask]
            parameters = [Parameter('p2')]
    
        def run(self, j, b, p2) -> int:
            ...
    
    class LTask(Task):
        class Meta:
            input_tasks = [KTask]
            parameters = [Parameter('p2')]
    
        def run(self, k, pc) -> int:
            ...

    from taskchain.utils.testing import TestChain
    
    chain = TestChain(
        tasks = [KTask, LTask],
        mock_tasks = {
            'j': ...,
            BTask: ...,
        },
        parameters={
            'p1': ...,
            'p2': ...,
        },
    )
    assert chain.k.value == ...
    assert chain.l.value == ...
    ```
