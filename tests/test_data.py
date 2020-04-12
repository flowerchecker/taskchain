from taskchain.task import Task, Config, InMemoryData, JSONData


def test_persistence(tmp_path):
    class A(Task):
        class Meta:
            task_group = 'x'

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> int:
            self.run_called += 1
            return 1

    config = Config(tmp_path, name='test')

    a = A(config)
    assert a.value == 1
    assert a.run_called == 1
    assert a.value == 1
    assert a.run_called == 1

    assert (tmp_path / 'x' / 'a' / 'test.json').exists()

    a2 = A(config)
    assert a2.value == 1
    assert a2.run_called == 0

    config2 = Config(tmp_path, name='test2')

    a3 = A(config2)
    assert a3.value == 1
    assert a3.run_called == 1


def test_object_persistence(tmp_path):
    class A(Task):
        class Meta:
            task_group = 'x'

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> JSONData:
            self.run_called += 1
            data = JSONData()
            data.set_value(1)
            return data

    config = Config(tmp_path, name='test')

    a = A(config)
    assert a.value == 1
    assert a.run_called == 1
    assert a.value == 1
    assert a.run_called == 1

    assert (tmp_path / 'x' / 'a' / 'test.json').exists()

    a2 = A(config)
    assert a2.value == 1
    assert a2.run_called == 0

    config2 = Config(tmp_path, name='test2')

    a3 = A(config2)
    assert a3.value == 1
    assert a3.run_called == 1


def test_in_memory_data(tmp_path):
    class A(Task):
        class Meta:
            task_group = 'x'
            data_class = InMemoryData

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> int:
            self.run_called += 1
            return 1

    config = Config(tmp_path, name='test')

    a = A(config)
    assert a.value == 1
    assert a.run_called == 1
    assert a.value == 1
    assert a.run_called == 1

    assert not (tmp_path / 'x' / 'a').exists()

    a2 = A(config)
    assert a2.value == 1
    assert a2.run_called == 1


def test_returned_in_memory_data(tmp_path):
    class MyData(InMemoryData):
        def __init__(self, a):
            super().__init__()
            self.set_value(1)

    class B(Task):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.run_called = 0

        def run(self) -> MyData:
            self.run_called += 1
            data = MyData(1)
            return data

    config = Config(tmp_path, name='test')

    b = B(config)
    assert b.value == 1
    assert b.run_called == 1
    assert b.value == 1
    assert b.run_called == 1

    assert not (tmp_path / 'x' / 'b').exists()

    a2 = B(config)
    assert a2.value == 1
    assert a2.run_called == 1
