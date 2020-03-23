from taskchain.task import Task, Config


class A(Task):

    class Meta:
        task_group = 'x'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_called = 0

    def run(self) -> int:
        self.run_called += 1
        return 1


def test_persistence(tmp_path):
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
