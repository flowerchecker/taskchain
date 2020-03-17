from taskchain.utils.clazz import persistent


class Clazz:
    def __init__(self):
        self.calls = 0

    @persistent
    def method(self):
        self.calls += 1
        return 7

    @property
    @persistent
    def property(self):
        return 5


def test_persistent():

    clz = Clazz()
    assert clz.calls == 0

    assert clz.method() == 7
    assert clz.calls == 1
    assert clz._method == 7

    assert clz.method() == 7
    assert clz.calls == 1

    assert clz.property == 5
    assert clz._property == 5
    assert clz.property == 5
