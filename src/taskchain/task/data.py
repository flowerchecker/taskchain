from pathlib import Path
from typing import Any


class Data:

    def __init__(self, value: Any = None):
        self.path = None
        if value is not None:
            self._value = None

    @property
    def value(self):
        if not hasattr(self, '_value'):
            pass

        return self._value


class FileData(Data):

    pass


class JSONData(FileData):

    pass


class BasicData(JSONData):

    TYPES = [str, int, float, bool, dict, list]

    pass


class GeneratedData(Data):

    pass
