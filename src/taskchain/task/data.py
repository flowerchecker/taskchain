from typing import Any, List, Dict, Generator


class Data:
    DATA_TYPES = []

    @classmethod
    def is_data_type_accepted(cls, data_type):
        return data_type in cls.DATA_TYPES

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

    DATA_TYPES = [str, int, float, bool, Dict, List]


class GeneratedData(Data):

    DATA_TYPES = [Generator]
