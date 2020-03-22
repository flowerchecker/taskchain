from pathlib import Path

from taskchain.utils.clazz import persistent


class Config:

    def __init__(self):
        pass

    @property
    @persistent
    def base_dir(self) -> Path:
        pass

    @property
    @persistent
    def name(self) -> str:
        pass
