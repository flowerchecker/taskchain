from pathlib import Path
from typing import Union


class Config:

    def __init__(self, base_dir: Union[Path, str], name: str):

        self.base_dir = base_dir
        self.name = name

