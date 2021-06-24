from pathlib import Path

import pandas as pd

from taskchain.task import ModuleTask
from taskchain.task.parameter import Parameter


class AllMovies(ModuleTask):

    class Meta:
        input_tasks = []
        parameters = [
            Parameter('source_file', dtype=Path)
        ]

    def run(self, source_file) -> pd.DataFrame:
        df = pd.read_csv(source_file)
        return df
