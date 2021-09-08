from pkg_resources import get_distribution

__version__ = get_distribution('taskchain').version

from taskchain.task.task import Task, ModuleTask, DoubleModuleTask
from taskchain.task.data import Data, InMemoryData, JSONData, DirData, FileData, NumpyData, GeneratedData
from taskchain.task.config import Config, Context
from taskchain.task.chain import Chain, MultiChain
from taskchain.task.parameter import Parameter


from icecream import install
install()
