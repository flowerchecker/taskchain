from pkg_resources import get_distribution

__version__ = get_distribution('taskchain').version

from taskchain.task import Task, ModuleTask, DoubleModuleTask
from taskchain.data import Data, InMemoryData, JSONData, DirData, FileData, NumpyData, GeneratedData
from taskchain.config import Config, Context
from taskchain.chain import Chain, MultiChain
from taskchain.parameter import Parameter


from icecream import install
install()
