try:
    import uvloop
    uvloop.install()
    del uvloop
except ImportError:
    pass

__version__ = '0.1.0'

PRODUCT = "Aiopika Python Client Library"

from .parameter   import *
from .exceptions  import *
from .credentials import *
from .connection  import *
from .channel     import *

