try:
    import uvloop
    uvloop.install()
    del uvloop
except ImportError:
    pass

__version__ = '0.1.0'

PRODUCT = "Aiopika Python Client Library"

from .channel import MAX_CHANNELS
from .parameter import (
    Parameters,
    ConnectionParameters,
    URLParameters,
    SSLOptions
)
from .exceptions  import *
from .credentials import PlainCredentials, ExternalCredentials, VALID_TYPES

from .connection  import *
from .channel     import *

