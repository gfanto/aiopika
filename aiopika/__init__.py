try:
    import uvloop
    uvloop.install()
    del uvloop
except ImportError:
    pass

__version__ = '0.001'

import pika.amqp_object as amqp_object
import pika.spec as spec
import pika.frame as frame
import pika.exceptions as exceptions

from pika.connection import PRODUCT
from pika.channel import MAX_CHANNELS
from pika.connection import (
    Parameters,
    ConnectionParameters,
    URLParameters,
    SSLOptions
)
from pika.exceptions  import *
from pika.credentials import PlainCredentials, ExternalCredentials, VALID_TYPES

from .connection  import *
from .channel     import *
from .blocking_connection import *