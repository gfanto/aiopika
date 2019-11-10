import logging
import struct
import asyncio

from typing import Union, Iterable

from . import exceptions
from . import spec
from . import amqp_object


LOGGER = logging.getLogger(__name__)


class Frame(amqp_object.AMQPObject):
    """Base Frame object mapping. Defines a behavior for all child classes for
    assignment of core attributes and implementation of the a core _marshal
    method which child classes use to create the binary AMQP frame.

    """
    NAME = 'Frame'

    def __init__(self, frame_type, channel_number):
        """Create a new instance of a frame

        :param int frame_type: The frame type
        :param int channel_number: The channel number for the frame

        """
        self.frame_type = frame_type
        self.channel_number = channel_number

    def _marshal(self, pieces):
        """Create the full AMQP wire protocol frame data representation

        :rtype: bytes

        """
        payload = b''.join(pieces)
        return struct.pack('>BHI', self.frame_type, self.channel_number,
                           len(payload)) + payload + bytes((spec.FRAME_END,))

    def marshal(self):
        """To be ended by child classes

        :raises NotImplementedError

        """
        raise NotImplementedError


class Method(Frame):
    """Base Method frame object mapping. AMQP method frames are mapped on top
    of this class for creating or accessing their data and attributes.

    """
    NAME = 'METHOD'

    def __init__(self, channel_number, method):
        """Create a new instance of a frame

        :param int channel_number: The frame type
        :param pika.Spec.Class.Method method: The AMQP Class.Method

        """
        Frame.__init__(self, spec.FRAME_METHOD, channel_number)
        self.method = method

    def marshal(self):
        """Return the AMQP binary encoded value of the frame

        :rtype: str

        """
        pieces = self.method.encode()
        pieces.insert(0, struct.pack('>I', self.method.INDEX))
        return self._marshal(pieces)


class Header(Frame):
    """Header frame object mapping. AMQP content header frames are mapped
    on top of this class for creating or accessing their data and attributes.

    """
    NAME = 'Header'

    def __init__(self, channel_number, body_size, props):
        """Create a new instance of a AMQP ContentHeader object

        :param int channel_number: The channel number for the frame
        :param int body_size: The number of bytes for the body
        :param pika.spec.BasicProperties props: Basic.Properties object

        """
        Frame.__init__(self, spec.FRAME_HEADER, channel_number)
        self.body_size = body_size
        self.properties = props

    def marshal(self):
        """Return the AMQP binary encoded value of the frame

        :rtype: str

        """
        pieces = self.properties.encode()
        pieces.insert(
            0, struct.pack('>HxxQ', self.properties.INDEX, self.body_size))
        return self._marshal(pieces)


class Body(Frame):
    """Body frame object mapping class. AMQP content body frames are mapped on
    to this base class for getting/setting of attributes/data.

    """
    NAME = 'Body'

    def __init__(self, channel_number, fragment):
        """
        Parameters:

        - channel_number: int
        - fragment: unicode or str
        """
        Frame.__init__(self, spec.FRAME_BODY, channel_number)
        self.fragment = fragment

    def marshal(self):
        """Return the AMQP binary encoded value of the frame

        :rtype: str

        """
        return self._marshal([self.fragment])


class Heartbeat(Frame):
    """Heartbeat frame object mapping class. AMQP Heartbeat frames are mapped
    on to this class for a common access structure to the attributes/data
    values.

    """
    NAME = 'Heartbeat'

    def __init__(self):
        """Create a new instance of the Heartbeat frame"""
        Frame.__init__(self, spec.FRAME_HEARTBEAT, 0)

    def marshal(self):
        """Return the AMQP binary encoded value of the frame

        :rtype: str

        """
        return self._marshal(list())


class ProtocolHeader(amqp_object.AMQPObject):
    """AMQP Protocol header frame class which provides a pythonic interface
    for creating AMQP Protocol headers

    """
    NAME = 'ProtocolHeader'

    def __init__(self, major=None, minor=None, revision=None):
        """Construct a Protocol Header frame object for the specified AMQP
        version

        :param int major: Major version number
        :param int minor: Minor version number
        :param int revision: Revision

        """
        self.frame_type = -1
        self.major = major or spec.PROTOCOL_VERSION[0]
        self.minor = minor or spec.PROTOCOL_VERSION[1]
        self.revision = revision or spec.PROTOCOL_VERSION[2]

    def marshal(self):
        """Return the full AMQP wire protocol frame data representation of the
        ProtocolHeader frame

        :rtype: str

        """
        return b'AMQP' + struct.pack('BBBB', 0, self.major, self.minor,
                                     self.revision)


def get_key(method_frame: Method):
    return type(method_frame.method)


def is_method(frame_value: Frame) -> bool:
    return isinstance(frame_value, Method)


def is_header(frame_value: Frame) -> bool:
    return isinstance(frame_value, Header)


def is_heartbeat(frame_value: Frame) -> bool:
    return isinstance(frame_value, Heartbeat)


def is_body(frame_value: Frame) -> bool:
    return isinstance(frame_value, Body)


def is_protocol_header(frame_value: Frame) -> bool:
    return isinstance(frame_value, ProtocolHeader)


def is_method_instance(
    frame_value: Frame,
    cls_or_iter: Union[type, Iterable[type]]
) -> bool:
    ism = is_method(frame_value)
    if not ism:
        return False
    if isinstance(cls_or_iter, Iterable):
        return isinstance(frame_value.method, tuple(cls_or_iter))
    else:
        return isinstance(frame_value.method, cls_or_iter)


def is_method_instance_of(
    channel_number: int,
    cls_or_iter: Union[type, Iterable[type]],
    frame_value: Frame
) -> bool:
    right_channel = frame_value.channel_number == channel_number
    if not right_channel:
        return False
    if isinstance(cls_or_iter, Iterable):
        return is_method_instance(frame_value, tuple(cls_or_iter))
    else:
        return is_method_instance(frame_value, cls_or_iter)


def has_content(frame_value: Method):
    return spec.has_content(frame_value.method.INDEX)


class FrameDecoder:
    def __init__(self, reader):
        self._reader = reader
        self._lock = asyncio.Lock()

    async def read_frame(self):
        async with self._lock:
            header_data = await self._reader.readexactly(spec.FRAME_HEADER_SIZE)
            try:
                if header_data[0:4] == b'AMQP':
                    major, minor, revision = struct.unpack_from(
                        'BBB',
                        header_data,
                        5
                    )
                    return ProtocolHeader(major, minor, revision)
            except struct.error:
                pass

            try:
                (frame_type, channel_number, frame_size) = struct.unpack(
                    '>BHL', header_data)
            except struct.error:
                raise exceptions.InvalidFrameError('Invalid frame header')

            frame_data = await self._reader.readexactly(frame_size)
            frame_end = await self._reader.readexactly(spec.FRAME_END_SIZE)

        if frame_end != bytes((spec.FRAME_END,)):
            raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

        if frame_type == spec.FRAME_METHOD:
            method_id = struct.unpack_from('>I', frame_data)[0]
            method = spec.methods[method_id]()
            method.decode(frame_data, 4)
            return Method(channel_number, method)

        elif frame_type == spec.FRAME_HEADER:
            class_id, weight, body_size = struct.unpack_from('>HHQ', frame_data)
            properties = spec.props[class_id]()
            out = properties.decode(frame_data[12:])
            return Header(channel_number, body_size, properties)

        elif frame_type == spec.FRAME_BODY:
            return Body(channel_number, frame_data)

        elif frame_type == spec.FRAME_HEARTBEAT:
            return Heartbeat()

        raise exceptions.InvalidFrameError(f"Unknown frame type: {frame_type}")

