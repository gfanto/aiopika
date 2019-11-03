import logging
import struct
import asyncio

from typing import Union, Iterable

from . import exceptions
from . import spec
from . import frame

from ._pika_compat import *


LOGGER = logging.getLogger(__name__)


def get_key(method_frame: frame.Method):
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
                    major, minor, revision = struct.unpack_from('BBB', header_data, 5)
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

        raise exceptions.InvalidFrameError("Unknown frame type: %i" % frame_type)

