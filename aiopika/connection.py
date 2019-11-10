import logging
import asyncio
import typing
import copy
import platform
import math
import enum
import traceback

from io import StringIO
from functools import partial
from typing import (
    Callable,
    Iterable,
    Union,
    Dict,
    List,
    Set,
    Any,
    Coroutine,
    Optional
)

from . import __version__, PRODUCT
from . import spec
from . import frame
from . import amqp_object
from . import exceptions
from . import channel
from .channel import MAX_CHANNELS
from .parameter import Parameters, ConnectionParameters

from ._dispatch import EventDispatcherObject, Waiter
from ._heartbeat import HeartbeatChecker
from .frame import is_method, is_protocol_header, has_content, FrameDecoder, Heartbeat
from .channel import Channel


__all__ = [
    'Connection',
    'BlockingConnection',
    'BlockingChannel',
    'AsyncChannel',
    'AsyncConnection'
]

LOGGER = logging.getLogger(__name__)


def _create_task(coro, exception_handler):
    def _result_handler(f):
        try:
            f.result()
        except asyncio.CancelledError:
            pass
        except BaseException as ex:
            exception_handler(f, f.exception())

    t = asyncio.create_task(coro)
    t.add_done_callback(_result_handler)
    return t


class _RWStream:
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer

        self.__drain_lock = asyncio.Lock()

        self.bytes_sent = 0
        self.bytes_received = 0

    async def flush(self):
        async with self.__drain_lock:
            await self._writer.drain()

    async def read(self, n=-1):
        data = await self._reader.read(n)
        self.bytes_received += len(data)
        return data

    async def readexactly(self, n):
        data = await self._reader.readexactly(n)
        self.bytes_received += n
        return data

    def write(self, data):
        self._writer.write(data)
        self.bytes_sent += len(data)

    async def awrite(self, data):
        self.write(data)
        await self.flush()

    def terminate(self):
        self._writer.close()

    async def close(self):
        if not self._writer.is_closing():
            self.terminate()
        await self._writer.wait_closed()


CLOSED = 1
PROTOCOL = 2
START = 3
TUNE = 4
OPEN = 5
CLOSING = 6
OPENING = 7
INIT = 8
ERROR = 9

CONNECTION_STATE = {
    CLOSED: 'CLOSED',
    PROTOCOL: 'PROTOCOL',
    START: 'START',
    TUNE: 'TUNE',
    OPEN: 'OPEN',
    CLOSING: 'CLOSING',
    OPENING: 'OPENING',
    INIT: 'INIT',
    ERROR: 'ERROR',
}


class Connection(EventDispatcherObject):
    _channels: Dict[int, Channel]
    _error: Optional[BaseException]
    _state: int
    server_capabilities: Dict

    __state_waiter: Optional[Waiter]

    def _init_connection_state(self) -> None:
        """TODO: for implementing reconnect and continuing store the data

        """
        self._set_connection_state(INIT)

    def _set_connection_state(self, state: int) -> None:
        LOGGER.debug(
            'New Connection state: %s (prev=%s)',
            CONNECTION_STATE[state],
            CONNECTION_STATE[self._state]
        )
        self._state = state
        if self.__state_waiter is not None:
            self.__state_waiter.check(state)

    def __init__(self, parameters: Parameters = None):
        super(Connection, self).__init__()
        self._loop = asyncio.get_event_loop()

        if parameters is not None:
            saved_ssl_options = parameters.ssl_options
            parameters.ssl_options = None
            try:
                self.params = copy.deepcopy(parameters)
                self.params.ssl_options = saved_ssl_options
            finally:
                parameters.ssl_options = saved_ssl_options
        else:
            self.params = ConnectionParameters()

        self._state = CLOSED

        self.server_capabilities = {}
        self.server_properties = None
        self.known_hosts = None

        self._body_max_length = -1
        self._channels = dict()
        self._frames_sent = 0
        self._frames_received = 0
        self._heartbeat_checker = None
        self._blocked_conn_waiter = Waiter()

        #self._stream = None
        #self._frame_decoder = None

        self.__state_waiter = None
        self.__process_frame_lock = asyncio.Lock()

        self._init_connection_state()

    def __del__(self):
        if not self.is_closed:
            self.terminate()

    @property
    def is_closed(self) -> bool:
        return self._state == CLOSED or \
            self._state == INIT

    @property
    def is_closing(self) -> bool:
        return self._state == CLOSING

    @property
    def is_open(self) -> bool:
        return self._state == OPEN

    @property
    def is_opening(self) -> bool:
        return self._state == PROTOCOL or \
            self._state == OPENING

    @property
    def basic_nack(self) -> bool:
        return self.server_capabilities.get('basic.nack', False)

    @property
    def consumer_cancel_notify(self) -> bool:
        return self.server_capabilities.get('consumer_cancel_notify', False)

    @property
    def exchange_exchange_bindings(self) -> bool:
        return self.server_capabilities.get('exchange_exchange_bindings', False)

    @property
    def publisher_confirms(self) -> bool:
        return self.server_capabilities.get('publisher_confirms', False)

    @property
    def frames_sent(self) -> int:
        return self._frames_sent

    @property
    def frames_received(self) -> int:
        return self._frames_received

    @property
    def bytes_sent(self) -> int:
        return self._stream.bytes_sent

    @property
    def bytes_received(self) -> int:
        return self._stream.bytes_received

    @property
    def closing_reason(self) -> Optional[BaseException]:
        return self._error

    def _raise_if_not_open(self) -> None:
        if not self.is_open:
            raise exceptions.ConnectionWrongStateError(
                f'Connection is not open: {self._state}'
            )

    def _remove_heartbeat(self) -> None:
        if self._heartbeat_checker is not None:
            self._heartbeat_checker.stop()
            self._heartbeat_checker = None

    def terminate(self, error: BaseException = None) -> None:
        self._error = error
        if self.is_closing and error:
            LOGGER.warning('Error while connection is closing')
            return
        if self.is_closed:
            raise exceptions.ConnectionWrongStateError(
                'Trying to terminate an already closed connection'
            )

        if error:
            LOGGER.error('Stream terminated in unexpected fashion: %s', error)
        self._remove_heartbeat()
        self._stream.terminate()
        self._set_connection_state(CLOSED)

    async def _read_data(self, n: int = -1):
        if n < 0:
            return await self._stream.read()
        return await self._stream.readexactly(n)

    def _emit_data(self, data: bytes):
        self._stream.write(data)

    def _emit_marshaled_frames(self, marshaled_frames: Iterable[bytes]):
        for marshaled_frame in marshaled_frames:
            self._frames_sent += 1
            self._emit_data(marshaled_frame)

    def _emit_frame(self, frame_value: frame.Frame):
        marshaled_frame = frame_value.marshal()
        self._emit_marshaled_frames([marshaled_frame])

    def _emit_message(self,
                      channel_number: int,
                      method_frame: amqp_object.Method,
                      content: tuple):
        length = len(content[1])
        marshaled_body_frames = []

        frame_method = frame.Method(channel_number, method_frame)
        frame_header = frame.Header(channel_number, length, content[0])
        marshaled_body_frames.append(frame_method.marshal())
        marshaled_body_frames.append(frame_header.marshal())

        if content[1]:
            chunks = int(math.ceil(float(length) / self._body_max_length))
            for chunk in range(0, chunks):
                start = chunk * self._body_max_length
                end = start + self._body_max_length
                if end > length:
                    end = length
                frame_body = frame.Body(channel_number, content[1][start:end])
                marshaled_body_frames.append(frame_body.marshal())

        self._emit_marshaled_frames(marshaled_body_frames)

    def _emit_method(self,
                     channel_number: int,
                     method: amqp_object.Method,
                     content: tuple = None):
        if content:
            self._emit_message(channel_number, method, content)
        else:
            self._emit_frame(frame.Method(channel_number, method))

    async def _flush_data(self):
        await self._stream.flush()

    async def _send_frame(self, frame_value: frame.Frame):
        self._emit_frame(frame_value)
        await self._flush_data()

    # @[UNUSED]
    async def _send_message(
        self,
        channel_number: int,
        method: amqp_object.Method,
        content: tuple
    ):
        self._emit_message(channel_number, method, content)
        await self._flush_data()

    async def _send_method(
        self,
        channel_number: int,
        method: amqp_object.Method,
        content: tuple = None
    ):
        self._emit_method(channel_number, method, content)
        await self._flush_data()

    async def _stream_connected(self, reader, writer):
        self._set_connection_state(PROTOCOL)
        self._stream = _RWStream(reader, writer)
        self._frame_decoder = FrameDecoder(self._stream)
        await self._send_frame(frame.ProtocolHeader())

    async def connect(self):
        if not self.is_closed:
            raise exceptions.ConnectionWrongStateError(
                f'Connection require closed connection: {self}'
            )
        await self._stream_connected(
            *await asyncio.open_connection(self.params.host, self.params.port)
        )

    async def _deliver_frame_to_channel(self, frame_value: frame.Frame):
        channel_number = frame_value.channel_number
        if not channel_number in self._channels:
            LOGGER.critical(
                'Received %s frame for unregistered channel %i on %s',
                frame_value.NAME,
                frame_value.channel_number,
                self
            )
            return
        await self._channels[channel_number]._handle_frame(frame_value)

    async def _dispatch_frame(self, frame_value: frame.Frame):
        await self._dispatch_event(frame_value)(frame_value)

    # @[ASYNC]
    def _next_frame_available(self) -> frame.Frame:
        return self._frame_decoder.read_frame()

    async def _process_next_frame(self, frame_value: frame.Frame=None):
        async with self.__process_frame_lock:
            frame_value = frame_value or await self._next_frame_available()

            self._frames_received += 1

            if isinstance(frame_value, frame.Heartbeat):
                if self._heartbeat_checker is not None:
                    self._heartbeat_checker.received()
                else:
                    LOGGER.warning(
                        'Received heartbeat frame without a heartbeat checker'
                    )
            else:
                channel_number = frame_value.channel_number
                if channel_number > 0:
                    await self._deliver_frame_to_channel(frame_value)
                elif channel_number == 0:
                    if is_method(frame_value):
                        try:
                            await self._dispatch_frame(frame_value)
                        except exceptions.UnexpectedFrameError:
                            LOGGER.debug(
                                'unable to dispatch frame: %s',
                                frame_value
                            )
                    else:
                        LOGGER.info(
                            'Connection received non method frame: %s',
                            frame_value
                        )
                else:
                    LOGGER.info(
                        'Discarding frame: %s cause channel number < 0',
                        frame_value
                    )

    async def run_until_closed(self):
        while not self.is_closed:
            await self._process_next_frame()

    async def _wait_state(self, expected: int):
        assert self.__state_waiter is None, 'Frame waiter override error'
        self.__state_waiter = Waiter(lambda state: state == expected)
        await self.__state_waiter.wait()
        self.__state_waiter = None

    async def open(self):
        if not self.is_opening:
            raise exceptions.ConnectionWrongStateError(
                f'Connection require connected connection: {self}'
            )

        self._set_connection_state(OPENING)
        try:
            await self._wait_state(OPEN)
        except Exception as error:
            self.terminate(error)
            raise

    def _next_channel_number(self) -> int:
        limit = self.params.channel_max or MAX_CHANNELS
        if len(self._channels) >= limit:
            raise exceptions.NoFreeChannels()

        for num in range(1, len(self._channels) + 1):
            if num not in self._channels:
                return num
        return len(self._channels) + 1

    def _create_channel(self, channel_number: int) -> channel.Channel:
        LOGGER.debug('Creating channel %s', channel_number)
        return Channel(self, channel_number)

    def channel(self, channel_number: int = -1):
        if not self.is_open:
            raise exceptions.ConnectionWrongStateError(
                f'Channel allocation requires an open connection: {self}'
            )

        if channel_number <= 0:
            channel_number = self._next_channel_number()
        ch = self._create_channel(channel_number)
        self._channels[channel_number] = ch

        try:
            return ch
        except:
            del self._channels[channel_number]
            raise

    def _channel_cleanup(self, ch: Channel):
        del self._channels[ch.channel_number]
        LOGGER.debug('Removed channel %s', ch.channel_number)

    async def _close_channel(self, ch: Channel, reply_code: int, reply_text: str):
        try:
            await ch.close(reply_code, reply_text)
        except:
            self._channels.pop(ch.channel_number, None)
            raise

    async def _close_channels(self, reply_code: int, reply_text: str):
        # @[TODO] add timeout
        await asyncio.gather(
            *(self._close_channel(ch, reply_code, reply_text)
                for ch in self._channels.values()
            )
        )

    async def disconnect(self, error: BaseException = None):
        await self._stream.close()
        self._set_connection_state(CLOSED)

    async def close(self, reply_code: int = 200, reply_text: str = 'Normal shutdown'):
        if self.is_closing or self.is_closed:
            msg = (
                f'Illegal close({reply_code}, {reply_text}) request on {self} '
                f'because it was called while connection state={self._state}.'
            )
            LOGGER.error(msg)
            raise exceptions.ConnectionWrongStateError(msg)

        if not self.is_open:
            LOGGER.info(
                'Connection.close() is terminating stream and '
                'bypassing graceful AMQP close, since AMQP is still '
                'opening.'
            )

            error = exceptions.ConnectionOpenAborted(
                f'Connection.close() called before connection '
                f'finished opening: prev_state={self._state} ({reply_code}): {reply_text}')
            self.terminate(error)
            raise error

        self._set_connection_state(CLOSING)
        LOGGER.info('Closing connection (%s): %s', reply_code, reply_text)

        if self._channels:
            await self._close_channels(reply_code, reply_text)

        error = exceptions.ConnectionClosedByClient(reply_code, reply_text)
        try:
            await self._send_method(
                0,
                spec.Connection.Close(error.reply_code, error.reply_text, 0, 0)
            )
            await self._wait_state(CLOSED)
        except:
            self.terminate(error)
            raise

    #
    ## event callbacks
    #

    def _check_for_protocol_mismatch(self, frame_value):
        version = (
            frame_value.method.version_major,
            frame_value.method.version_minor
        )
        if version != spec.PROTOCOL_VERSION[0:2]:
            raise exceptions.ProtocolVersionMismatch(
                frame.ProtocolHeader(),
                frame_value
            )

    def _set_server_information(self, method_frame):
        self.server_properties = method_frame.method.server_properties
        self.server_capabilities = self.server_properties.get(
            'capabilities',
            dict()
        )
        if hasattr(self.server_properties, 'capabilities'):
            del self.server_properties['capabilities']

    @property
    def _client_properties(self):
        properties = {
            'product': PRODUCT,
            'platform': 'Python %s' % platform.python_version(),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See http://pika.rtfd.org',
            'version': __version__
        }

        if self.params.client_properties:
            properties.update(self.params.client_properties)

        return properties

    def _get_credentials(self, method_frame):
        auth_type, response = self.params.credentials. \
                                response_for(method_frame.method)
        if not auth_type:
            raise exceptions.AuthenticationError(self.params.credentials.TYPE)
        self.params.credentials.erase_credentials()
        return auth_type, response

    async def _on_connection_start(self, method_frame):
        self._set_connection_state(START)

        if is_protocol_header(method_frame):
            raise exceptions.UnexpectedFrameError(method_frame)
        self._check_for_protocol_mismatch(method_frame)

        self._set_server_information(method_frame)
        await self._send_method(
            0,
            spec.Connection.StartOk(
                self._client_properties,
                *self._get_credentials(method_frame),
                self.params.locale
            )
        )

    @staticmethod
    def _tune_heartbeat_timeout(client_value, server_value):
        if client_value is None:
            timeout = server_value
        else:
            timeout = client_value

        return timeout

    def _get_body_frame_max_length(self):
        return (self.params.frame_max - spec.FRAME_HEADER_SIZE -
                spec.FRAME_END_SIZE)

    def _create_heartbeat_checker(self):
        if self.params.heartbeat is not None and self.params.heartbeat > 0:
            LOGGER.debug(
                f'Creating a HeartbeatChecker: {self.params.heartbeat}'
            )
            hbc = HeartbeatChecker(self, self.params.heartbeat)
            hbc.start()
            return hbc
        return None

    @staticmethod
    def _negotiate_integer_value(client_value, server_value):
        if client_value is None:
            client_value = 0
        if server_value is None:
            server_value = 0

        if client_value == 0 or server_value == 0:
            val = max(client_value, server_value)
        else:
            val = min(client_value, server_value)

        return val

    async def _on_connection_tune(self, method_frame):
        self._set_connection_state(TUNE)

        self.params.channel_max = Connection._negotiate_integer_value(
            self.params.channel_max, method_frame.method.channel_max)
        self.params.frame_max = Connection._negotiate_integer_value(
            self.params.frame_max,
            method_frame.method.frame_max
        )

        if callable(self.params.heartbeat):
            ret_heartbeat = self.params.heartbeat(
                self,
                method_frame.method.heartbeat
            )
            if ret_heartbeat is None or callable(ret_heartbeat):
                raise TypeError(
                    'heartbeat callback must not return None '
                    'or callable, but got %r' % (ret_heartbeat,)
                )

            self.params.heartbeat = ret_heartbeat

        self.params.heartbeat = self._tune_heartbeat_timeout(
            client_value=self.params.heartbeat,
            server_value=method_frame.method.heartbeat)

        self._body_max_length = self._get_body_frame_max_length()

        if self.params.heartbeat is not None:
            self._heartbeat_checker = self._create_heartbeat_checker()

        await self._send_method(
            0,
            spec.Connection.TuneOk(
                self.params.channel_max,
                self.params.frame_max,
                self.params.heartbeat
            )
        )
        await self._send_method(
            0,
            spec.Connection.Open(self.params.virtual_host, insist=True)
        )

    async def _on_connection_closeok(self, method_frame):
        LOGGER.debug('_on_connection_closeok: frame=%s', method_frame)
        self._set_connection_state(CLOSED)

    async def _on_connection_close(self, method_frame):
        LOGGER.debug(
            '_on_connection_close_from_broker: frame=%s',
            method_frame
        )

        error = exceptions.ConnectionClosedByBroker(
            method_frame.method.reply_code,
            method_frame.method.reply_text
        )
        self.terminate(error)

    async def _on_connection_blocked(self, method_frame):
        LOGGER.warning('Received %s from broker', method_frame)

        if self._blocked_conn_waiter.is_waiting:
            LOGGER.warning(
                'connection is already blocked, %s received',
                method_frame
            )
            return

        try:
            await asyncio.wait_for(
                self._blocked_conn_waiter.wait(),
                self.params.blocked_connection_timeout
            )
        except asyncio.TimeoutError as e:
            self._blocked_conn_waiter.clear()
            error = exceptions.ConnectionBlockedTimeout(
                'Blocked connection timeout expired.'
            )
            await self.disconnect(error)
            raise exceptions.ConnectionBlockedTimeout from e

    async def _on_connection_unblocked(self, method_frame):
        LOGGER.info('Received %s from broker', method_frame)

        if not self._blocked_conn_waiter.is_waiting:
            LOGGER.warning(
                '_blocked_conn_waiter was not active when '
                '_on_connection_unblocked called'
            )
            return
        self._blocked_conn_waiter.check()

    async def _on_connection_openok(self, method_frame):
        self.known_hosts = method_frame.method.known_hosts
        self._set_connection_state(OPEN)

    #
    ## end
    #

    @classmethod
    def run(cls, main: Coroutine, parameters: Parameters = None):
        self = cls(parameters)

        async def closure():
            await self.connect()
            loop = asyncio.create_task(self.run_until_closed())
            await self.open()
            await main(self)
            await self.close()
            await self.disconnect()
            await loop

        loop = asyncio.get_event_loop()
        loop.run_until_complete(closure())


class AsyncChannel(Channel):
    async def _dispatch_consumer(
        self,
        consumer_tag,
        method_frame,
        header_frame,
        body
    ):
        return _create_task(
            super()._dispatch_consumer(
                consumer_tag,
                method_frame,
                header_frame,
                body
            ),
            partial(self._handle_consumer_ex, consumer_tag)
        )

    # async def _dispatch_callback(self, callback, *args, **kwargs):
    #     return _create_task(
    #         super()._dispatch_callback(callback, *args, **kwargs),
    #         self._handle_callback_ex
    #     )

    # async def _dispatch_frame(self, frame_value):
    #     return _create_task(
    #         super()._dispatch_frame(frame_value),
    #         self._handle_frame_ex
    #     )

    # async def _dispatch_method(self, method_frame, header_frame, body):
    #     return _create_task(
    #         super()._dispatch_method(
    #             method_frame,
    #             header_frame,
    #             body
    #         ),
    #         self._handle_method_ex
    #     )

    def _handle_consumer_ex(self, consumer_tag, fut, ex):
        LOGGER.warning(
            'During consumer (%s) dispatch an exception occourred: %s',
            consumer_tag,
            ex
        )
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    # def _handle_callback_ex(self, fut, ex):
    #     LOGGER.warning(
    #         'During callback dispatch an exception occourred: %s',
    #         ex
    #     )
    #     traceback_buf = StringIO()
    #     fut.print_stack(file=traceback_buf)
    #     LOGGER.debug(traceback_buf.getvalue())

    # def _handle_frame_ex(self, fut, ex):
    #     LOGGER.warning('During frame dispatch an exception occourred: %s', ex)
    #     traceback_buf = StringIO()
    #     fut.print_stack(file=traceback_buf)
    #     LOGGER.debug(traceback_buf.getvalue())

    # def _handle_method_ex(self, fut, ex):
    #     LOGGER.warning('During method dispatch an exception occourred: %s', ex)
    #     traceback_buf = StringIO()
    #     fut.print_stack(file=traceback_buf)
    #     LOGGER.debug(traceback_buf.getvalue())

    async def __aenter__(self):
        if self.is_closed:
            await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


class AsyncConnection(Connection):
    def __init__(self, parameters: Parameters = None):
        super().__init__(parameters)
        self.__process_frame_loop = None

    def _create_channel(self, channel_number: int):
        LOGGER.debug('Creating channel %s', channel_number)
        return AsyncChannel(self, channel_number)

    async def connect(self):
        await super().connect()
        self.start_loop()

    async def disconnect(self):
        self.stop_loop()
        await super().disconnect()

    async def open(self):
        await self.connect()
        await super().open()

    async def close(self):
        await super().close()
        await self.disconnect()

    def start_loop(self):
        if not self.is_opening:
            raise exceptions.ConnectionWrongStateError(
                'Start loop require an already connected connection'
            )
        if self.__process_frame_loop is not None:
            raise RuntimeError('Process loop is already started')
        self.__process_frame_loop = asyncio.create_task(self.run_until_closed())

    def stop_loop(self):
        if self.__process_frame_loop is None:
            raise RuntimeError('stop_loop has been called when no process loop')
        if not self.__process_frame_loop.done():
            self.__process_frame_loop.cancel()
        else:
            self.__process_frame_loop.result()
        self.__process_frame_loop = None

    # async def _dispatch_frame(self, frame_value: frame.Frame):
    #     return _create_task(
    #         super()._dispatch_frame(frame_value),
    #         self._handle_frame_ex
    #     )

    async def _deliver_frame_to_channel(self, frame_value):
        return _create_task(
            super()._deliver_frame_to_channel(frame_value),
            partial(self._handle_deliver_ex, frame_value.channel_number)
        )

    # def _handle_frame_ex(self, fut, ex):
    #     LOGGER.warning('During frame dispatch an exception occourred: %s', ex)
    #     traceback_buf = StringIO()
    #     fut.print_stack(file=traceback_buf)
    #     LOGGER.debug(traceback_buf.getvalue())

    def _handle_deliver_ex(self, channel, fut, ex):
        LOGGER.warning(
            'During deliver dispatch (channel: %s) an exception occourred: %s',
            channel,
            ex
        )
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    async def __aenter__(self):
        if self.is_closed:
            await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


class BlockingChannel(AsyncChannel):
    def __init__(self, connection, channel_number: int):
        super().__init__(connection, channel_number)
        self.__consume_waiter = None

    async def start_consuming(self):
        if self.__consume_waiter is not None:
            raise RuntimeError(
                f'Channel {self.channel_number} is already consuming'
            )
        self.__consume_waiter = Waiter()
        await self.__consume_waiter.wait()
        self.__consume_waiter = None

    async def _cancel_all_consumers(self):
        LOGGER.debug(f'Cancelling %s consumers', len(self._consumers))

        await asyncio.gather(
            *(self.basic_cancel(consumer_tag)
                for consumer_tag in self._consumers
            )
        )

    async def stop_consuming(self, consumer_tag=None):
        try:
            if consumer_tag:
                await self.basic_cancel(consumer_tag)
            else:
                await self._cancel_all_consumers()
        finally:
            self.terminate_consuming()

    def terminate_consuming(self):
        if self.__consume_waiter is not None:
            self.__consume_waiter.check()
            self.__consume_waiter =  None

    def _transition_to_closed(self):
        self.terminate_consuming()
        super()._transition_to_closed()


class BlockingConnection(AsyncConnection):
    def _create_channel(self, channel_number: int):
        LOGGER.debug('Creating channel %s', channel_number)
        return BlockingChannel(self, channel_number)

    async def channel(self, channel_number: int = -1):
        ch = super().channel(channel_number)
        await ch.open()
        return ch

