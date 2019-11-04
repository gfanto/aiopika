import logging
import traceback
import logging
import asyncio

from typing import Iterable, Coroutine
from io import StringIO

from . import exceptions
from . import connection
from . import channel
from . import Parameters
from . import channel
from . import frame

from ._dispatch import Waiter


__all__ = ['BlockingConnection', 'BlockingChannel', 'create_connection']


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


class BlockingChannel(channel.Channel):
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
            self._handle_consumer_ex
        )

    # @[OPT] toggle comment this enable/disable the task creation of rpc callbacks
    async def _dispatch_callback(self, callback, *args, **kwargs):
        return _create_task(
            super()._dispatch_callback(callback, *args, **kwargs),
            self._handle_callback_ex
        )

    async def _dispatch_frame(self, frame_value):
        return _create_task(
            super()._dispatch_frame(frame_value),
            self._handle_frame_ex
        )

    async def _dispatch_method(self, method_frame, header_frame, body):
        return _create_task(
            super()._dispatch_method(
                method_frame,
                header_frame,
                body
            ),
            self._handle_method_ex
        )

    def _handle_consumer_ex(self, fut, ex):
        LOGGER.warning('During consumer dispatch an exception occourred: %s', ex)
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    def _handle_callback_ex(self, fut, ex):
        LOGGER.warning('During callback dispatch an exception occourred: %s', ex)
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    def _handle_frame_ex(self, fut, ex):
        LOGGER.warning('During frame dispatch an exception occourred: %s', ex)
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    def _handle_method_ex(self, fut, ex):
        LOGGER.warning('During method dispatch an exception occourred: %s', ex)
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    async def __aenter__(self):
        if self.is_closed:
            await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


class BlockingConnection(connection.Connection):
    def __init__(self, parameters: Parameters = None):
        super().__init__(parameters)
        self.__process_frame_loop = None

    def _create_channel(self, channel_number: int):
        LOGGER.debug('Creating channel %s', channel_number)
        return BlockingChannel(self, channel_number)

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

    async def _dispatch_frame(self, frame_value: frame.Frame):
        return _create_task(
            super()._dispatch_frame(frame_value),
            self._handle_frame_ex
        )

    async def _deliver_frame_to_channel(self, frame_value):
        return _create_task(
            super()._deliver_frame_to_channel(frame_value),
            self._handle_deliver_ex
        )

    def _handle_frame_ex(self, fut, ex):
        LOGGER.warning('During frame dispatch an exception occourred: %s', ex)
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    def _handle_deliver_ex(self, fut, ex):
        LOGGER.warning('During deliver dispatch an exception occourred: %s', ex)
        traceback_buf = StringIO()
        fut.print_stack(file=traceback_buf)
        LOGGER.debug(traceback_buf.getvalue())

    async def __aenter__(self):
        if self.is_closed:
            await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


async def create_connection(params: Parameters = None):
    conn = BlockingConnection(params)
    await conn.open()
    return conn

