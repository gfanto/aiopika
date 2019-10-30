import logging
import traceback
import aiologger
import asyncio

from typing import Iterable, Coroutine

from . import exceptions
from . import connection
from . import channel
from . import Parameters
from . import channel
from . import frame

from ._dispatch import Waiter
from .connection import ConnectionState


__all__ = ['BlockingConnection', 'BlockingChannel', 'create_connection']


LOGGER = aiologger.Logger.with_default_handlers(name=__name__)


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
        LOGGER.debug(f'Cancelling {len(self._consumers)} consumers')

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
        on_message_callback,
        channel,
        method_frame,
        header_frame,
        body
    ):
        return self.connection._create_task(
            super()._dispatch_consumer(
                on_message_callback,
                channel,
                method_frame,
                header_frame,
                body
            ),
            self._handle_consumer_ex
        )

    # @[OPT] toggle comment this enable/disable the task creation of rpc callbacks
    async def _dispatch_callback(self, callback, *args, **kwargs):
        return self.connection._create_task(
            super()._dispatch_callback(callback, *args, **kwargs),
            self._handle_callback_ex
        )

    async def _dispatch_frame(self, frame_value):
        return self.connection._create_task(
            super()._dispatch_frame(frame_value),
            self._handle_frame_ex
        )

    async def _dispatch_method(self, method_frame, header_frame, body):
        return self.connection._create_task(
            super()._dispatch_method(
                method_frame,
                header_frame,
                body
            ),
            self._handle_method_ex
        )

    def _handle_consumer_ex(self, ex):
        LOGGER.warning(f'During consumer dispatch an exception occourred: {ex}')
        LOGGER.debug(traceback.print_tb(ex.__traceback__))

    def _handle_callback_ex(self, ex):
        LOGGER.warning(f'During callback dispatch an exception occourred: {ex}')
        LOGGER.debug(traceback.print_tb(ex.__traceback__))

    def _handle_frame_ex(self, ex):
        LOGGER.warning(f'During frame dispatch an exception occourred: {ex}')
        LOGGER.debug(traceback.print_tb(ex.__traceback__))

    def _handle_method_ex(self, ex):
        LOGGER.warning(f'During method dispatch an exception occourred: {ex}')
        LOGGER.debug(traceback.print_tb(ex.__traceback__))

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
        LOGGER.debug(f'Creating channel {channel_number}')
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
        if not self._state == ConnectionState.PROTOCOL or \
            self._state == ConnectionState.OPENING:
            raise exceptions.ConnectionWrongStateError(
                'Start loop require an opened connection'
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

    def _create_task(self, coro, exception_handler):
        def _result_handler(f):
            try:
                f.result()
            except BaseException as ex:
                exception_handler(ex)

        t = asyncio.create_task(coro)
        t.add_done_callback(_result_handler)
        return t

    async def _dispatch_frame(self, frame_value: frame.Frame):
        return self._create_task(
            super()._dispatch_frame(frame_value),
            self._handle_frame_ex
        )

    async def _deliver_frame_to_channel(self, frame_value):
        return self._create_task(
            super()._deliver_frame_to_channel(frame_value),
            self._handle_deliver_ex
        )

    def _handle_frame_ex(self, ex):
        LOGGER.warning(f'During frame dispatch an exception occourred: {ex}')
        LOGGER.debug(traceback.print_tb(ex.__traceback__))

    def _handle_deliver_ex(self, ex):
        LOGGER.warning(f'During deliver dispatch an exception occourred: {ex}')
        LOGGER.debug(traceback.print_tb(ex.__traceback__))

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

