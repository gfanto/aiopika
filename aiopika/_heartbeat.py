import asyncio
import logging

from . import exceptions
from ._frame import Heartbeat


LOGGER = logging.getLogger(__name__)


class HeartbeatChecker:
    def __init__(self, connection, timeout):
        if timeout < 1:
            raise ValueError(f'timeout must be > 0, but got: {timeout}')

        self._connection = connection
        self._timeout = timeout

        self._send_interval = float(timeout) / 2.0
        self._check_interval = timeout + 5

        LOGGER.debug(
            f'timeout: {self._timeout} send_interval: {self._send_interval} '
            f'check_interval: {self._check_interval}'
        )

        self._bytes_received = 0
        self._bytes_sent = 0
        self._heartbeat_frames_received = 0
        self._heartbeat_frames_sent = 0
        self._idle_byte_intervals = 0

        self._sender = None
        self._checker = None

        # self._heartbeat_received = asyncio.Event()

    @property
    def bytes_received_on_connection(self):
        return self._connection.bytes_received

    @property
    def connection_is_idle(self):
        return self._idle_byte_intervals > 0

    @property
    def _has_received_data(self):
        return self._bytes_received != self.bytes_received_on_connection

    @staticmethod
    def _new_heartbeat_frame():
        return Heartbeat()

    def _update_counters(self):
        self._bytes_sent = self._connection.bytes_sent
        self._bytes_received = self._connection.bytes_received

    def received(self):
        LOGGER.debug('Received heartbeat frame')
        self._heartbeat_frames_received += 1
        # self._heartbeat_received.set()

    async def _send_heartbeat_frame(self):
        LOGGER.debug('Sending heartbeat frame')
        await self._connection._send_frame(Heartbeat())
        self._heartbeat_frames_sent += 1

    async def _heartbeat_sender(self):
        while not self._connection.is_closed:
            await asyncio.sleep(self._send_interval)
            await self._send_heartbeat_frame()

    async def _heartbeat_checker(self):
        while not self._connection.is_closed:
            self._update_counters()
            await asyncio.sleep(self._check_interval)
            if self._has_received_data:
                self._idle_byte_intervals = 0
            else:
                self._idle_byte_intervals += 1

            LOGGER.debug(
                f'Received {self._heartbeat_frames_received} heartbeat frames, '
                f'sent {self._heartbeat_frames_sent}, '
                f'idle intervals {self._idle_byte_intervals}'
            )

            if self.connection_is_idle:
                self._close_connection()
                return

        # while not self._connection.is_closed:
        #     self._update_counters()
        #     try:
        #         await asyncio.wait_for(
        #             self._heartbeat_received.wait(),
        #             self._check_interval
        #         )
        #         if self._has_received_data:
        #             self._idle_byte_intervals = 0
        #         else:
        #             self._idle_byte_intervals += 1

        #         LOGGER.debug(
        #             f'Received {self._heartbeat_frames_received} heartbeat '
        #             f'frames, sent {self._heartbeat_frames_sent}, '
        #             f'idle intervals {self._idle_byte_intervals}'
        #         )
        #         self._heartbeat_received.clear()
        #     except asyncio.TimeoutError:
        #         self._close_connection()
        #         return

    def _close_connection(self):
        LOGGER.info(
            f'Connection is idle, '
            f'{self._idle_byte_intervals} stale byte intervals',
        )

        self._connection.terminate(
            exceptions.AMQPHeartbeatTimeout(
                f'No activity or too many missed heartbeats '
                f'in the last {self._timeout} seconds'
            )
        )

    def start(self):
        self._sender = asyncio.create_task(self._heartbeat_sender())
        self._checker = asyncio.create_task(self._heartbeat_checker())

    def stop(self):
        if not self._sender.done():
            self._sender.cancel()
        else:
            self._sender.result()
        if not self._checker.done():
            self._checker.cancel()
        else:
            self._checker.result()

