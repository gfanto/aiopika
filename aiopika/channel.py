import collections
import aiologger
import uuid
import enum
import asyncio

from typing import (
    Callable,
    Iterable,
    Union,
    Dict,
    List,
    Set,
    Coroutine,
    Optional
)
from inspect import iscoroutinefunction

from . import frame
from . import exceptions
from . import spec
from . import amqp_object

from ._frame import (
    is_method,
    is_header,
    is_body,
    has_content,
    is_method_instance_of
)
from ._dispatch import EventDispatcherObject, Waiter


__all__ = ['Channel']


LOGGER = aiologger.Logger.with_default_handlers(name=__name__)


class _ContentFrameAssembler(object):
    _method_frame: frame.Frame
    _header_frame: frame.Frame
    _body_fragments: List[bytes]

    def __init__(self):
        self._reset()

    @property
    def method_frame(self):
        return self._method_frame

    @method_frame.setter
    def method_frame(self, frame_value: frame.Method):
        self._method_frame = frame_value

    @property
    def header_frame(self):
        return self._header_frame

    @header_frame.setter
    def header_frame(self, frame_value):
        self._header_frame = frame_value
        if frame_value.body_size == 0:
            self._ready = True

    @property
    def body_fragments(self):
        return self._body_fragments

    @property
    def ready(self):
        return self._ready

    def append_fragments(self, body_frame):
        self._seen_so_far += len(body_frame.fragment)
        self._body_fragments.append(body_frame.fragment)
        if self._seen_so_far == self._header_frame.body_size:
            self._ready = True
        elif self._seen_so_far > self._header_frame.body_size:
            raise exceptions.BodyTooLongError(self._seen_so_far,
                                              self._header_frame.body_size)

    def assemble(self):
        content = (self._method_frame, self._header_frame,
                   b''.join(self._body_fragments))
        self._reset()
        return content

    def _reset(self):
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()
        self._ready = False


class Channel(EventDispatcherObject):
    _cancelled: Set[str]
    _consumers: Dict[str, Callable]
    _consumers_with_noack: Set[str]

    _closing_reason: Optional[BaseException]

    __flowok_callback: Optional[Callable]
    __ack_nack_callback: Optional[Callable]
    __return_callback: Optional[Callable]
    __getok_callback: Optional[Callable]

    __frame_waiter: Optional[Waiter]

    @enum.unique
    class ChannelState(enum.Enum):
        CLOSE    = 'CLOSE'
        OPENING  = 'OPENING'
        OPEN     = 'OPEN'
        CLOSING  = 'CLOSING'

    def __init__(self, connection, channel_number: int):
        super(Channel, self).__init__()

        if channel_number <= 0:
            raise exceptions.InvalidChannelNumber

        self.channel_number = channel_number
        self.connection = connection
        self.flow_active = True

        self._content_assembler = _ContentFrameAssembler()

        self._cancelled = set()
        self._consumers = dict()
        self._consumers_with_noack = set()
        self._state = self.ChannelState.CLOSE
        self._closing_reason = None

        self.__getok_callback = None
        self.__ack_nack_callback = None
        self.__flowok_callback = None

        self.__rpc_lock = asyncio.Lock()
        self.__frame_waiter = None

    def __int__(self):
        return self.channel_number

    def __repr__(self):
        return '<%s number=%s %s conn=%r>' % (
            self.__class__.__name__, self.channel_number,
            self._state, self.connection)

    def add_on_flow_callback(self, callback: Callable):
        self._validate_coroutine(callback)
        self._has_on_flow_callback = True
        self.__flowok_callback = callback

    def add_on_return_callback(self, callback: Callable):
        self._validate_coroutine(callback)
        self.__return_callback = callback

    async def basic_ack(self, delivery_tag: int = 0, multiple: bool = False):
        self._raise_if_not_open()
        await self._send_method(spec.Basic.Ack(delivery_tag, multiple))

    async def basic_cancel(
        self,
        consumer_tag: str = '',
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        if consumer_tag in self._cancelled:
            LOGGER.warning(
                f'basic_cancel - consumer is already cancelling: {consumer_tag}'
            )
            return

        if consumer_tag not in self._consumers:
            LOGGER.warning(f'basic_cancel - consumer not found: {consumer_tag}')
            return

        LOGGER.debug(f'Cancelling consumer: {consumer_tag} (nowait={nowait})')

        if nowait:
            del self._consumers[consumer_tag]

        self._cancelled.add(consumer_tag)

        await self._rpc(
            spec.Basic.Cancel(consumer_tag=consumer_tag, nowait=nowait),
            [spec.Basic.CancelOk] if not nowait else [],
            callback,
            kwargs={'consumer_tag': consumer_tag}
        )

    async def basic_consume(
        self,
        queue: str,
        on_message_callback: Callable,
        auto_ack: bool = False,
        exclusive: bool = False,
        consumer_tag: str = '',
        arguments: dict = {},
        callback: Callable = None
    ):
        self._validate_coroutine(callback, on_message_callback)
        self._raise_if_not_open()
        nowait = callback is None

        if not consumer_tag:
            consumer_tag = self._generate_consumer_tag()

        if consumer_tag in self._consumers or consumer_tag in self._cancelled:
            raise exceptions.DuplicateConsumerTag(consumer_tag)

        if auto_ack:
            self._consumers_with_noack.add(consumer_tag)

        self._consumers[consumer_tag] = on_message_callback

        await self._rpc(
            spec.Basic.Consume(
                queue=queue,
                consumer_tag=consumer_tag,
                no_ack=auto_ack,
                exclusive=exclusive,
                arguments=arguments
            ),
            [spec.Basic.ConsumeOk],
            callback,
            kwargs={'consumer_tag': consumer_tag}
        )

        return consumer_tag

    def _generate_consumer_tag(self):
        return 'ctag%i.%s' % (self.channel_number, uuid.uuid4().hex)

    async def basic_get(
        self,
        queue: str,
        callback: Callable,
        auto_ack: bool = False
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        if self.__getok_callback is not None:
            raise exceptions.DuplicateGetOkCallback()
        self.__getok_callback = callback
        await self._send_method(spec.Basic.Get(queue=queue, no_ack=auto_ack))

    async def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        self._raise_if_not_open()
        await self._send_method(
            spec.Basic.Nack(delivery_tag, multiple, requeue)
        )

    async def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties: spec.BasicProperties = None,
        mandatory: bool = False
    ):
        self._raise_if_not_open()
        properties = properties or spec.BasicProperties()

        await self._send_method(
            spec.Basic.Publish(
                exchange=exchange,
                routing_key=routing_key,
                mandatory=mandatory
            ),
            (properties, body)
        )

    async def basic_qos(
        self,
        prefetch_size: int = 0,
        prefetch_count: int = 0,
        global_qos: bool = False,
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()

        if prefetch_size < 0:
            raise ValueError(f'prefetch_size must be >= 0')
        if prefetch_count < 0:
            raise ValueError(f'prefetch_count must be >= 0')

        await self._rpc(
            spec.Basic.Qos(prefetch_size, prefetch_count, global_qos),
            [spec.Basic.QosOk],
            callback
        )

    async def basic_reject(self, delivery_tag: int, requeue: bool = True):
        self._raise_if_not_open()
        await self._send_method(spec.Basic.Reject(delivery_tag, requeue))

    async def basic_recover(
        self,
        requeue: bool = False,
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        await self._rpc(
            spec.Basic.Recover(requeue),
            [spec.Basic.RecoverOk],
            callback
        )

    async def _on_basic_ack(self, method_frame: frame.Method):
        if self.__ack_nack_callback is None:
            LOGGER.error('Ack received without ack callback')
        else:
            await self._dispatch_callback(self.__ack_nack_callback, method_frame)

    async def _on_basic_nack(self, method_frame: frame.Method):
        await self._on_basic_ack(method_frame)

    async def _on_basic_qosok(self, method_frame: frame.Method):
        LOGGER.debug(f'Basic.QosOk Received: {method_frame}')

    async def _on_basic_consumeok(self, method_frame: frame.Method):
        LOGGER.debug(f'Basic.ConsumeOk Received: {method_frame}')

    async def _on_basic_cancel(self, method_frame:  frame.Method):
        if method_frame.method.consumer_tag in self._cancelled:
            return
        self._cleanup_consumer_ref(method_frame.method.consumer_tag)

    async def _on_basic_cancelok(self, method_frame: frame.Method):
        self._cleanup_consumer_ref(method_frame.method.consumer_tag)

    async def _on_basic_recoverok(self, method_frame: frame.Method):
        LOGGER.debug(f'Basic.RecoverOk Received: {method_frame}')

    async def _on_basic_getempty(self, method_frame: frame.Method):
        LOGGER.debug(f'Received Basic.GetEmpty: {method_frame}')
        self.__getok_callback = None

    #@has_content
    async def _on_basic_getok(self, method_frame, header_frame, body):
        if self.__getok_callback is not None:
            callback = self.__getok_callback
            self.__getok_callback = None
            await self._dispatch_callback(
                callback,
                method_frame.method,
                header_frame.properties,
                body
            )
        else:
            LOGGER.error('Basic.GetOk received with no active callback')

    #@has_content
    async def _on_basic_deliver(self, method_frame, header_frame, body):
        consumer_tag = method_frame.method.consumer_tag

        if consumer_tag in self._cancelled:
            if self.is_open and consumer_tag not in self._consumers_with_noack:
                await self.basic_reject(method_frame.method.delivery_tag)
            return

        if consumer_tag not in self._consumers:
            LOGGER.error(f'Unexpected delivery: {method_frame}')
            return

        await self._dispatch_consumer(
            self._consumers[consumer_tag],
            self,
            method_frame.method,
            header_frame.properties,
            body
        )

    #@has_content
    async def _on_basic_return(self, method_frame, header_frame, body):
        if self.__return_callback is not None:
            await self._dispatch_callback(
                self.__return_callback,
                method_frame.method,
                header_frame.properties,
                body
            )
            LOGGER.warning(
                'Basic.Return received from server '
                f'({method_frame.method}, {header_frame.properties})'
            )
        else:
            LOGGER.error(
                'Received Basic.Return without return callback, '
                f'discarding message {method_frame} {header_frame} {body}'
            )

    async def confirm_delivery(
        self,
        ack_nack_callback: Callable,
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        if not (self.connection.publisher_confirms and
                self.connection.basic_nack):
            raise exceptions.MethodNotImplemented(
                'Confirm.Select not Supported by Server')

        self.__ack_nack_callback = ack_nack_callback

        await self._rpc(
            spec.Confirm.Select(nowait),
            [spec.Confirm.SelectOk] if not nowait else [],
            callback
        )

    async def _on_confirm_selectok(self, method_frame):
        LOGGER.debug(f'Confirm.SelectOk Received: {method_frame}')

    @property
    def consumer_tags(self):
        return list(self._consumers.keys())

    async def exchange_bind(
        self,
        destination: str,
        source: str,
        routing_key: str = '',
        arguments: dict = {},
        callback: Callable = None
):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        await self._rpc(
            spec.Exchange.Bind(
                0,
                destination,
                source,
                routing_key,
                nowait,
                arguments
            ),
            [spec.Exchange.BindOk] if not nowait else [],
            callback
        )

    async def exchange_declare(
        self,
        exchange: str,
        exchange_type: str = 'direct',
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        arguments: dict = {},# @[!!!] replaced with None
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        await self._rpc(
            spec.Exchange.Declare(0, exchange, exchange_type, passive, durable,
                                  auto_delete, internal, nowait, arguments),
            [spec.Exchange.DeclareOk] if not nowait else [],
            callback
        )

    async def exchange_delete(
        self,
        exchange: str = None,
        if_unused: bool = False,
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        await self._rpc(
            spec.Exchange.Delete(0, exchange, if_unused, nowait),
            [spec.Exchange.DeleteOk] if not nowait else [],
            callback
        )

    async def exchange_unbind(
        self,
        destination: str = None,
        source: str = None,
        routing_key: str = '',
        arguments: dict = {},
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        await self._rpc(
            spec.Exchange.Unbind(
                0,
                destination,
                source,
                routing_key,
                nowait,
                arguments
            ),
            [spec.Exchange.UnbindOk] if not nowait else [],
            callback
        )

    async def flow(self, active: bool, callback: Callable):
        self._validate_coroutine(callback)
        self._raise_if_not_open()

        self.__flowok_callback = callback
        await self._rpc(
            spec.Channel.Flow(active),
            [spec.Channel.FlowOk]
        )

    async def _on_exchange_bindok(self,  method_frame):
        LOGGER.debug(f'Exchange.BindOk Received: {method_frame}')

    async def _on_exchange_unbindok(self, method_frame):
        LOGGER.debug(f'Exchange.UnbindOk Received: {method_frame}')

    async def _on_exchange_declareok(self, method_frame):
        LOGGER.debug(f'Exchange.DeclareOk Received: {method_frame}')

    async def _on_exchange_deleteok(self, method_frame):
        LOGGER.debug(f'Exchange.DeleteOk Received: {method_frame}')

    @property
    def is_closed(self):
        return self._state == self.ChannelState.CLOSE

    @property
    def is_closing(self):
        return self._state == self.ChannelState.CLOSING

    @property
    def is_open(self):
        return self._state == self.ChannelState.OPEN

    async def open(self):
        if not self.is_closed:
            raise exceptions.ChannelWrongStateError(
                'Channel open was called but channel is not open'
            )

        self._set_channel_state(self.ChannelState.OPENING)
        await self._rpc(spec.Channel.Open(), [spec.Channel.OpenOk])

    async def _remove_consumers(self):
        await asyncio.gather(
            *(self.basic_cancel(consumer_tag=tag)
                for tag in self._consumers if tag not in self._cancelled
            )
        )

    async def close(
        self,
        reply_code: int = 0,
        reply_text: str = "Normal shutdown"
    ):
        self._raise_if_not_open()

        LOGGER.info(f'Closing channel ({reply_code}): {reply_text} on {self}')
        self._closing_reason = exceptions.ChannelClosedByClient(
            reply_code,
            reply_text
        )

        await self._remove_consumers()
        self._set_channel_state(self.ChannelState.CLOSING)

        await self._rpc(
            spec.Channel.Close(reply_code, reply_text, 0, 0),
            [spec.Channel.CloseOk]
        )

    async def queue_bind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = None,
        arguments: dict = {},
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        if routing_key is None:
            routing_key = queue
        await self._rpc(
            spec.Queue.Bind(0, queue, exchange, routing_key, nowait, arguments),
            [spec.Queue.BindOk] if not nowait else [],
            callback
        )

    async def queue_declare(
        self,
        queue: str,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict = {},
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        await self._rpc(
            spec.Queue.Declare(0, queue, passive, durable, exclusive,
                               auto_delete, nowait, arguments),
            [spec.Queue.DeclareOk] if not nowait else [],
            callback
        )

    async def queue_delete(
        self,
        queue,
        if_unused: bool = False,
        if_empty: bool = False,
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback is None

        await self._rpc(
            spec.Queue.Delete(0, queue, if_unused, if_empty, nowait),
            [spec.Queue.DeleteOk] if not nowait else [],
            callback
        )

    async def queue_purge(self, queue: str, callback: Callable = None):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        nowait = callback  is None

        await self._rpc(spec.Queue.Purge(0, queue, nowait),
            [spec.Queue.PurgeOk] if callback else [],
            callback

        )

    async def queue_unbind(
        self,
        queue: str,
        exchange: str = None,
        routing_key: str = None,
        arguments: dict = {},
        callback: Callable = None
    ):
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        #non ce il nowait??

        if routing_key is None:
            routing_key = queue
        await self._rpc(
            spec.Queue.Unbind(0, queue, exchange, routing_key, arguments),
            [spec.Queue.UnbindOk],
            callback
        )

    async def _on_queue_declareok(self, method_frame):
        LOGGER.debug(f'Queue.DeclareOk Received: {method_frame}')

    async def _on_queue_bindok(self, method_frame):
        LOGGER.debug(f'Queue.BindOk Received: {method_frame}')

    async def _on_queue_unbindok(self, method_frame):
        LOGGER.debug(f'Queue.UnbindOk Received: {method_frame}')

    async def _on_queue_purgeok(self, method_frame):
        LOGGER.debug(f'Queue.PurgeOk Received: {method_frame}')

    async def _on_queue_deleteok(self, method_frame):
        LOGGER.debug(f'Queue.DeleteOk Received: {method_frame}')

    async def tx_commit(self, callback: Callable = None):#ma un po di nowait qui??
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        await self._rpc(spec.Tx.Commit(), [spec.Tx.CommitOk], callback)

    async def tx_rollback(self, callback: Callable = None):#ma un po di nowait qui??
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        await self._rpc(spec.Tx.Rollback(), [spec.Tx.RollbackOk], callback)

    async def tx_select(self, callback: Callable = None):#ma un po di nowait qui??
        self._validate_coroutine(callback)
        self._raise_if_not_open()
        await self._rpc(spec.Tx.Select(), [spec.Tx.SelectOk], callback)

    async def _on_tx_selectok(self, method_frame: frame.Method):
        LOGGER.debug(f'Tx.SelectOk Received: {method_frame}')

    async def _on_tx_rollbackok(self, method_frame: frame.Method):
        LOGGER.debug(f'Tx.RollbackOk Received: {method_frame}')

    async def _on_tx_commitok(self, method_frame: frame.Method):
        LOGGER.debug(f'Tx.CommitOk Received: {method_frame}')

    def _cleanup(self):
        self.connection._channel_cleanup(self)
        self._consumers = dict()

    def _cleanup_consumer_ref(self, consumer_tag: str):
        self._consumers_with_noack.discard(consumer_tag)
        self._consumers.pop(consumer_tag, None)
        self._cancelled.discard(consumer_tag)

    def _transition_to_closed(self):
        assert not self.is_closed
        assert self._closing_reason is not None

        self._set_channel_state(self.ChannelState.CLOSE)
        self._cleanup()

    async def _on_channel_close(self, method_frame):
        reply_code = method_frame.method.reply_code
        reply_text = method_frame.method.reply_text
        LOGGER.warning(f'Received remote Channel.Close ({reply_code}):'
                       f' {reply_text} on {self}'
        )
        assert not self.is_closed

        await self._send_method(spec.Channel.CloseOk())
        self._closing_reason = exceptions.ChannelClosedByBroker(
            method_frame.method.reply_code, method_frame.method.reply_text)

        if not self.is_closing:
            self._transition_to_closed()
        elif self.is_closed:
            raise exceptions.ConnectionWrongStateError(
                f'Channel {self.channel_number} is closed'
            )

    def _close_meta(self, reason: BaseException):
        LOGGER.debug(f'Handling meta-close on {self}: {reason}')

        if not self.is_closed:
            self._closing_reason = reason
            if not self.is_closing:
                self._transition_to_closed()

    async def _on_channel_closeok(self, method_frame: frame.Method):
        LOGGER.info(f'Received {method_frame.method} on {self}')
        self._transition_to_closed()

    async def _on_channel_flow(self, _method_frame_unused: frame.Method):
        if self._has_on_flow_callback is False:
            LOGGER.warning('Channel.Flow received from server')

    async def _on_channel_flowok(self, method_frame: frame.Method):
        self.flow_active = method_frame.method.active
        if self.__flowok_callback:
            await self._dispatch_callback(
                self.__flowok_callback,
                method_frame.method.active
            )
            self.__flowok_callback = None
        else:
            LOGGER.warning('Channel.FlowOk received with no active callbacks')

    async def _on_channel_openok(self, method_frame: frame.Method):
        if self.is_closing:
            LOGGER.debug(f'Suppressing while in closing state: {method_frame}')
        elif self.is_open:
            LOGGER.debug(f'Got open-ok while already open: {method_frame}')
        else:
            LOGGER.info(f'Channel open: {self}')
            self._set_channel_state(self.ChannelState.OPEN)

    async def _dispatch_consumer(
        self,
        on_message_callback,
        channel,
        method_frame,
        header_frame,
        body
    ):
        return await on_message_callback(
            channel,
            method_frame,
            header_frame,
            body
        )

    async def _dispatch_callback(self, callback, *args, **kwargs):
        assert iscoroutinefunction(callback)
        return await callback(*args, **kwargs)

    async def _dispatch_frame(self, frame_value: frame.Frame):
        return await self._dispatch_event(frame_value)(frame_value)

    async def _dispatch_method(self, method_frame, header_frame, body):
        return await self._dispatch_event(method_frame)(
            method_frame,
            header_frame,
            body
        )

    async def _handle_frame(self, frame_value: frame.Frame):
        if is_method(frame_value):
            if has_content(frame_value):
                self._content_assembler.method_frame = frame_value
            else:
                await self._dispatch_frame(frame_value)
        elif is_header(frame_value):
            self._content_assembler.header_frame = frame_value
            if self._content_assembler.ready:
                await self._handle_content_frame(
                    *self._content_assembler.assemble()
                )
        elif is_body(frame_value):
            self._content_assembler.append_fragments(frame_value)
            if self._content_assembler.ready:
                await self._handle_content_frame(
                    *self._content_assembler.assemble()
                )
        else:
            LOGGER.error(f'Unexpected frame: {frame_value}')
            raise exceptions.UnexpectedFrameError(frame_value)

        if self.__frame_waiter is not None:
            self.__frame_waiter.check(frame_value)

    async def _handle_content_frame(self, method_frame, header_frame, body):
        await self._dispatch_method(method_frame, header_frame, body)

    async def _wait_for_reply(self, acceptable_replies: Iterable):
        assert self.__frame_waiter is None, 'Trying to override frame waiter'
        self.__frame_waiter = Waiter(
            lambda frame_value: is_method_instance_of(
                self.channel_number, acceptable_replies, frame_value
            )
        )

        await self.__frame_waiter.wait()
        self.__frame_waiter = None

    async def _rpc(
        self,
        method: amqp_object.Method,
        acceptable_replies: Iterable,
        callback: Callable = None,
        args=(),
        kwargs={}
    ):
        assert method.synchronous, (
            f'Only synchronous-capable methods may be used with _rpc: {method}'
        )

        async with self.__rpc_lock:
            await self._send_method(method)

            if acceptable_replies:
                await self._wait_for_reply(acceptable_replies)
                if callback:
                    await self._dispatch_callback(callback, *args, **kwargs)

    def _raise_if_not_open(self):
        if self._state == self.ChannelState.OPEN:
            return
        if self._state == self.ChannelState.OPENING:
            raise exceptions.ChannelWrongStateError(
                'Channel is opening, but is not usable yet.')
        elif self._state == self.ChannelState.CLOSING:
            raise exceptions.ChannelWrongStateError('Channel is closing.')
        else:
            assert self._state == self.ChannelState.CLOSE
            raise exceptions.ChannelWrongStateError('Channel is closed.')

    def _validate_coroutine(self, *args):
        for callback in filter(None, args):
            if not iscoroutinefunction(callback):
                raise TypeError('Callback must be async function')

    async def _send_method(
        self,
        method: amqp_object.Method,
        content: tuple = None
    ):
        await self.connection._send_method(self.channel_number, method, content)

    def _set_channel_state(self, state):
        LOGGER.debug(f'New channel state: {state} (prev={self._state})')
        self._state = state

