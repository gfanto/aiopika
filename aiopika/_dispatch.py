import re
import asyncio
import typing
import logging

from inspect import isclass
from copy import copy
from typing import Callable, Union, Dict

from . import spec
from . import amqp_object
from . import exceptions
from .frame import get_key


__all__ = ['EventManagerObject', 'Waiter']


LOGGER = logging.getLogger(__name__)


class Waiter(asyncio.Event):
    def __init__(self, predicate=lambda: True, *, loop=None):
        super(Waiter, self).__init__(loop=loop)

        self._predicate = predicate
        self._waiting = False

    def check(self, *args, **kwargs):
        result = self._predicate(*args, **kwargs)

        if result:
            self.set()
        return result

    @property
    def is_waiting(self):
        return self._waiting

    async def wait(self):
        self._waiting = True
        await super(Waiter, self).wait()
        self._waiting = False

    def clear(self):
        self._waiting = False
        return super(Waiter, self).clear()


class EventDispatcherObject:

    EVENT_PREFIX = '_on_'
    EVENT_REGEX  = re.compile(EVENT_PREFIX + r'(:?\w+_)*?[0-9A-Za-z]+')

    _method_to_callback_name = dict()
    for class_name in dir(spec):
        class_ = getattr(spec, class_name)
        if isclass(class_) and issubclass(class_, amqp_object.Class):
            for method_name in dir(class_):
                method = getattr(class_, method_name)
                if isclass(method) and issubclass(method, amqp_object.Method):
                    _method_to_callback_name[method] = (
                        f'_on_{class_name.lower()}_{method_name.lower()}'
                    )

    try:
        del class_name
    except NameError:
        pass
    try:
        del class_
    except NameError:
        pass
    try:
        del method_name
    except NameError:
        pass
    try:
        del method
    except NameError:
        pass

    def __inspect_event_callbacks(self):
        for method in self._method_to_callback_name:
            self.__dispatcher[method] = getattr(
                self,
                self._method_to_callback_name[method],
                None
            )
        for attr in dir(self):
            if attr.startswith(self.EVENT_PREFIX) and \
                attr not in self._method_to_callback_name.values():
                LOGGER.warning('%s starts with event prefix', attr)

    def __init__(self):
        self.__dispatcher = dict()
        self.__inspect_event_callbacks()

    def _dispatch_event(
        self,
        event,
        apply: Callable = get_key
    ):
        try:
            return self.__dispatcher[apply(event)]
        except KeyError:
            raise exceptions.UnexpectedFrameError(event)


def create_task(coro, exception_handler):
    def result_handler(f):
        try:
            f.result()
        except asyncio.CancelledError:
            pass
        except BaseException as ex:
            exception_handler(f, f.exception())

    t = asyncio.create_task(coro)
    t.add_done_callback(result_handler)
    return t

