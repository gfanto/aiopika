# Copyright
#  (c) 2009-2019, Tony Garnock-Jones, Gavin M. Roy, Pivotal Software, Inc and others.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:

#  * Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#  * Neither the name of the Pika project nor the names of its contributors may be used
#    to endorse or promote products derived from this software without specific
#    prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import ast
import copy
import logging
import numbers
import ssl

from urllib.parse import (
    unquote as url_unquote,
    parse_qs as url_parse_qs,
    urlparse
)

from .channel import MAX_CHANNELS
from . import spec
from . import credentials


__all__ = ['Parameters', 'ConnectionParameters', 'URLParameters', 'SSLOptions']

LOGGER = logging.getLogger(__name__)



class Parameters(object):  # pylint: disable=R0902
    """Base connection parameters class definition

    """

    # Declare slots to protect against accidental assignment of an invalid
    # attribute
    __slots__ = ('_blocked_connection_timeout', '_channel_max',
                 '_client_properties', '_connection_attempts', '_credentials',
                 '_frame_max', '_heartbeat', '_host', '_locale', '_port',
                 '_retry_delay', '_socket_timeout', '_stack_timeout',
                 '_ssl_options', '_virtual_host', '_tcp_options')

    DEFAULT_USERNAME = 'guest'
    DEFAULT_PASSWORD = 'guest'

    DEFAULT_BLOCKED_CONNECTION_TIMEOUT = None
    DEFAULT_CHANNEL_MAX = MAX_CHANNELS
    DEFAULT_CLIENT_PROPERTIES = None
    DEFAULT_CREDENTIALS = credentials.PlainCredentials(
        DEFAULT_USERNAME, DEFAULT_PASSWORD)
    DEFAULT_CONNECTION_ATTEMPTS = 1
    DEFAULT_FRAME_MAX = spec.FRAME_MAX_SIZE
    DEFAULT_HEARTBEAT_TIMEOUT = None  # None accepts server's proposal
    DEFAULT_HOST = 'localhost'
    DEFAULT_LOCALE = 'en_US'
    DEFAULT_PORT = 5672
    DEFAULT_RETRY_DELAY = 2.0
    DEFAULT_SOCKET_TIMEOUT = 10.0  # socket.connect() timeout
    DEFAULT_STACK_TIMEOUT = 15.0  # full-stack TCP/[SSl]/AMQP bring-up timeout
    DEFAULT_SSL = False
    DEFAULT_SSL_OPTIONS = None
    DEFAULT_SSL_PORT = 5671
    DEFAULT_VIRTUAL_HOST = '/'
    DEFAULT_TCP_OPTIONS = None

    def __init__(self):
        # If not None, blocked_connection_timeout is the timeout, in seconds,
        # for the connection to remain blocked; if the timeout expires, the
        # connection will be torn down, triggering the connection's
        # on_close_callback
        self._blocked_connection_timeout = None
        self.blocked_connection_timeout = (
            self.DEFAULT_BLOCKED_CONNECTION_TIMEOUT)

        self._channel_max = None
        self.channel_max = self.DEFAULT_CHANNEL_MAX

        self._client_properties = None
        self.client_properties = self.DEFAULT_CLIENT_PROPERTIES

        self._connection_attempts = None
        self.connection_attempts = self.DEFAULT_CONNECTION_ATTEMPTS

        self._credentials = None
        self.credentials = self.DEFAULT_CREDENTIALS

        self._frame_max = None
        self.frame_max = self.DEFAULT_FRAME_MAX

        self._heartbeat = None
        self.heartbeat = self.DEFAULT_HEARTBEAT_TIMEOUT

        self._host = None
        self.host = self.DEFAULT_HOST

        self._locale = None
        self.locale = self.DEFAULT_LOCALE

        self._port = None
        self.port = self.DEFAULT_PORT

        self._retry_delay = None
        self.retry_delay = self.DEFAULT_RETRY_DELAY

        self._socket_timeout = None
        self.socket_timeout = self.DEFAULT_SOCKET_TIMEOUT

        self._stack_timeout = None
        self.stack_timeout = self.DEFAULT_STACK_TIMEOUT

        self._ssl_options = None
        self.ssl_options = self.DEFAULT_SSL_OPTIONS

        self._virtual_host = None
        self.virtual_host = self.DEFAULT_VIRTUAL_HOST

        self._tcp_options = None
        self.tcp_options = self.DEFAULT_TCP_OPTIONS

    def __repr__(self):
        """Represent the info about the instance.

        :rtype: str

        """
        return ('<%s host=%s port=%s virtual_host=%s ssl=%s>' %
                (self.__class__.__name__, self.host, self.port,
                 self.virtual_host, bool(self.ssl_options)))

    def __eq__(self, other):
        if isinstance(other, Parameters):
            return self._host == other._host and self._port == other._port  # pylint: disable=W0212
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is not NotImplemented:
            return not result
        return NotImplemented

    @property
    def blocked_connection_timeout(self):
        """
        :returns: blocked connection timeout. Defaults to
            `DEFAULT_BLOCKED_CONNECTION_TIMEOUT`.
        :rtype: float|None

        """
        return self._blocked_connection_timeout

    @blocked_connection_timeout.setter
    def blocked_connection_timeout(self, value):
        """
        :param value: If not None, blocked_connection_timeout is the timeout, in
            seconds, for the connection to remain blocked; if the timeout
            expires, the connection will be torn down, triggering the
            connection's on_close_callback

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('blocked_connection_timeout must be a Real '
                                'number, but got %r' % (value,))
            if value < 0:
                raise ValueError('blocked_connection_timeout must be >= 0, but '
                                 'got %r' % (value,))
        self._blocked_connection_timeout = value

    @property
    def channel_max(self):
        """
        :returns: max preferred number of channels. Defaults to
            `DEFAULT_CHANNEL_MAX`.
        :rtype: int

        """
        return self._channel_max

    @channel_max.setter
    def channel_max(self, value):
        """
        :param int value: max preferred number of channels, between 1 and
           `channel.MAX_CHANNELS`, inclusive

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('channel_max must be an int, but got %r' % (value,))
        if value < 1 or value > MAX_CHANNELS:
            raise ValueError('channel_max must be <= %i and > 0, but got %r' %
                             (MAX_CHANNELS, value))
        self._channel_max = value

    @property
    def client_properties(self):
        """
        :returns: client properties used to override the fields in the default
            client properties reported  to RabbitMQ via `Connection.StartOk`
            method. Defaults to `DEFAULT_CLIENT_PROPERTIES`.
        :rtype: dict|None

        """
        return self._client_properties

    @client_properties.setter
    def client_properties(self, value):
        """
        :param value: None or dict of client properties used to override the
            fields in the default client properties reported to RabbitMQ via
            `Connection.StartOk` method.
        """
        if not isinstance(value, (
                dict,
                type(None),
        )):
            raise TypeError('client_properties must be dict or None, '
                            'but got %r' % (value,))
        # Copy the mutable object to avoid accidental side-effects
        self._client_properties = copy.deepcopy(value)

    @property
    def connection_attempts(self):
        """
        :returns: number of socket connection attempts. Defaults to
            `DEFAULT_CONNECTION_ATTEMPTS`. See also `retry_delay`.
        :rtype: int

        """
        return self._connection_attempts

    @connection_attempts.setter
    def connection_attempts(self, value):
        """
        :param int value: number of socket connection attempts of at least 1.
            See also `retry_delay`.

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('connection_attempts must be an int')
        if value < 1:
            raise ValueError(
                'connection_attempts must be > 0, but got %r' % (value,))
        self._connection_attempts = value

    @property
    def credentials(self):
        """
        :rtype: one of the classes from `aiopika.credentials.VALID_TYPES`. Defaults
            to `DEFAULT_CREDENTIALS`.

        """
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        """
        :param value: authentication credential object of one of the classes
            from  `aiopika.credentials.VALID_TYPES`

        """
        if not isinstance(value, tuple(credentials.VALID_TYPES)):
            raise TypeError('credentials must be an object of type: %r, but '
                            'got %r' % (credentials.VALID_TYPES, value))
        # Copy the mutable object to avoid accidental side-effects
        self._credentials = copy.deepcopy(value)

    @property
    def frame_max(self):
        """
        :returns: desired maximum AMQP frame size to use. Defaults to
            `DEFAULT_FRAME_MAX`.
        :rtype: int

        """
        return self._frame_max

    @frame_max.setter
    def frame_max(self, value):
        """
        :param int value: desired maximum AMQP frame size to use between
            `spec.FRAME_MIN_SIZE` and `spec.FRAME_MAX_SIZE`, inclusive

        """
        if not isinstance(value, numbers.Integral):
            raise TypeError('frame_max must be an int, but got %r' % (value,))
        if value < spec.FRAME_MIN_SIZE:
            raise ValueError('Min AMQP 0.9.1 Frame Size is %i, but got %r' % (
                spec.FRAME_MIN_SIZE,
                value,
            ))
        elif value > spec.FRAME_MAX_SIZE:
            raise ValueError('Max AMQP 0.9.1 Frame Size is %i, but got %r' % (
                spec.FRAME_MAX_SIZE,
                value,
            ))
        self._frame_max = value

    @property
    def heartbeat(self):
        """
        :returns: AMQP connection heartbeat timeout value for negotiation during
            connection tuning or callable which is invoked during connection tuning.
            None to accept broker's value. 0 turns heartbeat off. Defaults to
            `DEFAULT_HEARTBEAT_TIMEOUT`.
        :rtype: int|callable|None

        """
        return self._heartbeat

    @heartbeat.setter
    def heartbeat(self, value):
        """
        :param int|None|callable value: Controls AMQP heartbeat timeout negotiation
            during connection tuning. An integer value always overrides the value
            proposed by broker. Use 0 to deactivate heartbeats and None to always
            accept the broker's proposal. If a callable is given, it will be called
            with the connection instance and the heartbeat timeout proposed by broker
            as its arguments. The callback should return a non-negative integer that
            will be used to override the broker's proposal.
        """
        if value is not None:
            if not isinstance(value, numbers.Integral) and not callable(value):
                raise TypeError(
                    'heartbeat must be an int or a callable function, but got %r'
                    % (value,))
            if not callable(value) and value < 0:
                raise ValueError('heartbeat must >= 0, but got %r' % (value,))
        self._heartbeat = value

    @property
    def host(self):
        """
        :returns: hostname or ip address of broker. Defaults to `DEFAULT_HOST`.
        :rtype: str

        """
        return self._host

    @host.setter
    def host(self, value):
        """
        :param str value: hostname or ip address of broker

        """
        if not isinstance(value, str):
            raise TypeError(
                f'host must be a str or unicode str, but got {value}'
            )
        self._host = value

    @property
    def locale(self):
        """
        :returns: locale value to pass to broker; e.g., 'en_US'. Defaults to
            `DEFAULT_LOCALE`.
        :rtype: str

        """
        return self._locale

    @locale.setter
    def locale(self, value):
        """
        :param str value: locale value to pass to broker; e.g., "en_US"

        """
        if not isinstance(value, str):
            raise TypeError(
                f'host locale be a str or unicode str, but got {value}'
        )
        self._locale = value

    @property
    def port(self):
        """
        :returns: port number of broker's listening socket. Defaults to
            `DEFAULT_PORT`.
        :rtype: int

        """
        return self._port

    @port.setter
    def port(self, value):
        """
        :param int value: port number of broker's listening socket

        """
        try:
            self._port = int(value)
        except (TypeError, ValueError):
            raise TypeError('port must be an int, but got %r' % (value,))

    @property
    def retry_delay(self):
        """
        :returns: interval between socket connection attempts; see also
            `connection_attempts`. Defaults to `DEFAULT_RETRY_DELAY`.
        :rtype: float

        """
        return self._retry_delay

    @retry_delay.setter
    def retry_delay(self, value):
        """
        :param int | float value: interval between socket connection attempts;
            see also `connection_attempts`.

        """
        if not isinstance(value, numbers.Real):
            raise TypeError(
                'retry_delay must be a float or int, but got %r' % (value,))
        self._retry_delay = value

    @property
    def socket_timeout(self):
        """
        :returns: socket connect timeout in seconds. Defaults to
            `DEFAULT_SOCKET_TIMEOUT`. The value None disables this timeout.
        :rtype: float|None

        """
        return self._socket_timeout

    @socket_timeout.setter
    def socket_timeout(self, value):
        """
        :param int | float | None value: positive socket connect timeout in
            seconds. None to disable this timeout.

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('socket_timeout must be a float or int, '
                                'but got %r' % (value,))
            if value <= 0:
                raise ValueError(
                    'socket_timeout must be > 0, but got %r' % (value,))
            value = float(value)

        self._socket_timeout = value

    @property
    def stack_timeout(self):
        """
        :returns: full protocol stack TCP/[SSL]/AMQP bring-up timeout in
            seconds. Defaults to `DEFAULT_STACK_TIMEOUT`. The value None
            disables this timeout.
        :rtype: float

        """
        return self._stack_timeout

    @stack_timeout.setter
    def stack_timeout(self, value):
        """
        :param int | float | None value: positive full protocol stack
            TCP/[SSL]/AMQP bring-up timeout in seconds. It's recommended to set
            this value higher than `socket_timeout`. None to disable this
            timeout.

        """
        if value is not None:
            if not isinstance(value, numbers.Real):
                raise TypeError('stack_timeout must be a float or int, '
                                'but got %r' % (value,))
            if value <= 0:
                raise ValueError(
                    'stack_timeout must be > 0, but got %r' % (value,))
            value = float(value)

        self._stack_timeout = value

    @property
    def ssl_options(self):
        """
        :returns: None for plaintext or `aiopika.SSLOptions` instance for SSL/TLS.
        :rtype: `aiopika.SSLOptions`|None
        """
        return self._ssl_options

    @ssl_options.setter
    def ssl_options(self, value):
        """
        :param `aiopika.SSLOptions`|None value: None for plaintext or
            `aiopika.SSLOptions` instance for SSL/TLS. Defaults to None.

        """
        if not isinstance(value, (SSLOptions, type(None))):
            raise TypeError(
                'ssl_options must be None or SSLOptions but got %r' % (value,))
        self._ssl_options = value

    @property
    def virtual_host(self):
        """
        :returns: rabbitmq virtual host name. Defaults to
            `DEFAULT_VIRTUAL_HOST`.
        :rtype: str

        """
        return self._virtual_host

    @virtual_host.setter
    def virtual_host(self, value):
        """
        :param str value: rabbitmq virtual host name

        """
        if not isinstance(value, str):
            raise TypeError(
                f'virtual_host must be a str or unicode str, but got {value}'
        )
        self._virtual_host = value

    @property
    def tcp_options(self):
        """
        :returns: None or a dict of options to pass to the underlying socket
        :rtype: dict|None
        """
        return self._tcp_options

    @tcp_options.setter
    def tcp_options(self, value):
        """
        :param dict|None value: None or a dict of options to pass to the underlying
            socket. Currently supported are TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT
            and TCP_USER_TIMEOUT. Availability of these may depend on your platform.
        """
        if not isinstance(value, (dict, type(None))):
            raise TypeError(
                'tcp_options must be a dict or None, but got %r' % (value,))
        self._tcp_options = value


class ConnectionParameters(Parameters):
    """Connection parameters object that is passed into the connection adapter
    upon construction.

    """

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ()

    class _DEFAULT(object):
        """Designates default parameter value; internal use"""

    def __init__( # pylint: disable=R0913,R0914
            self,
            host=_DEFAULT,
            port=_DEFAULT,
            virtual_host=_DEFAULT,
            credentials=_DEFAULT,
            channel_max=_DEFAULT,
            frame_max=_DEFAULT,
            heartbeat=_DEFAULT,
            ssl_options=_DEFAULT,
            connection_attempts=_DEFAULT,
            retry_delay=_DEFAULT,
            socket_timeout=_DEFAULT,
            stack_timeout=_DEFAULT,
            locale=_DEFAULT,
            blocked_connection_timeout=_DEFAULT,
            client_properties=_DEFAULT,
            tcp_options=_DEFAULT,
            **kwds
        ):
        """Create a new ConnectionParameters instance. See `Parameters` for
        default values.

        :param str host: Hostname or IP Address to connect to
        :param int port: TCP port to connect to
        :param str virtual_host: RabbitMQ virtual host to use
        :param aiopika.credentials.Credentials credentials: auth credentials
        :param int channel_max: Maximum number of channels to allow
        :param int frame_max: The maximum byte size for an AMQP frame
        :param int|None|callable heartbeat: Controls AMQP heartbeat timeout negotiation
            during connection tuning. An integer value always overrides the value
            proposed by broker. Use 0 to deactivate heartbeats and None to always
            accept the broker's proposal. If a callable is given, it will be called
            with the connection instance and the heartbeat timeout proposed by broker
            as its arguments. The callback should return a non-negative integer that
            will be used to override the broker's proposal.
        :param `aiopika.SSLOptions`|None ssl_options: None for plaintext or
            `aiopika.SSLOptions` instance for SSL/TLS. Defaults to None.
        :param int connection_attempts: Maximum number of retry attempts
        :param int|float retry_delay: Time to wait in seconds, before the next
        :param int|float socket_timeout: Positive socket connect timeout in
            seconds.
        :param int|float stack_timeout: Positive full protocol stack
            (TCP/[SSL]/AMQP) bring-up timeout in seconds. It's recommended to
            set this value higher than `socket_timeout`.
        :param str locale: Set the locale value
        :param int|float|None blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection:
            passing `ConnectionBlockedTimeout` exception to on_close_callback
            in asynchronous adapters or raising it in `BlockingConnection`.
        :param client_properties: None or dict of client properties used to
            override the fields in the default client properties reported to
            RabbitMQ via `Connection.StartOk` method.
        :param tcp_options: None or a dict of TCP options to set for socket
        """
        super(ConnectionParameters, self).__init__()

        if blocked_connection_timeout is not self._DEFAULT:
            self.blocked_connection_timeout = blocked_connection_timeout

        if channel_max is not self._DEFAULT:
            self.channel_max = channel_max

        if client_properties is not self._DEFAULT:
            self.client_properties = client_properties

        if connection_attempts is not self._DEFAULT:
            self.connection_attempts = connection_attempts

        if credentials is not self._DEFAULT:
            self.credentials = credentials

        if frame_max is not self._DEFAULT:
            self.frame_max = frame_max

        if heartbeat is not self._DEFAULT:
            self.heartbeat = heartbeat

        if host is not self._DEFAULT:
            self.host = host

        if locale is not self._DEFAULT:
            self.locale = locale

        if retry_delay is not self._DEFAULT:
            self.retry_delay = retry_delay

        if socket_timeout is not self._DEFAULT:
            self.socket_timeout = socket_timeout

        if stack_timeout is not self._DEFAULT:
            self.stack_timeout = stack_timeout

        if ssl_options is not self._DEFAULT:
            self.ssl_options = ssl_options

        # Set port after SSL status is known
        if port is not self._DEFAULT:
            self.port = port
        else:
            self.port = self.DEFAULT_SSL_PORT if self.ssl_options else self.DEFAULT_PORT

        if virtual_host is not self._DEFAULT:
            self.virtual_host = virtual_host

        if tcp_options is not self._DEFAULT:
            self.tcp_options = tcp_options

        if kwds:
            raise TypeError('unexpected kwds: %r' % (kwds,))


class URLParameters(Parameters):
    """Connect to RabbitMQ via an AMQP URL in the format::

         amqp://username:password@host:port/<virtual_host>[?query-string]

    Ensure that the virtual host is URI encoded when specified. For example if
    you are using the default "/" virtual host, the value should be `%2f`.

    See `Parameters` for default values.

    Valid query string values are:

        - channel_max:
            Override the default maximum channel count value
        - client_properties:
            dict of client properties used to override the fields in the default
            client properties reported to RabbitMQ via `Connection.StartOk`
            method
        - connection_attempts:
            Specify how many times aiopika should try and reconnect before it gives up
        - frame_max:
            Override the default maximum frame size for communication
        - heartbeat:
            Desired connection heartbeat timeout for negotiation. If not present
            the broker's value is accepted. 0 turns heartbeat off.
        - locale:
            Override the default `en_US` locale value
        - ssl_options:
            None for plaintext; for SSL: dict of public ssl context-related
            arguments that may be passed to :meth:`ssl.SSLSocket` as kwds,
            except `sock`, `server_side`,`do_handshake_on_connect`, `family`,
            `type`, `proto`, `fileno`.
        - retry_delay:
            The number of seconds to sleep before attempting to connect on
            connection failure.
        - socket_timeout:
            Socket connect timeout value in seconds (float or int)
        - stack_timeout:
            Positive full protocol stack (TCP/[SSL]/AMQP) bring-up timeout in
            seconds. It's recommended to set this value higher than
            `socket_timeout`.
        - blocked_connection_timeout:
            Set the timeout, in seconds, that the connection may remain blocked
            (triggered by Connection.Blocked from broker); if the timeout
            expires before connection becomes unblocked, the connection will be
            torn down, triggering the connection's on_close_callback
        - tcp_options:
            Set the tcp options for the underlying socket.

    :param str url: The AMQP URL to connect to

    """

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ('_all_url_query_values',)

    # The name of the private function for parsing and setting a given URL query
    # arg is constructed by catenating the query arg's name to this prefix
    _SETTER_PREFIX = '_set_url_'

    def __init__(self, url):
        """Create a new URLParameters instance.

        :param str url: The URL value

        """
        super(URLParameters, self).__init__()

        self._all_url_query_values = None

        # Handle the Protocol scheme
        #
        # Fix up scheme amqp(s) to http(s) so urlparse won't barf on python
        # prior to 2.7. On Python 2.6.9,
        # `urlparse('amqp://127.0.0.1/%2f?socket_timeout=1')` produces an
        # incorrect path='/%2f?socket_timeout=1'
        if url[0:4].lower() == 'amqp':
            url = 'http' + url[4:]

        parts = urlparse(url)

        if parts.scheme == 'https':
            # Create default context which will get overridden by the
            # ssl_options URL arg, if any
            self.ssl_options = credentials.SSLOptions(
                context=ssl.create_default_context())
        elif parts.scheme == 'http':
            self.ssl_options = None
        elif parts.scheme:
            raise ValueError('Unexpected URL scheme %r; supported scheme '
                             'values: amqp, amqps' % (parts.scheme,))

        if parts.hostname is not None:
            self.host = parts.hostname

        # Take care of port after SSL status is known
        if parts.port is not None:
            self.port = parts.port
        else:
            self.port = (self.DEFAULT_SSL_PORT
                         if self.ssl_options else self.DEFAULT_PORT)

        if parts.username is not None:
            self.credentials = credentials.PlainCredentials(
                url_unquote(parts.username), url_unquote(parts.password))

        # Get the Virtual Host
        if len(parts.path) > 1:
            self.virtual_host = url_unquote(parts.path.split('/')[1])

        # Handle query string values, validating and assigning them
        self._all_url_query_values = url_parse_qs(parts.query)

        for name, value in self._all_url_query_values.items():
            try:
                set_value = getattr(self, self._SETTER_PREFIX + name)
            except AttributeError:
                raise ValueError('Unknown URL parameter: %r' % (name,))

            try:
                (value,) = value
            except ValueError:
                raise ValueError(
                    'Expected exactly one value for URL parameter '
                    '%s, but got %i values: %s' % (name, len(value), value))

            set_value(value)

    def _set_url_blocked_connection_timeout(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            blocked_connection_timeout = float(value)
        except ValueError as exc:
            raise ValueError(
                'Invalid blocked_connection_timeout value %r: %r' % (
                    value,
                    exc,
                ))
        self.blocked_connection_timeout = blocked_connection_timeout

    def _set_url_channel_max(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            channel_max = int(value)
        except ValueError as exc:
            raise ValueError('Invalid channel_max value %r: %r' % (
                value,
                exc,
            ))
        self.channel_max = channel_max

    def _set_url_client_properties(self, value):
        """Deserialize and apply the corresponding query string arg"""
        self.client_properties = ast.literal_eval(value)

    def _set_url_connection_attempts(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            connection_attempts = int(value)
        except ValueError as exc:
            raise ValueError('Invalid connection_attempts value %r: %r' % (
                value,
                exc,
            ))
        self.connection_attempts = connection_attempts

    def _set_url_frame_max(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            frame_max = int(value)
        except ValueError as exc:
            raise ValueError('Invalid frame_max value %r: %r' % (
                value,
                exc,
            ))
        self.frame_max = frame_max

    def _set_url_heartbeat(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            heartbeat_timeout = int(value)
        except ValueError as exc:
            raise ValueError('Invalid heartbeat value %r: %r' % (
                value,
                exc,
            ))
        self.heartbeat = heartbeat_timeout

    def _set_url_locale(self, value):
        """Deserialize and apply the corresponding query string arg"""
        self.locale = value

    def _set_url_retry_delay(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            retry_delay = float(value)
        except ValueError as exc:
            raise ValueError('Invalid retry_delay value %r: %r' % (
                value,
                exc,
            ))
        self.retry_delay = retry_delay

    def _set_url_socket_timeout(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            socket_timeout = float(value)
        except ValueError as exc:
            raise ValueError('Invalid socket_timeout value %r: %r' % (
                value,
                exc,
            ))
        self.socket_timeout = socket_timeout

    def _set_url_stack_timeout(self, value):
        """Deserialize and apply the corresponding query string arg"""
        try:
            stack_timeout = float(value)
        except ValueError as exc:
            raise ValueError('Invalid stack_timeout value %r: %r' % (
                value,
                exc,
            ))
        self.stack_timeout = stack_timeout

    def _set_url_ssl_options(self, value):
        """Deserialize and apply the corresponding query string arg

        """
        opts = ast.literal_eval(value)
        if opts is None:
            if self.ssl_options is not None:
                raise ValueError(
                    'Specified ssl_options=None URL arg is inconsistent with '
                    'the specified https URL scheme.')
        else:
            # Older versions of aiopika would take the opts dict and pass it
            # directly as kwds to the deprecated ssl.wrap_socket method.
            # Here, we take the valid options and translate them into args
            # for various SSLContext methods.
            #
            # https://docs.python.org/3/library/ssl.html#ssl.wrap_socket
            #
            # SSLContext.load_verify_locations(cafile=None, capath=None, cadata=None)
            try:
                opt_protocol = ssl.PROTOCOL_TLS
            except AttributeError:
                opt_protocol = ssl.PROTOCOL_TLSv1
            if 'protocol' in opts:
                opt_protocol = opts['protocol']

            cxt = ssl.SSLContext(protocol=opt_protocol)

            opt_cafile = opts.get('ca_certs') or opts.get('cafile')
            opt_capath = opts.get('ca_path') or opts.get('capath')
            opt_cadata = opts.get('ca_data') or opts.get('cadata')
            cxt.load_verify_locations(opt_cafile, opt_capath, opt_cadata)

            # SSLContext.load_cert_chain(certfile, keyfile=None, password=None)
            if 'certfile' in opts:
                opt_certfile = opts['certfile']
                opt_keyfile = opts.get('keyfile')
                opt_password = opts.get('password')
                cxt.load_cert_chain(opt_certfile, opt_keyfile, opt_password)

            if 'ciphers' in opts:
                opt_ciphers = opts['ciphers']
                cxt.set_ciphers(opt_ciphers)

            server_hostname = opts.get('server_hostname')
            self.ssl_options = credentials.SSLOptions(
                context=cxt, server_hostname=server_hostname)

    def _set_url_tcp_options(self, value):
        """Deserialize and apply the corresponding query string arg"""
        self.tcp_options = ast.literal_eval(value)


class SSLOptions(object):
    """Class used to provide parameters for optional fine grained control of SSL
    socket wrapping.

    """

    # Protect against accidental assignment of an invalid attribute
    __slots__ = ('context', 'server_hostname')

    def __init__(self, context, server_hostname=None):
        """
        :param ssl.SSLContext context: SSLContext instance
        :param str|None server_hostname: SSLContext.wrap_socket, used to enable
            SNI
        """
        if not isinstance(context, ssl.SSLContext):
            raise TypeError(
                'context must be of ssl.SSLContext type, but got {!r}'.format(
                    context))

        self.context = context
        self.server_hostname = server_hostname
