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


"""The credentials classes are used to encapsulate all authentication
information for the :class:`~pika.connection.ConnectionParameters` class.

The :class:`~pika.credentials.PlainCredentials` class returns the properly
formatted username and password to the :class:`~pika.connection.Connection`.

To authenticate with Pika, create a :class:`~pika.credentials.PlainCredentials`
object passing in the username and password and pass it as the credentials
argument value to the :class:`~pika.connection.ConnectionParameters` object.

If you are using :class:`~pika.connection.URLParameters` you do not need a
credentials object, one will automatically be created for you.

If you are looking to implement SSL certificate style authentication, you would
extend the :class:`~pika.credentials.ExternalCredentials` class implementing
the required behavior.

"""
import logging

LOGGER = logging.getLogger(__name__)


def _as_bytes(value):
    if not isinstance(value, bytes):
        return value.encode('UTF-8')
    return value


class PlainCredentials(object):
    """A credentials object for the default authentication methodology with
    RabbitMQ.

    If you do not pass in credentials to the ConnectionParameters object, it
    will create credentials for 'guest' with the password of 'guest'.

    If you pass True to erase_on_connect the credentials will not be stored
    in memory after the Connection attempt has been made.

    :param str username: The username to authenticate with
    :param str password: The password to authenticate with
    :param bool erase_on_connect: erase credentials on connect.

    """
    TYPE = 'PLAIN'

    def __init__(self, username, password, erase_on_connect=False):
        """Create a new instance of PlainCredentials

        :param str username: The username to authenticate with
        :param str password: The password to authenticate with
        :param bool erase_on_connect: erase credentials on connect.

        """
        self.username = username
        self.password = password
        self.erase_on_connect = erase_on_connect

    def __eq__(self, other):
        if isinstance(other, PlainCredentials):
            return (self.username == other.username and
                    self.password == other.password and
                    self.erase_on_connect == other.erase_on_connect)
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is not NotImplemented:
            return not result
        return NotImplemented

    def response_for(self, start):
        """Validate that this type of authentication is supported

        :param spec.Connection.Start start: Connection.Start method
        :rtype: tuple(str|None, str|None)

        """
        if _as_bytes(PlainCredentials.TYPE) not in\
                _as_bytes(start.mechanisms).split():
            return None, None
        return (
            PlainCredentials.TYPE,
            b'\0' + _as_bytes(self.username) + b'\0' + _as_bytes(self.password))

    def erase_credentials(self):
        """Called by Connection when it no longer needs the credentials"""
        if self.erase_on_connect:
            LOGGER.info("Erasing stored credential values")
            self.username = None
            self.password = None


class ExternalCredentials(object):
    """The ExternalCredentials class allows the connection to use EXTERNAL
    authentication, generally with a client SSL certificate.

    """
    TYPE = 'EXTERNAL'

    def __init__(self):
        """Create a new instance of ExternalCredentials"""
        self.erase_on_connect = False

    def __eq__(self, other):
        if isinstance(other, ExternalCredentials):
            return self.erase_on_connect == other.erase_on_connect
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is not NotImplemented:
            return not result
        return NotImplemented

    def response_for(self, start):  # pylint: disable=R0201
        """Validate that this type of authentication is supported

        :param spec.Connection.Start start: Connection.Start method
        :rtype: tuple(str or None, str or None)

        """
        if _as_bytes(ExternalCredentials.TYPE) not in\
                _as_bytes(start.mechanisms).split():
            return None, None
        return ExternalCredentials.TYPE, b''

    def erase_credentials(self):  # pylint: disable=R0201
        """Called by Connection when it no longer needs the credentials"""
        LOGGER.debug('Not supported by this Credentials type')


# Append custom credential types to this list for validation support
VALID_TYPES = [PlainCredentials, ExternalCredentials]
