# Copyright 2021 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations

import asyncio
import ssl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nats.aio.subscription import Subscription


class Error(Exception):
    pass


class TimeoutError(Error, asyncio.TimeoutError):
    def __str__(self) -> str:
        return "nats: timeout"


class NoRespondersError(Error):
    def __str__(self) -> str:
        return "nats: no responders available for request"


class StaleConnectionError(Error):
    def __str__(self) -> str:
        return "nats: stale connection"


class OutboundBufferLimitError(Error):
    def __str__(self) -> str:
        return "nats: outbound buffer limit exceeded"


class UnexpectedEOF(StaleConnectionError):
    def __str__(self) -> str:
        return "nats: unexpected EOF"


class FlushTimeoutError(TimeoutError):
    def __str__(self) -> str:
        return "nats: flush timeout"


class ConnectionClosedError(Error):
    def __str__(self) -> str:
        return "nats: connection closed"


class SecureConnRequiredError(Error):
    def __str__(self) -> str:
        return "nats: secure connection required"


class SecureConnWantedError(Error):
    def __str__(self) -> str:
        return "nats: secure connection not available"


class SecureConnFailedError(Error):
    def __str__(self) -> str:
        return "nats: secure connection failed"


class TLSError(Error, ssl.SSLError):
    """
    The TLS handshake failed, as nats.go's ErrTLS. It is still an
    ssl.SSLError carrying the original error's errno and text.
    """

    def __str__(self) -> str:
        return f"nats: tls error: {ssl.SSLError.__str__(self)}"


class TLSCertVerificationError(TLSError, ssl.SSLCertVerificationError):
    """
    A TLSError raised for an ssl.SSLCertVerificationError.
    """

    pass


class ConnectionNotTLSError(Error):
    def __str__(self) -> str:
        return "nats: connection is not tls"


class ClientIDNotSupportedError(Error):
    def __str__(self) -> str:
        return "nats: client ID not supported by this server"


class ClientIPNotSupportedError(Error):
    def __str__(self) -> str:
        return "nats: client IP not supported by this server"


class DisconnectedError(Error):
    def __str__(self) -> str:
        return "nats: server is disconnected"


class HeadersNotSupportedError(Error):
    def __str__(self) -> str:
        return "nats: headers not supported by this server"


class NoEchoNotSupportedError(Error):
    def __str__(self) -> str:
        return "nats: no echo option not supported by this server"


class WebSocketHeadersAlreadySetError(Error):
    def __str__(self) -> str:
        return "nats: websocket connection headers already set"


class MixingWebsocketSchemesError(Error):
    def __str__(self) -> str:
        return "nats: mixing of websocket and non websocket URLs is not allowed"


class BadSubscriptionError(Error):
    def __str__(self) -> str:
        return "nats: invalid subscription"


class BadSubjectError(Error):
    def __str__(self) -> str:
        return "nats: invalid subject"


class BadQueueNameError(BadSubjectError):
    def __str__(self) -> str:
        return "nats: invalid queue name"


class BadHeaderError(Error):
    def __init__(self, key: str = "") -> None:
        self.key = key

    def __str__(self) -> str:
        if self.key:
            return f"nats: invalid header: {self.key!r}"
        return "nats: invalid header"


class BadHeaderMsgError(Error):
    """
    The headers of a received message could not be decoded,
    as nats.go's ErrBadHeaderMsg. The decoding error is its __cause__.
    """

    def __str__(self) -> str:
        return "nats: message could not decode headers"


class SlowConsumerError(Error):
    def __init__(self, subject: str, reply: str, sid: int, sub: Subscription) -> None:
        self.subject = subject
        self.reply = reply
        self.sid = sid
        self.sub = sub

    def __str__(self) -> str:
        return f"nats: slow consumer, messages dropped subject: {self.subject}, sid: {self.sid}, sub: {self.sub}"


class SyncSubRequiredError(Error):
    """
    next_msg was called on a subscription with a callback,
    as nats.go's ErrSyncSubRequired.
    """

    def __str__(self) -> str:
        return "nats: next_msg cannot be used in async subscriptions"


class MaxMessagesError(Error):
    """
    The subscription already delivered the messages its auto-unsubscribe
    limit allows, as nats.go's ErrMaxMessages.
    """

    def __str__(self) -> str:
        return "nats: maximum messages delivered"


class BadTimeoutError(Error):
    def __str__(self) -> str:
        return "nats: timeout invalid"


class AuthenticationExpiredError(Error):
    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: authentication expired"


class AccountAuthExpiredError(AuthenticationExpiredError):
    """
    The server reported that the account's authentication expired,
    as nats.go's ErrAccountAuthExpired.
    """

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: account authentication expired"


class AuthorizationError(Error):
    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: authorization failed"


class AuthRevokedError(Error):
    """
    The server revoked the user's authentication, as nats.go's ErrAuthRevoked.
    """

    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: authentication revoked"


class PermissionViolationError(Error):
    """
    The server rejected a publish or subscribe for lack of permissions,
    as nats.go's ErrPermissionViolation. ``description`` holds the
    server's text, e.g. ``permissions violation for subscription to "foo"``.
    """

    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: permissions violation"


class MaxSubscriptionsExceededError(Error):
    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: server maximum subscriptions exceeded"


class MaxConnectionsExceededError(Error):
    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: server maximum connections exceeded"


class MaxAccountConnectionsExceededError(Error):
    def __init__(self, description: str = "") -> None:
        super().__init__(description)
        self.description = description

    def __str__(self) -> str:
        if self.description:
            return f"nats: {self.description}"
        return "nats: maximum account active connections exceeded"


class NoServersError(Error):
    def __str__(self) -> str:
        return "nats: no servers available for connection"


class JsonParseError(Error):
    def __str__(self) -> str:
        return "nats: connect message, json parse err"


class MaxPayloadError(Error):
    def __str__(self) -> str:
        return "nats: maximum payload exceeded"


class DrainTimeoutError(TimeoutError):
    def __str__(self) -> str:
        return "nats: draining connection timed out"


class ConnectionDrainingError(Error):
    def __str__(self) -> str:
        return "nats: connection draining"


class ConnectionReconnectingError(Error):
    def __str__(self) -> str:
        return "nats: connection reconnecting"


class InvalidUserCredentialsError(Error):
    def __str__(self) -> str:
        return "nats: invalid user credentials"


class NkeyAndUserError(Error):
    def __str__(self) -> str:
        return "nats: user callback and nkey defined"


class NkeyButNoSigCBError(Error):
    def __str__(self) -> str:
        return "nats: nkey defined without a signature handler"


class UserButNoSigCBError(Error):
    def __str__(self) -> str:
        return "nats: user callback defined without a signature handler"


class NoUserCBError(Error):
    def __str__(self) -> str:
        return "nats: user callback not defined"


class TokenAlreadySetError(Error):
    def __str__(self) -> str:
        return "nats: token and token handler both set"


class UserInfoAlreadySetError(Error):
    def __str__(self) -> str:
        return "nats: cannot set user info callback and user/pass"


class NkeysNotSupportedError(Error):
    def __str__(self) -> str:
        return "nats: nkeys not supported by the server"


class InvalidCallbackTypeError(Error):
    def __str__(self) -> str:
        return "nats: callbacks must be coroutine functions"


class ProtocolError(Error):
    def __str__(self) -> str:
        return "nats: protocol error"


class NoInfoReceivedError(Error):
    """
    The server did not start the connection with INFO,
    as nats.go's ErrNoInfoReceived.
    """

    def __str__(self) -> str:
        return "nats: empty response from server when expecting INFO message"


class ServerNotInPoolError(Error):
    def __str__(self) -> str:
        return "nats: selected server is not in the pool"


class NotJSMessageError(Error):
    """
    When it is attempted to use an API meant for JetStream on a message
    that does not belong to a stream.
    """

    def __str__(self) -> str:
        return "nats: not a JetStream message"


class InvalidMsgError(Error):
    def __str__(self) -> str:
        return "nats: invalid message or message nil"


class MsgAlreadyAckdError(Error):
    def __init__(self, msg=None) -> None:
        self._msg = msg

    def __str__(self) -> str:
        return f"nats: message was already acknowledged: {self._msg}"


class MsgNoReplyError(NotJSMessageError):
    """
    Raised when acknowledging or reading the metadata of a message without a reply subject.
    """

    def __str__(self) -> str:
        return "nats: message does not have a reply"


class MsgNotBoundError(Error):
    """
    Raised when acknowledging a message that was not received from a connection.
    """

    def __str__(self) -> str:
        return "nats: message is not bound to subscription/connection"
