# Copyright 2016-2023 The NATS Authors
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
import base64
import inspect
import ipaddress
import json
import logging
import random
import re
import ssl
import string
import time
from collections import UserString
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from random import shuffle
from secrets import token_hex
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

try:
    from fast_mail_parser import parse_email
except ImportError:
    parse_email = None

import nats.js
from nats import errors
from nats.nuid import NUID
from nats.protocol import command as prot_command
from nats.protocol.parser import (
    ACCOUNT_AUTHENTICATION_EXPIRED,
    AUTHENTICATION_EXPIRED,
    AUTHENTICATION_REVOKED,
    AUTHORIZATION_VIOLATION,
    MAX_ACCOUNT_CONNECTIONS_ERR,
    MAX_CONNECTIONS_ERR,
    MAX_SUBSCRIPTIONS_ERR,
    PERMISSIONS_ERR,
    PONG,
    STALE_CONNECTION,
    Parser,
)

from .errors import ErrInvalidUserCredentials, ErrStaleConnection
from .msg import Msg
from .subscription import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    Subscription,
    _Barrier,
)
from .transport import TcpTransport, Transport, WebSocketHeadersCallback, WebSocketTransport

try:
    from importlib.metadata import version

    __version__ = version("nats-py")
except Exception:
    __version__ = "0.0.0"

__lang__ = "python3"
_logger = logging.getLogger(__name__)
PROTOCOL = 1

INFO_OP = b"INFO"
CONNECT_OP = b"CONNECT"
PING_OP = b"PING"
PONG_OP = b"PONG"
OK_OP = b"+OK"
ERR_OP = b"-ERR"
_CRLF_ = b"\r\n"
_CRLF_LEN_ = len(_CRLF_)
_SPC_ = b" "
_SPC_BYTE_ = 32
EMPTY = ""

PING_PROTO = PING_OP + _CRLF_
PONG_PROTO = PONG_OP + _CRLF_
DEFAULT_INBOX_PREFIX = b"_INBOX"

DEFAULT_PENDING_SIZE = 2 * 1024 * 1024
DEFAULT_BUFFER_SIZE = 32768
DEFAULT_RECONNECT_TIME_WAIT = 2  # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 60
DEFAULT_PING_INTERVAL = 120  # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS = 2
DEFAULT_MAX_PAYLOAD_SIZE = 1048576
DEFAULT_MAX_FLUSHER_QUEUE_SIZE = 1024
DEFAULT_FLUSH_TIMEOUT = 10  # in seconds
DEFAULT_CONNECT_TIMEOUT = 2  # in seconds
DEFAULT_DRAIN_TIMEOUT = 30  # in seconds
MAX_CONTROL_LINE_SIZE = 1024

NATS_HDR_LINE = bytearray(b"NATS/1.0")
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
NO_RESPONDERS_STATUS = "503"
CTRL_STATUS = "100"
STATUS_MSG_LEN = 3  # e.g. 20x, 40x, 50x

_SUBJECT_INVALID_RE = re.compile(r"[ \t\r\n]")
_PERMISSION_SUB_RE = re.compile(r'subscription to "([^"]*)"(?: using queue "([^"]*)")?', re.IGNORECASE)


def _validate_subject(subject: str, *, strict: bool = False) -> None:
    """Validate a NATS subject, raising BadSubjectError if it is malformed.

    Always rejects empty subjects and whitespace or CRLF. CRLF in particular
    would allow a caller to inject arbitrary protocol commands.

    With ``strict=True`` (the subscribe path), also rejects empty tokens and
    misplaced wildcards. The publish path leaves token shape to the server.
    """
    if not subject:
        raise errors.BadSubjectError
    if _SUBJECT_INVALID_RE.search(subject):
        raise errors.BadSubjectError
    if not strict:
        return
    tokens = subject.split(".")
    last = len(tokens) - 1
    for index, token in enumerate(tokens):
        if not token:
            raise errors.BadSubjectError
        if token == ">":
            if index != last:
                raise errors.BadSubjectError
        elif ">" in token:
            raise errors.BadSubjectError
        elif "*" in token and token != "*":
            raise errors.BadSubjectError


def _validate_queue(queue: str) -> None:
    """Validate a queue group name, raising BadSubjectError if it is malformed.

    An empty queue is treated as unset. Only whitespace and CRLF are rejected;
    wildcards and dots are valid tokens on the wire.
    """
    if queue and _SUBJECT_INVALID_RE.search(queue):
        raise errors.BadQueueNameError


# Header keys must be printable ASCII without separators (RFC 7230 token
# characters); anything else, CRLF in particular, is rejected outright.
_HEADER_KEY_RE = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")

# Header values are trimmed and CR/LF are replaced by spaces so a value can
# never terminate the header line or inject further protocol commands.
_HEADER_VALUE_NEWLINES = str.maketrans({"\r": " ", "\n": " "})

Callback = Callable[[], Awaitable[None]]
ErrorCallback = Callable[[Exception], Awaitable[None]]
DisconnectedErrorCallback = Callable[[Optional[Exception]], Awaitable[None]]
JWTCallback = Callable[[], Union[bytearray, bytes]]
SignatureCallback = Callable[[str], bytes]
TokenCallback = Callable[[], str]
# Called synchronously on the event loop during connect and reconnect,
# so it must not block (e.g. return a cached or pre-fetched value rather
# than fetching credentials over the network).
CredentialCallback = Callable[[], str]
# Returns the (user, password) pair; same constraints as CredentialCallback.
UserInfoCallback = Callable[[], Tuple[str, str]]
# Loads a client certificate or trusted CAs into the SSLContext built for a
# TLS handshake (e.g. with load_cert_chain or load_verify_locations).
TLSContextCallback = Callable[[ssl.SSLContext], None]


class RawCredentials(UserString):
    pass


Credentials = Union[str, Tuple[str, str], RawCredentials, Path]


@dataclass
class Server:
    """
    Server represents a NATS server in the connection pool.
    """

    uri: ParseResult
    reconnects: int = 0


@dataclass
class Srv:
    """
    Srv is a helper data structure to hold state of a server.
    """

    uri: ParseResult
    reconnects: int = 0
    last_attempt: Optional[float] = None
    did_connect: bool = False
    discovered: bool = False
    tls_name: Optional[str] = None
    server_version: Optional[str] = None
    last_auth_error: Optional[type] = None


ReconnectToServerHandler = Callable[[List[Server], Dict[str, Any]], Tuple[Optional[Server], float]]
# Called with the number of times the client backed off while reconnecting
# (1 the first time), returns the seconds to wait before trying again.
ReconnectDelayHandler = Callable[[int], float]


class ServerVersion:
    def __init__(self, server_version: str) -> None:
        self._server_version = server_version
        self._major_version: Optional[int] = None
        self._minor_version: Optional[int] = None
        self._patch_version: Optional[int] = None
        self._prerelease_version: Optional[str] = None
        self._build_version: Optional[str] = None
        self._dev_version: Optional[str] = None

    def parse_version(self) -> None:
        # https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
        _SEMVER_REGEX = r"""
            ^
            (?P<major>0|[1-9]\d*)
            \.
            (?P<minor>0|[1-9]\d*)
            \.
            (?P<patch>0|[1-9]\d*)
            (?:-(?P<prerelease>
                (?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)
                (?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*
            ))?
            (?:\+(?P<buildmetadata>
                [0-9a-zA-Z-]+
                (?:\.[0-9a-zA-Z-]+)*
            ))?
            $
        """
        _REGEX = re.compile(_SEMVER_REGEX, re.VERBOSE)
        match = _REGEX.match(self._server_version)
        if match is None:
            raise ValueError(f"{self._server_version} is not a valid Semantic Version")
        matches = match.groupdict()
        self._major_version = int(matches["major"])
        self._minor_version = int(matches["minor"])
        self._patch_version = int(matches["patch"])
        self._prerelease_version = matches["prerelease"] or ""
        self._build_version = matches["buildmetadata"] or ""
        if self._build_version:
            self._dev_version = "+".join([self._prerelease_version, self._build_version])
        else:
            self._dev_version = self._prerelease_version

    @property
    def major(self) -> int:
        if not self._major_version:
            self.parse_version()
        return self._major_version

    @property
    def minor(self) -> int:
        if not self._minor_version:
            self.parse_version()
        return self._minor_version

    @property
    def patch(self) -> int:
        if not self._patch_version:
            self.parse_version()
        return self._patch_version

    @property
    def prerelease(self) -> int:
        if not self._prerelease_version:
            self.parse_version()
        return self._prerelease_version

    @property
    def build(self) -> int:
        if not self._build_version:
            self.parse_version()
        return self._build_version

    @property
    def dev(self) -> str:
        if not self._dev_version:
            self.parse_version()
        return self._dev_version or ""

    def __repr__(self) -> str:
        return f"<nats server v{self._server_version}>"


def _server_error(description: str) -> errors.Error:
    """
    Maps the text of a server -ERR to its error class, as nats.go's
    processErr and checkAuthError. The error keeps the server's text.
    """
    text = description.strip().strip("'").lower()
    cls: type = errors.Error
    if text.startswith(AUTHORIZATION_VIOLATION):
        cls = errors.AuthorizationError
    elif text.startswith(ACCOUNT_AUTHENTICATION_EXPIRED):
        cls = errors.AccountAuthExpiredError
    elif AUTHENTICATION_EXPIRED in text:
        cls = errors.AuthenticationExpiredError
    elif AUTHENTICATION_REVOKED in text:
        cls = errors.AuthRevokedError
    elif text.startswith(PERMISSIONS_ERR):
        cls = errors.PermissionViolationError
    elif text.startswith(MAX_SUBSCRIPTIONS_ERR):
        cls = errors.MaxSubscriptionsExceededError
    elif text.startswith(MAX_ACCOUNT_CONNECTIONS_ERR):
        cls = errors.MaxAccountConnectionsExceededError
    elif text.startswith(MAX_CONNECTIONS_ERR):
        cls = errors.MaxConnectionsExceededError
    if cls is errors.Error:
        return errors.Error(f"nats: {description}")
    return cls(description)


_inbox_nuid = NUID()


def new_inbox(prefix: str = DEFAULT_INBOX_PREFIX.decode()) -> str:
    """
    Returns a unique inbox subject (``_INBOX.<nuid>`` by default) that is
    not tied to a connection, as nats.go's NewInbox.
    """
    inbox = bytearray(prefix.encode())
    inbox.extend(b".")
    inbox.extend(_inbox_nuid.next())
    return inbox.decode()


def _host_port(address: Any) -> Optional[str]:
    """Formats a socket address as nats.go's net.Addr.String does."""
    if not address:
        return None
    if isinstance(address, (tuple, list)) and len(address) >= 2:
        host, port = address[0], address[1]
        if ":" in str(host):
            return f"[{host}]:{port}"
        return f"{host}:{port}"
    return str(address)


async def _default_error_callback(ex: Exception) -> None:
    """
    Provides a default way to handle async errors if the user
    does not provide one.
    """
    _logger.error("nats: encountered error", exc_info=ex)


class Client:
    """
    Asyncio based client for NATS.
    """

    msg_class: type[Msg] = Msg

    # FIXME: Use an enum instead.
    DISCONNECTED = 0
    CONNECTED = 1
    CLOSED = 2
    RECONNECTING = 3
    CONNECTING = 4
    DRAINING_SUBS = 5
    DRAINING_PUBS = 6

    def __repr__(self) -> str:
        return f"<nats client v{__version__}>"

    def __init__(self) -> None:
        self._current_server: Optional[Srv] = None
        self._server_info: Dict[str, Any] = {}
        self._server_pool: List[Srv] = []
        self._reading_task: Optional[asyncio.Task] = None
        self._ping_interval_task: Optional[asyncio.Task] = None
        self._pings_outstanding: int = 0
        self._pongs_received: int = 0
        self._pongs: List[asyncio.Future] = []
        self._transport: Optional[Transport] = None
        self._err: Optional[Exception] = None

        # callbacks
        self._error_cb: ErrorCallback = _default_error_callback
        self._disconnected_cb: Optional[Callback] = None
        self._closed_cb: Optional[Callback] = None
        self._discovered_server_cb: Optional[Callback] = None
        self._reconnected_cb: Optional[Callback] = None
        self._reconnect_to_server_handler: Optional[ReconnectToServerHandler] = None
        self._lame_duck_mode_cb: Optional[Callback] = None
        self._connected_cb: Optional[Callback] = None
        self._disconnected_err_cb: Optional[DisconnectedErrorCallback] = None
        self._reconnect_error_cb: Optional[ErrorCallback] = None
        # The errors handed to disconnected_err_cb when reconnecting or closing.
        self._disconnect_err: Optional[Exception] = None
        self._close_err: Optional[Exception] = None

        self._reconnection_task: Optional[asyncio.Task[None]] = None
        self._reconnect_backoffs: int = 0
        self._reconnection_task_future: Optional[asyncio.Future] = None
        self._max_payload: int = DEFAULT_MAX_PAYLOAD_SIZE

        # client id that the NATS server knows about.
        self._client_id: Optional[int] = None
        self._client_ip: Optional[str] = None
        self._sid: int = 0
        self._subs: Dict[int, Subscription] = {}
        self._status: int = Client.DISCONNECTED
        self._ps: Parser = Parser(self)

        # pending queue of commands that will be flushed to the server.
        self._pending: List[bytes] = []

        # current size of pending data in total.
        self._pending_data_size: int = 0

        # max pending size is the maximum size of the data that can be buffered.
        self._max_pending_size: int = 0

        self._flush_queue: Optional[asyncio.Queue[asyncio.Future[Any]]] = None
        self._flusher_task: Optional[asyncio.Task] = None
        self._flush_timeout: Optional[float] = 0

        # New style request/response
        self._resp_map: Dict[str, asyncio.Future] = {}
        self._resp_sub_prefix: Optional[bytearray] = None
        self._resp_mux_subscribed: bool = False
        self._nuid = NUID()
        self._inbox_prefix = bytearray(DEFAULT_INBOX_PREFIX)
        self._auth_configured: bool = False
        self._skip_subject_validation: bool = False

        # NKEYS support
        #
        # user_jwt_cb is used to fetch and return the account
        # signed JWT for this user.
        self._user_jwt_cb: Optional[JWTCallback] = None

        # signature_cb is used to sign a nonce from the server while
        # authenticating with nkeys. The user should sign the nonce and
        # return the base64 encoded signature.
        self._signature_cb: Optional[SignatureCallback] = None

        # user credentials file can be a tuple or single file.
        self._user_credentials: Optional[Credentials] = None

        # file that contains the nkeys seed and its public key as a string.
        self._nkeys_seed: Optional[str] = None
        self._nkeys_seed_str: Optional[str] = None
        self._public_nkey: Optional[str] = None

        self.options: Dict[str, Any] = {}
        self.stats = {
            "in_msgs": 0,
            "out_msgs": 0,
            "in_bytes": 0,
            "out_bytes": 0,
            "reconnects": 0,
            "errors_received": 0,
        }

    async def connect(
        self,
        servers: Union[str, List[str]] = "nats://localhost:4222",
        error_cb: Optional[ErrorCallback] = None,
        disconnected_cb: Optional[Callback] = None,
        closed_cb: Optional[Callback] = None,
        discovered_server_cb: Optional[Callback] = None,
        reconnected_cb: Optional[Callback] = None,
        name: Optional[str] = None,
        pedantic: bool = False,
        verbose: bool = False,
        allow_reconnect: bool = True,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: int = DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: bool = False,
        flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: bool = False,
        tls: Optional[ssl.SSLContext] = None,
        tls_hostname: Optional[str] = None,
        tls_handshake_first: bool = False,
        user: Optional[Union[str, CredentialCallback]] = None,
        password: Optional[Union[str, CredentialCallback]] = None,
        token: Optional[Union[str, TokenCallback]] = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Optional[SignatureCallback] = None,
        user_jwt_cb: Optional[JWTCallback] = None,
        user_credentials: Optional[Credentials] = None,
        nkeys_seed: Optional[str] = None,
        nkeys_seed_str: Optional[str] = None,
        inbox_prefix: Union[str, bytes] = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: Optional[float] = None,
        ws_connection_headers: Optional[Dict[str, List[str]]] = None,
        reconnect_to_server_handler: Optional[ReconnectToServerHandler] = None,
        lame_duck_mode_cb: Optional[Callback] = None,
        skip_subject_validation: bool = False,
        connected_cb: Optional[Callback] = None,
        disconnected_err_cb: Optional[DisconnectedErrorCallback] = None,
        reconnect_error_cb: Optional[ErrorCallback] = None,
        no_callbacks_after_client_close: bool = False,
        ignore_auth_error_abort: bool = False,
        permission_err_on_subscribe: bool = False,
        retry_on_failed_connect: bool = False,
        custom_reconnect_delay_cb: Optional[ReconnectDelayHandler] = None,
        reconnect_jitter: float = 0,
        reconnect_jitter_tls: float = 0,
        ignore_discovered_servers: bool = False,
        nkey: Optional[str] = None,
        user_info_cb: Optional[UserInfoCallback] = None,
        user_jwt_and_seed: Optional[Tuple[str, str]] = None,
        tls_cert_cb: Optional[TLSContextCallback] = None,
        tls_roots_cb: Optional[TLSContextCallback] = None,
        ws_connection_headers_cb: Optional[WebSocketHeadersCallback] = None,
        ws_compression: bool = False,
        ws_proxy_path: Optional[str] = None,
    ) -> None:
        """
        Establishes a connection to NATS.

        :param servers: NATS Connection
        :param name: Label the connection with name (shown in NATS monitoring)
        :param error_cb: Callback to report errors.
        :param disconnected_cb: Callback to report disconnection from NATS.
        :param closed_cb: Callback to report when client stops reconnection to NATS.
        :param discovered_server_cb: Callback to report when a new server joins the cluster.
        :param pending_size: Max size of the pending buffer for publishing commands.
        :param flush_timeout: Max duration to wait for a forced flush to occur.
        :param skip_subject_validation: Disable client-side validation of subjects,
            reply subjects and queue groups on publish, subscribe and request.
            Not recommended: the performance gain is minimal and it removes the
            protection against CRLF injection into the protocol stream.
        :param connected_cb: Callback to report that the initial connection
            was established (reconnections are reported to reconnected_cb).
        :param disconnected_err_cb: Callback to report disconnection from NATS
            with the error that caused it (None when the connection was closed
            by the user). When set, it is called instead of disconnected_cb.
        :param reconnect_error_cb: Callback to report each failed reconnect attempt.
        :param no_callbacks_after_client_close: Do not call the disconnected and
            closed callbacks when the connection is closed by close() or drain().
        :param ignore_auth_error_abort: Keep reconnecting to a server that rejected
            the credentials twice in a row with the same authentication error.
            By default the connection is closed then, as in nats.go.
        :param permission_err_on_subscribe: Make next_msg and iteration of a
            subscription raise the PermissionViolationError the server reported
            for its subject (and queue group), instead of waiting for messages
            that will never come. The error is still reported to error_cb.
        :param retry_on_failed_connect: When no server of the pool can be
            connected to, return right away and keep connecting in the
            background, as reconnections do (publishes are buffered meanwhile).
            connected_cb reports when the connection is established.
        :param custom_reconnect_delay_cb: Function called with the number of
            times the client backed off while reconnecting (starting at 1) that
            returns the seconds to wait, replacing reconnect_time_wait.
        :param reconnect_jitter: Up to this many seconds, chosen at random, are
            added to reconnect_time_wait, so that clients do not all reconnect
            at once.
        :param reconnect_jitter_tls: Like reconnect_jitter, for TLS connections.
        :param ignore_discovered_servers: Do not add the servers the cluster
            announces to the server pool; only the given servers are used.
        :param nkey: Public nkey to authenticate with, together with a
            signature_cb that signs the server's nonce with its seed.
        :param user_info_cb: Function returning the (user, password) pair,
            called on each connection attempt.
        :param user_jwt_and_seed: The user JWT and nkey seed as strings,
            instead of a credentials file.
        :param tls_cert_cb: Called with a new SSLContext before every TLS
            handshake to load the client certificate into it, so a rotated
            certificate is picked up on reconnect. Implies TLS.
        :param tls_roots_cb: Called with a new SSLContext before every TLS
            handshake to load the trusted CAs into it (the system ones are
            then not loaded). Implies TLS.
        :param ws_connection_headers_cb: Function (or coroutine function)
            returning the WebSocket handshake headers, called on every
            connection attempt; cannot be combined with ws_connection_headers.
        :param ws_compression: Ask for permessage-deflate compression of
            WebSocket frames.
        :param ws_proxy_path: Path used for every WebSocket connection, in
            place of the server URL's path, e.g. when behind a proxy.

        Connecting setting all callbacks::

            import asyncio
            import nats

            async def main():
                async def disconnected_cb():
                    print('Got disconnected!')

                async def reconnected_cb():
                    print(f'Got reconnected to {nc.connected_url.netloc}')

                async def error_cb(e):
                    print(f'There was an error: {e}')

                async def closed_cb():
                    print('Connection is closed')

                # Connect to NATS with logging callbacks.
                nc = await nats.connect('demo.nats.io',
                                         error_cb=error_cb,
                                         reconnected_cb=reconnected_cb,
                                         disconnected_cb=disconnected_cb,
                                         closed_cb=closed_cb,
                                         )

                async def handler(msg):
                    print(f'Received a message on {msg.subject} {msg.reply}: {msg.data}')
                    await msg.respond(b'OK')

                sub = await nc.subscribe('help.please', cb=handler)

                resp = await nc.request('help.please', b'help')
                print('Response:', resp)

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        Using a context manager::

            import asyncio
            import nats

            async def main():

                is_done = asyncio.Future()

                async def closed_cb():
                    print('Connection to NATS is closed.')
                    is_done.set_result(True)

                async with (await nats.connect('nats://demo.nats.io:4222', closed_cb=closed_cb)) as nc:
                    print(f'Connected to NATS at {nc.connected_url.netloc}...')

                    async def subscribe_handler(msg):
                        subject = msg.subject
                        reply = msg.reply
                        data = msg.data.decode()
                        print('Received a message on '{subject} {reply}': {data}'.format(
                            subject=subject, reply=reply, data=data))

                    await nc.subscribe('discover', cb=subscribe_handler)
                    await nc.flush()

                    for i in range(0, 10):
                        await nc.publish('discover', b'hello world')
                        await asyncio.sleep(0.1)

                await asyncio.wait_for(is_done, 60.0)

            if __name__ == '__main__':
                asyncio.run(main())

        """

        for cb in [
            error_cb,
            disconnected_cb,
            closed_cb,
            reconnected_cb,
            discovered_server_cb,
            lame_duck_mode_cb,
            connected_cb,
            disconnected_err_cb,
            reconnect_error_cb,
        ]:
            if cb and not inspect.iscoroutinefunction(cb):
                raise errors.InvalidCallbackTypeError

        self._setup_server_pool(servers)
        self._error_cb = error_cb or _default_error_callback
        self._closed_cb = closed_cb
        self._discovered_server_cb = discovered_server_cb
        self._reconnected_cb = reconnected_cb
        self._disconnected_cb = disconnected_cb
        self._reconnect_to_server_handler = reconnect_to_server_handler
        self._lame_duck_mode_cb = lame_duck_mode_cb
        self._connected_cb = connected_cb
        self._disconnected_err_cb = disconnected_err_cb
        self._reconnect_error_cb = reconnect_error_cb

        # Custom inbox prefix
        if isinstance(inbox_prefix, str):
            inbox_prefix = inbox_prefix.encode()
        assert isinstance(inbox_prefix, bytes)
        self._inbox_prefix = bytearray(inbox_prefix)

        self._check_auth_options(
            user=user,
            password=password,
            token=token,
            signature_cb=signature_cb,
            user_jwt_cb=user_jwt_cb,
            user_credentials=user_credentials,
            nkeys_seed=nkeys_seed,
            nkeys_seed_str=nkeys_seed_str,
            nkey=nkey,
            user_info_cb=user_info_cb,
            user_jwt_and_seed=user_jwt_and_seed,
        )

        # NKEYS support
        self._signature_cb = signature_cb
        self._user_jwt_cb = user_jwt_cb
        self._user_credentials = user_credentials
        self._nkeys_seed = nkeys_seed
        self._nkeys_seed_str = nkeys_seed_str
        self.options["nkey"] = nkey
        self.options["user_info_cb"] = user_info_cb

        # Customizable options
        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["allow_reconnect"] = allow_reconnect
        self.options["dont_randomize"] = dont_randomize
        self.options["reconnect_time_wait"] = reconnect_time_wait
        self.options["max_reconnect_attempts"] = max_reconnect_attempts
        self.options["ping_interval"] = ping_interval
        self.options["max_outstanding_pings"] = max_outstanding_pings
        self.options["no_echo"] = no_echo
        self.options["user"] = user
        self.options["password"] = password
        self.options["token"] = token
        self.options["connect_timeout"] = connect_timeout
        self.options["drain_timeout"] = drain_timeout
        self.options["tls_handshake_first"] = tls_handshake_first
        self.options["ws_connection_headers"] = ws_connection_headers
        if ws_connection_headers_cb is not None:
            if ws_connection_headers:
                raise errors.WebSocketHeadersAlreadySetError
            if not callable(ws_connection_headers_cb):
                raise errors.Error("nats: ws_connection_headers_cb must be callable")
        self.options["ws_connection_headers_cb"] = ws_connection_headers_cb
        self.options["ws_compression"] = ws_compression
        self.options["ws_proxy_path"] = ws_proxy_path
        self.options["skip_subject_validation"] = skip_subject_validation
        self._skip_subject_validation = skip_subject_validation
        self.options["no_callbacks_after_client_close"] = no_callbacks_after_client_close
        self.options["ignore_auth_error_abort"] = ignore_auth_error_abort
        self.options["permission_err_on_subscribe"] = permission_err_on_subscribe
        self.options["retry_on_failed_connect"] = retry_on_failed_connect
        if custom_reconnect_delay_cb is not None and not callable(custom_reconnect_delay_cb):
            raise errors.Error("nats: custom_reconnect_delay_cb must be callable")
        self.options["custom_reconnect_delay_cb"] = custom_reconnect_delay_cb
        self.options["reconnect_jitter"] = reconnect_jitter
        self.options["reconnect_jitter_tls"] = reconnect_jitter_tls
        self.options["ignore_discovered_servers"] = ignore_discovered_servers

        if tls:
            self.options["tls"] = tls
        for tls_cb in (tls_cert_cb, tls_roots_cb):
            if tls_cb is not None and not callable(tls_cb):
                raise errors.Error("nats: tls_cert_cb and tls_roots_cb must be callable")
        if tls and (tls_cert_cb is not None or tls_roots_cb is not None):
            raise errors.Error("nats: tls cannot be combined with tls_cert_cb or tls_roots_cb")
        self.options["tls_cert_cb"] = tls_cert_cb
        self.options["tls_roots_cb"] = tls_roots_cb
        if tls_hostname:
            self.options["tls_hostname"] = tls_hostname

        # Check if the username or password was set in the server URI
        server_auth_configured = False
        if len(self._server_pool) > 0:
            for server in self._server_pool:
                if server.uri.username or server.uri.password:
                    server_auth_configured = True
                    break
        if user or password or token or server_auth_configured or user_info_cb is not None:
            self._auth_configured = True

        if self._user_credentials is not None or self._nkeys_seed is not None or self._nkeys_seed_str is not None:
            self._auth_configured = True
            self._setup_nkeys_connect()
        elif user_jwt_and_seed is not None:
            self._auth_configured = True
            self._setup_jwt_and_seed_connect(*user_jwt_and_seed)
        elif nkey:
            self._auth_configured = True
            self._public_nkey = nkey

        # Queue used to trigger flushes to the socket.
        self._flush_queue = asyncio.Queue(maxsize=flusher_queue_size)

        # Max size of buffer used for flushing commands to the server.
        self._max_pending_size = pending_size

        # Max duration for a force flush (happens when a buffer is full).
        self._flush_timeout = flush_timeout

        if self.options["dont_randomize"] is False:
            shuffle(self._server_pool)

        if retry_on_failed_connect and not await self._connect_to_pool_once():
            # Keep connecting in the background, as nats.go's RetryOnFailedConnect.
            self._status = Client.RECONNECTING
            self._reconnection_task = asyncio.get_running_loop().create_task(self._attempt_reconnect(initial=True))
            return

        while not retry_on_failed_connect:
            try:
                await self._select_next_server()
                await self._process_connect_init()
                assert self._current_server, "the current server must be set by _select_next_server"
                self._current_server.reconnects = 0
                break
            except errors.NoServersError as e:
                if self.options["max_reconnect_attempts"] < 0:
                    # Never stop reconnecting
                    continue
                self._err = e
                raise e
            except PermissionError:
                raise
            except (OSError, errors.Error, asyncio.TimeoutError) as e:
                self._err = e
                await self._error_cb(e)

                # Bail on first attempt if reconnecting is disallowed.
                if not self.options["allow_reconnect"]:
                    raise e

                await self._close(Client.DISCONNECTED, False)
                if self._current_server is not None:
                    self._current_server.last_attempt = time.monotonic()
                    self._current_server.reconnects += 1

        if self._connected_cb is not None:
            await self._connected_cb()

    async def _connect_to_pool_once(self) -> bool:
        """
        Tries each server of the pool once, as nats.go's initial connect.
        Returns whether a connection was established.
        """
        for s in list(self._server_pool):
            try:
                await self._connect_to_server(s)
                self._current_server = s
                await self._process_connect_init()
                s.reconnects = 0
                s.did_connect = True
                return True
            except PermissionError:
                raise
            except (OSError, errors.Error, asyncio.TimeoutError) as e:
                self._err = e
                await self._error_cb(e)
                await self._close(Client.DISCONNECTED, False)
                s.last_attempt = time.monotonic()
                s.reconnects += 1
        if self._current_server is None and self._server_pool:
            self._current_server = self._server_pool[0]
        return False

    def _check_auth_options(
        self,
        user: Any,
        password: Any,
        token: Any,
        signature_cb: Optional[SignatureCallback],
        user_jwt_cb: Optional[JWTCallback],
        user_credentials: Optional[Credentials],
        nkeys_seed: Optional[str],
        nkeys_seed_str: Optional[str],
        nkey: Optional[str],
        user_info_cb: Optional[UserInfoCallback],
        user_jwt_and_seed: Optional[Tuple[str, str]],
    ) -> None:
        """
        Rejects conflicting authentication options, as nats.go's UserJWT,
        Nkey, UserInfoHandler and TokenHandler options and connectProto.
        """
        generated = user_credentials is not None or nkeys_seed is not None or nkeys_seed_str is not None
        if nkey:
            if user_jwt_cb is not None or user_credentials is not None or user_jwt_and_seed is not None:
                raise errors.NkeyAndUserError
            if signature_cb is None:
                raise errors.NkeyButNoSigCBError
        if user_jwt_cb is not None and not generated and user_jwt_and_seed is None:
            if not callable(user_jwt_cb):
                raise errors.NoUserCBError
            if signature_cb is None:
                raise errors.UserButNoSigCBError
        if (
            signature_cb is not None
            and user_jwt_cb is None
            and not nkey
            and not generated
            and user_jwt_and_seed is None
        ):
            raise errors.NoUserCBError
        if user_info_cb is not None and (user or password):
            raise errors.UserInfoAlreadySetError
        if callable(token):
            for server in self._server_pool:
                if server.uri.username and server.uri.password is None:
                    raise errors.TokenAlreadySetError

    def _setup_jwt_and_seed_connect(self, user_jwt: str, seed: str) -> None:
        import nkeys

        def user_cb() -> bytearray:
            return bytearray(user_jwt.encode())

        def sig_cb(nonce: str) -> bytes:
            kp = nkeys.from_seed(bytearray(seed.encode()))
            sig = base64.b64encode(kp.sign(nonce.encode()))
            kp.wipe()
            return sig

        self._user_jwt_cb = user_cb
        self._signature_cb = sig_cb

    def _setup_nkeys_connect(self) -> None:
        if self._user_credentials is not None:
            self._setup_nkeys_jwt_connect()
        else:
            self._setup_nkeys_seed_connect()

    def _setup_nkeys_jwt_connect(self) -> None:
        assert self._user_credentials, "_user_credentials required"
        import os

        import nkeys

        creds: Credentials = self._user_credentials

        if isinstance(creds, tuple):
            assert len(creds) == 2

            def user_cb() -> bytearray:
                contents = None
                with open(creds[0], "rb") as f:
                    contents = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(contents)  # type: ignore[attr-defined]
                return contents

            self._user_jwt_cb = user_cb

            def sig_cb(nonce: str) -> bytes:
                seed = None
                with open(creds[1], "rb") as f:
                    seed = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(seed)  # type: ignore[attr-defined]
                kp = nkeys.from_seed(seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Best effort attempt to clear from memory.
                kp.wipe()
                del kp
                del seed
                return sig

            self._signature_cb = sig_cb
        elif isinstance(creds, str) or isinstance(creds, UserString) or isinstance(creds, Path):
            # Define the functions to be able to sign things using nkeys.
            def user_cb() -> bytearray:
                return self._read_creds_user_jwt(creds)

            self._user_jwt_cb = user_cb

            def sig_cb(nonce: str) -> bytes:
                user_seed = self._read_creds_user_nkey(creds)
                kp = nkeys.from_seed(user_seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Delete all state related to the keys.
                kp.wipe()
                del user_seed
                del kp
                return sig

            self._signature_cb = sig_cb

    def _read_creds_user_nkey(self, creds: str | UserString | Path) -> bytearray:
        def get_user_seed(f):
            for line in f:
                # Detect line where the NKEY would start and end,
                # then seek and read into a fixed bytearray that
                # can be wiped.
                if b"BEGIN USER NKEY SEED" in line:
                    nkey_start_pos = f.tell()
                    try:
                        next(f)
                    except StopIteration:
                        raise ErrInvalidUserCredentials
                    nkey_end_pos = f.tell()
                    nkey_size = nkey_end_pos - nkey_start_pos - 1
                    f.seek(nkey_start_pos)

                    # Only gather enough bytes for the user seed
                    # into the pre allocated bytearray.
                    user_seed = bytearray(nkey_size)
                    f.readinto(user_seed)  # type: ignore[attr-defined]
                    return user_seed

        if isinstance(creds, UserString):
            return get_user_seed(BytesIO(creds.data.encode()))

        with open(creds, "rb", buffering=0) as f:
            return get_user_seed(f)

    def _read_creds_user_jwt(self, creds: str | RawCredentials | Path):
        def get_user_jwt(f):
            user_jwt = None
            while True:
                line = bytearray(f.readline())
                if b"BEGIN NATS USER JWT" in line:
                    user_jwt = bytearray(f.readline())
                    break
            # Remove trailing line break but reusing same memory view.
            return user_jwt[: len(user_jwt) - 1]

        if isinstance(creds, UserString):
            return get_user_jwt(BytesIO(creds.data.encode()))

        with open(creds, "rb") as f:
            return get_user_jwt(f)

    def _setup_nkeys_seed_connect(self) -> None:
        assert self._nkeys_seed or self._nkeys_seed_str, "Client.connect must be called first"

        import nkeys

        def _get_nkeys_seed() -> nkeys.KeyPair:
            if self._nkeys_seed_str:
                seed = bytearray(self._nkeys_seed_str.strip().encode())
            else:
                creds = self._nkeys_seed
                with open(creds, "rb") as f:
                    seed = bytearray(f.read().strip())
            key_pair = nkeys.from_seed(seed)
            del seed
            return key_pair

        kp = _get_nkeys_seed()
        self._public_nkey = kp.public_key.decode()
        kp.wipe()
        del kp

        def sig_cb(nonce: str) -> bytes:
            kp = _get_nkeys_seed()
            raw_signed = kp.sign(nonce.encode())
            sig = base64.b64encode(raw_signed)

            # Best effort attempt to clear from memory.
            kp.wipe()
            del kp
            return sig

        self._signature_cb = sig_cb

    async def close(self) -> None:
        """
        Closes the socket to which we are connected and
        sets the client to be in the CLOSED state.
        No further reconnections occur once reaching this point.
        """
        await self._close(Client.CLOSED, not self.options.get("no_callbacks_after_client_close", False))

    async def _close(self, status: int, do_cbs: bool = True) -> None:
        if self.is_closed:
            self._status = status
            return
        self._status = Client.CLOSED
        close_err, self._close_err = self._close_err, None

        # Avoid cancelling the current task when _close is called from within
        # one of these tasks (e.g. _read_loop via _process_op_err), otherwise
        # the cancellation fires during the asyncio.sleep(0) below and the
        # disconnect/close callbacks are never invoked.
        current = asyncio.current_task()

        if self._reading_task is not None and not self._reading_task.cancelled() and self._reading_task is not current:
            self._reading_task.cancel()

        if (
            self._ping_interval_task is not None
            and not self._ping_interval_task.cancelled()
            and self._ping_interval_task is not current
        ):
            self._ping_interval_task.cancel()

        if self._flusher_task is not None and not self._flusher_task.cancelled() and self._flusher_task is not current:
            self._flusher_task.cancel()

        if self._reconnection_task is not None and not self._reconnection_task.done():
            self._reconnection_task.cancel()

            # Wait for the reconnection task to be done which should be soon.
            try:
                if self._reconnection_task_future is not None and not self._reconnection_task_future.cancelled():
                    await asyncio.wait_for(
                        self._reconnection_task_future,
                        self.options["reconnect_time_wait"],
                    )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # Relinquish control to allow background tasks to wrap up.
        await asyncio.sleep(0)

        if self._current_server is not None and self._transport:
            # In case there is any pending data at this point, flush before disconnecting.
            if self._pending_data_size > 0:
                self._transport.writelines(self._pending[:])
                self._pending = []
                self._pending_data_size = 0
                await self._transport.drain()

        # Cleanup subscriptions since not reconnecting so no need
        # to replay the subscriptions anymore.
        for sub in self._subs.values():
            # Async subs use join when draining already so just cancel here.
            if sub._wait_for_msgs_task and not sub._wait_for_msgs_task.done():
                sub._wait_for_msgs_task.cancel()
            if sub._message_iterator:
                sub._message_iterator._cancel()
            # Sync subs may have some inflight next_msg calls that could be blocking
            # so cancel them here to unblock them.
            if sub._pending_next_msgs_calls:
                for fut in sub._pending_next_msgs_calls.values():
                    fut.cancel()
                sub._pending_next_msgs_calls.clear()
        self._subs.clear()

        if self._transport is not None:
            self._transport.close()
            try:
                await self._transport.wait_closed()
            except Exception as e:
                await self._error_cb(e)

        if do_cbs:
            await self._notify_disconnected(close_err)
            if self._closed_cb is not None:
                await self._closed_cb()

        # Set the client_id and subscription prefix back to None
        self._client_id = None
        self._client_ip = None
        self._resp_sub_prefix = None
        self._resp_mux_subscribed = False

    async def drain(self) -> None:
        """
        drain will put a connection into a drain state. All subscriptions will
        immediately be put into a drain state. Upon completion, the publishers
        will be drained and can not publish any additional messages. Upon draining
        of the publishers, the connection will be closed. Use the `closed_cb`
        option to know when the connection has moved from draining to closed.

        """
        if self.is_draining:
            return
        if self.is_closed:
            raise errors.ConnectionClosedError
        if self.is_connecting or self.is_reconnecting:
            raise errors.ConnectionReconnectingError

        drain_tasks = []
        for sub in self._subs.values():
            coro = sub._drain()
            task = asyncio.get_running_loop().create_task(coro)
            drain_tasks.append(task)

        drain_is_done = asyncio.gather(*drain_tasks)

        # Start draining the subscriptions.
        # Relinquish CPU to allow drain tasks to start in the background,
        # before setting state to draining.
        await asyncio.sleep(0)
        self._status = Client.DRAINING_SUBS

        try:
            await asyncio.wait_for(drain_is_done, self.options["drain_timeout"])
        except asyncio.TimeoutError:
            drain_is_done.exception()
            drain_is_done.cancel()
            await self._error_cb(errors.DrainTimeoutError())
        except asyncio.CancelledError:
            pass
        finally:
            self._status = Client.DRAINING_PUBS
            await self.flush()
            await self._close(Client.CLOSED, not self.options.get("no_callbacks_after_client_close", False))

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        reply: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Publishes a NATS message.

        :param subject: Subject to which the message will be published.
        :param payload: Message data.
        :param reply: Inbox to which a responder can respond.
        :param headers: Optional message header.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect('demo.nats.io')

                # Publish as message with an inbox.
                inbox = nc.new_inbox()
                sub = await nc.subscribe('hello')

                # Simple publishing
                await nc.publish('hello', b'Hello World!')

                # Publish with a reply
                await nc.publish('hello', b'Hello World!', reply=inbox)

                # Publish with headers
                await nc.publish('hello', b'With Headers', headers={'Foo':'Bar'})

                while True:
                    try:
                        msg = await sub.next_msg()
                    except:
                        break
                    print('----------------------')
                    print('Subject:', msg.subject)
                    print('Reply  :', msg.reply)
                    print('Data   :', msg.data)
                    print('Headers:', msg.header)

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if not self._skip_subject_validation:
            _validate_subject(subject)
            if reply:
                _validate_subject(reply)

        if self.is_closed:
            raise errors.ConnectionClosedError
        if self.is_draining_pubs:
            raise errors.ConnectionDrainingError

        payload_size = len(payload)
        if not self.is_connected:
            if self._max_pending_size <= 0 or payload_size + self._pending_data_size > self._max_pending_size:
                # Cannot publish during a reconnection when the buffering is disabled,
                # or if pending buffer is already full.
                raise errors.OutboundBufferLimitError

        if payload_size > self._max_payload:
            raise errors.MaxPayloadError
        if headers is not None and self._server_info and not self._server_info.get("headers", False):
            raise errors.HeadersNotSupportedError
        await self._send_publish(subject, reply, payload, payload_size, headers)

    async def _send_publish(
        self,
        subject: str,
        reply: str,
        payload: bytes,
        payload_size: int,
        headers: Optional[Dict[str, Any]],
    ) -> None:
        """
        Sends PUB command to the NATS server.
        """
        if subject == "":
            # Avoid sending messages with empty replies.
            raise errors.BadSubjectError

        pub_cmd = None
        if headers is None:
            pub_cmd = prot_command.pub_cmd(subject, reply, payload)
        else:
            hdr = bytearray()
            hdr.extend(NATS_HDR_LINE)
            hdr.extend(_CRLF_)
            for k, v in headers.items():
                key = k.strip()
                if not key:
                    # Skip empty keys
                    continue
                if not _HEADER_KEY_RE.fullmatch(key):
                    raise errors.BadHeaderError(key)
                hdr.extend(key.encode())
                hdr.extend(b": ")
                value = v.strip().translate(_HEADER_VALUE_NEWLINES)
                hdr.extend(value.encode())
                hdr.extend(_CRLF_)
            hdr.extend(_CRLF_)
            pub_cmd = prot_command.hpub_cmd(subject, reply, hdr, payload)

        self.stats["out_msgs"] += 1
        self.stats["out_bytes"] += payload_size
        await self._send_command(pub_cmd)
        if self._flush_queue is not None and self._flush_queue.empty():
            await self._flush_pending()

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Optional[Callable[[Msg], Awaitable[None]]] = None,
        future: Optional[asyncio.Future] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
        """
        subscribe registers interest in a given subject.

        If a callback is provided, messages will be processed asynchronously.

        If a callback isn't provided, messages can be retrieved via an
        asynchronous iterator on the returned subscription object.
        """
        if self._skip_subject_validation:
            if not subject:
                raise errors.BadSubjectError
        else:
            _validate_subject(subject, strict=True)
            _validate_queue(queue)

        if self.is_closed:
            raise errors.ConnectionClosedError

        if self.is_draining:
            raise errors.ConnectionDrainingError

        self._sid += 1
        sid = self._sid

        sub = Subscription(
            self,
            sid,
            subject,
            queue=queue,
            cb=cb,
            future=future,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

        sub._start(self._dispatch_error)
        self._subs[sid] = sub
        await self._send_subscribe(sub)
        return sub

    def _remove_sub(self, sid: int, max_msgs: int = 0) -> None:
        self._subs.pop(sid, None)

    async def _send_subscribe(self, sub: Subscription) -> None:
        sub_cmd = None
        if sub._queue is None:
            sub_cmd = prot_command.sub_cmd(sub._subject, EMPTY, sub._id)
        else:
            sub_cmd = prot_command.sub_cmd(sub._subject, sub._queue, sub._id)
        await self._send_command(sub_cmd)
        await self._flush_pending()

    def _init_resp_sub_prefix(self) -> bytearray:
        if not self._resp_sub_prefix:
            self._resp_sub_prefix = self._inbox_prefix[:]
            self._resp_sub_prefix.extend(b".")
            self._resp_sub_prefix.extend(self._nuid.next())
            self._resp_sub_prefix.extend(b".")
        return self._resp_sub_prefix

    async def _init_request_sub(self) -> None:
        self._resp_map = {}

        self._init_resp_sub_prefix()
        assert self._resp_sub_prefix
        self._resp_mux_subscribed = True
        resp_mux_subject = self._resp_sub_prefix[:]
        resp_mux_subject.extend(b"*")
        await self.subscribe(resp_mux_subject.decode(), cb=self._request_sub_callback)

    async def _request_sub_callback(self, msg: Msg) -> None:
        token = msg.subject[len(self._inbox_prefix) + 22 + 2 :]
        future = self._resp_map.get(token)

        if not future:
            return

        if not future.done():
            future.set_result(msg)

    async def request(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: float = 0.5,
        old_style: bool = False,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Msg:
        """
        Implements the request/response pattern via pub/sub
        using a single wildcard subscription that handles
        the responses.

        """
        # Validated here as well as in publish() so a bad subject fails
        # before the inbox subscription and response future are set up.
        if not self._skip_subject_validation:
            _validate_subject(subject)

        if old_style:
            # FIXME: Support headers in old style requests.
            return await self._request_old_style(subject, payload, timeout=timeout)
        else:
            msg = await self._request_new_style(subject, payload, timeout=timeout, headers=headers)
        if msg.headers and msg.headers.get(nats.js.api.Header.STATUS) == NO_RESPONDERS_STATUS:
            raise errors.NoRespondersError
        return msg

    async def _request_new_style(
        self,
        subject: str,
        payload: bytes,
        timeout: float = 1,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Msg:
        if self.is_draining_pubs:
            raise errors.ConnectionDrainingError

        if not self._resp_sub_prefix or not self._resp_mux_subscribed:
            await self._init_request_sub()
        assert self._resp_sub_prefix

        # Use a new NUID + couple of unique token bytes to identify the request,
        token = self._nuid.next()
        token.extend(token_hex(2).encode())
        inbox = self._resp_sub_prefix[:]
        inbox.extend(token)

        # Then use the future to get the response.
        future: asyncio.Future = asyncio.Future()
        future.add_done_callback(lambda f: self._resp_map.pop(token.decode(), None))
        self._resp_map[token.decode()] = future

        # Publish the request
        await self.publish(subject, payload, reply=inbox.decode(), headers=headers)

        # Wait for the response or give up on timeout.
        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            raise errors.TimeoutError

    def new_inbox(self) -> str:
        """
        new_inbox returns a unique inbox that can be used
        for NATS requests or subscriptions::

           # Create unique subscription to receive direct messages.
           inbox = nc.new_inbox()
           sub = await nc.subscribe(inbox)
           nc.publish('broadcast', b'', reply=inbox)
           msg = sub.next_msg()
        """
        next_inbox = self._inbox_prefix[:]
        next_inbox.extend(b".")
        next_inbox.extend(self._nuid.next())
        return next_inbox.decode()

    def new_resp_inbox(self) -> str:
        """
        Returns a new inbox under the subject of the subscription that
        receives the responses of request(), as nats.go's NewRespInbox.
        """
        inbox = self._init_resp_sub_prefix()[:]
        inbox.extend(self._nuid.next())
        inbox.extend(token_hex(2).encode())
        return inbox.decode()

    async def _request_old_style(self, subject: str, payload: bytes, timeout: float = 1) -> Msg:
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with a limited interest of 1 reply returning the response
        or raising a Timeout error.
        """
        inbox = self.new_inbox()

        future: asyncio.Future[Msg] = asyncio.Future()
        sub = await self.subscribe(inbox, future=future, max_msgs=1)
        await sub.unsubscribe(limit=1)
        await self.publish(subject, payload, reply=inbox)

        try:
            msg = await asyncio.wait_for(future, timeout)
            if msg.headers:
                if msg.headers.get(nats.js.api.Header.STATUS) == NO_RESPONDERS_STATUS:
                    raise errors.NoRespondersError
            return msg
        except asyncio.TimeoutError:
            await sub.unsubscribe()
            future.cancel()
            raise errors.TimeoutError

    async def _send_unsubscribe(self, sid: int, limit: int = 0) -> None:
        unsub_cmd = prot_command.unsub_cmd(sid, limit)
        await self._send_command(unsub_cmd)
        await self._flush_pending()

    async def rtt(self, timeout: int = DEFAULT_FLUSH_TIMEOUT) -> float:
        """
        Returns the round trip time between the client and server
        in seconds by performing a PING/PONG exchange.
        In case a pong is not returned within the allowed timeout,
        then it will raise nats.errors.TimeoutError
        """
        if timeout <= 0:
            raise errors.BadTimeoutError

        if self.is_closed:
            raise errors.ConnectionClosedError
        if self.is_reconnecting:
            raise errors.DisconnectedError

        future: asyncio.Future = asyncio.Future()
        loop = asyncio.get_running_loop()
        start = loop.time()
        try:
            await self._send_ping(future)
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise errors.TimeoutError
        return loop.time() - start

    async def flush(self, timeout: int = DEFAULT_FLUSH_TIMEOUT) -> None:
        """
        Sends a ping to the server expecting a pong back ensuring
        what we have written so far has made it to the server and
        also enabling measuring of roundtrip time.
        In case a pong is not returned within the allowed timeout,
        then it will raise nats.errors.TimeoutError
        """
        if timeout <= 0:
            raise errors.BadTimeoutError

        if self.is_closed:
            raise errors.ConnectionClosedError

        # If the internal loops are dead (e.g. cancelled externally by
        # Python < 3.11 SIGINT handling), fall back to a direct flush
        # since a PING/PONG round-trip requires the read loop.
        if (self._reading_task is None or self._reading_task.done()) or (
            self._flusher_task is None or self._flusher_task.done()
        ):
            await self._flush_pending()
            return

        future: asyncio.Future = asyncio.Future()
        try:
            await self._send_ping(future)
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise errors.FlushTimeoutError

    @property
    def connected_url(self) -> Optional[ParseResult]:
        if self._current_server and self.is_connected:
            return self._current_server.uri
        return None

    @property
    def connected_url_redacted(self) -> Optional[str]:
        """
        The URL of the connected server with any password replaced by
        "xxxxx", as nats.go's ConnectedUrlRedacted.
        """
        uri = self.connected_url
        if uri is None:
            return None
        if uri.password is not None:
            userinfo = f"{uri.username}:xxxxx@" if uri.username is not None else ":xxxxx@"
            uri = uri._replace(netloc=userinfo + uri.netloc.rpartition("@")[2])
        return uri.geturl()

    @property
    def connected_addr(self) -> Optional[str]:
        """The address of the connected server's socket, as nats.go's ConnectedAddr."""
        if not self.is_connected or not self._transport:
            return None
        return _host_port(self._transport.get_extra_info("peername"))

    @property
    def local_addr(self) -> Optional[str]:
        """The local address of the connection's socket, as nats.go's LocalAddr."""
        if not self.is_connected or not self._transport:
            return None
        return _host_port(self._transport.get_extra_info("sockname"))

    def _connected_info(self) -> Dict[str, Any]:
        return self._server_info if self.is_connected else {}

    @property
    def connected_server_id(self) -> Optional[str]:
        """The id of the connected server, as nats.go's ConnectedServerId."""
        return self._connected_info().get("server_id")

    @property
    def connected_server_name(self) -> Optional[str]:
        """The name of the connected server, as nats.go's ConnectedServerName."""
        return self._connected_info().get("server_name")

    @property
    def connected_cluster_name(self) -> Optional[str]:
        """The cluster of the connected server, as nats.go's ConnectedClusterName."""
        return self._connected_info().get("cluster")

    @property
    def connected_domain(self) -> Optional[str]:
        """The JetStream domain of the connected server, as nats.go's ConnectedDomain."""
        return self._connected_info().get("domain")

    @property
    def connected_server_jetstream(self) -> bool:
        """Whether the connected server has JetStream enabled, as nats.go's ConnectedServerJetStream."""
        return bool(self._connected_info().get("jetstream", False))

    @property
    def connected_server_api_level(self) -> int:
        """The JetStream API level of the connected server (second result of ConnectedServerJetStream)."""
        return int(self._connected_info().get("api_lvl", 0))

    @property
    def is_system_account(self) -> bool:
        """Whether the connection's account is the system account, as nats.go's IsSystemAccount."""
        return bool(self._connected_info().get("acc_is_sys", False))

    @property
    def auth_required(self) -> bool:
        """Whether the server requires authentication, as nats.go's AuthRequired."""
        return bool(self._server_info.get("auth_required", False))

    @property
    def tls_required(self) -> bool:
        """Whether the server requires TLS, as nats.go's TLSRequired."""
        return bool(self._server_info.get("tls_required", False))

    @property
    def headers_supported(self) -> bool:
        """Whether the server supports message headers, as nats.go's HeadersSupported."""
        return bool(self._server_info.get("headers", False))

    @property
    def num_subscriptions(self) -> int:
        """The number of active subscriptions, as nats.go's NumSubscriptions."""
        return len(self._subs)

    def buffered(self) -> int:
        """
        The number of bytes waiting to be written to the server,
        as nats.go's Buffered.
        """
        if self.is_closed:
            raise errors.ConnectionClosedError
        return self._pending_data_size

    def get_client_id(self) -> int:
        """
        The client id assigned by the server, as nats.go's GetClientID.
        Unlike the client_id property, it raises when it is not known.
        """
        if self.is_closed:
            raise errors.ConnectionClosedError
        if self._client_id is None:
            raise errors.ClientIDNotSupportedError
        return self._client_id

    def get_client_ip(self) -> str:
        """
        The client IP as seen by the server, as nats.go's GetClientIP.
        Unlike the client_ip property, it raises when it is not known.
        """
        if self.is_closed:
            raise errors.ConnectionClosedError
        if not self._client_ip:
            raise errors.ClientIPNotSupportedError
        return self._client_ip

    def tls_connection_state(self) -> ssl.SSLObject:
        """
        The TLS session of the connection, as nats.go's TLSConnectionState:
        an ssl.SSLObject with version(), cipher(), getpeercert(), ...
        """
        if not self.is_connected or not self._transport:
            raise errors.DisconnectedError
        ssl_object = self._transport.get_extra_info("ssl_object")
        if ssl_object is None:
            raise errors.ConnectionNotTLSError
        return ssl_object

    async def barrier(self, fn: Callable[[], Any]) -> None:
        """
        Schedules fn (a function or coroutine function) to run once every
        subscription with a callback has processed the messages it received
        before this call, as nats.go's Barrier. Without such subscriptions
        fn runs right away.
        """
        if self.is_closed:
            raise errors.ConnectionClosedError
        subs = [
            sub
            for sub in self._subs.values()
            if sub._cb is not None and sub._wait_for_msgs_task is not None and not sub._wait_for_msgs_task.done()
        ]
        if not subs:
            result = fn()
            if inspect.isawaitable(result):
                await result
            return
        marker = _Barrier(len(subs), fn)
        for sub in subs:
            await sub._pending_queue.put(marker)  # type: ignore[arg-type]

    @property
    def servers(self) -> List[ParseResult]:
        servers = []
        for srv in self._server_pool:
            servers.append(srv.uri)
        return servers

    @property
    def discovered_servers(self) -> List[ParseResult]:
        servers = []
        for srv in self._server_pool:
            if srv.discovered:
                servers.append(srv.uri)
        return servers

    @property
    def server_pool(self) -> List[Server]:
        """
        Returns a copy of the current server pool.
        """
        return [Server(uri=srv.uri, reconnects=srv.reconnects) for srv in self._server_pool]

    def set_server_pool(self, servers: List[str]) -> None:
        """
        Replaces the current server pool with the provided list of server URLs.

        The new pool will be used on the next reconnect attempt. It does not
        trigger an immediate reconnect. The new pool is subject to the same
        rules as the default one (randomization unless disabled, max reconnect
        attempts, etc).

        Unless advertised server discovery is disabled, the client will
        continue to discover and add new servers to the pool as it receives
        INFO messages from the server.

        :param servers: List of server URLs to use as the new pool.
        :raises errors.ConnectionClosedError: If the connection is closed.
        """
        if self.is_closed:
            raise errors.ConnectionClosedError

        # Parse and validate all URLs first without modifying state.
        new_pool: List[Srv] = []
        for server in servers:
            uri = self._parse_server_uri(server)
            srv = Srv(uri)
            # Preserve state from existing pool entries.
            for old_srv in self._server_pool:
                if old_srv.uri.netloc == uri.netloc:
                    srv.reconnects = old_srv.reconnects
                    srv.did_connect = old_srv.did_connect
                    srv.last_attempt = old_srv.last_attempt
                    break
            new_pool.append(srv)

        self._server_pool = new_pool

        # Update _current_server to point to the corresponding server
        # in the new pool if it exists, so _select_next_server works correctly.
        if self._current_server is not None:
            current_netloc = self._current_server.uri.netloc
            found = False
            for srv in new_pool:
                if srv.uri.netloc == current_netloc:
                    self._current_server = srv
                    found = True
                    break
            if not found and len(new_pool) > 0:
                self._current_server = new_pool[0]

    @property
    def max_payload(self) -> int:
        """
        Returns the max payload which we received from the servers INFO
        """
        return self._max_payload

    @property
    def client_id(self) -> Optional[int]:
        """
        Returns the client id which we received from the servers INFO
        """
        return self._client_id

    @property
    def client_ip(self) -> Optional[str]:
        """
        Returns the client IP as reported by the server.
        """
        return self._client_ip

    @property
    def last_error(self) -> Optional[Exception]:
        """
        Returns the last error which may have occurred.
        """
        return self._err

    @property
    def pending_data_size(self) -> int:
        return self._pending_data_size

    @property
    def is_closed(self) -> bool:
        return self._status == Client.CLOSED

    @property
    def is_reconnecting(self) -> bool:
        return self._status == Client.RECONNECTING

    @property
    def is_connected(self) -> bool:
        return (self._status == Client.CONNECTED) or self.is_draining

    @property
    def is_connecting(self) -> bool:
        return self._status == Client.CONNECTING

    @property
    def is_draining(self) -> bool:
        return self._status == Client.DRAINING_SUBS or self._status == Client.DRAINING_PUBS

    @property
    def is_draining_pubs(self) -> bool:
        return self._status == Client.DRAINING_PUBS

    @property
    def connected_server_version(self) -> ServerVersion:
        """
        Returns the ServerVersion of the server to which the client
        is currently connected.
        """
        if self._current_server and self._current_server.server_version:
            return ServerVersion(self._current_server.server_version)
        return ServerVersion("0.0.0-unknown")

    @property
    def ssl_context(self) -> ssl.SSLContext:
        ssl_context: Optional[ssl.SSLContext] = None
        if "tls" in self.options:
            ssl_context = self.options.get("tls")
        else:
            ssl_context = ssl.create_default_context()
        if ssl_context is None:
            raise errors.Error("nats: no ssl context provided")
        return ssl_context

    def _handshake_ssl_context(self) -> ssl.SSLContext:
        """
        The SSLContext for a TLS handshake. With tls_cert_cb or tls_roots_cb
        it is built anew each time, as nats.go's ClientTLSConfig callbacks
        run on every handshake.
        """
        cert_cb = self.options.get("tls_cert_cb")
        roots_cb = self.options.get("tls_roots_cb")
        if cert_cb is None and roots_cb is None:
            return self.ssl_context
        if roots_cb is None:
            ssl_context = ssl.create_default_context()
        else:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            roots_cb(ssl_context)
        if cert_cb is not None:
            cert_cb(ssl_context)
        return ssl_context

    def _secure_wanted(self) -> bool:
        if self.options.get("tls") is not None:
            return True
        if self.options.get("tls_cert_cb") is not None or self.options.get("tls_roots_cb") is not None:
            return True
        return any(srv.uri.scheme == "tls" for srv in self._server_pool)

    async def _send_command(self, cmd: bytes) -> None:
        self._pending.append(cmd)
        self._pending_data_size += len(cmd)
        if self._max_pending_size > 0 and self._pending_data_size > self._max_pending_size:
            # Only flush force timeout on publish
            await self._flush_pending(force_flush=True)

    async def _flush_pending(
        self,
        force_flush: bool = False,
    ) -> Any:
        assert self._flush_queue, "Client.connect must be called first"
        try:
            future: asyncio.Future = asyncio.Future()
            if not self.is_connected:
                future.set_result(None)
                return future

            # If the flusher task is dead (e.g. cancelled externally by
            # Python < 3.11 SIGINT handling), flush inline instead of
            # queueing a future that will never be resolved.
            if self._flusher_task is None or self._flusher_task.done():
                if self._pending_data_size > 0:
                    self._transport.writelines(self._pending[:])
                    self._pending = []
                    self._pending_data_size = 0
                    await self._transport.drain()
                future.set_result(None)
                return future

            # kick the flusher!
            await self._flush_queue.put(future)

            if force_flush:
                try:
                    await asyncio.wait_for(future, self._flush_timeout)
                except asyncio.TimeoutError:
                    # Report to the async callback that there was a timeout.
                    await self._error_cb(errors.FlushTimeoutError())

        except asyncio.CancelledError:
            pass

    @staticmethod
    def _parse_server_uri(connect_url: str) -> ParseResult:
        """
        Parse a single server URL string into a ParseResult.
        Handles scheme defaults and port defaults.
        """
        try:
            if "nats://" in connect_url or "tls://" in connect_url:
                normalized = connect_url
            elif "ws://" in connect_url or "wss://" in connect_url:
                normalized = connect_url
            elif ":" in connect_url:
                normalized = f"nats://{connect_url}"
            else:
                normalized = f"nats://{connect_url}:4222"
            uri = urlparse(normalized)

            if uri.port is None and uri.scheme not in ("ws", "wss"):
                # Keep the scheme and any userinfo, add the default port.
                uri = uri._replace(netloc=f"{uri.netloc.rstrip(':')}:4222")
        except ValueError:
            raise errors.Error("nats: invalid connect url option")

        if uri.hostname is None or uri.hostname == "none":
            raise errors.Error("nats: invalid hostname in connect url")
        return uri

    def _setup_server_pool(self, connect_url: Union[List[str]]) -> None:
        if isinstance(connect_url, str):
            uri = self._parse_server_uri(connect_url)
            self._server_pool.append(Srv(uri))
        elif isinstance(connect_url, list):
            for server in connect_url:
                # Route through _parse_server_uri so the list path shares
                # the single-string path's scheme/port defaults.
                uri = self._parse_server_uri(server)
                self._server_pool.append(Srv(uri))
            # make sure protocols aren't mixed
            if not (
                all(server.uri.scheme in ("nats", "tls") for server in self._server_pool)
                or all(server.uri.scheme in ("ws", "wss") for server in self._server_pool)
            ):
                raise errors.MixingWebsocketSchemesError
        else:
            raise errors.Error("nats: invalid connect url option")

    async def _connect_to_server(self, s: Srv) -> None:
        """
        Establishes a TCP/WebSocket connection to the given server.
        """
        s.last_attempt = time.monotonic()
        if not self._transport:
            if s.uri.scheme in ("ws", "wss"):
                self._transport = WebSocketTransport(
                    ws_headers=self.options["ws_connection_headers"],
                    ws_headers_cb=self.options.get("ws_connection_headers_cb"),
                    compression=self.options.get("ws_compression", False),
                    proxy_path=self.options.get("ws_proxy_path"),
                )
            else:
                self._transport = TcpTransport()
        if s.uri.scheme == "wss":
            await self._connect_tls(s.uri)
        else:
            await self._transport.connect(
                s.uri,
                buffer_size=DEFAULT_BUFFER_SIZE,
                connect_timeout=self.options["connect_timeout"],
            )

    async def _connect_tls(self, uri: Union[str, ParseResult]) -> None:
        """
        Runs the TLS handshake, raising a failure as errors.TLSError,
        as nats.go wraps it in ErrTLS.
        """
        assert self._transport
        try:
            await self._transport.connect_tls(
                uri,
                self._handshake_ssl_context(),
                DEFAULT_BUFFER_SIZE,
                self.options["connect_timeout"],
            )
        except errors.TLSError:
            raise
        except ssl.SSLError as e:
            cls = errors.TLSCertVerificationError if isinstance(e, ssl.SSLCertVerificationError) else errors.TLSError
            err = cls(*e.args)
            for attr in ("library", "reason", "verify_code", "verify_message"):
                if hasattr(e, attr):
                    setattr(err, attr, getattr(e, attr))
            raise err from e

    async def _select_next_server(self) -> None:
        """
        Looks up in the server pool for an available server
        and attempts to connect.
        """

        while True:
            if len(self._server_pool) == 0:
                self._current_server = None
                raise errors.NoServersError

            now = time.monotonic()
            s = self._server_pool.pop(0)
            if self.options["max_reconnect_attempts"] > 0:
                if s.reconnects > self.options["max_reconnect_attempts"]:
                    # Discard server since already tried to reconnect too many times
                    continue

            # Not yet exceeded max_reconnect_attempts so can still use
            # this server in the future.
            self._server_pool.append(s)
            if s.last_attempt is not None and now < s.last_attempt + self.options["reconnect_time_wait"]:
                # Backoff connecting to server if we attempted recently.
                await asyncio.sleep(self._reconnect_delay())
            try:
                await self._connect_to_server(s)
                self._current_server = s
                break
            except Exception as e:
                s.last_attempt = time.monotonic()
                s.reconnects += 1

                self._err = e
                await self._error_cb(e)
                if self.is_reconnecting:
                    await self._notify_reconnect_error(e)
                continue

    def _reconnect_delay(self) -> float:
        """
        How long to back off before trying a server again, as nats.go's
        doReconnect: the custom delay callback's answer, or else
        reconnect_time_wait plus a random jitter.
        """
        self._reconnect_backoffs += 1
        custom_delay = self.options.get("custom_reconnect_delay_cb")
        if custom_delay is not None:
            return custom_delay(self._reconnect_backoffs)
        delay = self.options["reconnect_time_wait"]
        secure = self._secure_wanted() or bool(self._server_info.get("tls_required", False))
        jitter = self.options.get("reconnect_jitter_tls" if secure else "reconnect_jitter", 0)
        if jitter and jitter > 0:
            delay += random.random() * jitter
        return delay

    async def _process_err(self, err_msg: str) -> None:
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        if STALE_CONNECTION in err_msg:
            await self._process_op_err(errors.StaleConnectionError())
            return

        if AUTHENTICATION_EXPIRED in err_msg:
            if ACCOUNT_AUTHENTICATION_EXPIRED in err_msg:
                await self._process_op_err(errors.AccountAuthExpiredError())
            else:
                await self._process_op_err(errors.AuthenticationExpiredError())
            return

        if AUTHORIZATION_VIOLATION in err_msg:
            self._err = errors.AuthorizationError()
        else:
            prot_err = err_msg.strip("'")
            err = _server_error(prot_err)
            self._err = err

            # Neither error makes the server close the connection, so they
            # are only reported, as nats.go's processTransientError.
            if isinstance(err, (errors.PermissionViolationError, errors.MaxSubscriptionsExceededError)):
                if isinstance(err, errors.PermissionViolationError) and self.options.get(
                    "permission_err_on_subscribe", False
                ):
                    self._process_permission_violation(prot_err, err)
                await self._error_cb(err)
                return

        do_cbs = False
        if not self.is_connecting:
            do_cbs = True

        # FIXME: Some errors such as 'Invalid Subscription'
        # do not cause the server to close the connection.
        # For now we handle similar as other clients and close.
        self._close_err = self._err
        asyncio.create_task(self._close(Client.CLOSED, do_cbs))

    def _process_permission_violation(self, description: str, err: Exception) -> None:
        """
        Hands a subscribe permissions violation to the subscriptions with that
        subject and queue group, as nats.go's PermissionErrOnSubscribe.
        The server's text reads: Permissions Violation for Subscription to
        "<subject>" [using queue "<queue>"]. The parser lowercases it, so
        subjects are compared without case.
        """
        match = _PERMISSION_SUB_RE.search(description)
        if match is None:
            return
        subject = match.group(1)
        queue = match.group(2) or ""
        for sub in list(self._subs.values()):
            if sub._subject.lower() == subject.lower() and (sub._queue or "").lower() == queue.lower():
                sub._set_permission_error(err)

    async def force_reconnect(self) -> None:
        """
        Initiate a reconnection to another server in the pool.
        """
        if self.is_closed or self.is_reconnecting or not self.is_connected:
            return
        if not self.options["allow_reconnect"]:
            return
        self._status = Client.RECONNECTING
        self._disconnect_err = None
        self._ps.reset()
        if self._reconnection_task is not None and not self._reconnection_task.cancelled():
            self._reconnection_task.cancel()
        self._reconnection_task = asyncio.get_running_loop().create_task(self._attempt_reconnect())

    async def _process_op_err(self, e: Exception) -> None:
        """
        Process errors which occurred while reading or parsing
        the protocol. If allow_reconnect is enabled it will
        try to switch the server to which it is currently connected
        otherwise it will disconnect.
        """
        if self.is_closed or self.is_reconnecting:
            return

        if self.options["allow_reconnect"] and (self.is_connected or self.is_connecting):
            self._status = Client.RECONNECTING
            self._disconnect_err = e
            self._ps.reset()

            if self._reconnection_task is not None and not self._reconnection_task.cancelled():
                # Cancel the previous task in case it may still be running.
                self._reconnection_task.cancel()

            self._reconnection_task = asyncio.get_running_loop().create_task(self._attempt_reconnect())
        else:
            self._process_disconnect()
            self._err = e
            self._close_err = e
            await self._close(Client.CLOSED, True)

    async def _attempt_reconnect(self, initial: bool = False) -> None:
        """
        Reconnects to a server of the pool. With ``initial``, it establishes
        the first connection instead, for retry_on_failed_connect.
        """
        assert self._current_server, "Client.connect must be called first"
        if self._reading_task is not None and not self._reading_task.cancelled():
            self._reading_task.cancel()

        if self._ping_interval_task is not None and not self._ping_interval_task.cancelled():
            self._ping_interval_task.cancel()

        if self._flusher_task is not None and not self._flusher_task.cancelled():
            self._flusher_task.cancel()

        if self._transport is not None:
            self._transport.close()
            try:
                await self._transport.wait_closed()
            except Exception as e:
                await self._error_cb(e)

        self._err = None
        disconnect_err, self._disconnect_err = self._disconnect_err, None
        if not initial:
            await self._notify_disconnected(disconnect_err)

        if self.is_closed:
            return

        if "dont_randomize" not in self.options or not self.options["dont_randomize"]:
            shuffle(self._server_pool)

        self._reconnect_backoffs = 0

        # Create a future that the client can use to control waiting
        # on the reconnection attempts.
        self._reconnection_task_future = asyncio.Future()
        while True:
            try:
                if self._reconnect_to_server_handler is not None:
                    # Invoke the user-provided handler to select a server.
                    max_reconnect = self.options["max_reconnect_attempts"]
                    if max_reconnect > 0:
                        eligible = [s for s in self._server_pool if s.reconnects <= max_reconnect]
                    else:
                        eligible = list(self._server_pool)

                    if len(eligible) == 0:
                        raise errors.NoServersError

                    server_snapshot = [Server(uri=s.uri, reconnects=s.reconnects) for s in eligible]
                    try:
                        selected, callback_delay = self._reconnect_to_server_handler(
                            server_snapshot, self._server_info.copy()
                        )
                    except Exception as e:
                        await self._error_cb(e)
                        await self._notify_reconnect_error(e)
                        continue

                    if selected is not None:
                        matched = None
                        for s in eligible:
                            if s.uri.netloc == selected.uri.netloc:
                                matched = s
                                break
                        if matched is not None:
                            self._current_server = matched
                        else:
                            not_in_pool = errors.ServerNotInPoolError()
                            await self._error_cb(not_in_pool)
                            await self._notify_reconnect_error(not_in_pool)
                            selected = None

                    if selected is None:
                        self._current_server = eligible[0]

                    if callback_delay > 0:
                        await asyncio.sleep(callback_delay)

                    await self._connect_to_server(self._current_server)
                else:
                    # Default server selection via round-robin.
                    await self._select_next_server()

                assert self._transport
                await self._process_connect_init()

                # Consider a reconnect to be done once CONNECT was
                # processed by the server successfully.
                if not initial:
                    self.stats["reconnects"] += 1

                # Reset reconnect attempts for this server
                # since have successfully connected.
                self._current_server.did_connect = True
                self._current_server.reconnects = 0
                self._current_server.last_auth_error = None

                # Replay all the subscriptions in case there were some.
                subs_to_remove = []
                for sid, sub in self._subs.items():
                    # The new server decides afresh about permissions.
                    sub._clear_permission_error()
                    max_msgs = 0
                    if sub._max_msgs > 0:
                        # If we already hit the message limit, remove the subscription and don't
                        # resubscribe.
                        if sub._received >= sub._max_msgs:
                            subs_to_remove.append(sid)
                            continue
                        # auto unsubscribe the number of messages we have left
                        max_msgs = sub._max_msgs - sub._received

                    sub_cmd = prot_command.sub_cmd(sub._subject, sub._queue, sid)
                    self._transport.write(sub_cmd)

                    if max_msgs > 0:
                        unsub_cmd = prot_command.unsub_cmd(sid, max_msgs)
                        self._transport.write(unsub_cmd)

                for sid in subs_to_remove:
                    self._subs.pop(sid)

                await self._transport.drain()

                # Flush pending data before continuing in connected status.
                # FIXME: Could use future here and wait for an error result
                # to bail earlier in case there are errors in the connection.
                # await self._flush_pending(force_flush=True)
                await self._flush_pending()
                self._status = Client.CONNECTED
                await self.flush()
                if initial:
                    if self._connected_cb is not None:
                        await self._connected_cb()
                elif self._reconnected_cb is not None:
                    await self._reconnected_cb()
                self._reconnection_task_future = None
                break
            except errors.NoServersError as e:
                self._err = e
                self._close_err = e
                await self._close(Client.CLOSED)
                break
            except PermissionError:
                raise
            except (OSError, errors.Error, asyncio.TimeoutError) as e:
                self._err = e
                await self._error_cb(e)
                await self._notify_reconnect_error(e)
                if self._abort_on_auth_error(e):
                    self._close_err = e
                    await self._close(Client.CLOSED)
                    break
                self._status = Client.RECONNECTING
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1
            except asyncio.CancelledError:
                break

        if self._reconnection_task_future is not None and not self._reconnection_task_future.cancelled():
            self._reconnection_task_future.set_result(True)

    def _abort_on_auth_error(self, e: Exception) -> bool:
        """
        Whether reconnecting should stop because the current server rejected
        the credentials twice in a row with the same authentication error,
        as nats.go's processAuthError unless IgnoreAuthErrorAbort is set.
        """
        if not isinstance(
            e,
            (errors.AuthorizationError, errors.AuthenticationExpiredError, errors.AuthRevokedError),
        ):
            return False
        server = self._current_server
        if server is None:
            return False
        if server.last_auth_error is type(e) and not self.options.get("ignore_auth_error_abort", False):
            return True
        server.last_auth_error = type(e)
        return False

    async def _dispatch_error(self, e: Exception) -> None:
        """
        Reports an asynchronous error to the error callback set at the time.
        """
        await self._error_cb(e)

    async def _notify_disconnected(self, err: Optional[Exception]) -> None:
        if self._disconnected_err_cb is not None:
            await self._disconnected_err_cb(err)
        elif self._disconnected_cb is not None:
            await self._disconnected_cb()

    async def _notify_reconnect_error(self, err: Exception) -> None:
        if self._reconnect_error_cb is not None:
            await self._reconnect_error_cb(err)

    @staticmethod
    def _check_callback(cb: Optional[Callable[..., Any]]) -> None:
        if cb is not None and not inspect.iscoroutinefunction(cb):
            raise errors.InvalidCallbackTypeError

    def set_error_cb(self, cb: Optional[ErrorCallback]) -> None:
        """
        Replaces the callback that reports asynchronous errors,
        as nats.go's SetErrorHandler. None restores the default logging one.
        """
        self._check_callback(cb)
        self._error_cb = cb or _default_error_callback

    def set_disconnected_cb(self, cb: Optional[Callback]) -> None:
        """Replaces the disconnected callback, as nats.go's SetDisconnectHandler."""
        self._check_callback(cb)
        self._disconnected_cb = cb

    def set_disconnected_err_cb(self, cb: Optional[DisconnectedErrorCallback]) -> None:
        """
        Replaces the disconnected callback that receives the error,
        as nats.go's SetDisconnectErrHandler.
        """
        self._check_callback(cb)
        self._disconnected_err_cb = cb

    def set_reconnected_cb(self, cb: Optional[Callback]) -> None:
        """Replaces the reconnected callback, as nats.go's SetReconnectHandler."""
        self._check_callback(cb)
        self._reconnected_cb = cb

    def set_closed_cb(self, cb: Optional[Callback]) -> None:
        """Replaces the closed callback, as nats.go's SetClosedHandler."""
        self._check_callback(cb)
        self._closed_cb = cb

    def set_discovered_server_cb(self, cb: Optional[Callback]) -> None:
        """
        Replaces the callback that reports discovered servers,
        as nats.go's SetDiscoveredServersHandler.
        """
        self._check_callback(cb)
        self._discovered_server_cb = cb

    @property
    def error_cb(self) -> Optional[ErrorCallback]:
        """The error callback, None when unset, as nats.go's ErrorHandler."""
        if self._error_cb is _default_error_callback:
            return None
        return self._error_cb

    @property
    def disconnected_cb(self) -> Optional[Callback]:
        return self._disconnected_cb

    @property
    def disconnected_err_cb(self) -> Optional[DisconnectedErrorCallback]:
        return self._disconnected_err_cb

    @property
    def reconnected_cb(self) -> Optional[Callback]:
        return self._reconnected_cb

    @property
    def closed_cb(self) -> Optional[Callback]:
        return self._closed_cb

    @property
    def discovered_server_cb(self) -> Optional[Callback]:
        return self._discovered_server_cb

    def _connect_command(self) -> bytes:
        """
        Generates a JSON string with the params to be used
        when sending CONNECT to the server.

          ->> CONNECT {"lang": "python3"}

        """
        options = {
            "verbose": self.options["verbose"],
            "pedantic": self.options["pedantic"],
            "lang": __lang__,
            "version": __version__,
            "protocol": PROTOCOL,
        }
        if "headers" in self._server_info:
            options["headers"] = self._server_info["headers"]
            options["no_responders"] = self._server_info["headers"]

        if self._auth_configured:
            if "nonce" in self._server_info and self._signature_cb is not None:
                sig = self._signature_cb(self._server_info["nonce"])
                options["sig"] = sig.decode()

                if self._user_jwt_cb is not None:
                    jwt = self._user_jwt_cb()
                    options["jwt"] = jwt.decode()
                elif self._public_nkey is not None:
                    options["nkey"] = self._public_nkey

                # Token can be sent alongside nkey/JWT for auth callouts.
                if self.options["token"] is not None:
                    token = self.options["token"]
                    if callable(token):
                        token = token()
                    options["auth_token"] = token
                if self.options["password"] is not None:
                    password = self.options["password"]
                    if callable(password):
                        password = password()
                    options["pass"] = password
            # In case there is no password, then consider handle
            # sending a token instead.
            elif self.options["user"] is not None and self.options["password"] is not None:
                user = self.options["user"]
                if callable(user):
                    user = user()
                password = self.options["password"]
                if callable(password):
                    password = password()
                options["user"] = user
                options["pass"] = password
            elif self.options.get("user_info_cb") is not None:
                options["user"], options["pass"] = self.options["user_info_cb"]()
            elif self.options["token"] is not None:
                token = self.options["token"]
                if callable(token):
                    token = token()
                options["auth_token"] = token
            elif self._current_server and self._current_server.uri.username is not None:
                if self._current_server.uri.password is None:
                    options["auth_token"] = self._current_server.uri.username
                else:
                    options["user"] = self._current_server.uri.username
                    options["pass"] = self._current_server.uri.password

        if self.options["name"] is not None:
            options["name"] = self.options["name"]
        if self.options["no_echo"] is not None:
            options["echo"] = not self.options["no_echo"]

        connect_opts = json.dumps(options, sort_keys=True)
        return b"".join([CONNECT_OP + _SPC_ + connect_opts.encode() + _CRLF_])

    async def _process_ping(self) -> None:
        """
        Process PING sent by server.
        """
        await self._send_command(PONG)
        await self._flush_pending()

    async def _process_pong(self) -> None:
        """
        Process PONG sent by server.
        """
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            # The future is already done if its flush() timed out or was cancelled.
            if not future.done():
                future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding = 0

    @staticmethod
    def _parse_header_lines(raw: bytes) -> Dict[str, str]:
        """Parse a NATS message-header block (`Name: Value\\r\\n` per line).

        NATS headers are HTTP-flavoured but not emails — no RFC 2047
        encoded-words, no folding, no charset decoding. A byte-level
        split + decode avoids `email.parser.BytesParser`'s Header-object
        return for non-ASCII values, which silently dropped the entire
        headers dict via `_default_error_callback` (see #491, #924).

        Values are decoded with `errors="replace"` so malformed UTF-8
        becomes U+FFFD rather than raising. Net improvement over the
        previous silent-`None` failure, but a U+FFFD in a value may
        indicate transport corruption rather than an intentional code
        point — callers needing to round-trip raw bytes can use
        `errors="surrogateescape"` instead.
        """
        out: Dict[str, str] = {}
        for line in raw.split(_CRLF_):
            if not line:
                continue
            # Split on `:` (not `: `) so `Name:Value` and `Name:\tValue`
            # forms parse — `email.parser` normalised OWS after the
            # colon, so accepting them here keeps behaviour parity.
            name, sep, value = line.partition(b":")
            if not sep:
                continue
            try:
                key = name.strip().decode("ascii")
            except UnicodeDecodeError:
                # Malformed name (non-ASCII bytes) — skip rather than emit
                # a U+FFFD-laced key that's unreachable via normal lookup.
                continue
            # Header names are tokens; whitespace inside the name is
            # invalid (RFC 5322 §3.6.8 — same rule the existing
            # `fast_mail_parser` post-pass enforces).
            if any(c.isspace() for c in key):
                continue
            out[key] = value.strip().decode("utf-8", "replace")
        return out

    def _is_control_message(self, data, header: Dict[str, str]) -> Optional[str]:
        if len(data) > 0:
            return None
        status = header.get(nats.js.api.Header.STATUS)
        if status == CTRL_STATUS:
            return header.get(nats.js.api.Header.DESCRIPTION)
        return None

    async def _process_headers(self, headers) -> Optional[Dict[str, str]]:
        if not headers:
            return None

        hdr: Optional[Dict[str, str]] = None
        raw_headers = headers[NATS_HDR_LINE_SIZE:]

        # If the first character is an empty space, then this is
        # an inline status message sent by the server.
        #
        # NATS/1.0 404\r\n\r\n
        # NATS/1.0 503\r\n\r\n
        # NATS/1.0 404 No Messages\r\n\r\n
        #
        # Note: it is possible to receive a message with both inline status
        # and a set of headers.
        #
        # NATS/1.0 100\r\nIdle Heartbeat\r\nNats-Last-Consumer: 1016\r\nNats-Last-Stream: 1024\r\n\r\n
        #
        if raw_headers[0] == _SPC_BYTE_:
            # Special handling for status messages.
            line = headers[len(NATS_HDR_LINE) + 1 :]
            status = line[:STATUS_MSG_LEN]
            desc = line[STATUS_MSG_LEN + 1 : len(line) - _CRLF_LEN_ - _CRLF_LEN_]
            stripped_status = status.strip().decode()

            # Process as status only when it is a valid integer.
            hdr = {}
            if stripped_status.isdigit():
                hdr[nats.js.api.Header.STATUS.value] = stripped_status

            # Move the raw_headers to end of line
            i = raw_headers.find(_CRLF_)
            raw_headers = raw_headers[i + _CRLF_LEN_ :]

            if len(desc) > 0:
                # Heartbeat messages can have both headers and inline status,
                # check that there are no pending headers to be parsed.
                i = desc.find(_CRLF_)
                if i > 0:
                    hdr[nats.js.api.Header.DESCRIPTION] = desc[:i].decode()
                    hdr.update(self._parse_header_lines(desc[i + _CRLF_LEN_ :]))
                else:
                    # Just inline status...
                    hdr[nats.js.api.Header.DESCRIPTION] = desc.decode()

        if not len(raw_headers) > _CRLF_LEN_:
            return hdr

        #
        # Example header without status:
        #
        # NATS/1.0\r\nfoo: bar\r\nhello: world
        #
        raw_headers = headers[NATS_HDR_LINE_SIZE + _CRLF_LEN_ :]
        try:
            if parse_email:
                parsed_hdr = parse_email(raw_headers).headers
            else:
                parsed_hdr = self._parse_header_lines(raw_headers)
            if hdr:
                hdr.update(parsed_hdr)
            else:
                hdr = parsed_hdr

            if parse_email:
                to_delete = []
                for k in hdr.keys():
                    if any(c in k for c in string.whitespace):
                        to_delete.append(k)
                for k in to_delete:
                    del hdr[k]

        except Exception as e:
            err = errors.BadHeaderMsgError()
            err.__cause__ = e
            await self._error_cb(err)
            return hdr

        return hdr or None

    async def _process_msg(
        self,
        sid: int,
        subject: bytes,
        reply: bytes,
        data: bytes,
        headers: bytes,
    ) -> None:
        """
        Process MSG sent by server.
        """
        payload_size = len(data)
        self.stats["in_msgs"] += 1
        self.stats["in_bytes"] += payload_size

        sub = self._subs.get(sid)
        if not sub:
            # Skip in case no subscription present.
            return

        sub._received += 1
        if sub._max_msgs > 0 and sub._received >= sub._max_msgs:
            # Enough messages so can throwaway subscription now, the
            # pending messages will still be in the subscription
            # internal queue and the task will finish once the last
            # message is processed.
            self._subs.pop(sid, None)

        hdr = await self._process_headers(headers)
        msg = self._build_message(sid, subject, reply, data, hdr)
        if not msg:
            return

        # Process flow control messages in case of using a JetStream context.
        ctrl_msg = None
        fc_reply = None
        if sub._jsi:
            #########################################
            #                                       #
            # JetStream Control Messages Processing #
            #                                       #
            #########################################
            jsi = sub._jsi
            jsi._active = True
            if hdr:
                ctrl_msg = self._is_control_message(data, hdr)

                # Check if the heartbeat has a "Consumer Stalled" header, if
                # so, the value is the FC reply to send a nil message to.
                # We will send it at the end of this function.
                if ctrl_msg and ctrl_msg.startswith("Idle"):
                    fc_reply = hdr.get(nats.js.api.Header.CONSUMER_STALLED)

            # OrderedConsumer: checkOrderedMsgs
            if not ctrl_msg and jsi._ordered and msg.reply:
                did_reset = None
                tokens = Msg.Metadata._get_metadata_fields(msg.reply)
                # FIXME: Support JS Domains.
                sseq = int(tokens[5])
                dseq = int(tokens[6])
                if dseq != jsi._dseq:
                    # Pick up from where we last left.
                    did_reset = await jsi.reset_ordered_consumer(jsi._sseq + 1)
                else:
                    # Update our tracking
                    jsi._dseq = dseq + 1
                    jsi._sseq = sseq
                if did_reset:
                    return

        # Skip processing if this is a control message.
        if not ctrl_msg:
            # Check if it is an old style request.
            if sub._future:
                if sub._future.cancelled():
                    # Already gave up, nothing to do.
                    return
                sub._future.set_result(msg)
                return

            # Let subscription wait_for_msgs coroutine process the messages,
            # but in case sending to the subscription task would block,
            # then consider it to be an slow consumer and drop the message.
            try:
                sub._pending_size += payload_size
                # allow setting pending_bytes_limit to 0 to disable
                if sub._pending_bytes_limit > 0 and sub._pending_size >= sub._pending_bytes_limit:
                    # Subtract the bytes since the message will be thrown away
                    # so it would not be pending data.
                    sub._pending_size -= payload_size

                    await self._error_cb(
                        errors.SlowConsumerError(subject=msg.subject, reply=msg.reply, sid=sid, sub=sub)
                    )
                    return
                sub._pending_queue.put_nowait(msg)
            except asyncio.QueueFull:
                sub._pending_size -= len(msg.data)
                await self._error_cb(errors.SlowConsumerError(subject=msg.subject, reply=msg.reply, sid=sid, sub=sub))

            # Store the ACK metadata from the message to
            # compare later on with the received heartbeat.
            if sub._jsi:
                sub._jsi.track_sequences(msg.reply)
        elif ctrl_msg.startswith("Flow") and msg.reply and sub._jsi:
            # This is a flow control message.
            # We will schedule the send of the FC reply once we have delivered the
            # DATA message that was received before this flow control message, which
            # has sequence `jsi.fciseq`. However, it is possible that this message
            # has already been delivered, in that case, we need to send the FC reply now.
            if sub._jsi.get_js_delivered() >= sub._jsi._fciseq:
                fc_reply = msg.reply
            else:
                # Schedule a reply after the previous message is delivered.
                sub._jsi.schedule_flow_control_response(msg.reply)

        # Handle flow control response.
        if fc_reply:
            await self.publish(fc_reply)

        if ctrl_msg and not msg.reply and ctrl_msg.startswith("Idle"):
            if sub._jsi:
                await sub._jsi.check_for_sequence_mismatch(msg)

    def _build_message(
        self,
        sid: int,
        subject: bytes,
        reply: bytes,
        data: bytes,
        headers: Optional[Dict[str, str]],
    ):
        return self.msg_class(
            subject=subject.decode(),
            reply=reply.decode(),
            data=data,
            headers=headers,
            _client=self,
            _sid=sid,
        )

    def _process_disconnect(self) -> None:
        """
        Process disconnection from the server and set client status
        to DISCONNECTED.
        """
        self._status = Client.DISCONNECTED

    async def _process_info(self, info: Dict[str, Any], initial_connection: bool = False) -> None:
        """
        Process INFO lines sent by the server to reconfigure client
        with latest updates from cluster to enable server discovery.
        """
        assert self._current_server, "Client.connect must be called first"
        if "connect_urls" in info and not self.options.get("ignore_discovered_servers", False):
            if info["connect_urls"]:
                connect_urls = []
                for connect_url in info["connect_urls"]:
                    scheme = ""
                    if self._current_server.uri.scheme in ("tls", "ws", "wss"):
                        # A WebSocket client is sent WebSocket URLs.
                        scheme = self._current_server.uri.scheme
                    else:
                        scheme = "nats"

                    uri = urlparse(f"{scheme}://{connect_url}")
                    srv = Srv(uri)
                    srv.discovered = True

                    # Check whether we should reuse the original hostname.
                    if (
                        "tls_required" in self._server_info
                        and self._server_info["tls_required"]
                        and self._host_is_ip(uri.hostname)
                    ):
                        srv.tls_name = self._current_server.uri.hostname

                    # Filter for any similar server in the server pool already.
                    should_add = True
                    for s in self._server_pool:
                        if uri.netloc == s.uri.netloc:
                            should_add = False
                    if should_add:
                        connect_urls.append(srv)

                if self.options["dont_randomize"] is not True:
                    shuffle(connect_urls)
                for srv in connect_urls:
                    self._server_pool.append(srv)

                if not initial_connection and connect_urls and self._discovered_server_cb:
                    await self._discovered_server_cb()

        if not initial_connection and info.get("ldm", False):
            if self._lame_duck_mode_cb is not None:
                await self._lame_duck_mode_cb()

    def _host_is_ip(self, connect_url: Optional[str]) -> bool:
        if connect_url is None:
            return False
        try:
            ipaddress.ip_address(connect_url)
            return True
        except Exception:
            return False

    async def _process_connect_init(self) -> None:
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.  It is also responsible of setting up the
        reading and ping interval tasks from the client.
        """
        assert self._transport, "must be called only from Client.connect"
        assert self._current_server, "must be called only from Client.connect"
        self._status = Client.CONNECTING

        # Check whether to reuse the original hostname for an implicit route.
        hostname = None
        if "tls_hostname" in self.options:
            hostname = self.options["tls_hostname"]
        elif self._current_server.tls_name is not None:
            hostname = self._current_server.tls_name
        else:
            hostname = self._current_server.uri.hostname

        handshake_first = self.options["tls_handshake_first"]
        if handshake_first:
            await self._connect_tls(hostname)

        connection_completed = self._transport.readline()
        info_line = await asyncio.wait_for(connection_completed, self.options["connect_timeout"])
        if INFO_OP not in info_line:
            # FIXME: Handle PING/PONG arriving first as well.
            raise errors.NoInfoReceivedError

        _, info = info_line.split(INFO_OP + _SPC_, 1)

        try:
            srv_info = json.loads(info.decode())
            self._server_info = srv_info
        except Exception:
            raise errors.Error("nats: info message, json parse error")

        # In case 'auth_required' is part of INFO, then need to send credentials.
        if srv_info.get("auth_required", False):
            self._auth_configured = True

        await self._process_info(srv_info, initial_connection=True)

        if "version" in self._server_info:
            self._current_server.server_version = self._server_info["version"]

        if "max_payload" in self._server_info:
            self._max_payload = self._server_info["max_payload"]

        if "client_id" in self._server_info:
            self._client_id = self._server_info["client_id"]

        if "client_ip" in self._server_info:
            self._client_ip = self._server_info["client_ip"]

        if self.options.get("nkey") and "nonce" not in self._server_info:
            raise errors.NkeysNotSupportedError

        if self.options["no_echo"] and self._server_info.get("proto", 0) < 1:
            raise errors.NoEchoNotSupportedError

        scheme = self._current_server.uri.scheme
        tls_required = bool(self._server_info.get("tls_required", False))
        # A tls:// URL or an explicit TLS context asks for a secure connection
        # even when the server does not require one, as nats.go's Secure option.
        secure = scheme not in ("ws", "wss") and self._secure_wanted()
        if secure and not tls_required and not self._server_info.get("tls_available", False):
            raise errors.SecureConnWantedError

        if (tls_required and scheme != "ws") or secure:
            if not handshake_first:
                await self._transport.drain()  # just in case something is left

                # connect to transport via tls
                await self._connect_tls(hostname)

        # Refresh state of parser upon reconnect.
        if self.is_reconnecting:
            self._ps.reset()

        assert self._transport
        connect_cmd = self._connect_command()
        self._transport.write(connect_cmd)
        try:
            await self._transport.drain()
        except OSError as e:
            raise await self._handshake_write_error(e)
        if self.options["verbose"]:
            future = self._transport.readline()
            next_op = await asyncio.wait_for(future, self.options["connect_timeout"])
            if OK_OP in next_op:
                # Do nothing
                pass
            elif ERR_OP in next_op:
                err_line = next_op.decode()
                _, err_msg = err_line.split(" ", 1)
                raise _server_error(err_msg.rstrip("\r\n"))

        self._transport.write(PING_PROTO)
        try:
            await self._transport.drain()
        except OSError as e:
            raise await self._handshake_write_error(e)

        future = self._transport.readline()
        next_op = await asyncio.wait_for(future, self.options["connect_timeout"])

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED
        elif ERR_OP in next_op:
            err_line = next_op.decode()
            _, err_msg = err_line.split(" ", 1)
            raise _server_error(err_msg.rstrip("\r\n"))

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED

        self._reading_task = asyncio.get_running_loop().create_task(self._read_loop())
        self._pongs = []
        self._pings_outstanding = 0
        self._ping_interval_task = asyncio.get_running_loop().create_task(self._ping_interval())

        # Task for kicking the flusher queue
        self._flusher_task = asyncio.get_running_loop().create_task(self._flusher())

    async def _handshake_write_error(self, error: Exception) -> Exception:
        """
        A server that rejects a connection (e.g. maximum connections
        exceeded) sends -ERR and closes it, so writing CONNECT may fail
        first. Reports the server's error when it was received.
        """
        assert self._transport
        try:
            line = await asyncio.wait_for(self._transport.readline(), 0.5)
        except Exception:
            # A reset connection fails reads even with the -ERR received.
            buffered_line = getattr(self._transport, "_buffered_line", None)
            line = buffered_line() if buffered_line is not None else b""
        if line.startswith(ERR_OP):
            _, err_msg = line.decode().split(" ", 1)
            return _server_error(err_msg.rstrip("\r\n"))
        return error

    async def _send_ping(self, future: Optional[asyncio.Future] = None) -> None:
        assert self._transport, "Client.connect must be called first"
        if future is None:
            future = asyncio.Future()
        self._pongs.append(future)
        # Queue the PING behind the pending commands so that its PONG
        # confirms they reached the server.
        await self._send_command(PING_PROTO)
        await self._flush_pending()

    async def _flusher(self) -> None:
        """
        Coroutine which continuously tries to consume pending commands
        and then flushes them to the socket.
        """
        assert self._transport, "Client.connect must be called first"
        assert self._flush_queue, "Client.connect must be called first"
        while True:
            if not self.is_connected or self.is_connecting:
                break

            future: asyncio.Future = await self._flush_queue.get()

            try:
                if self._pending_data_size > 0:
                    self._transport.writelines(self._pending[:])
                    self._pending = []
                    self._pending_data_size = 0
                    await self._transport.drain()
            except OSError as e:
                await self._error_cb(e)
                await self._process_op_err(e)
                break
            except (asyncio.CancelledError, RuntimeError, AttributeError):
                # RuntimeError in case the event loop is closed
                break
            finally:
                # future might have been cancelled.  See issue #624
                if not future.done():
                    future.set_result(None)

    async def _ping_interval(self) -> None:
        while True:
            await asyncio.sleep(self.options["ping_interval"])
            if not self.is_connected:
                continue
            try:
                self._pings_outstanding += 1
                if self._pings_outstanding > self.options["max_outstanding_pings"]:
                    await self._process_op_err(ErrStaleConnection())
                    return
                await self._send_ping()
            except (asyncio.CancelledError, RuntimeError, AttributeError):
                break
            # except asyncio.InvalidStateError:
            #     pass

    async def _read_loop(self) -> None:
        """
        Coroutine which gathers bytes sent by the server
        and feeds them to the protocol parser.
        In case of error while reading, it will stop running
        and its task has to be rescheduled.
        """
        while True:
            try:
                should_bail = self.is_closed or self.is_reconnecting
                if should_bail or self._transport is None:
                    break
                if self._transport.at_eof():
                    err = errors.UnexpectedEOF()
                    await self._error_cb(err)
                    await self._process_op_err(err)
                    break

                b = await self._transport.read(DEFAULT_BUFFER_SIZE)
                await self._ps.parse(b)
            except errors.ProtocolError:
                await self._process_op_err(errors.ProtocolError())
                break
            except OSError as e:
                await self._process_op_err(e)
                break
            except asyncio.CancelledError:
                break
            except Exception as ex:
                _logger.error("nats: encountered error", exc_info=ex)
                break
            # except asyncio.InvalidStateError:
            #     pass

    async def __aenter__(self) -> "Client":
        """For when NATS client is used in a context manager"""
        return self

    async def __aexit__(self, *exc_info) -> None:
        """Close connection to NATS when used in a context manager"""
        await self._close(Client.CLOSED, do_cbs=not self.options.get("no_callbacks_after_client_close", False))

    def jetstream(self, **opts) -> nats.js.JetStreamContext:
        """
        jetstream returns a context that can be used to produce and consume
        messages from NATS JetStream.

        :param prefix: Default JetStream API Prefix.
        :param domain: Optional domain used by the JetStream API.
        :param timeout: Timeout for all JS API actions.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='hello', subjects=['hello'])
                ack = await js.publish('hello', b'Hello JS!')
                print(f'Ack: stream={ack.stream}, sequence={ack.seq}')
                # Ack: stream=hello, sequence=1
                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())
        """
        return nats.js.JetStreamContext(self, **opts)

    def jsm(self, **opts) -> nats.js.JetStreamManager:
        """JetStream context for managing JetStream via JS API"""
        return nats.js.JetStreamManager(self, **opts)
