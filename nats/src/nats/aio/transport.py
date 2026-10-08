from __future__ import annotations

import abc
import asyncio
import inspect
import ssl
from typing import Awaitable, Callable, Dict, List, Optional, Union
from urllib.parse import ParseResult

try:
    import aiohttp
    import multidict
except ImportError:
    aiohttp = None  # type: ignore[assignment]
    multidict = None  # type: ignore[assignment]

from nats.errors import ProtocolError


class Transport(abc.ABC):
    @abc.abstractmethod
    async def connect(self, uri: ParseResult, buffer_size: int, connect_timeout: int):
        """
        Connects to a server using the implemented transport. The uri passed is of type ParseResult that can be
        obtained calling urllib.parse.urlparse.
        """
        pass

    @abc.abstractmethod
    async def connect_tls(
        self,
        uri: Union[str, ParseResult],
        ssl_context: ssl.SSLContext,
        buffer_size: int,
        connect_timeout: int,
    ):
        """
        connect_tls is similar to connect except it tries to connect to a secure endpoint, using the provided ssl
        context. The uri can be provided as string in case the hostname differs from the uri hostname, in case it
        was provided as 'tls_hostname' on the options.
        """
        pass

    @abc.abstractmethod
    def write(self, payload: bytes):
        """
        Write bytes to underlying transport. Needs a call to drain() to be successfully written.
        """
        pass

    @abc.abstractmethod
    def writelines(self, payload: List[bytes]):
        """
        Writes a list of bytes, one by one, to the underlying transport. Needs a call to drain() to be successfully
        written.
        """
        pass

    @abc.abstractmethod
    async def read(self, buffer_size: int) -> bytes:
        """
        Reads a sequence of bytes from the underlying transport, up to buffer_size. The buffer_size is ignored in case
        the transport carries already frames entire messages (i.e. websocket).
        """
        pass

    @abc.abstractmethod
    async def readline(self) -> bytes:
        """
        Reads one whole frame of bytes (or message) from the underlying transport.
        """
        pass

    @abc.abstractmethod
    async def drain(self):
        """
        Flushes the bytes queued for transmission when calling write() and writelines().
        """
        pass

    @abc.abstractmethod
    async def wait_closed(self):
        """
        Waits until the connection is successfully closed.
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        Closes the underlying transport.
        """
        pass

    @abc.abstractmethod
    def at_eof(self) -> bool:
        """
        Returns if underlying transport is at eof.
        """
        pass

    @abc.abstractmethod
    def __bool__(self):
        """
        Returns if the transport was initialized, either by calling connect of connect_tls.
        """
        pass

    def get_extra_info(self, name: str, default=None):
        """
        Returns information about the underlying connection, as
        asyncio.BaseTransport.get_extra_info (e.g. "peername", "sockname",
        "ssl_object"). Transports that cannot tell return the default.
        """
        return default


class TcpTransport(Transport):
    def __init__(self):
        self._bare_io_reader: Optional[asyncio.StreamReader] = None
        self._io_reader: Optional[asyncio.StreamReader] = None
        self._bare_io_writer: Optional[asyncio.StreamWriter] = None
        self._io_writer: Optional[asyncio.StreamWriter] = None

    async def connect(self, uri: ParseResult, buffer_size: int, connect_timeout: int):
        r, w = await asyncio.wait_for(
            asyncio.open_connection(
                host=uri.hostname,
                port=uri.port,
                limit=buffer_size,
            ),
            connect_timeout,
        )
        # We keep a reference to the initial transport we used when
        # establishing the connection in case we later upgrade to TLS
        # after getting the first INFO message. This is in order to
        # prevent the GC closing the socket after we send CONNECT
        # and replace the transport.
        #
        # See https://github.com/nats-io/asyncio-nats/issues/43
        self._bare_io_reader = self._io_reader = r
        self._bare_io_writer = self._io_writer = w

    async def connect_tls(
        self,
        uri: Union[str, ParseResult],
        ssl_context: ssl.SSLContext,
        buffer_size: int,
        connect_timeout: int,
    ) -> None:
        assert self._io_writer, f"{type(self).__name__}.connect must be called first"

        # manually recreate the stream reader/writer with a tls upgraded transport
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport_future = asyncio.get_running_loop().start_tls(
            self._io_writer.transport,
            protocol,
            ssl_context,
            # hostname here will be passed directly as string
            server_hostname=uri if isinstance(uri, str) else uri.hostname,
        )
        transport = await asyncio.wait_for(transport_future, connect_timeout)
        writer = asyncio.StreamWriter(transport, protocol, reader, asyncio.get_running_loop())
        self._io_reader, self._io_writer = reader, writer

    def write(self, payload):
        return self._io_writer.write(payload)

    def writelines(self, payload):
        return self._io_writer.writelines(payload)

    async def read(self, buffer_size: int):
        assert self._io_reader, f"{type(self).__name__}.connect must be called first"
        return await self._io_reader.read(buffer_size)

    async def readline(self):
        return await self._io_reader.readline()

    async def drain(self):
        return await self._io_writer.drain()

    def _buffered_line(self) -> bytes:
        """
        Returns a complete line that was received but not read yet, which
        a reader whose connection was reset no longer hands out.
        """
        buffer = getattr(self._io_reader, "_buffer", None)
        if not buffer:
            return b""
        end = buffer.find(b"\r\n")
        if end < 0:
            return b""
        return bytes(buffer[: end + 2])

    async def wait_closed(self):
        if self._io_writer is not None:
            return await self._io_writer.wait_closed()

    def close(self):
        if self._io_writer is not None:
            return self._io_writer.close()

    def at_eof(self):
        if self._io_reader is None:
            return True
        return self._io_reader.at_eof()

    def __bool__(self):
        return bool(self._io_writer) and bool(self._io_reader)

    def get_extra_info(self, name: str, default=None):
        if self._io_writer is None:
            return default
        return self._io_writer.get_extra_info(name, default)


WebSocketHeaders = Dict[str, Union[str, List[str]]]
WebSocketHeadersCallback = Callable[[], Union[WebSocketHeaders, Awaitable[WebSocketHeaders]]]


class WebSocketTransport(Transport):
    def __init__(
        self,
        ws_headers: Optional[Dict[str, List[str]]] = None,
        ws_headers_cb: Optional[WebSocketHeadersCallback] = None,
        compression: bool = False,
        proxy_path: Optional[str] = None,
    ):
        if not aiohttp:
            raise ImportError("Could not import aiohttp transport, please install it with `pip install aiohttp`")
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._client: aiohttp.ClientSession = aiohttp.ClientSession()
        self._pending = asyncio.Queue()
        self._close_task = asyncio.Future()
        self._using_tls: Optional[bool] = None
        self._ws_headers = ws_headers
        self._ws_headers_cb = ws_headers_cb
        # permessage-deflate with the largest window, as aiohttp's compress=15.
        self._compress = 15 if compression else 0
        self._proxy_path = proxy_path

    def _url(self, uri: ParseResult) -> str:
        """The URL to dial; a proxy path replaces the server URL's path, as in nats.go."""
        if self._proxy_path:
            path = self._proxy_path if self._proxy_path.startswith("/") else "/" + self._proxy_path
            uri = uri._replace(path=path)
        return uri.geturl()

    async def connect(self, uri: ParseResult, buffer_size: int, connect_timeout: int):
        headers = await self._headers()
        # for websocket library, the uri must contain the scheme already
        self._ws = await self._client.ws_connect(
            self._url(uri),
            timeout=connect_timeout,
            headers=headers,
            max_msg_size=0,
            compress=self._compress,
        )
        self._using_tls = False

    async def connect_tls(
        self,
        uri: Union[str, ParseResult],
        ssl_context: ssl.SSLContext,
        buffer_size: int,
        connect_timeout: int,
    ):
        if self._ws and not self._ws.closed:
            if self._using_tls:
                return
            raise ProtocolError("ws: cannot upgrade to TLS")

        headers = await self._headers()
        self._ws = await self._client.ws_connect(
            uri if isinstance(uri, str) else self._url(uri),
            ssl=ssl_context,
            timeout=connect_timeout,
            headers=headers,
            max_msg_size=0,
            compress=self._compress,
        )
        self._using_tls = True

    def write(self, payload):
        self._pending.put_nowait(payload)

    def writelines(self, payload):
        for message in payload:
            self.write(message)

    async def read(self, buffer_size: int):
        return await self.readline()

    async def readline(self):
        data = await self._ws.receive()
        if data.type == aiohttp.WSMsgType.BINARY:
            return data.data
        if data.type == aiohttp.WSMsgType.ERROR:
            # msg.data is the underlying aiohttp exception. Wrap it in a
            # ConnectionError so the read loop's OSError handler triggers a
            # reconnect instead of its generic-Exception log-and-break branch.
            raise ConnectionError("nats: websocket error") from data.data
        # CLOSE, CLOSING, CLOSED, TEXT, or any other unexpected frame type:
        # signal EOF so the read loop goes through its normal reconnect path.
        return b""

    async def drain(self):
        # send all the messages pending
        while not self._pending.empty():
            message = self._pending.get_nowait()
            await self._ws.send_bytes(message)

    async def wait_closed(self):
        if self._ws is not None:
            await self._close_task
        if self._client:
            await self._client.close()
        self._ws = self._client = None

    def close(self):
        if self._ws is None:
            return
        self._close_task = asyncio.create_task(self._ws.close())

    def at_eof(self):
        return self._ws.closed

    def __bool__(self):
        return bool(self._client)

    def get_extra_info(self, name: str, default=None):
        if self._ws is None:
            return default
        return self._ws.get_extra_info(name, default)

    async def _headers(self):
        """
        The handshake headers: the static ones, or the ones the headers
        callback returns for this connection attempt.
        """
        if self._ws_headers_cb is None:
            return self._get_custom_headers()
        headers = self._ws_headers_cb()
        if inspect.isawaitable(headers):
            headers = await headers
        return self._get_custom_headers(headers)

    def _get_custom_headers(self, ws_headers=None):
        if ws_headers is None:
            ws_headers = self._ws_headers
        if ws_headers is None:
            return None
        md: multidict.CIMultiDict[str] = multidict.CIMultiDict()
        for name, values in ws_headers.items():
            if isinstance(values, list):
                for v in values:
                    md.add(name, v)
            elif isinstance(values, str):
                md.add(name, values)
        return md
