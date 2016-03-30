# Copyright 2015 Apcera Inc. All rights reserved.

"""
NATS network protocol parser.
"""

import asyncio

INFO_OP     = b'INFO'
CONNECT_OP  = b'CONNECT'
PUB_OP      = b'PUB'
MSG_OP      = b'MSG'
SUB_OP      = b'SUB'
UNSUB_OP    = b'UNSUB'
PING_OP     = b'PING'
PONG_OP     = b'PONG'
OK_OP       = b'+OK'
ERR_OP      = b'-ERR'
MSG_END     = b'\n'
_CRLF_      = b'\r\n'
_SPC_       = b' '

OK          = OK_OP + _CRLF_
PING        = PING_OP + _CRLF_
PONG        = PONG_OP + _CRLF_
CRLF_SIZE   = len(_CRLF_)
OK_SIZE     = len(OK)
PING_SIZE   = len(PING)
PONG_SIZE   = len(PONG)
MSG_OP_SIZE = len(MSG_OP)
ERR_OP_SIZE = len(ERR_OP)

# States
AWAITING_CONTROL_LINE   = 1
AWAITING_MSG_ARG        = 2
AWAITING_MSG_PAYLOAD    = 3
AWAITING_MINUS_ERR_ARG  = 4
MAX_CONTROL_LINE_SIZE   = 1024

class Msg(object):

    def __init__(self,
                 subject='',
                 reply='',
                 data=b'',
                 sid=0,
                 ):
        self.subject = subject
        self.reply   = reply
        self.data    = data
        self.sid     = sid

class Parser(object):

    OP_START         = 1
    OP_PLUS          = 2
    OP_PLUS_O        = 3
    OP_PLUS_OK       = 4
    OP_MINUS         = 5
    OP_MINUS_E       = 6
    OP_MINUS_ER      = 7
    OP_MINUS_ERR     = 8
    OP_MINUS_ERR_SPC = 9
    MINUS_ERR_ARG    = 10
    OP_M             = 11
    OP_MS            = 12
    OP_MSG           = 13
    OP_MSG_SPC       = 14
    MSG_ARG          = 15
    MSG_PAYLOAD      = 16
    MSG_END          = 17
    OP_P             = 18
    OP_PI            = 19
    OP_PIN           = 20
    OP_PING          = 21
    OP_PO            = 22
    OP_PON           = 23
    OP_PONG          = 24

    def __init__(self, nc=None):
        self.nc = nc
        self.reset()

    def __repr__(self):
        return "<nats protocol parser state={0}>".format(self.state)

    def reset(self):
        self.state = Parser.OP_START
        self.needed = 0

        # New style parsing
        self.msg_arg = {}
        self.msg_buf = None
        self.msg_arg_buf = None
        self.pos = 0
        self.drop = 0

    @asyncio.coroutine
    def parse(self, data=''):
        """
        Parses the wire protocol from NATS for the client
        and dispatches the subscription callbacks.
        """
        i = 0
        buflen = len(data)
        buf = memoryview(data)
        while i < buflen:
            c = buf[i:i+1]

            if self.state is Parser.OP_START:
                # In case there is a parsing error setting the next state,
                # then it will stop parsing and raise and exception which
                # process_op_error in client should handle.
                if c == b'M' or c == b'm':
                    self.state = Parser.OP_M
                elif c == b'P' or c == b'p':
                    self.state = Parser.OP_P
                elif c == b'+':
                    self.state = Parser.OP_PLUS
                elif c == b'-':
                    self.state = Parser.OP_MINUS
                else:
                    raise ErrProtocol("nats: parsing error in OP_START state")
            elif self.state is Parser.OP_M:
                if c == b'S' or c == b's':
                    self.state = Parser.OP_MS
                else:
                    raise ErrProtocol("nats: parsing error in OP_M state")
            elif self.state is Parser.OP_MS:
                if c == b'G' or c == b'g':
                    self.state = Parser.OP_MSG
                else:
                    raise ErrProtocol("nats: parsing error in OP_MS state")
            elif self.state is Parser.OP_MSG:
                if c == b' ' or c == b'\t':
                    self.state = Parser.OP_MSG_SPC
                else:
                    raise ErrProtocol("nats: parsing error in OP_MSG state")
            elif self.state is Parser.OP_MSG_SPC:
                if c == b' ' or c == b'\t':
                    continue
                else:
                    # Matched start of subject
                    self.state = Parser.MSG_ARG
                    self.pos = i
            elif self.state is Parser.MSG_ARG:
                if c == b'\r':
                    self.drop = 1
                elif c == b'\n':
                    # Ready to process msg arguments
                    msg_arg_buf = bytearray()
                    if self.msg_arg_buf is not None:
                        msg_arg_buf = self.msg_arg_buf
                    else:
                        msg_arg_buf.extend(bytes(buf[self.pos:i-self.drop]))
                    args = msg_arg_buf.split(_SPC_)

                    # Check in case of using a queue.
                    args_size = len(args)
                    if args_size == 4:
                        self.msg_arg["subject"] = args[0]
                        self.msg_arg["sid"] = int(args[1])
                        self.msg_arg["reply"] = args[2]
                        self.needed = int(args[3])
                    elif args_size == 3:
                        self.msg_arg["subject"] = args[0]
                        self.msg_arg["sid"] = int(args[1])
                        self.msg_arg["reply"] = b''
                        self.needed = int(args[2])
                    else:
                        raise ErrProtocol("nats: wrong number of arguments in MSG")
                    self.drop = 0
                    self.pos = i+1
                    self.state = Parser.MSG_PAYLOAD

                    # Jump ahead with the index. If this overruns
                    # what is left we fall out from the loop and
                    # process the rest as split buffer.
                    i = self.pos + self.needed - 1
                else:
                    if self.msg_arg_buf is not None:
                        self.msg_arg_buf.extend(c)
            elif self.state is Parser.MSG_PAYLOAD:
                if self.msg_buf is not None:
                    have_bytes = len(self.msg_buf)
                    if have_bytes >= self.needed:
                        # Still need _CRLF_ to follow the protocol
                        # but we can dispatch the message here already.
                        subject = self.msg_arg["subject"].decode()
                        reply   = self.msg_arg["reply"]
                        sid     = self.msg_arg["sid"]
                        payload = bytes(self.msg_buf)
                        msg     = Msg(subject=subject, reply=reply, data=payload, sid=sid)
                        yield from self.nc.msg_queue.put(msg)
                        self.msg_arg_buf = None
                        self.msg_buf = None
                        self.state = Parser.MSG_END
                    else:
                        # Copy as much as we can from the buffer
                        to_copy = self.needed - have_bytes
                        avail = buflen - i

                        if avail < to_copy:
                            to_copy = avail

                        if to_copy > 0:
                            self.msg_buf = b''.join([self.msg_buf, bytes(buf[i:i+to_copy])])
                            i = i + to_copy - 1
                        else:
                            self.msg_buf = b''.join([self.msg_buf, c])
                elif i-self.pos >= self.needed:
                    # Can slice upto enough bytes now since there are enough
                    # in current read and buffer is not split..
                    subject = self.msg_arg["subject"].decode()
                    reply   = self.msg_arg["reply"]
                    sid     = self.msg_arg["sid"]
                    payload = bytes(buf[self.pos:i])
                    msg     = Msg(subject=subject, reply=reply, data=payload, sid=sid)
                    yield from self.nc.msg_queue.put(msg)
                    self.msg_arg_buf = None
                    self.msg_buf = None
                    self.state = Parser.MSG_END
            elif self.state is Parser.MSG_END:
                if c == b'\n':
                    self.drop = 0
                    self.pos = i + 1
                    self.state = Parser.OP_START
                else:
                    continue

            # -----------------------------------------------------------
            i += 1

        # Split buffer with control line
        if self.state in (Parser.MSG_ARG, Parser.MINUS_ERR_ARG) and self.msg_arg_buf is None:
            # FIXME: Should be used for '-ERR' arguments as well.
            self.msg_arg_buf = bytearray()
            self.msg_arg_buf.extend(bytes(buf[self.pos:i-self.drop]))

        # Check for split message payload
        if self.state is Parser.MSG_PAYLOAD and self.msg_buf is None:
            self.msg_buf = data[self.pos:]

class ErrProtocol(Exception):
    def __str__(self):
        return "nats: Protocol Error"
