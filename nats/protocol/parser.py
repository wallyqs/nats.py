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
AWAITING_MSG_END        = 5
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

    def __init__(self, nc=None):
        self.nc = nc
        self.reset()

    def __repr__(self):
        return "<nats protocol parser state={0}>".format(self.state)

    def reset(self):
        self.state   = AWAITING_CONTROL_LINE
        self.needed  = 0
        self.msg_arg = {}
        self.msg_buf = bytearray()
        self.msg_buf_size = 0

        self.scratch = bytearray()
        self.scratch_size = 0

    @asyncio.coroutine
    def parse(self, buf=''):
        """
        Parses the wire protocol from NATS for the client.
        """
        i = 0
        buflen = len(buf)
        while i < buflen:
            if self.state == AWAITING_CONTROL_LINE:
                # Get at least enough bytes for processing a control line.
                scratch = buf[i:i+MAX_CONTROL_LINE_SIZE]
                self.scratch.extend(scratch)

                # Split buffer so break already.
                if _CRLF_ not in self.scratch:
                    self.scratch_size += len(scratch)
                    break

                # MSG
                if self.scratch.startswith(MSG_OP):
                    # Move position to end of this control line and process
                    # the message arguments already.
                    si = self.scratch.find(_CRLF_)
                    line = self.scratch[:si]
                    args = line.split(_SPC_)

                    # Check in case of using a queue.
                    args_size = len(args)
                    if args_size == 5:
                        self.msg_arg["subject"] = args[1]
                        self.msg_arg["sid"] = int(args[2])
                        self.msg_arg["reply"] = args[3]
                        self.needed = int(args[4])
                    elif args_size == 4:
                        self.msg_arg["subject"] = args[1]
                        self.msg_arg["sid"] = int(args[2])
                        self.msg_arg["reply"] = b''
                        self.needed = int(args[3])
                    else:
                        raise ErrProtocol("nats: Wrong number of arguments in MSG")

                    # Set next state and position
                    i += len(line) + CRLF_SIZE - self.scratch_size
                    self.scratch = bytearray()
                    self.scratch_size = 0
                    self.state = AWAITING_MSG_PAYLOAD

                # OK
                elif self.scratch.startswith(OK):

                    # Move position to be after +OK
                    i += OK_SIZE
                    self.scratch = bytearray()
                    self.scratch_size = 0
                    self.state = AWAITING_CONTROL_LINE

                # -ERR
                elif self.scratch.startswith(ERR_OP):

                    # TODO: Handle ERROR
                    si = self.scratch.find(_CRLF_)
                    line = self.scratch[:si]
                    _, err = line.split(_SPC_, 1)
                    i += len(line) + CRLF_SIZE - self.scratch_size
                    self.scratch = bytearray()
                    self.scratch_size = 0
                    yield from self.nc._process_err(err)

                # PONG
                elif self.scratch.startswith(PONG):

                    # Move position to be after PONG
                    i += PONG_SIZE
                    self.scratch = bytearray()
                    self.scratch_size = 0
                    self.state = AWAITING_CONTROL_LINE
                    yield from self.nc._process_pong()

                # PING
                elif self.scratch.startswith(PING):

                    # Move position to be after PING
                    i += PING_SIZE
                    self.scratch = bytearray()
                    self.scratch_size = 0
                    self.state = AWAITING_CONTROL_LINE
                    yield from self.nc._process_ping()

                else:
                    raise ErrProtocol("nats: Unknown Protocol")

            elif self.state == AWAITING_MSG_PAYLOAD:

                if self.msg_buf_size < self.needed:
                    # Need to take enough bytes and append to current buf
                    before_copy = self.msg_buf_size
                    needed_buf = buf[i:i+self.needed]
                    self.msg_buf.extend(needed_buf)

                    i += self.needed - before_copy
                    self.msg_buf_size = len(self.msg_buf)
                    if self.msg_buf_size < self.needed:
                        # Still not enough bytes and split buffer so just break
                        # and handle in next read.
                        break
                else:
                    i += self.needed

                # Set next stage already before dispatching to callback.
                self.scratch = bytearray()
                self.scratch_size = 0
                self.state = AWAITING_MSG_END

                subject = self.msg_arg["subject"].decode()
                sid     = self.msg_arg["sid"]
                reply   = self.msg_arg["reply"]
                payload = self.msg_buf
                msg     = Msg(subject=subject, reply=reply, data=payload, sid=sid)
                yield from self.nc._process_msg(msg)

            elif self.state == AWAITING_MSG_END:
                c = memoryview(buf[i:i+1])
                if c == b'\n':
                    self.msg_buf = bytearray()
                    self.msg_buf_size = 0
                    self.msg_arg = {}
                    self.scratch = bytearray()
                    self.scratch_size = 0
                    self.state = AWAITING_CONTROL_LINE

                # Continue until msg end or until we can process next command.
                i += 1

class ErrProtocol(Exception):
    def __str__(self):
        return "nats: Protocol Error"
