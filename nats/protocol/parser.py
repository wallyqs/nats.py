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
        self.scratch = b''
        self.msg_buf = b''

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
                orig_scratch_size = len(self.scratch)
                self.scratch = b''.join([self.scratch, buf[i:i+MAX_CONTROL_LINE_SIZE]])

                # Split buffer so break already.
                if _CRLF_ not in self.scratch:
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
                    i += len(line) + CRLF_SIZE - orig_scratch_size
                    self.state = AWAITING_MSG_PAYLOAD

                    # Reset scratch buffer
                    self.scratch = b''

                # OK
                elif self.scratch.startswith(OK):

                    # Move position to be after +OK
                    i += OK_SIZE
                    self.scratch = b''
                    self.state = AWAITING_CONTROL_LINE

                # -ERR
                elif self.scratch.startswith(ERR_OP):

                    # TODO: Handle ERROR
                    si = scratch.find(_CRLF_)
                    line = scratch[:si]
                    _, err = line.split(_SPC_, 1)
                    print("ERROR!!!!!!", err)
                    yield from self.nc._process_err(err)
                    self.scratch = b''
                    i += len(line) + CRLF_SIZE - orig_scratch_size

                # PONG
                elif self.scratch.startswith(PONG):

                    # Move position to be after PONG
                    i += PONG_SIZE
                    self.scratch = b''
                    self.state = AWAITING_CONTROL_LINE
                    yield from self.nc._process_pong()

                # PING
                elif self.scratch.startswith(PING):

                    # Move position to be after PING
                    i += PING_SIZE
                    self.scratch = b''
                    self.state = AWAITING_CONTROL_LINE
                    yield from self.nc._process_ping()

                else:
                    raise ErrProtocol("nats: Unknown Protocol")

            elif self.state == AWAITING_MSG_PAYLOAD:
                if len(self.msg_buf) < self.needed:
                    # Need to take enough bytes and append to current buf
                    before_copy = len(self.msg_buf)
                    needed_buf = buf[i:i+self.needed]
                    self.msg_buf = b''.join([self.msg_buf, needed_buf])

                    i += self.needed - before_copy
                    if len(self.msg_buf) < self.needed:
                        # Still not enough bytes and split buffer so just break
                        # and handle in next read.
                        break
                else:
                    i += self.needed

                # Set next stage already before dispatching to callback.
                self.scratch = b''
                self.state = AWAITING_MSG_END

                subject = self.msg_arg["subject"].decode()
                sid     = self.msg_arg["sid"]
                reply   = self.msg_arg["reply"]
                payload = self.msg_buf
                msg     = Msg(subject=subject, reply=reply, data=payload, sid=sid)

                # Nothing: peak 135416, high cpu (PUB 86,439 msgs/sec)
                # PUB/SUB: delta out: 60816 -- delta in: 61240
                self.nc.stats["in_msgs"] += 1

                # Result: Buffer queue gets stuck?
                # Worst...
                # self.nc._loop.create_task(self.nc._process_msg(msg))

                # Result: PUB/SUB delta out: 21720 -- delta in: 22734
                # Most balanced.
                # yield from self.nc.msg_queue.put(msg)

                # Result: delta out: 30662 -- delta in: 56833
                # Using sleep of 0.0001 after flushing with buffer queue pending limit.
                # yield from self.nc._process_msg(msg)

                # Result: High cpu and 48968 msgs/sec
                # yield from self.nc._process_msg(msg)

                # Slowest: 47474 msgs/sec
                # self.nc.msg_queue.put_nowait(msg)

            elif self.state == AWAITING_MSG_END:
                c = memoryview(buf[i:i+1])
                if c == b'\n':
                    self.msg_buf = b''
                    self.msg_arg = {}
                    self.scratch = b''
                    self.state = AWAITING_CONTROL_LINE

                # Continue until we get the end of message.
                i += 1

class ErrProtocol(Exception):
    def __str__(self):
        return "nats: Protocol Error"
