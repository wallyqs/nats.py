# cython: language_level=3
"""
Cython declarations for NATS C client library.
"""

cdef extern from "nats.h":
    # Types
    ctypedef int natsStatus
    ctypedef void* natsConnection
    ctypedef void* natsOptions
    
    # Status codes
    cdef enum:
        NATS_OK = 0
        NATS_ERR = 1
        NATS_PROTOCOL_ERROR = 2
        NATS_IO_ERROR = 3
        NATS_CONNECTION_CLOSED = 4
        NATS_NO_SERVER = 5
        NATS_STALE_CONNECTION = 6
        NATS_SECURE_CONNECTION_WANTED = 7
        NATS_SECURE_CONNECTION_REQUIRED = 8
        NATS_CONNECTION_DISCONNECTED = 9
        NATS_CONNECTION_AUTH_FAILED = 10
        NATS_NOT_PERMITTED = 11
        NATS_NOT_FOUND = 12
        NATS_ADDRESS_MISSING = 13
        NATS_INVALID_SUBJECT = 14
        NATS_INVALID_ARG = 15
        NATS_INVALID_SUBSCRIPTION = 16
        NATS_INVALID_TIMEOUT = 17
        NATS_ILLEGAL_STATE = 18
        NATS_SLOW_CONSUMER = 19
        NATS_MAX_PAYLOAD = 20
        NATS_MAX_DELIVERED = 21
        NATS_BAD_SUBSCRIPTION = 22
        NATS_TOO_MANY_CONNECTIONS = 23
        NATS_DRAINING = 24
        NATS_INVALID_QUEUE_NAME = 25
        NATS_NO_RESPONDERS = 26
        NATS_NOT_YET_CONNECTED = 27
    
    # Core functions
    natsStatus nats_Open(long opts)
    void nats_Close()
    const char* natsStatus_GetText(natsStatus s)
    
    # Connection functions
    natsStatus natsConnection_ConnectTo(natsConnection **nc, const char *urls)
    natsStatus natsConnection_Connect(natsConnection **nc, natsOptions *options)
    void natsConnection_Destroy(natsConnection *nc)
    natsStatus natsConnection_Flush(natsConnection *nc)
    natsStatus natsConnection_FlushTimeout(natsConnection *nc, long timeout)
    bint natsConnection_IsClosed(natsConnection *nc)
    bint natsConnection_IsReconnecting(natsConnection *nc)
    
    # Publishing functions
    natsStatus natsConnection_Publish(natsConnection *nc, const char *subj, 
                                     const void *data, int dataLen)
    natsStatus natsConnection_PublishString(natsConnection *nc, const char *subj, 
                                           const char *str)
    natsStatus natsConnection_PublishRequest(natsConnection *nc, const char *subj,
                                            const char *reply, const void *data, int dataLen)