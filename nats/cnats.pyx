# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
"""
High-performance Cython bindings for NATS C client.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport strlen

cimport cnats
from cnats cimport natsConnection, natsStatus, NATS_OK

class NATSError(Exception):
    """NATS operation error."""
    pass

cdef class CythonNATS:
    """High-performance NATS client using Cython."""
    
    cdef natsConnection *_nc
    cdef bint _connected
    
    def __cinit__(self):
        self._nc = NULL
        self._connected = False
    
    def __dealloc__(self):
        self.close()
    
    @staticmethod
    def connect(str url):
        """Connect to NATS server."""
        cdef CythonNATS instance = CythonNATS()
        instance._connect(url)
        return instance
    
    cdef void _connect(self, str url):
        """Internal connection method."""
        cdef bytes url_bytes = url.encode('utf-8')
        cdef const char* url_cstr = url_bytes
        cdef natsStatus status
        
        status = cnats.natsConnection_ConnectTo(&self._nc, url_cstr)
        if status != NATS_OK:
            raise NATSError(f"Failed to connect to NATS server: {status}")
        
        self._connected = True
    
    def publish(self, str subject, data):
        """Publish a message to a subject."""
        if not self._connected:
            raise NATSError("Not connected to NATS server")
        
        cdef bytes subject_bytes = subject.encode('utf-8')
        cdef const char* subject_cstr = subject_bytes
        cdef const char* data_ptr
        cdef int data_len
        cdef natsStatus status
        
        # Handle different data types
        if isinstance(data, str):
            data_bytes = data.encode('utf-8')
            data_ptr = data_bytes
            data_len = len(data_bytes)
        elif isinstance(data, bytes):
            data_ptr = data
            data_len = len(data)
        else:
            # Convert to bytes
            data_bytes = bytes(data)
            data_ptr = data_bytes
            data_len = len(data_bytes)
        
        status = cnats.natsConnection_Publish(self._nc, subject_cstr, data_ptr, data_len)
        if status != NATS_OK:
            raise NATSError(f"Failed to publish message: {status}")
    
    def publish_many(self, str subject, data, int count):
        """Publish the same message multiple times efficiently."""
        if not self._connected:
            raise NATSError("Not connected to NATS server")
        
        cdef bytes subject_bytes = subject.encode('utf-8')
        cdef const char* subject_cstr = subject_bytes
        cdef const char* data_ptr
        cdef int data_len
        cdef natsStatus status
        cdef int i
        
        # Prepare data once
        if isinstance(data, str):
            data_bytes = data.encode('utf-8')
            data_ptr = data_bytes
            data_len = len(data_bytes)
        elif isinstance(data, bytes):
            data_ptr = data
            data_len = len(data)
        else:
            data_bytes = bytes(data)
            data_ptr = data_bytes
            data_len = len(data_bytes)
        
        # Publish in tight C loop
        for i in range(count):
            status = cnats.natsConnection_Publish(self._nc, subject_cstr, data_ptr, data_len)
            if status != NATS_OK:
                raise NATSError(f"Failed to publish message {i}: {status}")
    
    def flush(self):
        """Flush pending messages."""
        if not self._connected:
            raise NATSError("Not connected to NATS server")
        
        cdef natsStatus status = cnats.natsConnection_Flush(self._nc)
        if status != NATS_OK:
            raise NATSError(f"Failed to flush: {status}")
    
    def flush_timeout(self, long timeout_ms):
        """Flush pending messages with timeout."""
        if not self._connected:
            raise NATSError("Not connected to NATS server")
        
        cdef natsStatus status = cnats.natsConnection_FlushTimeout(self._nc, timeout_ms)
        if status != NATS_OK:
            raise NATSError(f"Failed to flush with timeout: {status}")
    
    def is_closed(self):
        """Check if connection is closed."""
        if not self._connected:
            return True
        return cnats.natsConnection_IsClosed(self._nc)
    
    def is_reconnecting(self):
        """Check if connection is reconnecting."""
        if not self._connected:
            return False
        return cnats.natsConnection_IsReconnecting(self._nc)
    
    def close(self):
        """Close the connection."""
        if self._connected and self._nc != NULL:
            cnats.natsConnection_Destroy(self._nc)
            self._nc = NULL
            self._connected = False

# Module initialization
def init_nats():
    """Initialize the NATS library."""
    cdef natsStatus status = cnats.nats_Open(-1)
    if status != NATS_OK:
        raise NATSError(f"Failed to initialize NATS library: {status}")

def close_nats():
    """Close the NATS library."""
    cnats.nats_Close()

# Auto-initialize when module is imported
init_nats()

# Ensure cleanup on module exit
import atexit
atexit.register(close_nats)