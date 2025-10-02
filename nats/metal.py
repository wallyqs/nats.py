"""
NATS Metal implementation.
"""

class NATS:
    def __init__(self, url):
        self.url = url
        self.connected = False
    
    @classmethod
    def connect(cls, url):
        instance = cls(url)
        instance._connect()
        return instance
    
    def _connect(self):
        self.connected = True
    
    def publish(self, subject, payload):
        if not self.connected:
            raise RuntimeError("Not connected to NATS server")
        
    def close(self):
        if self.connected:
            self.connected = False