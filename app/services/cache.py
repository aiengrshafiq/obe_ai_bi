import time
import threading
from typing import Any, Optional

class InMemoryCache:
    """
    Thread-safe Singleton Cache.
    Replaces Redis for Phase 1.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._store = {}
        
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(InMemoryCache, cls).__new__(cls)
                    cls._instance._store = {}
        return cls._instance

    def set(self, key: str, value: Any, ttl_seconds: int = 300):
        """Set value with Expiry (TTL)"""
        expiry = time.time() + ttl_seconds
        with self._lock:
            self._store[key] = (value, expiry)

    def get(self, key: str) -> Optional[Any]:
        """Get value if not expired"""
        with self._lock:
            data = self._store.get(key)
            
        if not data:
            return None
            
        value, expiry = data
        if time.time() > expiry:
            # Lazy delete
            with self._lock:
                self._store.pop(key, None)
            return None
            
        return value

    def clear(self):
        with self._lock:
            self._store.clear()

# Global Instance
cache = InMemoryCache()