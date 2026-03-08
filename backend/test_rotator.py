import asyncio
import time

class ApiKeyRotator:
    def __init__(self, keys_str: str):
        if not keys_str:
            self.keys = []
        else:
            self.keys = [k.strip() for k in keys_str.split(',') if k.strip()]
        self._index = 0
        self._lock = asyncio.Lock()
        
    async def get_next_key(self) -> str:
        if not self.keys:
            raise ValueError("No API keys available in rotator.")
        async with self._lock:
            key = self.keys[self._index]
            self._index = (self._index + 1) % len(self.keys)
            return key

    def get_next_key_sync(self) -> str:
        if not self.keys:
            raise ValueError("No API keys available in rotator.")
        key = self.keys[self._index]
        self._index = (self._index + 1) % len(self.keys)
        return key
