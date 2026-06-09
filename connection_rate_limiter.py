class ConnectionRateLimiter:
    def __init__(self, rate=100):  # 100 messages/second
        self.tokens = rate
        self.rate = rate
        self.last_refill = time.time()
    
    def allow(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.rate,
            self.tokens + elapsed * self.rate
        )
        self.last_refill = now
        
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False  # rate limit exceeded


# enchanced version for per-user state
# class ConnectionRateLimiter:
#     def __init__(self, rate=100):  # 100 messages/second per user
#         self.rate = rate
#         # Per-user state — keyed by user_id
#         self.user_state: dict[str, dict] = {}
#         self.lock = asyncio.Lock()
    
#     async def allow(self, user_id: str) -> bool:
#         async with self.lock:
#             now = time.time()
            
#             if user_id not in self.user_state:
#                 # First message from this user — initialize state
#                 self.user_state[user_id] = {
#                     "tokens": self.rate,
#                     "last_refill": now
#                 }
            
#             state = self.user_state[user_id]
#             elapsed = now - state["last_refill"]
            
#             # Refill tokens based on elapsed time
#             state["tokens"] = min(
#                 self.rate,
#                 state["tokens"] + elapsed * self.rate
#             )
#             state["last_refill"] = now
            
#             if state["tokens"] >= 1:
#                 state["tokens"] -= 1
#                 return True
#             return False  # rate limit exceeded
    
#     async def cleanup_user(self, user_id: str):
#         # Called on disconnect — free memory for this user
#         async with self.lock:
#             self.user_state.pop(user_id, None)
    
#     def retry_after_ms(self, user_id: str) -> int:
#         state = self.user_state.get(user_id, {})
#         tokens = state.get("tokens", 0)
#         deficit = 1 - tokens
#         return int((deficit / self.rate) * 1000)