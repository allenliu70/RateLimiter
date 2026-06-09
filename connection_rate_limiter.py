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
        return False  # rate limit exceeded"