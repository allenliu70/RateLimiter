class ChatGateway:
    def __init__(self):
        self.backpressure_active = False
        self.kafka_producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            buffer_memory=33554432,    # 32MB producer buffer
            max_block_ms=1000,         # block up to 1s before raising exception
            acks="all"
        )

    async def handle_connection(self, websocket, user_id):
        await self.on_connect(user_id)
        
        try:
            async for raw_message in websocket:
                # Check backpressure state BEFORE processing
                if self.backpressure_active:
                    await self.handle_backpressure(websocket)
                    # Note: we do NOT continue reading — we pause here
                    # TCP receive buffer fills, zero window propagates to client
                
                await self.process_message(websocket, user_id, raw_message)
                
        except websockets.exceptions.ConnectionClosed:
            await self.on_disconnect(user_id)

    async def process_message(self, websocket, user_id, raw_message):

        # Proactive check BEFORE touching Kafka
        if not self.rate_limiter.allow(user_id):
            # Don't activate full backpressure — just reject this message
            await websocket.send(json.dumps({
                "type": "rate_limited",
                "temp_id": deserialize(raw_message).temp_id,
                "retry_after_ms": self.rate_limiter.retry_after_ms(user_id)
            }))
            return  # discard message, don't publish to Kafka
        
        # Proceed with Kafka publish...
        message = deserialize(raw_message)
        
        try:
            # Publish to Kafka
            future = self.kafka_producer.send(
                "messages",
                key=message.chat_id.encode(),
                value=serialize(message)
            )
            # Wait for broker ack (with timeout)
            metadata = future.get(timeout=1.0)
            
            # Ack sender
            await websocket.send(json.dumps({
                "type": "message_ack",
                "temp_id": message.temp_id,
                "message_id": metadata.offset,  # Kafka offset as sequence number
                "status": "sent"
            }))
            
            # Kafka healthy — clear backpressure if active
            if self.backpressure_active:
                await self.clear_backpressure()

        except BufferExhaustedException:
            # Kafka producer buffer full
            await self.activate_backpressure(websocket, reason="producer_buffer_full")

        except KafkaTimeoutError:
            # Broker not acknowledging — overwhelmed
            await self.activate_backpressure(websocket, reason="broker_timeout")


    async def activate_backpressure(self, websocket, reason):
        if self.backpressure_active:
            return  # already active, don't re-trigger

        self.backpressure_active = True
        
        # 1. Notify client explicitly to back off
        await websocket.send(json.dumps({
            "type": "backpressure",
            "retry_after_ms": 500,
            "reason": reason
        }))
        
        # 2. Log + emit metric for monitoring
        metrics.increment("gateway.backpressure.activated", tags={"reason": reason})
        logger.warning(f"Backpressure activated: {reason}")
        
        # 3. Stop reading from WebSocket — this is the key action
        # The async for loop in handle_connection checks
        # self.backpressure_active before each iteration
        # So no explicit "stop" call needed — the flag does it



    async def handle_backpressure(self, websocket):
        # Poll until Kafka recovers
        while self.backpressure_active:
            await asyncio.sleep(0.1)  # yield to event loop — don't busy-wait
            
            # Probe Kafka health
            kafka_healthy = await self.probe_kafka_health()
            
            if kafka_healthy:
                await self.clear_backpressure(websocket)
                return
            
            # Optionally send periodic keep-alive to prevent client timeout
            # without accepting new messages
            await websocket.send(json.dumps({
                "type": "backpressure",
                "retry_after_ms": 500
            }))

    async def probe_kafka_health(self):
        # Check if producer buffer has drained below threshold
        # Kafka Java client exposes metrics for buffer utilization
        buffer_available = self.kafka_producer.metrics().get(
            "buffer-available-bytes"
        )
        return buffer_available > BUFFER_LOW_WATERMARK  # e.g. 8MB free

    async def clear_backpressure(self, websocket):
        self.backpressure_active = False
        
        # Notify client it can resume sending
        await websocket.send(json.dumps({
            "type": "backpressure_cleared",
            "message": "resume"
        }))
        
        metrics.increment("gateway.backpressure.cleared")
        logger.info("Backpressure cleared — resuming normal operation")