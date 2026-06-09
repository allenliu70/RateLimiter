import asyncio
import websockets
import signal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # 1. Instantiate ChatGateway singleton — once per server process
    gateway = ChatGateway()
    
    # 2. Connection handler — called by websockets library for each new connection
    async def on_new_connection(websocket):
        # Authenticate first — before doing anything else
        try:
            user_id = await authenticate(websocket)
        except AuthenticationError as e:
            logger.warning(f"Authentication failed: {e}")
            await websocket.close(code=4001, reason="Unauthorized")
            return
        
        logger.info(f"[CONNECT] user={user_id} remote={websocket.remote_address}")
        
        # 3. Delegate to gateway — runs as its own async task
        await gateway.handle_connection(websocket, user_id)
        
        logger.info(f"[DISCONNECT] user={user_id}")
    
    # 4. Start WebSocket server
    # websockets library automatically creates a new asyncio task
    # for each incoming connection by calling on_new_connection()
    async with websockets.serve(
        on_new_connection,
        host="0.0.0.0",
        port=8765,
        # OS-level tuning for high connection counts
        reuse_port=True,           # allow multiple processes to bind same port
        compression=None,          # disable per-frame compression — too expensive at scale
        max_size=65536,            # max incoming message size (64KB)
        ping_interval=20,          # WS protocol-level ping every 20s
        ping_timeout=10,           # close connection if no pong within 10s
    ) as server:
        logger.info(f"Chat Gateway started on port 8765")
        
        # 5. Handle graceful shutdown
        loop = asyncio.get_event_loop()
        stop_event = asyncio.Event()
        
        def handle_shutdown(sig):
            logger.info(f"Received {sig.name} — initiating graceful shutdown")
            stop_event.set()
        
        loop.add_signal_handler(signal.SIGTERM, lambda: handle_shutdown(signal.SIGTERM))
        loop.add_signal_handler(signal.SIGINT,  lambda: handle_shutdown(signal.SIGINT))
        
        # Run until shutdown signal
        await stop_event.wait()
        
        # 6. Graceful shutdown sequence
        logger.info("Shutting down — closing new connections")
        server.close()
        await server.wait_closed()
        
        logger.info("Flushing Kafka producer buffer")
        gateway.kafka_producer.flush()  # ensure buffered messages are sent
        
        logger.info("Shutdown complete")

async def authenticate(websocket) -> str:
    # Wait for first message containing auth token
    try:
        auth_message = await asyncio.wait_for(
            websocket.recv(),
            timeout=5.0  # must authenticate within 5 seconds
        )
        payload = json.loads(auth_message)
        token = payload.get("token")
        user_id = token_service.verify(token)  # validate JWT / session token
        
        # Confirm authentication to client
        await websocket.send(json.dumps({
            "type": "auth_success",
            "user_id": user_id
        }))
        return user_id
        
    except asyncio.TimeoutError:
        raise AuthenticationError("Authentication timeout")
    except (KeyError, InvalidTokenError) as e:
        raise AuthenticationError(f"Invalid token: {e}")

if __name__ == "__main__":
    asyncio.run(main())