async def handle_connection(websocket, user_id):
    async for message in websocket:
        # If Kafka buffer full, stop reading
        while kafka_producer_buffer_full():
            await asyncio.sleep(0.1)  # pause reading
            # TCP receive buffer fills up on client side
            # Client's send() eventually blocks
        
        await process_message(user_id, message)