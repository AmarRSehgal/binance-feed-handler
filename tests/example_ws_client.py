"""Example WebSocket consumer -- connects and prints live BBO updates.

Usage:
    # Start the feed handler first:
    python python/feed_handler.py --max-symbols 10

    # Then run this:
    python tests/example_ws_client.py
    python tests/example_ws_client.py --stream book --symbols BTCUSDT ETHUSDT
"""
import argparse
import asyncio
import json

import websockets


async def main(url: str, stream: str, symbols: list[str] | None):
    async with websockets.connect(url) as ws:
        msg = {"action": "subscribe", "stream": stream}
        if symbols:
            msg["symbols"] = symbols
        await ws.send(json.dumps(msg))

        confirmation = json.loads(await ws.recv())
        print(f"subscribed: {json.dumps(confirmation)}")

        async for raw in ws:
            data = json.loads(raw)
            if stream == "book":
                print(f"{data['symbol']}  bids={len(data['bids'])}  asks={len(data['asks'])}  uid={data['last_update_id']}")
            else:
                print(f"{data['symbol']}  bid={data['bid_price']}  ask={data['ask_price']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Example WebSocket consumer")
    parser.add_argument("--url", default="ws://localhost:8081/ws")
    parser.add_argument("--stream", default="bbo", choices=["bbo", "book", "all"])
    parser.add_argument("--symbols", nargs="*", default=None)
    args = parser.parse_args()
    asyncio.run(main(args.url, args.stream, args.symbols))
