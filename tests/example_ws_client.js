// Example WebSocket consumer -- connects and prints live BBO updates.
//
// Usage:
//   Start the feed handler first, then:
//   node tests/example_ws_client.js
//   node tests/example_ws_client.js book BTCUSDT ETHUSDT

const stream = process.argv[2] || "bbo";
const symbols = process.argv.slice(3);

const ws = new WebSocket("ws://localhost:8081/ws");

ws.onopen = () => {
    const msg = { action: "subscribe", stream };
    if (symbols.length > 0) msg.symbols = symbols;
    ws.send(JSON.stringify(msg));
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.status) {
        console.log("subscribed:", JSON.stringify(data));
    } else if (data.bids) {
        console.log(`${data.symbol}  bids=${data.bids.length}  asks=${data.asks.length}  uid=${data.last_update_id}`);
    } else {
        console.log(`${data.symbol}  bid=${data.bid_price}  ask=${data.ask_price}`);
    }
};

ws.onclose = () => console.log("disconnected");
