"""Per-symbol order book state machine for Binance Futures depth stream."""
import logging
from collections import deque
from decimal import Decimal

logger = logging.getLogger(__name__)

BUFFER_MAX = 5000
BBO_MISMATCH_THRESHOLD = 10


def parse_levels(raw: list) -> list[tuple[Decimal, Decimal]]:
    """Convert [["price", "qty"], ...] to [(Decimal, Decimal), ...]."""
    return [(Decimal(p), Decimal(q)) for p, q in raw]


class Book:
    """One symbol's L2 order book, synced via depth diffs + REST snapshots.

    State machine: uninitialized -> buffering -> live
    On sequence gap or integrity failure: falls back to buffering, re-snapshots.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id = 0
        self.state = "uninitialized"
        self.buffer: deque = deque(maxlen=BUFFER_MAX)
        self.last_update_time = 0.0
        self.snapshot_count = 0
        self.gap_count = 0
        self.crossed_count = 0
        self._need_first_event = False
        self._ticker_bid = Decimal(0)
        self._ticker_ask = Decimal(0)
        self._bbo_mismatch_count = 0

    def on_depth(self, U: int, u: int, pu: int,
                 bids: list[tuple[Decimal, Decimal]],
                 asks: list[tuple[Decimal, Decimal]]) -> str | None:
        """Process a depth diff event.

        Returns "publish" if book was updated and should be published,
        "need_snapshot" if a REST snapshot should be fetched, or None.
        """
        if self.state == "uninitialized":
            self.state = "buffering"
            self.buffer.append((U, u, pu, bids, asks))
            return "need_snapshot"

        if self.state == "buffering":
            self.buffer.append((U, u, pu, bids, asks))
            return None

        # --- live ---

        if u <= self.last_update_id:
            return None

        if self._need_first_event:
            # Binance Futures uses global update IDs shared across all symbols.
            # After a snapshot, the first depth event for THIS symbol often has
            # U > snapshot.lastUpdateId because other symbols' events filled the
            # gap. Safe to apply — no depth changes for this symbol were missed.
            self._need_first_event = False
        elif pu != self.last_update_id:
            self.gap_count += 1
            logger.warning("%s: sequence gap (expected pu=%d, got %d) [#%d]",
                           self.symbol, self.last_update_id, pu, self.gap_count)
            self._to_buffering(U, u, pu, bids, asks)
            return "need_snapshot"

        self._apply_diff(bids, asks, u)

        if not self._check_integrity():
            self._to_buffering(U, u, pu, bids, asks)
            return "need_snapshot"

        return "publish"

    def on_snapshot(self, last_update_id: int,
                    bids: list[tuple[Decimal, Decimal]],
                    asks: list[tuple[Decimal, Decimal]]) -> bool:
        """Apply REST snapshot and replay buffered events. Returns True if live."""
        if self.state != "buffering":
            return True

        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}
        self.last_update_id = last_update_id
        self.snapshot_count += 1

        applied_any = False
        for buf_U, buf_u, buf_pu, buf_bids, buf_asks in self.buffer:
            if buf_u < last_update_id:
                continue
            if applied_any and buf_pu != self.last_update_id:
                logger.debug("%s: pu gap in buffer during sync", self.symbol)
                return False
            applied_any = True
            self._apply_diff(buf_bids, buf_asks, buf_u)

        self.buffer.clear()
        self.state = "live"
        self._need_first_event = not applied_any
        self._bbo_mismatch_count = 0

        if not self._check_integrity():
            self.state = "buffering"
            return False

        logger.info("%s: LIVE (%d bids, %d asks) [snapshot #%d]",
                    self.symbol, len(self.bids), len(self.asks), self.snapshot_count)
        return True

    def set_ticker_bbo(self, bid: Decimal, ask: Decimal):
        """Store the latest bookTicker BBO for cross-validation."""
        self._ticker_bid = bid
        self._ticker_ask = ask

    def reset(self):
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.state = "uninitialized"
        self.buffer.clear()
        self._need_first_event = False
        self._ticker_bid = Decimal(0)
        self._ticker_ask = Decimal(0)
        self._bbo_mismatch_count = 0

    def top_levels(self, n: int = 20):
        top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return top_bids, top_asks

    def _apply_diff(self, bids, asks, u):
        for price, qty in bids:
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in asks:
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update_id = u

    def _check_integrity(self) -> bool:
        """Returns False if book is detectably corrupt."""
        if not self.bids or not self.asks:
            return True

        book_bid = max(self.bids)
        book_ask = min(self.asks)

        if book_bid >= book_ask:
            self.crossed_count += 1
            logger.warning("%s: crossed book (bid=%s >= ask=%s) [#%d]",
                           self.symbol, book_bid, book_ask, self.crossed_count)
            return False

        if self._ticker_bid and self._ticker_ask:
            if book_bid != self._ticker_bid or book_ask != self._ticker_ask:
                self._bbo_mismatch_count += 1
                if self._bbo_mismatch_count >= BBO_MISMATCH_THRESHOLD:
                    logger.warning("%s: BBO diverged from ticker for %d updates "
                                   "(book=%s/%s, ticker=%s/%s)",
                                   self.symbol, self._bbo_mismatch_count,
                                   book_bid, book_ask, self._ticker_bid, self._ticker_ask)
                    return False
            else:
                self._bbo_mismatch_count = 0

        return True

    def _to_buffering(self, U, u, pu, bids, asks):
        self.state = "buffering"
        self.buffer.clear()
        self.buffer.append((U, u, pu, bids, asks))
        self._bbo_mismatch_count = 0
