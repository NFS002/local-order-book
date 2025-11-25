import collections
import contextlib
from dataclasses import dataclass
from decimal import Decimal
from statistics import mean
from typing import Deque, TypedDict, List, Dict, Tuple
import asyncio
import json
import websockets
import aiohttp
from aiohttp import ClientTimeout
from sortedcontainers import SortedDict
import traceback

# [price, qty][]
DepthLevels = List[Tuple[str, str]]


@dataclass
class SymbolConfig:
    symbol: str
    depth: int = 20

    def __post_init__(self):
        self.rest_url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit={self.depth}"
        self.wss_url = (
            f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth@1000ms"
        )


SYMBOL_CONFIGS: List[SymbolConfig] = [
    SymbolConfig(symbol="BTCUSDT"),
    # SymbolConfig(symbol="ETHUSDT")
    # SymbolConfig(symbol="BNBUSDT")
]


class DepthUpdate(TypedDict):
    e: str  # event type
    E: int  # event times
    U: int  # first update ID in event
    u: int  # final update ID in event
    pu: int  # previous update ID
    b: DepthLevels  # bids
    a: DepthLevels  # ssks


class SnapshotResponse(TypedDict):
    lastUpdateId: int
    bids: DepthLevels
    asks: DepthLevels


async def get_async(url: str):
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.get(url, timeout=ClientTimeout(total=10)) as resp:
            return await resp.json()


LevelTotals = TypedDict("LevelTotals", {"qty": Decimal, "px_qty": Decimal})


class OrderBook:
    bids: SortedDict[Decimal, Decimal]
    bid_totals: LevelTotals
    asks: SortedDict[Decimal, Decimal]
    ask_totals: LevelTotals
    update_id: int
    symbol_config: SymbolConfig
    depth_events_idx: int
    bid_vwap: Decimal
    ask_vwap: Decimal
    prices_for: int  # Keep historical prices for the last {value} events
    prices: Deque[Decimal]
    prices_acc: Deque[Decimal]
    depth_level: int
    spread: Decimal
    spread_pct: Decimal

    def __init__(self, symbol_config: SymbolConfig) -> None:
        self.symbol_config = symbol_config
        self._init_state()

    def _init_state(self) -> None:
        self.update_id = 0
        self.bids = SortedDict({})
        self.bid_totals = {"qty": Decimal(0), "px_qty": Decimal(0)}
        self.asks = SortedDict({})
        self.ask_totals = {"qty": Decimal(0), "px_qty": Decimal(0)}
        self.depth_events_idx = 0
        self.prices_for = 60
        self.prices = collections.deque(maxlen=self.prices_for)
        self.prices_acc = collections.deque([Decimal(0)], maxlen=self.prices_for + 1)
        self.depth_level = 20
        self.spread = Decimal(0)
        self.spread_pct = Decimal(0)
        self.bid_vwap = Decimal(0)
        self.ask_vwap = Decimal(0)

    async def stream_wss_events(
        self,
    ) -> None:
        ws_connection = await websockets.connect(self.symbol_config.wss_url)
        await self.warm_up(ws_connection)

        async for msg in ws_connection:
            self.depth_events_idx += 1
            event: DepthUpdate = json.loads(msg)
            u = event["u"]
            U = event["U"]

            if u < self.update_id:
                print(f"Dropping old event due to {u=} < {self.update_id=}")
                continue

            print(
                f"{self.symbol_config.symbol}\nidx={self.depth_events_idx}\nupdate_id={self.update_id}\n{u=}\n{U=}\n"
            )
            missed_events = U - (self.update_id + 1)
            if missed_events > 0:
                err_msg = f"Missed events: {self.symbol_config.symbol=}, {missed_events=}, {self.depth_events_idx=}, {self.update_id=}, {U=}, {u=}"
                raise Exception(err_msg)

            self.update_id = u
            self.process_depth_updates(bids=event["b"], asks=event["a"])
            if self.depth_events_idx % 30 == 0:
                print(self)

    def process_depth_updates(self, bids: DepthLevels, asks: DepthLevels):
        self.update_levels(new_ask_levels=asks, new_bid_levels=bids)
        self.calc_vwaps()

    def calc_vwaps(self):
        self.bid_vwap = self.bid_totals["px_qty"] / self.bid_totals["qty"] if self.bid_totals["qty"] else Decimal(0)
        self.ask_vwap = self.ask_totals["px_qty"] / self.ask_totals["qty"] if self.ask_totals["qty"] else Decimal(0)

        self.spread = self.ask_vwap - self.bid_vwap
        self.spread_pct = (
            self.spread / self.ask_vwap * 100 if self.ask_vwap else Decimal(0)
        )
        price = (self.bid_vwap + self.ask_vwap) / 2

        self.prices.append(price)
        self.prices_acc.append(self.prices_acc[-1] + price)

    def calc_average_price_for(self, n: int) -> Decimal:
        return mean(list(self.prices)[-n:])

    def process_snapshot(self, snapshot: SnapshotResponse):
        asks = snapshot["asks"]
        bids = snapshot["bids"]
        self.update_levels(new_ask_levels=asks, new_bid_levels=bids)

    async def warm_up(self, ws_connection):
        self._init_state()
        buffer: Deque[DepthUpdate] = collections.deque()
        has_event = asyncio.Event()

        async def reader():
            async for raw in ws_connection:
                buffer.append(json.loads(raw))
                has_event.set()

        _reader_task = asyncio.create_task(reader())

        snapshot: SnapshotResponse = await get_async(self.symbol_config.rest_url)
        last_id = snapshot["lastUpdateId"]
        print("Snapshot last_id: {}".format(last_id))

        while not buffer or buffer[-1]["u"] < last_id:
            has_event.clear()
            await has_event.wait()

        while buffer and buffer[0]["u"] <= last_id:
            buffer.popleft()
        while buffer and not (buffer[0]["U"] <= last_id + 1 <= buffer[0]["u"]):
            buffer.popleft()

        print(
            f"found overlapping event: {last_id - buffer[-1]['U']}, {buffer[-1]['u'] - last_id}"
        )
        self.process_snapshot(snapshot)
        for ev in buffer:
            print(
                f"Applying update from buffer: {last_id - ev['U']}, {ev['u'] - last_id}"
            )
            self.update_levels(new_ask_levels=ev["a"], new_bid_levels=ev["b"])
            self.update_id = ev["u"]

        buffer.clear()
        with contextlib.suppress(asyncio.CancelledError):
            _reader_task.cancel()
            await _reader_task

    def update_levels(
        self, new_bid_levels: DepthLevels, new_ask_levels: DepthLevels
    ) -> None:
        self._update_bids(new_bid_levels)
        self._update_asks(new_ask_levels)

    def _update_bids(self, new_levels: DepthLevels) -> None:
        for p_str, q_str in new_levels:
            p = Decimal(p_str)
            q = Decimal(q_str)
            old_q = self.bids.get(p, Decimal(0))
            if q <= 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
            dq = q - old_q
            self.bid_totals["qty"] += dq
            self.bid_totals["px_qty"] += p * dq
        self._trim_bids()

    def _update_asks(self, new_levels: DepthLevels) -> None:
        for p_str, q_str in new_levels:
            p = Decimal(p_str)
            q = Decimal(q_str)
            old_q = self.asks.get(p, Decimal(0))
            if q <= 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
            dq = q - old_q
            self.ask_totals["qty"] += dq
            self.ask_totals["px_qty"] += p * dq
        self._trim_asks()

    def _trim_bids(self) -> None:
        while len(self.bids) > self.depth_level:
            p, q = self.bids.popitem(index=0)
            self.bid_totals["qty"] -= q
            self.bid_totals["px_qty"] -= p * q

    def _trim_asks(self) -> None:
        while len(self.asks) > self.depth_level:
            p, q = self.asks.popitem(index=-1)
            self.ask_totals["qty"] -= q
            self.ask_totals["px_qty"] -= p * q

    def window_sum(self, n: int) -> Decimal:
        if len(self.prices_acc) < n + 1:
            return Decimal(0)
        return self.prices_acc[-1] - self.prices_acc[-1 - n]

    @property
    def avg_price_30(self):
        return None if len(self.prices) < 30 else self.window_sum(30) / Decimal(30)

    @property
    def avg_price_60(self):
        return None if len(self.prices) < 60 else self.window_sum(60) / Decimal(60)

    def __repr__(self) -> str:
        symbol = f"\n{self.symbol_config.symbol}:"
        summary = f"Update Id: {self.update_id}, idx={self.depth_events_idx}"
        vwaps = f"VWAPS: bid={self.bid_vwap}, ask={self.ask_vwap}"
        spreads = f"Spread: spread={self.spread}, spread_pct={self.spread_pct}"
        average_prices = (
            f"Average prices: 30={self.avg_price_30}, 60={self.avg_price_60}"
        )
        return "\n".join([symbol, summary, vwaps, spreads, average_prices])


async def run_orderbook(ob: OrderBook):
    backoff = 1.0
    while True:
        try:
            await ob.stream_wss_events()
            backoff = 1.0
        except asyncio.CancelledError:
            raise  # Proprogate cancellation
        except Exception as exc:
            traceback.print_exception(exc)
            print(f"\nRetrying in {backoff:.1f}s\n")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def sync_local_order_books() -> None:
    ob_tasks: Dict[str, Dict[str, asyncio.Task | OrderBook]] = {}

    async with asyncio.TaskGroup() as tg:
        for cfg in SYMBOL_CONFIGS:
            ob = OrderBook(cfg)
            ob_task = tg.create_task(run_orderbook(ob), name=cfg.symbol)
            print("Created task for {}".format(cfg.symbol))
            ob_tasks[cfg.symbol] = {"ob": ob, "ob_task": ob_task}


if __name__ == "__main__":
    asyncio.run(sync_local_order_books())
