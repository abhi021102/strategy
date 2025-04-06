import logging
from decimal import Decimal
from typing import Dict, List

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory, CandlesConfig
from hummingbot.connector.connector_base import ConnectorBase


class VolatilitySpreadMaker(ScriptStrategyBase):
    """
    Volatility-Adjusted Pure Market Making Strategy.
    Dynamically adjusts spreads based on ATR volatility indicator.
    """

    trading_pair = "ETH-USDT"
    exchange = "binance_paper_trade"
    price_source = PriceType.MidPrice
    order_amount = Decimal("0.01")
    order_refresh_time = 15
    create_timestamp = 0

    # Volatility spread config
    atr_window = 30
    atr_multiplier = 1.2
    min_spread = 0.0003  # 0.03%
    max_spread = 0.002   # 0.2%

    # Candles config
    candle_exchange = "binance"
    candles_interval = "1m"
    max_records = 1000
    candles = CandlesFactory.get_candle(CandlesConfig(
        connector=candle_exchange,
        trading_pair=trading_pair,
        interval=candles_interval,
        max_records=max_records
    ))

    markets = {exchange: {trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.candles.start()

    def on_stop(self):
        self.candles.stop()

    def on_tick(self):
        if self.create_timestamp <= self.current_timestamp:
            if not self.candles.ready:
                return

            self.cancel_all_orders()
            proposal = self.create_proposal()
            adjusted = self.adjust_proposal_to_budget(proposal)
            self.place_orders(adjusted)
            self.create_timestamp = self.current_timestamp + self.order_refresh_time

    def get_candles_with_atr(self):
        candles_df = self.candles.candles_df.copy()
        candles_df.ta.atr(length=self.atr_window, append=True)
        return candles_df

    def create_proposal(self) -> List[OrderCandidate]:
        ref_price = self.connectors[self.exchange].get_price_by_type(self.trading_pair, self.price_source)

        candles_df = self.get_candles_with_atr()
        atr = candles_df[f"ATR_{self.atr_window}"].iloc[-1]

        if atr is None or atr <= 0:
            atr = ref_price * self.min_spread  # fallback to static minimum

        # Calculate dynamic spread from ATR
        dynamic_spread = min(max(Decimal(atr * self.atr_multiplier) / ref_price, self.min_spread), self.max_spread)

        bid_price = ref_price * Decimal(1 - dynamic_spread)
        ask_price = ref_price * Decimal(1 + dynamic_spread)

        buy_order = OrderCandidate(
            trading_pair=self.trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=self.order_amount,
            price=bid_price
        )
        sell_order = OrderCandidate(
            trading_pair=self.trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.SELL,
            amount=self.order_amount,
            price=ask_price
        )
        return [buy_order, sell_order]

    def adjust_proposal_to_budget(self, proposal: List[OrderCandidate]) -> List[OrderCandidate]:
        return self.connectors[self.exchange].budget_checker.adjust_candidates(proposal, all_or_none=True)

    def place_orders(self, proposal: List[OrderCandidate]):
        for order in proposal:
            self.place_order(self.exchange, order)

    def place_order(self, connector_name: str, order: OrderCandidate):
        if order.order_side == TradeType.SELL:
            self.sell(connector_name, order.trading_pair, order.amount, order.order_type, order.price)
        elif order.order_side == TradeType.BUY:
            self.buy(connector_name, order.trading_pair, order.amount, order.order_type, order.price)

    def cancel_all_orders(self):
        for order in self.get_active_orders(self.exchange):
            self.cancel(self.exchange, order.trading_pair, order.client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        msg = f"{event.trade_type.name} {round(event.amount, 4)} {event.trading_pair} at {round(event.price, 4)}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = ["", "  Balances:"]
        balance_df = self.get_balance_df()
        lines += ["    " + line for line in balance_df.to_string(index=False).split("\n")]

        try:
            df = self.active_orders_df()
            lines += ["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")]
        except ValueError:
            lines += ["", "  No active maker orders."]

        candles_df = self.get_candles_with_atr()
        lines += ["", "-" * 70]
        lines += [f"  Candles: {self.candles.name} | Interval: {self.candles.interval}"]
        lines += ["    " + line for line in candles_df.tail(10).iloc[::-1].to_string(index=False).split("\n")]

        return "\n".join(lines)
