# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
import pandas as pd
from alpaca.trading.enums import OrderClass as AlpacaOrderClass
from alpaca.trading.enums import OrderSide as AlpacaOrderSide
from alpaca.trading.enums import OrderStatus as AlpacaOrderStatus
from alpaca.trading.enums import OrderType as AlpacaOrderType
from alpaca.trading.enums import TimeInForce as AlpacaTimeInForce
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.models import Position as AlpacaPosition
from alpaca.trading.models import TradeUpdate as AlpacaTradeUpdate
from alpaca.trading.requests import (
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    ReplaceOrderRequest,
    StopLimitOrderRequest,
    StopOrderRequest,
    TrailingStopOrderRequest,
)
from alpaca.trading.stream import TradingStream

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
    ModifyOrder,
    SubmitOrder,
    SubmitOrderList,
)
from nautilus_trader.execution.reports import (
    FillReport,
    OrderStatusReport,
    PositionStatusReport,
)
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import (
    AccountType,
    ContingencyType,
    LiquiditySide,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    TimeInForce,
    TrailingOffsetType,
    TriggerType,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    TradeId,
    VenueOrderId,
)
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import AccountBalance, Currency, Money, Price, Quantity

from .config import AlpacaLiveExecutionClientConfig
from .core import (
    ALPACA,
    ALPACA_VENUE,
    get_async_http_client,
    get_websocket_client,
    to_request_fields,
)
from .providers import AlpacaInstrumentProvider


# Sentinel for raising KeyError in translator
_raise = object()


def _datetime_to_unix_nanos(dt: datetime | None) -> int:
    """Convert a datetime to UNIX nanoseconds, returning 0 if None."""
    if dt is None:
        return 0
    try:
        ts = pd.Timestamp(dt)
        return int(ts.value)
    except Exception:
        return 0


class AlpacaLiveExecutionClient(LiveExecutionClient):
    """
    Provides a live execution client for Alpaca.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : AlpacaInstrumentProvider
        The instrument provider.
    config : AlpacaLiveExecutionClientConfig
        The configuration for the client.
    account_type : AccountType, default AccountType.CASH
        The account type (CASH or MARGIN).
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaLiveExecutionClientConfig,
        account_type: AccountType = AccountType.CASH,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            oms_type=OmsType.NETTING,
            account_type=account_type,
            base_currency=Currency.from_str("USD"),
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        self.config: AlpacaLiveExecutionClientConfig = config
        self._http_client: httpx.AsyncClient = get_async_http_client(
            self.config.credentials
        )
        self._ws_client: TradingStream = get_websocket_client(self.config.credentials)
        self._ws_client.subscribe_trade_updates(self._on_trade_updates_handler)

        # Set account ID
        account_id = AccountId(f"{name or ALPACA_VENUE.value}-001")
        self._set_account_id(account_id)
        self._log.info(f"account_id={account_id.value}", LogColor.BLUE)

    @property
    def instrument_provider(self) -> AlpacaInstrumentProvider:
        return self._instrument_provider  # type: ignore

    # -- CONNECTION HANDLERS ----------------------------------------------------------------------

    async def _connect(self) -> None:
        await self.instrument_provider.initialize()

        # Start WebSocket connection in background
        self._ws_client.run()

        # Update account state
        await self._update_account_state()

        self._set_connected(True)
        self._log.info("Connected to Alpaca", LogColor.GREEN)

    async def _disconnect(self) -> None:
        await self._http_client.aclose()
        await asyncio.to_thread(self._ws_client.stop)
        self._set_connected(False)
        self._log.info("Disconnected from Alpaca", LogColor.YELLOW)

    def reset(self) -> None:
        self._log.info("Resetting AlpacaLiveExecutionClient")

    def dispose(self) -> None:
        self._log.info("Disposing AlpacaLiveExecutionClient")

    # -- ACCOUNT STATE ----------------------------------------------------------------------------

    async def _update_account_state(self) -> None:
        """Fetch account info and generate account state event."""
        self._log.debug("Fetching account state from Alpaca")

        try:
            response = await self._http_client.get("/account")
            response.raise_for_status()
            account_data = response.json()

            # Parse account balances
            cash = Decimal(str(account_data.get("cash", "0")))
            buying_power = Decimal(str(account_data.get("buying_power", "0")))
            portfolio_value = Decimal(str(account_data.get("portfolio_value", "0")))

            usd = Currency.from_str("USD")

            # Calculate locked funds (portfolio value minus cash)
            locked = max(Decimal("0"), portfolio_value - cash)

            account_balance = AccountBalance(
                total=Money(cash, usd),
                locked=Money(locked, usd),
                free=Money(buying_power, usd),
            )

            self.generate_account_state(
                balances=[account_balance],
                margins=[],
                reported=True,
                ts_event=self._clock.timestamp_ns(),
            )
            self._log.debug(f"Account state updated: cash={cash}, buying_power={buying_power}")

        except httpx.HTTPStatusError as e:
            self._log.error(f"Failed to fetch account state: {e}")

    # -- EXECUTION REPORTS ------------------------------------------------------------------------

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        PyCondition.type_or_none(
            command.client_order_id, ClientOrderId, "client_order_id"
        )
        PyCondition.type_or_none(command.venue_order_id, VenueOrderId, "venue_order_id")

        if not (command.client_order_id or command.venue_order_id):
            self._log.warning(
                "One of `client_order_id` or `venue_order_id` must be specified"
            )
            return None

        try:
            if command.client_order_id:
                response = await self._http_client.get(
                    "/orders:by_client_order_id",
                    params={"client_order_id": str(command.client_order_id)},
                )
            else:
                response = await self._http_client.get(
                    f"/orders/{command.venue_order_id}"
                )

            response.raise_for_status()
            alpaca_order = AlpacaOrder.model_validate(response.json())
            return await self._parse_order_status_report(alpaca_order)

        except httpx.HTTPStatusError as e:
            self._log.warning(f"Failed to fetch order status: {e}")
            return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        params: dict[str, Any] = {
            "status": "open" if command.open_only else "all",
        }
        if command.start:
            params["after"] = command.start.isoformat()
        if command.end:
            params["until"] = command.end.isoformat()
        if command.instrument_id:
            params["symbols"] = command.instrument_id.symbol.value

        try:
            response = await self._http_client.get("/orders", params=params)
            response.raise_for_status()

            reports = []
            for raw_order in response.json():
                alpaca_order = AlpacaOrder.model_validate(raw_order)
                report = await self._parse_order_status_report(alpaca_order)
                if report:
                    reports.append(report)
            return reports

        except httpx.HTTPStatusError as e:
            self._log.error(f"Failed to fetch order status reports: {e}")
            return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        # Alpaca provides fill info through account activities endpoint
        params: dict[str, Any] = {"activity_types": "FILL"}
        if command.start:
            params["after"] = command.start.isoformat()
        if command.end:
            params["until"] = command.end.isoformat()

        try:
            response = await self._http_client.get(
                "/account/activities", params=params
            )
            response.raise_for_status()

            reports = []
            for activity in response.json():
                report = await self._parse_fill_report_from_activity(activity)
                if report:
                    reports.append(report)
            return reports

        except httpx.HTTPStatusError as e:
            self._log.error(f"Failed to fetch fill reports: {e}")
            return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        try:
            response = await self._http_client.get("/positions")
            response.raise_for_status()

            reports = []
            for raw_position in response.json():
                position = AlpacaPosition.model_validate(raw_position)
                report = await self._parse_position_status_report(position)
                if report:
                    # Filter by instrument_id if specified
                    if command.instrument_id and report.instrument_id != command.instrument_id:
                        continue
                    reports.append(report)
            return reports

        except httpx.HTTPStatusError as e:
            self._log.error(f"Failed to fetch position status reports: {e}")
            return []

    # -- COMMAND HANDLERS -------------------------------------------------------------------------

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order

        if order.is_closed:
            self._log.warning(f"Order {order.client_order_id} is already closed")
            return

        # Generate submitted event before sending to venue
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            # Build the appropriate order request
            order_request = self._build_order_request(order)

            response = await self._http_client.post(
                "/orders",
                json=to_request_fields(order_request),
            )
            response.raise_for_status()
            alpaca_order = AlpacaOrder.model_validate(response.json())

            venue_order_id = VenueOrderId(str(alpaca_order.id))

            # Cache the venue order ID mapping
            self._cache.add_venue_order_id(order.client_order_id, venue_order_id)

            # Generate accepted event
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order {order.client_order_id} accepted: {venue_order_id}",
                LogColor.GREEN,
            )

        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                error_detail = e.response.json().get("message", str(e))
            except Exception:
                error_detail = str(e)

            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=error_detail,
                ts_event=self._clock.timestamp_ns(),
            )
            self._log.error(f"Order {order.client_order_id} rejected: {error_detail}")

    def _build_order_request(self, order):
        """Build an Alpaca order request from a Nautilus order."""
        symbol = order.instrument_id.symbol.value
        qty = order.quantity.as_double()
        side = OrderTranslator.to_alpaca(order.side)
        tif = OrderTranslator.to_alpaca(order.time_in_force)
        client_order_id = str(order.client_order_id)

        if order.order_type == OrderType.MARKET:
            return MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=tif,
                client_order_id=client_order_id,
            )
        elif order.order_type == OrderType.LIMIT:
            return LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=tif,
                limit_price=float(order.price),
                client_order_id=client_order_id,
            )
        elif order.order_type == OrderType.STOP_MARKET:
            return StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=tif,
                stop_price=float(order.trigger_price),
                client_order_id=client_order_id,
            )
        elif order.order_type == OrderType.STOP_LIMIT:
            return StopLimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=tif,
                limit_price=float(order.price),
                stop_price=float(order.trigger_price),
                client_order_id=client_order_id,
            )
        elif order.order_type == OrderType.TRAILING_STOP_MARKET:
            return TrailingStopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=tif,
                trail_price=float(order.trailing_offset) if order.trailing_offset else None,
                client_order_id=client_order_id,
            )
        else:
            raise ValueError(f"Unsupported order type: {order.order_type}")

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        # Alpaca supports bracket orders via order_class parameter
        # For now, submit orders individually
        for order in command.order_list.orders:
            submit_command = SubmitOrder(
                trader_id=command.trader_id,
                strategy_id=command.strategy_id,
                order=order,
                position_id=command.position_id,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            await self._submit_order(submit_command)

    async def _modify_order(self, command: ModifyOrder) -> None:
        order = self._cache.order(command.client_order_id)

        if order is None:
            self._log.error(
                f"Cannot modify order: {command.client_order_id} not found in cache"
            )
            return

        if order.venue_order_id is None:
            self._log.error(
                f"Cannot modify order: {command.client_order_id} has no venue_order_id"
            )
            return

        try:
            # Build replace request
            replace_request = ReplaceOrderRequest(
                qty=command.quantity.as_double() if command.quantity else None,
                limit_price=float(command.price) if command.price else None,
                stop_price=float(command.trigger_price) if command.trigger_price else None,
            )

            response = await self._http_client.patch(
                f"/orders/{order.venue_order_id}",
                json=to_request_fields(replace_request),
            )
            response.raise_for_status()
            alpaca_order = AlpacaOrder.model_validate(response.json())

            new_venue_order_id = VenueOrderId(str(alpaca_order.id))

            self.generate_order_updated(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=new_venue_order_id,
                quantity=command.quantity or order.quantity,
                price=command.price,
                trigger_price=command.trigger_price,
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order {order.client_order_id} modified: {new_venue_order_id}",
                LogColor.GREEN,
            )

        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                error_detail = e.response.json().get("message", str(e))
            except Exception:
                error_detail = str(e)

            self.generate_order_modify_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=order.venue_order_id,
                reason=error_detail,
                ts_event=self._clock.timestamp_ns(),
            )
            self._log.error(f"Order {order.client_order_id} modify rejected: {error_detail}")

    async def _cancel_order(self, command: CancelOrder) -> None:
        order = self._cache.order(command.client_order_id)

        if order is None:
            self._log.warning(
                f"Cannot cancel order: {command.client_order_id} not found in cache"
            )
            return

        if order.is_closed:
            self._log.warning(
                f"Order {command.client_order_id} is already {order.status_string()}"
            )
            return

        venue_order_id = command.venue_order_id or order.venue_order_id
        if venue_order_id is None:
            self._log.error(
                f"Cannot cancel order: {command.client_order_id} has no venue_order_id"
            )
            return

        try:
            response = await self._http_client.delete(f"/orders/{venue_order_id}")
            response.raise_for_status()

            # Generate canceled event
            self.generate_order_canceled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order {order.client_order_id} canceled",
                LogColor.YELLOW,
            )

        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                error_detail = e.response.json().get("message", str(e))
            except Exception:
                error_detail = str(e)

            self.generate_order_cancel_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                reason=error_detail,
                ts_event=self._clock.timestamp_ns(),
            )
            self._log.error(f"Order {order.client_order_id} cancel rejected: {error_detail}")

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        try:
            response = await self._http_client.delete("/orders")
            response.raise_for_status()

            # The response contains a list of orders that were cancelled
            cancelled_orders = response.json()
            for cancelled in cancelled_orders:
                venue_order_id = VenueOrderId(str(cancelled.get("id")))
                client_order_id = self._cache.client_order_id(venue_order_id)

                if client_order_id:
                    order = self._cache.order(client_order_id)
                    if order:
                        self.generate_order_canceled(
                            strategy_id=order.strategy_id,
                            instrument_id=order.instrument_id,
                            client_order_id=client_order_id,
                            venue_order_id=venue_order_id,
                            ts_event=self._clock.timestamp_ns(),
                        )

            self._log.info(
                f"Cancelled {len(cancelled_orders)} orders",
                LogColor.YELLOW,
            )

        except httpx.HTTPStatusError as e:
            self._log.error(f"Failed to cancel all orders: {e}")

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel_command in command.cancels:
            await self._cancel_order(cancel_command)

    # -- WEBSOCKET HANDLERS -----------------------------------------------------------------------

    async def _on_trade_updates_handler(self, msg: AlpacaTradeUpdate) -> None:
        """Handle trade update messages from Alpaca WebSocket."""
        try:
            self._log.debug(f"Received trade update: {msg.event} - {msg.order}")

            event = msg.event
            alpaca_order = msg.order

            if alpaca_order is None:
                self._log.warning(f"Trade update has no order: {msg}")
                return

            venue_order_id = VenueOrderId(str(alpaca_order.id))
            client_order_id = self._cache.client_order_id(venue_order_id)

            # Try to find by client_order_id if not in cache
            if client_order_id is None and alpaca_order.client_order_id:
                client_order_id = ClientOrderId(alpaca_order.client_order_id)

            order = self._cache.order(client_order_id) if client_order_id else None

            if order is None:
                # External order - generate order status report
                self._log.info(
                    f"Received update for external order: {venue_order_id}",
                    LogColor.BLUE,
                )
                report = await self._parse_order_status_report(alpaca_order)
                if report:
                    self._send_order_status_report(report)
                return

            instrument = self._cache.instrument(order.instrument_id)

            # Handle different event types
            if event == "new":
                # Order accepted by exchange
                self.generate_order_accepted(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=_datetime_to_unix_nanos(alpaca_order.submitted_at)
                    or self._clock.timestamp_ns(),
                )

            elif event == "fill":
                # Complete fill
                await self._handle_fill(order, alpaca_order, msg)

            elif event == "partial_fill":
                # Partial fill
                await self._handle_fill(order, alpaca_order, msg)

            elif event == "canceled":
                self.generate_order_canceled(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=_datetime_to_unix_nanos(alpaca_order.canceled_at)
                    or self._clock.timestamp_ns(),
                )

            elif event == "expired":
                self.generate_order_expired(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=_datetime_to_unix_nanos(alpaca_order.expired_at)
                    or self._clock.timestamp_ns(),
                )

            elif event == "rejected":
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(alpaca_order.status),
                    ts_event=self._clock.timestamp_ns(),
                )

            elif event == "replaced":
                # Order was modified/replaced
                new_venue_order_id = VenueOrderId(str(alpaca_order.id))
                self._cache.add_venue_order_id(order.client_order_id, new_venue_order_id)

                self.generate_order_updated(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=new_venue_order_id,
                    quantity=Quantity.from_str(str(alpaca_order.qty)),
                    price=Price.from_str(str(alpaca_order.limit_price))
                    if alpaca_order.limit_price
                    else None,
                    trigger_price=Price.from_str(str(alpaca_order.stop_price))
                    if alpaca_order.stop_price
                    else None,
                    ts_event=_datetime_to_unix_nanos(alpaca_order.replaced_at)
                    or self._clock.timestamp_ns(),
                )

            elif event in ("pending_new", "accepted", "pending_cancel", "pending_replace"):
                # Intermediate states - just log
                self._log.debug(f"Order {order.client_order_id} state: {event}")

            elif event == "done_for_day":
                # Order is done for the day but may resume tomorrow
                self._log.info(f"Order {order.client_order_id} done for day")

            else:
                self._log.warning(f"Unhandled trade update event: {event}")

        except Exception as e:
            self._log.exception(f"Error handling trade update: {e}", e)

    async def _handle_fill(
        self,
        order,
        alpaca_order: AlpacaOrder,
        msg: AlpacaTradeUpdate,
    ) -> None:
        """Handle fill or partial fill event."""
        instrument = self._cache.instrument(order.instrument_id)
        if instrument is None:
            self._log.error(f"Instrument not found for order: {order.instrument_id}")
            return

        # Get fill details from the trade update
        filled_qty = Decimal(str(alpaca_order.filled_qty or 0))
        filled_avg_price = Decimal(str(alpaca_order.filled_avg_price or 0))

        # Calculate the incremental fill (last fill)
        # For partial fills, we need to track what was already filled
        previous_filled = order.filled_qty.as_decimal() if order.filled_qty else Decimal(0)
        last_qty = filled_qty - previous_filled

        if last_qty <= 0:
            self._log.debug("No new fill quantity, skipping")
            return

        # Generate unique trade ID
        trade_id = TradeId(str(UUID4()))

        # Determine liquidity side based on order type
        if order.order_type in (OrderType.MARKET,):
            liquidity_side = LiquiditySide.TAKER
        else:
            liquidity_side = LiquiditySide.MAKER

        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId(str(alpaca_order.id)),
            venue_position_id=None,
            trade_id=trade_id,
            order_side=order.side,
            order_type=order.order_type,
            last_qty=Quantity(float(last_qty), instrument.size_precision),
            last_px=Price(float(filled_avg_price), instrument.price_precision),
            quote_currency=instrument.quote_currency,
            commission=Money(0, Currency.from_str("USD")),  # Alpaca is commission-free
            liquidity_side=liquidity_side,
            ts_event=_datetime_to_unix_nanos(alpaca_order.filled_at)
            or self._clock.timestamp_ns(),
        )

        # Update account state after fill
        self.create_task(self._update_account_state())

    # -- PARSING HELPERS --------------------------------------------------------------------------

    async def _parse_order_status_report(
        self,
        alpaca_order: AlpacaOrder,
    ) -> OrderStatusReport | None:
        if not alpaca_order.symbol:
            self._log.warning("Order has no symbol")
            return None

        instrument = await self._get_instrument(alpaca_order.symbol)
        if instrument is None:
            self._log.warning(f"Instrument not found for symbol: {alpaca_order.symbol}")
            return None

        total_qty = Quantity.from_str(str(alpaca_order.qty))
        filled_qty = Quantity.from_str(str(alpaca_order.filled_qty or 0))

        # Note: Alpaca model has both 'type' and 'order_type' fields.
        # The 'type' field is populated from JSON, while 'order_type' may be None.
        alpaca_order_type = alpaca_order.type or alpaca_order.order_type

        report = OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            venue_order_id=VenueOrderId(str(alpaca_order.id)),
            client_order_id=ClientOrderId(alpaca_order.client_order_id)
            if alpaca_order.client_order_id
            else None,
            order_side=OrderTranslator.to_nautilus(alpaca_order.side),
            order_type=OrderTranslator.to_nautilus(alpaca_order_type),
            time_in_force=OrderTranslator.to_nautilus(alpaca_order.time_in_force),
            order_status=OrderTranslator.to_nautilus(alpaca_order.status),
            quantity=total_qty,
            filled_qty=filled_qty,
            report_id=UUID4(),
            ts_init=self._clock.timestamp_ns(),
            ts_accepted=_datetime_to_unix_nanos(alpaca_order.submitted_at)
            or self._clock.timestamp_ns(),
            ts_last=_datetime_to_unix_nanos(alpaca_order.updated_at)
            or self._clock.timestamp_ns(),
            order_list_id=None,
            expire_time=alpaca_order.expires_at,
            price=Price.from_str(str(alpaca_order.limit_price))
            if alpaca_order.limit_price
            else None,
            trigger_price=Price.from_str(str(alpaca_order.stop_price))
            if alpaca_order.stop_price
            else None,
            trigger_type=TriggerType.DEFAULT,
            trailing_offset=Decimal(str(alpaca_order.trail_price))
            if alpaca_order.trail_price
            else None,
            trailing_offset_type=TrailingOffsetType.PRICE
            if alpaca_order.trail_price
            else TrailingOffsetType.NO_TRAILING_OFFSET,
            avg_px=Decimal(str(alpaca_order.filled_avg_price))
            if alpaca_order.filled_avg_price
            else None,
            display_qty=total_qty,
        )
        self._log.debug(f"Parsed order status report: {report}")
        return report

    async def _parse_fill_report_from_activity(
        self,
        activity: dict,
    ) -> FillReport | None:
        """Parse a fill report from an account activity."""
        symbol = activity.get("symbol")
        if not symbol:
            return None

        instrument = await self._get_instrument(symbol)
        if instrument is None:
            return None

        order_id = activity.get("order_id")
        venue_order_id = VenueOrderId(order_id) if order_id else None
        client_order_id = self._cache.client_order_id(venue_order_id) if venue_order_id else None

        side_str = activity.get("side", "buy")
        order_side = OrderSide.BUY if side_str.lower() == "buy" else OrderSide.SELL

        qty = Decimal(str(activity.get("qty", 0)))
        price = Decimal(str(activity.get("price", 0)))

        report = FillReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            venue_order_id=venue_order_id,
            trade_id=TradeId(activity.get("id", str(UUID4()))),
            client_order_id=client_order_id,
            report_id=UUID4(),
            order_side=order_side,
            last_qty=Quantity(float(qty), instrument.size_precision),
            last_px=Price(float(price), instrument.price_precision),
            commission=Money(0, Currency.from_str("USD")),
            liquidity_side=LiquiditySide.TAKER,
            ts_event=_datetime_to_unix_nanos(activity.get("transaction_time"))
            or self._clock.timestamp_ns(),
            ts_init=self._clock.timestamp_ns(),
        )
        return report

    async def _parse_position_status_report(
        self,
        position: AlpacaPosition,
    ) -> PositionStatusReport | None:
        instrument = await self._get_instrument(position.symbol)
        if instrument is None:
            return None

        qty = Decimal(str(position.qty))
        side = PositionSide.LONG if qty > 0 else PositionSide.SHORT if qty < 0 else PositionSide.FLAT

        report = PositionStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            position_side=side,
            quantity=Quantity(abs(float(qty)), instrument.size_precision),
            report_id=UUID4(),
            ts_last=self._clock.timestamp_ns(),
            ts_init=self._clock.timestamp_ns(),
        )
        return report

    async def _get_instrument(self, symbol: str) -> Instrument | None:
        instrument_id = InstrumentId.from_str(f"{symbol}.{ALPACA}")
        instrument = self.instrument_provider.find(instrument_id)
        if instrument:
            return instrument

        await self.instrument_provider.load_async(instrument_id)
        return self.instrument_provider.find(instrument_id)


class OrderTranslator:
    """
    Translate between Alpaca and Nautilus order enums.

    Note: Nautilus enums use integer values and can compare equal across different
    enum types (e.g., OrderSide.BUY == ContingencyType.OCO == 1). To avoid hash
    collisions in dict lookups, we use (type, value) tuples as keys.
    """

    # Alpaca to Nautilus mapping using (type, value) as key
    a2n: dict[tuple[type, Any], Any] = {
        # OrderSide
        (type(AlpacaOrderSide.BUY), AlpacaOrderSide.BUY): OrderSide.BUY,
        (type(AlpacaOrderSide.SELL), AlpacaOrderSide.SELL): OrderSide.SELL,
        # OrderType
        (type(AlpacaOrderType.MARKET), AlpacaOrderType.MARKET): OrderType.MARKET,
        (type(AlpacaOrderType.LIMIT), AlpacaOrderType.LIMIT): OrderType.LIMIT,
        (type(AlpacaOrderType.STOP), AlpacaOrderType.STOP): OrderType.STOP_MARKET,
        (type(AlpacaOrderType.STOP_LIMIT), AlpacaOrderType.STOP_LIMIT): OrderType.STOP_LIMIT,
        (type(AlpacaOrderType.TRAILING_STOP), AlpacaOrderType.TRAILING_STOP): OrderType.TRAILING_STOP_MARKET,
        # TimeInForce
        (type(AlpacaTimeInForce.DAY), AlpacaTimeInForce.DAY): TimeInForce.DAY,
        (type(AlpacaTimeInForce.GTC), AlpacaTimeInForce.GTC): TimeInForce.GTC,
        (type(AlpacaTimeInForce.OPG), AlpacaTimeInForce.OPG): TimeInForce.AT_THE_OPEN,
        (type(AlpacaTimeInForce.CLS), AlpacaTimeInForce.CLS): TimeInForce.AT_THE_CLOSE,
        (type(AlpacaTimeInForce.IOC), AlpacaTimeInForce.IOC): TimeInForce.IOC,
        (type(AlpacaTimeInForce.FOK), AlpacaTimeInForce.FOK): TimeInForce.FOK,
        # OrderClass (to ContingencyType)
        (type(AlpacaOrderClass.OCO), AlpacaOrderClass.OCO): ContingencyType.OCO,
        (type(AlpacaOrderClass.OTO), AlpacaOrderClass.OTO): ContingencyType.OTO,
        # OrderStatus
        (type(AlpacaOrderStatus.NEW), AlpacaOrderStatus.NEW): OrderStatus.ACCEPTED,
        (type(AlpacaOrderStatus.PARTIALLY_FILLED), AlpacaOrderStatus.PARTIALLY_FILLED): OrderStatus.PARTIALLY_FILLED,
        (type(AlpacaOrderStatus.FILLED), AlpacaOrderStatus.FILLED): OrderStatus.FILLED,
        (type(AlpacaOrderStatus.DONE_FOR_DAY), AlpacaOrderStatus.DONE_FOR_DAY): OrderStatus.ACCEPTED,
        (type(AlpacaOrderStatus.CANCELED), AlpacaOrderStatus.CANCELED): OrderStatus.CANCELED,
        (type(AlpacaOrderStatus.EXPIRED), AlpacaOrderStatus.EXPIRED): OrderStatus.EXPIRED,
        (type(AlpacaOrderStatus.REPLACED), AlpacaOrderStatus.REPLACED): OrderStatus.ACCEPTED,
        (type(AlpacaOrderStatus.PENDING_CANCEL), AlpacaOrderStatus.PENDING_CANCEL): OrderStatus.PENDING_CANCEL,
        (type(AlpacaOrderStatus.PENDING_REPLACE), AlpacaOrderStatus.PENDING_REPLACE): OrderStatus.PENDING_UPDATE,
        (type(AlpacaOrderStatus.PENDING_REVIEW), AlpacaOrderStatus.PENDING_REVIEW): OrderStatus.PENDING_UPDATE,
        (type(AlpacaOrderStatus.ACCEPTED), AlpacaOrderStatus.ACCEPTED): OrderStatus.ACCEPTED,
        (type(AlpacaOrderStatus.PENDING_NEW), AlpacaOrderStatus.PENDING_NEW): OrderStatus.SUBMITTED,
        (type(AlpacaOrderStatus.ACCEPTED_FOR_BIDDING), AlpacaOrderStatus.ACCEPTED_FOR_BIDDING): OrderStatus.SUBMITTED,
        (type(AlpacaOrderStatus.STOPPED), AlpacaOrderStatus.STOPPED): OrderStatus.PENDING_UPDATE,
        (type(AlpacaOrderStatus.REJECTED), AlpacaOrderStatus.REJECTED): OrderStatus.REJECTED,
        (type(AlpacaOrderStatus.SUSPENDED), AlpacaOrderStatus.SUSPENDED): OrderStatus.PENDING_UPDATE,
        (type(AlpacaOrderStatus.CALCULATED), AlpacaOrderStatus.CALCULATED): OrderStatus.PENDING_UPDATE,
    }

    # Nautilus to Alpaca mapping using (type, value) as key
    n2a: dict[tuple[type, Any], Any] = {
        # OrderSide
        (OrderSide, OrderSide.BUY): AlpacaOrderSide.BUY,
        (OrderSide, OrderSide.SELL): AlpacaOrderSide.SELL,
        # OrderType
        (OrderType, OrderType.MARKET): AlpacaOrderType.MARKET,
        (OrderType, OrderType.LIMIT): AlpacaOrderType.LIMIT,
        (OrderType, OrderType.STOP_MARKET): AlpacaOrderType.STOP,
        (OrderType, OrderType.STOP_LIMIT): AlpacaOrderType.STOP_LIMIT,
        (OrderType, OrderType.TRAILING_STOP_MARKET): AlpacaOrderType.TRAILING_STOP,
        # TimeInForce
        (TimeInForce, TimeInForce.DAY): AlpacaTimeInForce.DAY,
        (TimeInForce, TimeInForce.GTC): AlpacaTimeInForce.GTC,
        (TimeInForce, TimeInForce.AT_THE_OPEN): AlpacaTimeInForce.OPG,
        (TimeInForce, TimeInForce.AT_THE_CLOSE): AlpacaTimeInForce.CLS,
        (TimeInForce, TimeInForce.IOC): AlpacaTimeInForce.IOC,
        (TimeInForce, TimeInForce.FOK): AlpacaTimeInForce.FOK,
        # ContingencyType (to OrderClass)
        (ContingencyType, ContingencyType.OCO): AlpacaOrderClass.OCO,
        (ContingencyType, ContingencyType.OTO): AlpacaOrderClass.OTO,
    }

    @classmethod
    def to_alpaca(cls, thing: Any, default: Any = _raise) -> Any:
        """Translate a Nautilus enum to Alpaca."""
        key = (type(thing), thing)
        v = cls.n2a.get(key, default)
        if v is _raise:
            raise KeyError(f"Unable to translate {thing} to Alpaca")
        return v

    @classmethod
    def to_nautilus(cls, thing: Any, default: Any = _raise) -> Any:
        """Translate an Alpaca enum to Nautilus."""
        key = (type(thing), thing)
        v = cls.a2n.get(key, default)
        if v is _raise:
            raise KeyError(f"Unable to translate {thing} to Nautilus")
        return v
