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
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest
from alpaca.trading.enums import OrderSide as AlpacaOrderSide
from alpaca.trading.enums import OrderStatus as AlpacaOrderStatus
from alpaca.trading.enums import OrderType as AlpacaOrderType
from alpaca.trading.enums import TimeInForce as AlpacaTimeInForce
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.models import Position as AlpacaPosition
from alpaca.trading.models import TradeUpdate as AlpacaTradeUpdate

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    CancelOrder,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
    ModifyOrder,
    SubmitOrder,
)
from nautilus_trader.model.enums import (
    AccountType,
    ContingencyType,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Currency, Price, Quantity
from nautilus_trader.model.orders import MarketOrder, LimitOrder
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from alpaca_adapter.config import (
    AlpacaCredentialsConfig,
    AlpacaInstrumentProviderConfig,
    AlpacaLiveExecutionClientConfig,
)
from alpaca_adapter.core import ALPACA, ALPACA_VENUE
from alpaca_adapter.execution import AlpacaLiveExecutionClient, OrderTranslator
from alpaca_adapter.providers import AlpacaInstrumentProvider


# -- Test Fixtures --------------------------------------------------------------------------------


@pytest.fixture
def credentials() -> AlpacaCredentialsConfig:
    return AlpacaCredentialsConfig(
        live_api_key="test_live_key",
        live_api_secret="test_live_secret",
        paper_api_key="test_paper_key",
        paper_api_secret="test_paper_secret",
        mode="paper",
    )


@pytest.fixture
def instrument_provider_config(credentials) -> AlpacaInstrumentProviderConfig:
    return AlpacaInstrumentProviderConfig(credentials=credentials)


@pytest.fixture
def exec_client_config(credentials) -> AlpacaLiveExecutionClientConfig:
    return AlpacaLiveExecutionClientConfig(credentials=credentials)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def clock() -> LiveClock:
    return LiveClock()


@pytest.fixture
def trader_id() -> TraderId:
    return TestIdStubs.trader_id()


@pytest.fixture
def msgbus(trader_id, clock) -> MessageBus:
    return MessageBus(trader_id=trader_id, clock=clock)


@pytest.fixture
def cache() -> Cache:
    return TestComponentStubs.cache()


@pytest.fixture
def instrument() -> Equity:
    instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
    return Equity(
        instrument_id=instrument_id,
        raw_symbol=instrument_id.symbol,
        currency=Currency.from_str("USD"),
        price_precision=4,
        price_increment=Price.from_str("0.0001"),
        lot_size=Quantity.from_int(1),
        ts_event=0,
        ts_init=0,
    )


@pytest.fixture
def instrument_provider(instrument_provider_config, instrument) -> AlpacaInstrumentProvider:
    provider = AlpacaInstrumentProvider(config=instrument_provider_config)
    provider.add(instrument)
    return provider


@pytest.fixture
def mock_http_client():
    """Create a mock HTTP client."""
    client = AsyncMock(spec=httpx.AsyncClient)
    return client


@pytest.fixture
def mock_ws_client():
    """Create a mock WebSocket client."""
    client = MagicMock()
    client.subscribe_trade_updates = MagicMock()
    client.run = MagicMock()
    client.stop = MagicMock()
    return client


@pytest.fixture
def execution_client(
    event_loop,
    msgbus,
    cache,
    clock,
    instrument_provider,
    exec_client_config,
    mock_http_client,
    mock_ws_client,
) -> AlpacaLiveExecutionClient:
    with patch(
        "alpaca_adapter.execution.get_async_http_client",
        return_value=mock_http_client,
    ), patch(
        "alpaca_adapter.execution.get_websocket_client",
        return_value=mock_ws_client,
    ):
        client = AlpacaLiveExecutionClient(
            loop=event_loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=exec_client_config,
        )
        return client


# -- OrderTranslator Tests ------------------------------------------------------------------------


class TestOrderTranslator:
    def test_to_alpaca_order_side_buy(self):
        result = OrderTranslator.to_alpaca(OrderSide.BUY)
        assert result == AlpacaOrderSide.BUY

    def test_to_alpaca_order_side_sell(self):
        result = OrderTranslator.to_alpaca(OrderSide.SELL)
        assert result == AlpacaOrderSide.SELL

    def test_to_alpaca_order_type_market(self):
        result = OrderTranslator.to_alpaca(OrderType.MARKET)
        assert result == AlpacaOrderType.MARKET

    def test_to_alpaca_order_type_limit(self):
        result = OrderTranslator.to_alpaca(OrderType.LIMIT)
        assert result == AlpacaOrderType.LIMIT

    def test_to_alpaca_order_type_stop_market(self):
        result = OrderTranslator.to_alpaca(OrderType.STOP_MARKET)
        assert result == AlpacaOrderType.STOP

    def test_to_alpaca_order_type_stop_limit(self):
        result = OrderTranslator.to_alpaca(OrderType.STOP_LIMIT)
        assert result == AlpacaOrderType.STOP_LIMIT

    def test_to_alpaca_time_in_force_day(self):
        result = OrderTranslator.to_alpaca(TimeInForce.DAY)
        assert result == AlpacaTimeInForce.DAY

    def test_to_alpaca_time_in_force_gtc(self):
        result = OrderTranslator.to_alpaca(TimeInForce.GTC)
        assert result == AlpacaTimeInForce.GTC

    def test_to_nautilus_order_side_buy(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderSide.BUY)
        assert result == OrderSide.BUY

    def test_to_nautilus_order_side_sell(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderSide.SELL)
        assert result == OrderSide.SELL

    def test_to_nautilus_order_type_market(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderType.MARKET)
        assert result == OrderType.MARKET

    def test_to_nautilus_order_type_limit(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderType.LIMIT)
        assert result == OrderType.LIMIT

    def test_to_nautilus_order_status_new(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderStatus.NEW)
        assert result == OrderStatus.ACCEPTED

    def test_to_nautilus_order_status_filled(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderStatus.FILLED)
        assert result == OrderStatus.FILLED

    def test_to_nautilus_order_status_canceled(self):
        result = OrderTranslator.to_nautilus(AlpacaOrderStatus.CANCELED)
        assert result == OrderStatus.CANCELED

    def test_to_alpaca_raises_on_unknown_value(self):
        with pytest.raises(KeyError):
            OrderTranslator.to_alpaca("UNKNOWN_VALUE")

    def test_to_nautilus_raises_on_unknown_value(self):
        with pytest.raises(KeyError):
            OrderTranslator.to_nautilus("UNKNOWN_VALUE")

    def test_to_alpaca_with_default(self):
        result = OrderTranslator.to_alpaca("UNKNOWN_VALUE", default=None)
        assert result is None

    def test_to_nautilus_with_default(self):
        result = OrderTranslator.to_nautilus("UNKNOWN_VALUE", default=None)
        assert result is None


# -- AlpacaLiveExecutionClient Tests --------------------------------------------------------------


class TestAlpacaLiveExecutionClient:
    def test_initialization(self, execution_client):
        assert execution_client is not None
        assert execution_client.venue == ALPACA_VENUE
        assert execution_client.account_id is not None

    def test_instrument_provider_property(self, execution_client, instrument_provider):
        assert execution_client.instrument_provider is instrument_provider

    @pytest.mark.asyncio
    async def test_connect_initializes_provider(
        self, execution_client, mock_http_client, instrument_provider
    ):
        # Setup mock response for account
        # AccountBalance requires: total - locked == free
        # With cash=10000, portfolio_value=15000, locked=5000
        # So free must be: 10000 - 5000 = 5000
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "cash": "10000.00",
            "buying_power": "5000.00",
            "portfolio_value": "15000.00",
        }
        mock_response.raise_for_status = MagicMock()
        mock_http_client.get.return_value = mock_response

        # Mock instrument provider initialize
        with patch.object(
            instrument_provider, "initialize", new_callable=AsyncMock
        ) as mock_init:
            await execution_client._connect()
            mock_init.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_closes_connections(
        self, execution_client, mock_http_client, mock_ws_client
    ):
        mock_http_client.aclose = AsyncMock()

        await execution_client._disconnect()

        mock_http_client.aclose.assert_called_once()

    def test_reset(self, execution_client):
        # Should not raise
        execution_client.reset()

    def test_dispose(self, execution_client):
        # Should not raise
        execution_client.dispose()


class TestAlpacaLiveExecutionClientOrderSubmission:
    @pytest.mark.asyncio
    async def test_submit_market_order_success(
        self, execution_client, mock_http_client, instrument, cache
    ):
        # Setup
        order = MarketOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=StrategyId("S-001"),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-001"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            init_id=UUID4(),
            ts_init=0,
        )
        cache.add_order(order)

        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": str(uuid4()),
            "client_order_id": "O-001",
            "symbol": "AAPL",
            "qty": "100",
            "filled_qty": "0",
            "side": "buy",
            "type": "market",
            "time_in_force": "day",
            "status": "new",
            "extended_hours": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_response.raise_for_status = MagicMock()
        mock_http_client.post.return_value = mock_response

        command = SubmitOrder(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            order=order,
            command_id=UUID4(),
            ts_init=0,
        )

        # Execute
        await execution_client._submit_order(command)

        # Verify HTTP call was made
        mock_http_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_submit_limit_order_success(
        self, execution_client, mock_http_client, instrument, cache
    ):
        order = LimitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=StrategyId("S-001"),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-002"),
            order_side=OrderSide.SELL,
            quantity=Quantity.from_int(50),
            price=Price.from_str("150.00"),
            init_id=UUID4(),
            ts_init=0,
        )
        cache.add_order(order)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": str(uuid4()),
            "client_order_id": "O-002",
            "symbol": "AAPL",
            "qty": "50",
            "filled_qty": "0",
            "side": "sell",
            "type": "limit",
            "time_in_force": "day",
            "status": "new",
            "extended_hours": False,
            "limit_price": "150.00",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_response.raise_for_status = MagicMock()
        mock_http_client.post.return_value = mock_response

        command = SubmitOrder(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            order=order,
            command_id=UUID4(),
            ts_init=0,
        )

        await execution_client._submit_order(command)
        mock_http_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_submit_order_rejected_on_http_error(
        self, execution_client, mock_http_client, instrument, cache
    ):
        order = MarketOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=StrategyId("S-001"),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-003"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            init_id=UUID4(),
            ts_init=0,
        )
        cache.add_order(order)

        # Mock HTTP error
        error_response = MagicMock()
        error_response.json.return_value = {"message": "Insufficient funds"}
        mock_http_client.post.side_effect = httpx.HTTPStatusError(
            "Error",
            request=MagicMock(),
            response=error_response,
        )

        command = SubmitOrder(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            order=order,
            command_id=UUID4(),
            ts_init=0,
        )

        # Should not raise, should generate rejection event
        await execution_client._submit_order(command)


class TestAlpacaLiveExecutionClientOrderCancellation:
    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self, execution_client, mock_http_client, instrument, cache
    ):
        order = MarketOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=StrategyId("S-001"),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-004"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            init_id=UUID4(),
            ts_init=0,
        )
        cache.add_order(order)

        venue_order_id = VenueOrderId(str(uuid4()))
        cache.add_venue_order_id(order.client_order_id, venue_order_id)

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_http_client.delete.return_value = mock_response

        command = CancelOrder(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        )

        await execution_client._cancel_order(command)
        mock_http_client.delete.assert_called_once()


class TestAlpacaLiveExecutionClientReports:
    @pytest.mark.asyncio
    async def test_generate_order_status_report_by_venue_order_id(
        self, execution_client, mock_http_client, instrument
    ):
        venue_order_id = VenueOrderId(str(uuid4()))

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": venue_order_id.value,
            "client_order_id": "O-005",
            "symbol": "AAPL",
            "qty": "100",
            "filled_qty": "50",
            "side": "buy",
            "type": "limit",
            "time_in_force": "gtc",
            "status": "partially_filled",
            "extended_hours": False,
            "limit_price": "150.00",
            "filled_avg_price": "149.50",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        mock_response.raise_for_status = MagicMock()
        mock_http_client.get.return_value = mock_response

        command = GenerateOrderStatusReport(
            instrument_id=instrument.id,
            venue_order_id=venue_order_id,
            client_order_id=None,
            command_id=UUID4(),
            ts_init=0,
        )

        report = await execution_client.generate_order_status_report(command)

        assert report is not None
        assert report.venue_order_id == venue_order_id

    @pytest.mark.asyncio
    async def test_generate_order_status_reports(
        self, execution_client, mock_http_client, instrument
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": str(uuid4()),
                "client_order_id": "O-006",
                "symbol": "AAPL",
                "qty": "100",
                "filled_qty": "0",
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
                "status": "new",
                "extended_hours": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        ]
        mock_response.raise_for_status = MagicMock()
        mock_http_client.get.return_value = mock_response

        command = GenerateOrderStatusReports(
            instrument_id=instrument.id,
            start=None,
            end=None,
            open_only=False,
            command_id=UUID4(),
            ts_init=0,
        )

        reports = await execution_client.generate_order_status_reports(command)

        assert len(reports) == 1

    @pytest.mark.asyncio
    async def test_generate_position_status_reports(
        self, execution_client, mock_http_client, instrument
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "asset_id": str(uuid4()),
                "symbol": "AAPL",
                "exchange": "NASDAQ",
                "asset_class": "us_equity",
                "qty": "100",
                "side": "long",
                "market_value": "15000.00",
                "cost_basis": "15000.00",
                "avg_entry_price": "150.00",
                "unrealized_pl": "500.00",
            },
        ]
        mock_response.raise_for_status = MagicMock()
        mock_http_client.get.return_value = mock_response

        command = GeneratePositionStatusReports(
            instrument_id=instrument.id,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=0,
        )

        reports = await execution_client.generate_position_status_reports(command)

        assert len(reports) == 1
        assert reports[0].instrument_id == instrument.id


class TestAlpacaLiveExecutionClientWebSocket:
    @pytest.mark.asyncio
    async def test_on_trade_updates_handler_fill_event(
        self, execution_client, instrument, cache
    ):
        # Setup an order in cache
        order = MarketOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=StrategyId("S-001"),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-007"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            init_id=UUID4(),
            ts_init=0,
        )
        cache.add_order(order)
        cache.add_instrument(instrument)

        venue_order_id = VenueOrderId(str(uuid4()))
        cache.add_venue_order_id(order.client_order_id, venue_order_id)

        # Create mock trade update
        alpaca_order = AlpacaOrder(
            id=uuid4(),
            client_order_id="O-007",
            symbol="AAPL",
            qty=Decimal("100"),
            filled_qty=Decimal("100"),
            filled_avg_price=Decimal("150.00"),
            side=AlpacaOrderSide.BUY,
            type=AlpacaOrderType.MARKET,
            time_in_force=AlpacaTimeInForce.DAY,
            status=AlpacaOrderStatus.FILLED,
            extended_hours=False,
            created_at=datetime.now(timezone.utc),
            submitted_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            filled_at=datetime.now(timezone.utc),
        )

        trade_update = MagicMock(spec=AlpacaTradeUpdate)
        trade_update.event = "fill"
        trade_update.order = alpaca_order

        # Execute - should not raise
        await execution_client._on_trade_updates_handler(trade_update)

    @pytest.mark.asyncio
    async def test_on_trade_updates_handler_canceled_event(
        self, execution_client, instrument, cache
    ):
        order = MarketOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=StrategyId("S-001"),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("O-008"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            init_id=UUID4(),
            ts_init=0,
        )
        cache.add_order(order)
        cache.add_instrument(instrument)

        venue_order_id = VenueOrderId(str(uuid4()))
        cache.add_venue_order_id(order.client_order_id, venue_order_id)

        alpaca_order = AlpacaOrder(
            id=uuid4(),
            client_order_id="O-008",
            symbol="AAPL",
            qty=Decimal("100"),
            filled_qty=Decimal("0"),
            side=AlpacaOrderSide.BUY,
            type=AlpacaOrderType.MARKET,
            time_in_force=AlpacaTimeInForce.DAY,
            status=AlpacaOrderStatus.CANCELED,
            extended_hours=False,
            created_at=datetime.now(timezone.utc),
            submitted_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            canceled_at=datetime.now(timezone.utc),
        )

        trade_update = MagicMock(spec=AlpacaTradeUpdate)
        trade_update.event = "canceled"
        trade_update.order = alpaca_order

        await execution_client._on_trade_updates_handler(trade_update)

    @pytest.mark.asyncio
    async def test_on_trade_updates_handler_external_order(
        self, execution_client, instrument, mock_http_client
    ):
        # External order - not in cache
        alpaca_order = AlpacaOrder(
            id=uuid4(),
            client_order_id="EXTERNAL-001",
            symbol="AAPL",
            qty=Decimal("50"),
            filled_qty=Decimal("50"),
            filled_avg_price=Decimal("155.00"),
            side=AlpacaOrderSide.SELL,
            type=AlpacaOrderType.LIMIT,
            time_in_force=AlpacaTimeInForce.GTC,
            status=AlpacaOrderStatus.FILLED,
            extended_hours=False,
            limit_price=Decimal("155.00"),
            created_at=datetime.now(timezone.utc),
            submitted_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            filled_at=datetime.now(timezone.utc),
        )

        trade_update = MagicMock(spec=AlpacaTradeUpdate)
        trade_update.event = "fill"
        trade_update.order = alpaca_order

        # Should handle gracefully without raising
        await execution_client._on_trade_updates_handler(trade_update)
