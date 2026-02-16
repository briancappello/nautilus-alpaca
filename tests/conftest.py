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

import pytest

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.model.identifiers import InstrumentId, TraderId, Venue
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Currency, Price, Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from alpaca_adapter.config import (
    AlpacaCredentialsConfig,
    AlpacaInstrumentProviderConfig,
    AlpacaLiveExecutionClientConfig,
)
from alpaca_adapter.core import ALPACA, ALPACA_VENUE
from alpaca_adapter.providers import AlpacaInstrumentProvider


@pytest.fixture
def venue() -> Venue:
    return ALPACA_VENUE


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
