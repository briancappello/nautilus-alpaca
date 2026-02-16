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

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest

from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.instruments import Equity

from alpaca_adapter.config import AlpacaCredentialsConfig, AlpacaInstrumentProviderConfig
from alpaca_adapter.core import ALPACA, ALPACA_VENUE
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
def provider_config(credentials) -> AlpacaInstrumentProviderConfig:
    return AlpacaInstrumentProviderConfig(credentials=credentials)


@pytest.fixture
def mock_http_client():
    """Create a mock HTTP client."""
    client = AsyncMock(spec=httpx.AsyncClient)
    return client


@pytest.fixture
def provider(provider_config, mock_http_client) -> AlpacaInstrumentProvider:
    """Create an instrument provider with a mocked HTTP client."""
    with patch(
        "alpaca_adapter.providers.get_async_http_client",
        return_value=mock_http_client,
    ):
        return AlpacaInstrumentProvider(config=provider_config)


def make_asset_data(
    symbol: str,
    *,
    asset_id: str | None = None,
    asset_class: str = "us_equity",
    exchange: str = "NASDAQ",
    name: str | None = None,
    status: str = "active",
    tradable: bool = True,
    marginable: bool = True,
    shortable: bool = True,
    easy_to_borrow: bool = True,
    fractionable: bool = True,
    price_increment: float | None = None,
    min_order_size: float | None = None,
    min_trade_increment: float | None = None,
    maintenance_margin_requirement: float | None = None,
    attributes: list | None = None,
) -> dict:
    """Create mock asset data that matches Alpaca API response format."""
    return {
        "id": asset_id or str(uuid4()),
        "class": asset_class,
        "exchange": exchange,
        "symbol": symbol,
        "name": name or f"{symbol} Inc.",
        "status": status,
        "tradable": tradable,
        "marginable": marginable,
        "shortable": shortable,
        "easy_to_borrow": easy_to_borrow,
        "fractionable": fractionable,
        "price_increment": price_increment,
        "min_order_size": min_order_size,
        "min_trade_increment": min_trade_increment,
        "maintenance_margin_requirement": maintenance_margin_requirement,
        "attributes": attributes or [],
    }


def make_mock_response(json_data, status_code: int = 200) -> MagicMock:
    """Create a mock httpx.Response."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = json_data
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=response,
        )
    return response


# -- Load All Tests -------------------------------------------------------------------------------


class TestLoadAllAsync:
    @pytest.mark.asyncio
    async def test_load_all_async_populates_provider(self, provider, mock_http_client):
        # Arrange
        assets_data = [
            make_asset_data("AAPL"),
            make_asset_data("MSFT"),
            make_asset_data("GOOGL"),
        ]
        mock_http_client.get = AsyncMock(return_value=make_mock_response(assets_data))

        # Act
        await provider.load_all_async()

        # Assert
        mock_http_client.get.assert_awaited_once()
        call_args = mock_http_client.get.call_args
        assert call_args[0][0] == "/assets"

        instruments = provider.get_all()
        assert len(instruments) == 3

        aapl_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        msft_id = InstrumentId.from_str(f"MSFT.{ALPACA}")
        googl_id = InstrumentId.from_str(f"GOOGL.{ALPACA}")

        assert aapl_id in instruments
        assert msft_id in instruments
        assert googl_id in instruments

    @pytest.mark.asyncio
    async def test_load_all_async_filters_non_tradable_assets(
        self, provider, mock_http_client
    ):
        # Arrange
        assets_data = [
            make_asset_data("AAPL", tradable=True),
            make_asset_data("DELISTED", tradable=False),
            make_asset_data("MSFT", tradable=True),
        ]
        mock_http_client.get = AsyncMock(return_value=make_mock_response(assets_data))

        # Act
        await provider.load_all_async()

        # Assert
        instruments = provider.get_all()
        assert len(instruments) == 2

        aapl_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        msft_id = InstrumentId.from_str(f"MSFT.{ALPACA}")
        delisted_id = InstrumentId.from_str(f"DELISTED.{ALPACA}")

        assert aapl_id in instruments
        assert msft_id in instruments
        assert delisted_id not in instruments

    @pytest.mark.asyncio
    async def test_load_all_async_filters_inactive_assets(
        self, provider, mock_http_client
    ):
        # Arrange
        assets_data = [
            make_asset_data("AAPL", status="active"),
            make_asset_data("INACTIVE", status="inactive"),
        ]
        mock_http_client.get = AsyncMock(return_value=make_mock_response(assets_data))

        # Act
        await provider.load_all_async()

        # Assert
        instruments = provider.get_all()
        assert len(instruments) == 1
        assert InstrumentId.from_str(f"AAPL.{ALPACA}") in instruments
        assert InstrumentId.from_str(f"INACTIVE.{ALPACA}") not in instruments

    @pytest.mark.asyncio
    async def test_load_all_async_filters_non_us_equity(
        self, provider, mock_http_client
    ):
        # Arrange
        assets_data = [
            make_asset_data("AAPL", asset_class="us_equity"),
            make_asset_data("BTCUSD", asset_class="crypto"),
        ]
        mock_http_client.get = AsyncMock(return_value=make_mock_response(assets_data))

        # Act
        await provider.load_all_async()

        # Assert
        instruments = provider.get_all()
        assert len(instruments) == 1
        assert InstrumentId.from_str(f"AAPL.{ALPACA}") in instruments
        assert InstrumentId.from_str(f"BTCUSD.{ALPACA}") not in instruments

    @pytest.mark.asyncio
    async def test_load_all_async_with_filters(self, provider, mock_http_client):
        # Arrange
        assets_data = [make_asset_data("AAPL")]
        mock_http_client.get = AsyncMock(return_value=make_mock_response(assets_data))
        filters = {"exchange": "NASDAQ"}

        # Act
        await provider.load_all_async(filters=filters)

        # Assert
        call_args = mock_http_client.get.call_args
        params = call_args[1]["params"]
        assert "exchange" in params
        assert params["exchange"] == "NASDAQ"

    @pytest.mark.asyncio
    async def test_load_all_async_raises_on_http_error(
        self, provider, mock_http_client
    ):
        # Arrange
        mock_http_client.get = AsyncMock(
            return_value=make_mock_response({}, status_code=500)
        )

        # Act & Assert
        with pytest.raises(httpx.HTTPStatusError):
            await provider.load_all_async()

    @pytest.mark.asyncio
    async def test_load_all_async_handles_empty_response(
        self, provider, mock_http_client
    ):
        # Arrange
        mock_http_client.get = AsyncMock(return_value=make_mock_response([]))

        # Act
        await provider.load_all_async()

        # Assert
        instruments = provider.get_all()
        assert len(instruments) == 0


# -- Load Single Instrument Tests -----------------------------------------------------------------


class TestLoadAsync:
    @pytest.mark.asyncio
    async def test_load_async_single_instrument(self, provider, mock_http_client):
        # Arrange
        asset_data = make_asset_data("AAPL")
        mock_http_client.get = AsyncMock(return_value=make_mock_response(asset_data))
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")

        # Act
        await provider.load_async(instrument_id)

        # Assert
        mock_http_client.get.assert_awaited_once()
        call_args = mock_http_client.get.call_args
        assert call_args[0][0] == "/assets/AAPL"

        instruments = provider.get_all()
        assert len(instruments) == 1
        assert instrument_id in instruments

    @pytest.mark.asyncio
    async def test_load_async_validates_venue(self, provider):
        # Arrange
        wrong_venue_id = InstrumentId.from_str("AAPL.NYSE")

        # Act & Assert
        with pytest.raises(Exception):  # PyCondition raises an exception
            await provider.load_async(wrong_venue_id)

    @pytest.mark.asyncio
    async def test_load_async_raises_on_404(self, provider, mock_http_client):
        # Arrange
        response = make_mock_response({}, status_code=404)
        mock_http_client.get = AsyncMock(return_value=response)
        instrument_id = InstrumentId.from_str(f"NOTFOUND.{ALPACA}")

        # Act & Assert
        with pytest.raises(httpx.HTTPStatusError):
            await provider.load_async(instrument_id)

    @pytest.mark.asyncio
    async def test_load_async_skips_non_tradable_asset(self, provider, mock_http_client):
        # Arrange
        asset_data = make_asset_data("DELISTED", tradable=False)
        mock_http_client.get = AsyncMock(return_value=make_mock_response(asset_data))
        instrument_id = InstrumentId.from_str(f"DELISTED.{ALPACA}")

        # Act
        await provider.load_async(instrument_id)

        # Assert - should not be added to provider
        instruments = provider.get_all()
        assert len(instruments) == 0


# -- Load IDs Tests -------------------------------------------------------------------------------


class TestLoadIdsAsync:
    @pytest.mark.asyncio
    async def test_load_ids_async_loads_multiple_instruments(
        self, provider, mock_http_client
    ):
        # Arrange
        aapl_data = make_asset_data("AAPL")
        msft_data = make_asset_data("MSFT")

        # Mock to return different responses for each call
        mock_http_client.get = AsyncMock(
            side_effect=[
                make_mock_response(aapl_data),
                make_mock_response(msft_data),
            ]
        )

        instrument_ids = [
            InstrumentId.from_str(f"AAPL.{ALPACA}"),
            InstrumentId.from_str(f"MSFT.{ALPACA}"),
        ]

        # Act
        await provider.load_ids_async(instrument_ids)

        # Assert
        assert mock_http_client.get.await_count == 2
        instruments = provider.get_all()
        assert len(instruments) == 2

    @pytest.mark.asyncio
    async def test_load_ids_async_handles_empty_list(self, provider, mock_http_client):
        # Act
        await provider.load_ids_async([])

        # Assert
        mock_http_client.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_load_ids_async_validates_all_venues(self, provider):
        # Arrange
        instrument_ids = [
            InstrumentId.from_str(f"AAPL.{ALPACA}"),
            InstrumentId.from_str("MSFT.NYSE"),  # Wrong venue
        ]

        # Act & Assert
        with pytest.raises(Exception):
            await provider.load_ids_async(instrument_ids)

    @pytest.mark.asyncio
    async def test_load_ids_async_continues_on_single_failure(
        self, provider, mock_http_client
    ):
        # Arrange
        aapl_data = make_asset_data("AAPL")

        # First call succeeds, second call fails with 404
        mock_http_client.get = AsyncMock(
            side_effect=[
                make_mock_response(aapl_data),
                make_mock_response({}, status_code=404),
            ]
        )

        instrument_ids = [
            InstrumentId.from_str(f"AAPL.{ALPACA}"),
            InstrumentId.from_str(f"NOTFOUND.{ALPACA}"),
        ]

        # Act - should not raise, but log warning
        await provider.load_ids_async(instrument_ids)

        # Assert - first one should be loaded
        instruments = provider.get_all()
        assert len(instruments) == 1
        assert InstrumentId.from_str(f"AAPL.{ALPACA}") in instruments


# -- Asset Filtering Tests ------------------------------------------------------------------------


class TestShouldAddAsset:
    def test_should_add_asset_returns_true_for_valid_asset(
        self, provider, mock_http_client
    ):
        # Arrange
        from alpaca.trading.enums import AssetClass, AssetStatus
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(make_asset_data("AAPL"))

        # Act
        result = provider._should_add_asset(asset)

        # Assert
        assert result is True

    def test_should_add_asset_returns_false_for_non_us_equity(
        self, provider, mock_http_client
    ):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(make_asset_data("BTCUSD", asset_class="crypto"))

        # Act
        result = provider._should_add_asset(asset)

        # Assert
        assert result is False

    def test_should_add_asset_returns_false_for_non_tradable(
        self, provider, mock_http_client
    ):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(make_asset_data("DELISTED", tradable=False))

        # Act
        result = provider._should_add_asset(asset)

        # Assert
        assert result is False

    def test_should_add_asset_returns_false_for_inactive(
        self, provider, mock_http_client
    ):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(make_asset_data("INACTIVE", status="inactive"))

        # Act
        result = provider._should_add_asset(asset)

        # Assert
        assert result is False


# -- Asset to Instrument Conversion Tests ---------------------------------------------------------


class TestAddAsset:
    def test_add_asset_creates_equity_with_correct_id(self, provider, mock_http_client):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(make_asset_data("AAPL"))

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        assert instrument is not None
        assert isinstance(instrument, Equity)
        assert instrument.id == instrument_id

    def test_add_asset_uses_price_increment_from_asset(
        self, provider, mock_http_client
    ):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(
            make_asset_data("AAPL", price_increment=0.01)
        )

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        assert instrument.price_precision == 2
        assert float(instrument.price_increment) == 0.01

    def test_add_asset_uses_default_precision_when_no_increment(
        self, provider, mock_http_client
    ):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(
            make_asset_data("AAPL", price_increment=None)
        )

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        assert instrument.price_precision == 4  # Default
        assert float(instrument.price_increment) == 0.0001

    def test_add_asset_handles_fractional_shares(self, provider, mock_http_client):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(
            make_asset_data("AAPL", fractionable=True, min_trade_increment=None)
        )

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        # Should use fractional lot size
        assert float(instrument.lot_size) == 0.001

    def test_add_asset_handles_min_trade_increment(self, provider, mock_http_client):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(
            make_asset_data("AAPL", min_trade_increment=0.0001)
        )

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        assert float(instrument.lot_size) == 0.0001

    def test_add_asset_handles_non_fractional_shares(self, provider, mock_http_client):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(
            make_asset_data(
                "AAPL",
                fractionable=False,
                min_trade_increment=None,
            )
        )

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        assert float(instrument.lot_size) == 1.0

    def test_add_asset_sets_min_quantity_from_asset(self, provider, mock_http_client):
        # Arrange
        from alpaca.trading.models import Asset

        asset = Asset.model_validate(
            make_asset_data("AAPL", min_order_size=0.01)
        )

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        assert instrument.min_quantity is not None
        assert float(instrument.min_quantity) == 0.01

    def test_add_asset_stores_metadata_in_info_dict(self, provider, mock_http_client):
        # Arrange
        from alpaca.trading.models import Asset

        asset_data = make_asset_data(
            "AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            marginable=True,
            shortable=True,
            easy_to_borrow=True,
            fractionable=True,
        )
        asset = Asset.model_validate(asset_data)

        # Act
        provider._add_asset(asset)

        # Assert
        instrument_id = InstrumentId.from_str(f"AAPL.{ALPACA}")
        instrument = provider.find(instrument_id)
        info = instrument.info

        assert info["name"] == "Apple Inc."
        assert info["exchange"] == "NASDAQ"
        assert info["marginable"] is True
        assert info["shortable"] is True
        assert info["easy_to_borrow"] is True
        assert info["fractionable"] is True


# -- Close Tests ----------------------------------------------------------------------------------


class TestClose:
    @pytest.mark.asyncio
    async def test_close_closes_http_client(self, provider, mock_http_client):
        # Act
        await provider.close()

        # Assert
        mock_http_client.aclose.assert_awaited_once()


# -- Config Tests ---------------------------------------------------------------------------------


class TestProviderConfig:
    def test_config_get_assets_request_params(self, credentials):
        # Arrange
        from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus

        config = AlpacaInstrumentProviderConfig(
            credentials=credentials,
            asset_status=AssetStatus.ACTIVE,
            asset_class=AssetClass.US_EQUITY,
            asset_exchange=AssetExchange.NASDAQ,
        )

        # Act
        params = config.get_assets_request_params()

        # Assert
        assert "status" in params
        assert params["status"] == "active"
        assert "asset_class" in params
        assert params["asset_class"] == "us_equity"
        assert "exchange" in params
        assert params["exchange"] == "NASDAQ"

    def test_config_default_values(self, credentials):
        # Arrange
        config = AlpacaInstrumentProviderConfig(credentials=credentials)

        # Assert
        from alpaca.trading.enums import AssetClass, AssetStatus

        assert config.asset_status == AssetStatus.ACTIVE
        assert config.asset_class == AssetClass.US_EQUITY
        assert config.asset_exchange is None
        assert config.load_all is False
        assert config.load_ids is None
