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

import httpx
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.models import Asset

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Currency, Price, Quantity

from .config import AlpacaInstrumentProviderConfig
from .core import ALPACA, ALPACA_VENUE, get_async_http_client


class AlpacaInstrumentProvider(InstrumentProvider):
    """
    Provides instrument definitions from Alpaca.

    Parameters
    ----------
    config : AlpacaInstrumentProviderConfig
        The instrument provider configuration.

    """

    def __init__(self, config: AlpacaInstrumentProviderConfig) -> None:
        super().__init__(config)
        self.config: AlpacaInstrumentProviderConfig = config
        self._http_client: httpx.AsyncClient = get_async_http_client(config.credentials)

    async def load_all_async(
        self,
        filters: dict | None = None,
    ) -> None:
        """
        Load all tradable assets from Alpaca.

        Parameters
        ----------
        filters : dict, optional
            Additional filters to apply to the asset request.

        """
        filters = filters or {}
        params = self.config.get_assets_request_params() | filters

        self._log.info(f"Loading all assets with params: {params}")

        try:
            response = await self._http_client.get("/assets", params=params)
            response.raise_for_status()

            assets_data = response.json()
            loaded_count = 0
            skipped_count = 0

            for asset_data in assets_data:
                asset = Asset.model_validate(asset_data)
                if self._should_add_asset(asset):
                    self._add_asset(asset)
                    loaded_count += 1
                else:
                    skipped_count += 1

            self._log.info(
                f"Loaded {loaded_count} instruments, skipped {skipped_count} non-tradable/unsupported"
            )

        except httpx.HTTPStatusError as e:
            self._log.error(f"Failed to load assets: {e}")
            raise

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        """
        Load specific instruments by their IDs.

        Parameters
        ----------
        instrument_ids : list[InstrumentId]
            The instrument IDs to load.
        filters : dict, optional
            Additional filters to apply.

        """
        if not instrument_ids:
            self._log.info("No instrument IDs given for loading")
            return

        # Validate all instrument IDs are for Alpaca
        for instrument_id in instrument_ids:
            PyCondition.equal(
                instrument_id.venue,
                ALPACA_VENUE,
                "instrument_id.venue",
                "ALPACA",
            )

        filters = filters or {}
        self._log.info(f"Loading {len(instrument_ids)} instruments")

        for instrument_id in instrument_ids:
            try:
                await self.load_async(instrument_id, filters)
            except httpx.HTTPStatusError as e:
                self._log.warning(f"Failed to load {instrument_id}: {e}")

    async def load_async(
        self,
        instrument_id: InstrumentId,
        filters: dict | None = None,
    ) -> None:
        """
        Load a single instrument by its ID.

        Parameters
        ----------
        instrument_id : InstrumentId
            The instrument ID to load.
        filters : dict, optional
            Additional filters to apply.

        """
        PyCondition.not_none(instrument_id, "instrument_id")
        PyCondition.equal(
            instrument_id.venue,
            ALPACA_VENUE,
            "instrument_id.venue",
            "ALPACA",
        )

        filters = filters or {}
        symbol = instrument_id.symbol.value

        try:
            response = await self._http_client.get(f"/assets/{symbol}", params=filters)
            response.raise_for_status()

            asset = Asset.model_validate(response.json())

            if self._should_add_asset(asset):
                self._add_asset(asset)
                self._log.debug(f"Loaded instrument: {instrument_id}")
            else:
                self._log.warning(
                    f"Asset {symbol} is not tradable or not supported, skipping"
                )

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._log.warning(f"Asset not found: {symbol}")
            else:
                self._log.error(f"Failed to load asset {symbol}: {e}")
            raise

    def _should_add_asset(self, asset: Asset) -> bool:
        """
        Check if the asset should be added to the provider.

        Parameters
        ----------
        asset : Asset
            The Alpaca asset to check.

        Returns
        -------
        bool

        """
        # Only add tradable US equities for now
        if asset.asset_class != AssetClass.US_EQUITY:
            return False

        if not asset.tradable:
            return False

        if asset.status != AssetStatus.ACTIVE:
            return False

        return True

    def _add_asset(self, asset: Asset) -> None:
        """
        Add an Alpaca asset as a Nautilus instrument.

        Parameters
        ----------
        asset : Asset
            The Alpaca asset to add.

        """
        instrument_id = InstrumentId.from_str(f"{asset.symbol}.{ALPACA}")

        # Determine price precision and increment from asset data
        # Alpaca provides price_increment for some assets
        if asset.price_increment is not None and asset.price_increment > 0:
            price_increment = Decimal(str(asset.price_increment))
            # Calculate precision from increment
            price_precision = abs(price_increment.as_tuple().exponent)
        else:
            # Default for US equities - 4 decimal places (sub-penny pricing)
            price_precision = 4
            price_increment = Decimal("0.0001")

        # Determine lot size / min trade increment
        if asset.min_trade_increment is not None and asset.min_trade_increment > 0:
            # Fractional shares supported
            size_increment = Decimal(str(asset.min_trade_increment))
            size_precision = abs(size_increment.as_tuple().exponent)
            lot_size = Quantity(float(size_increment), size_precision)
        elif asset.fractionable:
            # Fractional shares allowed, use small increment
            lot_size = Quantity.from_str("0.001")
        else:
            # Standard lot size for equities
            lot_size = Quantity.from_int(1)

        # Determine minimum order size
        if asset.min_order_size is not None and asset.min_order_size > 0:
            min_quantity = Quantity(float(asset.min_order_size), lot_size.precision)
        else:
            min_quantity = None

        equity = Equity(
            instrument_id=instrument_id,
            raw_symbol=instrument_id.symbol,
            currency=Currency.from_str("USD"),
            price_precision=price_precision,
            price_increment=Price(float(price_increment), price_precision),
            lot_size=lot_size,
            max_quantity=None,
            min_quantity=min_quantity,
            ts_event=0,
            ts_init=0,
            info={
                "asset_id": str(asset.id) if asset.id else None,
                "name": asset.name,
                "exchange": asset.exchange.value if asset.exchange else None,
                "tradable": asset.tradable,
                "marginable": asset.marginable,
                "shortable": asset.shortable,
                "easy_to_borrow": asset.easy_to_borrow,
                "fractionable": asset.fractionable,
                "maintenance_margin_requirement": asset.maintenance_margin_requirement,
                "attributes": asset.attributes,
            },
        )

        self.add(equity)

    async def close(self) -> None:
        """Close the HTTP client connection."""
        await self._http_client.aclose()
