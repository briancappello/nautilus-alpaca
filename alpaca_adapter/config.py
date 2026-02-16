from __future__ import annotations

from typing import Any, Literal

from alpaca.common.enums import BaseURL
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus
from alpaca.trading.requests import GetAssetsRequest

from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveExecClientConfig,
    NautilusConfig,
    RoutingConfig,
)
from nautilus_trader.model.identifiers import InstrumentId

from alpaca_adapter.core import to_request_fields


class AlpacaCredentialsConfig(NautilusConfig, frozen=True, kw_only=True):
    live_api_key: str
    live_api_secret: str
    paper_api_key: str
    paper_api_secret: str
    mode: Literal["live", "paper"] = "paper"

    http_base_url_override: str | None = None
    wss_base_url_override: str | None = None
    api_version: Literal["v2"] = "v2"

    @property
    def api_key(self) -> str:
        if self.mode == "live":
            return self.live_api_key
        return self.paper_api_key

    @property
    def api_secret(self) -> str:
        if self.mode == "live":
            return self.live_api_secret
        return self.paper_api_secret

    @property
    def http_base_url(self) -> str:
        base_url = (
            self.http_base_url_override
            or (
                BaseURL.TRADING_LIVE if self.mode == "live" else BaseURL.TRADING_PAPER
            ).value
        )
        return f"{base_url}/{self.api_version}"

    @property
    def wss_base_url(self) -> str:
        return (
            self.wss_base_url_override
            or (
                BaseURL.TRADING_STREAM_LIVE
                if self.mode == "live"
                else BaseURL.TRADING_STREAM_PAPER
            ).value
        )


class AlpacaInstrumentProviderConfig(InstrumentProviderConfig, frozen=True, kw_only=True):
    credentials: AlpacaCredentialsConfig

    asset_status: AssetStatus | None = AssetStatus.ACTIVE
    asset_class: AssetClass | None = AssetClass.US_EQUITY
    asset_exchange: AssetExchange | None = None

    load_all: bool = False
    load_ids: frozenset[InstrumentId] | None = None
    filters: dict[str, Any] | None = None
    filter_callable: str | None = None
    log_warnings: bool = True

    def get_assets_request_params(self) -> dict:
        return to_request_fields(
            GetAssetsRequest(
                status=self.asset_status,
                asset_class=self.asset_class,
                exchange=self.asset_exchange,
            )
        )


class AlpacaLiveExecutionClientConfig(LiveExecClientConfig, frozen=True, kw_only=True):
    credentials: AlpacaCredentialsConfig
    instrument_provider: InstrumentProviderConfig = InstrumentProviderConfig()
    routing: RoutingConfig = RoutingConfig()
