from __future__ import annotations

import asyncio
import os

from typing import Literal

from nautilus_trader.model.identifiers import InstrumentId

from alpaca_adapter.config import AlpacaCredentialsConfig, AlpacaInstrumentProviderConfig
from alpaca_adapter.execution import (
    AlpacaLiveExecutionClient,
    AlpacaLiveExecutionClientConfig,
)
from alpaca_adapter.providers import AlpacaInstrumentProvider


async def go(mode: Literal["live", "paper"] = "paper"):
    credentials = AlpacaCredentialsConfig(
        live_api_key=os.getenv("ALPACA_API_KEY_LIVE"),
        live_api_secret=os.getenv("ALPACA_API_SECRET_LIVE"),
        paper_api_key=os.getenv("ALPACA_API_KEY_PAPER"),
        paper_api_secret=os.getenv("ALPACA_API_SECRET_PAPER"),
        mode=mode,
    )
    provider_config = AlpacaInstrumentProviderConfig(
        credentials=credentials,
        load_all=True,
    )
    provider = AlpacaInstrumentProvider(config=provider_config)

    execution_client_config = AlpacaLiveExecutionClientConfig(
        credentials=credentials,
        instrument_provider=provider_config,
    )
    # execution_client = AlpacaLiveExecutionClient(
    #     loop=asyncio.get_event_loop(),
    # )


if __name__ == "__main__":
    asyncio.run(go("paper"))
