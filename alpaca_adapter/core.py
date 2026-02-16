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

from datetime import date, datetime, timezone
from enum import Enum
from functools import lru_cache
from ipaddress import IPv4Address, IPv6Address
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID

import httpx

from pydantic import BaseModel

from alpaca.common.requests import NonEmptyRequest
from alpaca.trading.stream import TradingStream

from nautilus_trader.model.identifiers import ClientId, Venue


if TYPE_CHECKING:
    from .config import AlpacaCredentialsConfig


# It's recommended to have one constant for the venue
ALPACA: Final[str] = "ALPACA"
ALPACA_VENUE: Final[Venue] = Venue(ALPACA)
ALPACA_CLIENT_ID: Final[ClientId] = ClientId(ALPACA)


def get_async_http_client(credentials: AlpacaCredentialsConfig) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=credentials.http_base_url,
        headers={
            "APCA-API-KEY-ID": credentials.api_key,
            "APCA-API-SECRET-KEY": credentials.api_secret,
        },
    )


def get_websocket_client(credentials: AlpacaCredentialsConfig) -> TradingStream:
    return TradingStream(
        api_key=credentials.api_key,
        secret_key=credentials.api_secret,
        paper=credentials.mode != "live",
    )


def to_request_fields(model: BaseModel) -> dict:
    """
    the equivalent of self::dict but removes empty values and handles converting non json serializable types.

    Ie say we only set trusted_contact.given_name instead of generating a dict like:
      {contact: {city: None, country: None...}, etc}
    we generate just:
      {trusted_contact:{given_name: "new value"}}

    NOTE: This function recurses to handle nested models, so do not use on a self-referential model

    Returns:
        dict: a dict containing any set fields
    """

    def map_values(val: Any) -> Any:
        """
        Some types have issues being json encoded, we convert them here to be encodable

        also handles nested models and lists
        """
        if isinstance(val, Enum):
            return val.value

        if isinstance(val, UUID):
            return str(val)

        if isinstance(val, NonEmptyRequest):
            return to_request_fields(val)

        if isinstance(val, dict):
            return {k: map_values(v) for k, v in val.items()}

        if isinstance(val, list):
            return [map_values(v) for v in val]

        # RFC 3339
        if isinstance(val, datetime):
            # if the datetime is naive, assume it is UTC
            # https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive
            if val.tzinfo is None or val.tzinfo.utcoffset(val) is None:
                val = val.replace(tzinfo=timezone.utc)
            return val.isoformat()

        if isinstance(val, date):
            return val.isoformat()

        if isinstance(val, IPv4Address):
            return str(val)

        if isinstance(val, IPv6Address):
            return str(val)

        return val

    d = model.model_dump(exclude_none=True)
    if "symbol_or_symbols" in d:
        s = d["symbol_or_symbols"]
        if isinstance(s, list):
            s = ",".join(s)
        d["symbols"] = s
        del d["symbol_or_symbols"]

    # pydantic almost has what we need by passing exclude_none to dict() but it returns:
    #  {trusted_contact: {}, contact: {}, identity: None, etc}
    # so we do a simple list comprehension to filter out None and {}
    return {
        key: map_values(val)
        for key, val in d.items()
        if val is not None and val != {} and len(str(val)) > 0
    }
