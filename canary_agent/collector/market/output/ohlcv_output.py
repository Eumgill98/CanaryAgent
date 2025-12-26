from pydantic import Field
from typing import Optional, Dict

from canary_agent.core.base import BaseMarketOutput

class OHLCVOutput(BaseMarketOutput):
    data: Dict = Field(...)
    type: str = Field(...)

    # time setting
    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
    interval: Optional[str] = Field(default=None)
