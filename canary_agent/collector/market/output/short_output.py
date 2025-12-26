from pydantic import Field
from typing import Dict, Optional

from canary_agent.core.base import BaseMarketOutput

class USShortOutput(BaseMarketOutput):
    data: Dict = Field(...)

class KRShortOutput(BaseMarketOutput):
    data: Dict = Field(...)
    type: str = Field(...)

    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
