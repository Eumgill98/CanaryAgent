from pydantic import Field
from typing import Optional, Dict, List, Any

from canary_agent.core.base import BaseMarketOutput

class TechIndicatorOutput(BaseMarketOutput):
    data: List[Dict[str, Any]] = Field(...)

    # time setting
    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
    interval: Optional[str] = Field(default=None)

