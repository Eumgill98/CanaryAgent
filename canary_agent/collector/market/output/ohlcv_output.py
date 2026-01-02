from pydantic import Field
from typing import Optional, Dict, List, Any, Union

from canary_agent.core.base import BaseMarketOutput

class OHLCVOutput(BaseMarketOutput):
    data: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(...)
    type: str = Field(...)

    # time setting
    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
    interval: Optional[str] = Field(default=None)
