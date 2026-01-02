from pydantic import Field
from typing import Dict, Optional, Union, Any, List

from canary_agent.core.base import BaseMarketOutput

class USShortOutput(BaseMarketOutput):
    data: Dict = Field(...)

class KRShortOutput(BaseMarketOutput):
    data: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(...)
    type: str = Field(...)

    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
