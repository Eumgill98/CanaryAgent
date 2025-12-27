from pydantic import Field
from typing import List, Dict, Any, Optional, Union

from canary_agent.core.base import BaseMarketOutput

class MacroIndicatorOutput(BaseMarketOutput):
    data: Union[Dict[str, Any], List[Dict[str, Any]]]= Field(...)
    type: str = Field(...)

    # time setting
    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)