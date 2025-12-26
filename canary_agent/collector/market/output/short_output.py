
from pydantic import Field
from typing import Dict

from canary_agent.core.base import BaseMarketOutput

class USShortOutput(BaseMarketOutput):
    data: Dict = Field(...)
