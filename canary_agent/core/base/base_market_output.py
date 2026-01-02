from typing import Any, Dict
from pydantic import BaseModel, Field, ConfigDict

class BaseMarketOutput(BaseModel):
    ticker: str = Field(...)
    data: Any = Field(...)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict:
        return self.model_dump()