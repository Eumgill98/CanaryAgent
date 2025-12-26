import pandas as pd
from pydantic import Field, field_serializer
from typing import Optional

from canary_agent.core.base import BaseMarketOutput

class IndicatorOutput(BaseMarketOutput):
    data: pd.DataFrame = Field(...)

    # time setting
    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
    interval: Optional[str] = Field(default=None)

    @field_serializer("data")
    def serialize_dataframe(self, df: pd.DataFrame):
        return df.to_dict(orient="records")