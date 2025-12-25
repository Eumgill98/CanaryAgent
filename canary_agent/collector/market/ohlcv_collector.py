from __future__ import annotations

import yfinance as yf
import pandas as pd

from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, ConfigDict

from canary_agent.core.base import BaseCollector

class OHLCVOutput(BaseModel):
    ticker: str = Field(...)
    type: str = Field(...)
    data: pd.DataFrame = Field(...)

    # time setting
    start: Optional[str] = Field(default=None)
    end: Optional[str] = Field(default=None)
    interval: Optional[str] = Field(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict:
        d = self.model_dump(exclude={"data"})
        d["data"] = self.data.to_dict(orient="records")
        return d

class OHLCVCollector(BaseCollector):
    """A collector that returns Open, High, Low, and Close corresponding to the ticker"""
    def __init__(
        self, 
        ticker: str,
    ):
        self._ticker: str = ticker
        self._support_interval: List[str] = [
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
        ]

    @property
    def ticker(self) -> str:
        return self._ticker
    
    @property
    def support_interval(self) -> List[str]:
        return self._support_interval

    def latest(self, return_dict: bool = True) -> Union[OHLCVOutput, Dict]:
        """
        Methods for collecting the latest OHLCV data.

        Args:
            return_dict (bool): if return_dict return Dict (default=True)
        
        Returns:
            output (Dict): Latest OHLCV data.
        """
        df = yf.download(
            self.ticker,
            period="1mo",
            interval="1d",
            progress=False,
        )

        if df.empty:
            raise ValueError(f"No OHLCV data for ticker: {self._ticker}")

        latest_df = df.tail(1).reset_index()

        output = OHLCVOutput(
            ticker=self.ticker,
            type="latest",
            data=latest_df,
        )
        if return_dict:
            return output.to_dict()
        return output
    
    def between(
        self,
        start: str,
        end: str,
        interval: str = "1d",
        return_dict: bool = True,
    ) ->  Union[OHLCVOutput, Dict]:
        """
        Method to collect OHLCV data between start and end.
        
        Args:
            start (str): start time
            end (str): end time
            interval (str, optional): interval (defaults="1d")
            return_dict (bool): if return_dict return Dict (default=True)

        Returns:
            output (Dict): OHLCV data between start and end.
        """
        df = yf.download(
            self.ticker,
            start=start,
            end=end,
            interval=interval,
            progress=False,
        )

        if df.empty:
            raise ValueError(f"No OHLCV data for ticker: {self._ticker}")
        
        df = df.reset_index()

        output = OHLCVOutput(
            ticker=self.ticker,
            type="between",
            data=df,
            start=start,
            end=end,
            interval=interval,
        )
        if return_dict:
            return output.to_dict()
        return output