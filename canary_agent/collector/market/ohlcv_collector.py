import yfinance as yf

from typing import Dict, List, Union, Any

from canary_agent.core.base import BaseCollector
from canary_agent.collector.market.output import OHLCVOutput

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

    def latest(
        self, 
        return_dict: bool = False
    ) -> Union[Dict[str, Any], OHLCVOutput]:
        """
        Methods for collecting the latest OHLCV data.

        Args:
            return_dict (bool): if return_dict return Dict (default=True)
        
        Returns:
            output (Dict, OHLCVOutput): Latest OHLCV data.
        """
        df = yf.download(
            self.ticker,
            period="1mo",
            interval="1d",
            progress=False,
        )

        if df.empty:
            raise ValueError(f"No OHLCV data for ticker: {self._ticker}")

        df.index.name = "date"
        latest_df = df.tail(1).reset_index()
        out = df.iloc[-1]

        output = OHLCVOutput(
            ticker=self.ticker,
            type="latest",
            data=out.to_dict(),
        )

        return output.to_dict() if return_dict else output
    
    def between(
        self,
        start: str,
        end: str,
        interval: str = "1d",
        return_dict: bool = False,
    ) ->  Union[Dict[str, Any], OHLCVOutput]:
        """
        Method to collect OHLCV data between start and end.
        
        Args:
            start (str): start time
            end (str): end time
            interval (str, optional): interval (defaults="1d")
            return_dict (bool): if return_dict return Dict (default=True)

        Returns:
            output (Dict, OHLCVOutput): OHLCV data between start and end.
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
        
        df.index.name = "date"
        df = df.reset_index()

        output = OHLCVOutput(
            ticker=self.ticker,
            type="between",
            data=df.to_dict(orient="records"),
            start=start,
            end=end,
            interval=interval,
        )
        
        return output.to_dict() if return_dict else output