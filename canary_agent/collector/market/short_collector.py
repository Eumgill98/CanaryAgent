import yfinance as yf

from typing import Union, Dict

from canary_agent.core.base import BaseCollector
from canary_agent.collector.market.output import USShortOutput

class USShortCollector(BaseCollector):
    def __init__(
        self,
        ticker: str,
    ):
        self._ticker: str = ticker

    @property
    def ticker(self) -> Union[Dict, USShortOutput]:
        return self._ticker
    
    def latest(
        self,
        return_dict: bool = True
    ) -> str:
        """
        Method to collect recently short selling information
        """

        info = yf.Ticker(self.ticker).info

        output = USShortOutput(
            ticker=self.ticker,
            data=info,
        )   
        
        if return_dict:
            return output.to_dict()
        return output
