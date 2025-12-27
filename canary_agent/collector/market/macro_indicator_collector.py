from fredapi import Fred
from typing import Tuple, Dict, Union, Optional, Any

import os

from canary_agent.core.base import BaseCollector
from canary_agent.collector.market.output import MacroIndicatorOutput

class USMacroIndicatorCollector(BaseCollector):
    """
    A Collector that returns US market macroeconomics inidicator.
    """
    INDICATORS = {
                "cpi": "CPIAUCSL",
                "core_cpi": "CPILFESL",
                "core_pce": "PCEPILFE",
                "gdp": "GDPC1",
                "industrial_production": "INDPRO",
                "unemployment_rate": "UNRATE",
                "nonfarm_payrolls": "PAYEMS",
                "federal_funds_rate": "FEDFUNDS",
                "10_year_bonds": "DGS10",
                "2_year_bonds": "DGS2",
                "vix": "VIXCLS",
                "nfci": "NFCI",
            }
    
    def __init__(self):
        self._check_key()
        self.fred = Fred(api_key=os.getenv("FRED_API_KEY"))

    def _check_key(self):
        """
        Check env key is valid. 
        """
        if os.getenv("FRED_API_KEY", None) is None:
            raise EnvironmentError("FRED_API_KEY is not set")
        return True
    
    @classmethod
    def keys(cls) -> Tuple[str, ...]:
        return tuple(cls.INDICATORS.keys())

    def can_search(self, key: str) -> bool:
        return key.lower() in self.keys()

    def latest(
        self,
        key: str = 'gdp',
        return_dict: bool = False
    ) -> Union[Dict[str, Any], MacroIndicatorOutput]:
        
        key = key.lower()
        if not self.can_search(key):
            raise ValueError(f"{key} is not valid type.")

        series_id = self.INDICATORS[key]

        df = (
        self.fred.get_series(series_id=series_id)
        .dropna()
        .to_frame(name=key)
        .reset_index()
        .rename(columns={"index": "date"})
        )
        
        out = df.iloc[-1]
        output = MacroIndicatorOutput(ticker=key, type='latest', data=out.to_dict())
        return output.to_dict() if return_dict else output
    
    def between(
        self,
        key: str = 'gdp',
        start: Optional[str] = None,
        end: Optional[str] = None,
        return_dict: bool = False,
    ) -> Union[Dict[str, Any], MacroIndicatorOutput]:

        key = key.lower()
        if not self.can_search(key):
            raise ValueError(f"{key} is not valid type.")
        
        series_id = self.INDICATORS[key]

        series = self.fred.get_series(
            series_id=series_id,
            observation_start=start,
            observation_end=end,
        )

        if series.empty:
            error_data = {"error": "empty"}
            return error_data if return_dict else MacroIndicatorOutput(self.ticker, type='between', data=error_data, start=start, end=end)

        df = (
            series
            .dropna()
            .to_frame(name=key)
            .reset_index()
            .rename(columns={"index": "date"})
        )
        output = MacroIndicatorOutput(ticker=key, data=df.to_dict(orient="records"), type='between', start=start, end=end)
        return output.to_dict() if return_dict else output
        

# class KRMacroIndicatorCollector(BaseCollector):
#     """
#     A Collector that returns KR market macroeconomics inidicator.
#     """
#     def __init__(self):
#         self._check_key()

#     def _check_key(self):
#         """
#         Check env key is valid.
#         """
#         if os.getenv("ECOS_API_KEY", None) is None:
#             raise EnvironmentError("ECOS_API_KEY is not set")
#         return True