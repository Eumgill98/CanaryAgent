import yfinance as yf
from pykrx import stock

from typing import Union, Dict

from canary_agent.core.base import BaseCollector
from canary_agent.collector.market.output import USShortOutput, KRShortOutput

class USShortCollector(BaseCollector):
    """A collector that returns Shot info of US market."""
    def __init__(
        self,
        ticker: str,
    ):
        self._ticker: str = ticker

    @property
    def ticker(self) -> str:
        return self._ticker
    
    def latest(
        self,
        return_dict: bool = True
    ) -> Union[Dict, USShortOutput]:
        """
        Method to collect recently US stock ticker short selling information
        """

        info = yf.Ticker(self.ticker).info
        data = {
            'sharesShort': info.get('sharesShort', 'N/A'),
            'shortRatio': info.get('shortRatio', 'N/A'), 
            'shortPercentOfFloat': info.get('shortPercentOfFloat', 'N/A'),
            'dateShortInterest': info.get('dateShortInterest', 'N/A'),
            'sharesPercentSharesOut': info.get('sharesPercentSharesOut', 'N/A'),
            'sharesShortPriorMonth': info.get('sharesShortPriorMonth', 'N/A'),
        }

        output = USShortOutput(
            ticker=self.ticker,
            data=data,
        )   
        
        return output.to_dict() if return_dict else output

class KRShortCollector(BaseCollector):
    """A collector that returns Shot info of KR makret."""
    def __init__(
        self,
        ticker: str,
    ):
        self._ticker = ticker

    @property
    def ticker(self) -> str:
        return self._ticker
    
    def latest(
        self,
        return_dict: bool = True,
    ) -> Union[Dict, KRShortOutput]:
        """
        Method to collect recently Korea short ticker selling information
        """
        try:
            today = stock.get_nearest_business_day_in_a_week()

            bal_df = stock.get_shorting_balance_by_date(
                fromdate=today,
                todate=today,
                ticker=self.ticker,
            )

            if bal_df.empty:
                error_data = {"error": "empty"}
                return error_data if return_dict else KRShortOutput(self.ticker, data=error_data, type='latest')

            vol_df = stock.get_shorting_volume_by_ticker(today)

            short_volume = (
                vol_df.loc[self.ticker, "공매도거래량"]
                if self.ticker in vol_df.index
                else None
            )

            row = bal_df.iloc[0]

            data = {
                "sharesShort": int(row["공매도잔고"]),
                "shortRatio": float(row["비중"]),
                "shortPercentOfFloat": float(row["비중"]),
                "sharesPercentSharesOut": float(row["비중"]),
                "date": today,
                "sharesShortPriorMonth": None,
                "shortVolume": short_volume,
                "marketCap": int(row["시가총액"]),
            }

            output = KRShortOutput(
                ticker=self.ticker,
                type="latest",
                data=data,
            )

            return output.to_dict() if return_dict else output

        except Exception as e:
            error_data = {"error": str(e)}
            return error_data if return_dict else KRShortOutput(self.ticker, data=error_data, type='latest')
        
    def between(
        self,
        start: str,
        end: str,
        return_dict: bool = True,
    ) -> Union[Dict, KRShortOutput]:
        """
        Method to collect Korea short ticker selling information between.
        """
        bal_df = stock.get_shorting_balance_by_date(start, end, self.ticker)
        vol_df = stock.get_shorting_volume_by_date(start, end, self.ticker)

        if bal_df.empty:
            data = {}
        else:
            bal_df = (
                bal_df[["공매도잔고", "비중", "시가총액"]]
                .rename(
                    columns={
                        "공매도잔고": "sharesShort",
                        "비중": "shortRatio",
                        "시가총액": "marketCap",
                    }
                )
            )

            vol_df = (
                vol_df[["공매도거래량"]]
                .rename(columns={"공매도거래량": "shortVolume"})
            )

            df = bal_df.join(vol_df, how="left")

            df["shortPercentOfFloat"] = df["shortRatio"]
            df["sharesPercentSharesOut"] = df["shortRatio"]
            df["sharesShortPriorMonth"] = None
            df["date"] = df.index

            data = df.to_dict(orient="records")

        output = KRShortOutput(
            ticker=self.ticker,
            type='between',
            start=start,
            end=end,
            data=data,
        )

        return output.to_dict() if return_dict else output