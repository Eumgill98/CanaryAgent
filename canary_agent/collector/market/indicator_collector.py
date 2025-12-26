import pandas as pd
import pandas_ta as ta
from typing import Dict, List, Optional, Union

from canary_agent.core.base import BaseCollector
from canary_agent.collector.market.output import IndicatorOutput

class InsufficientDataError(ValueError):
    """Raised when there is not enough data to compute technical indicators."""
    pass


class IndicatorCollector(BaseCollector):
    """
    Collector responsible for computing derivative variables and
    technical indicators from OHLCV time-series data.
    """

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize OHLCV column names to lowercase format
        required by pandas-ta.

        Args:
            df (pd.DataFrame): Input OHLCV DataFrame

        Returns:
            pd.DataFrame: DataFrame with normalized column names
        """
        return df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })

    @staticmethod
    def _validate_min_rows(
        df: pd.DataFrame,
        required: int,
        indicator_name: str,
    ) -> None:
        """
        Validate that the DataFrame contains enough rows
        to compute a given indicator.

        Args:
            df (pd.DataFrame): Input DataFrame
            required (int): Minimum required number of rows
            indicator_name (str): Indicator name for error message

        Raises:
            InsufficientDataError: If df has fewer rows than required
        """
        if len(df) < required:
            raise InsufficientDataError(
                f"{indicator_name} requires at least {required} rows, "
                f"but got {len(df)}."
            )

    @staticmethod
    def trend(
        ticker: str,
        df: pd.DataFrame,
        sma: List[int] = [20, 60],
        ema: List[int] = [12, 26],
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: Optional[str] = None,
        return_dict: bool = True,
    ) -> Union[IndicatorOutput, Dict]:
        """
        Compute trend indicators such as SMA and EMA.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            sma (List[int]): Window sizes for Simple Moving Averages
            ema (List[int]): Window sizes for Exponential Moving Averages

        Returns:
            output (Dict, IndicatorOutput): trend indicators data.
        """
        out = IndicatorCollector._normalize_columns(df.copy())

        min_required = max(sma + ema)
        IndicatorCollector._validate_min_rows(
            out,
            min_required,
            "Trend indicators (SMA/EMA)",
        )

        for w in sma:
            out.ta.sma(length=w, append=True)

        for w in ema:
            out.ta.ema(length=w, append=True)

        output = IndicatorOutput(
            ticker=ticker,
            data=out,
            start=start,
            end=end,
            interval=interval,
        )

        if return_dict:
            return output.to_dict()
        return output

    @staticmethod
    def momentum(
        ticker: str,
        df: pd.DataFrame,
        rsi: int = 14,
        macd: bool = True,
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: Optional[str] = None,
        return_dict: bool = True,
    ) -> Union[Dict, IndicatorOutput]:
        """
        Compute momentum indicators such as RSI and MACD.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            rsi (int): RSI lookback period
            macd (bool): Whether to compute MACD

        Returns:
            output (Dict, IndicatorOutput): momentum indicators data.
        """
        out = IndicatorCollector._normalize_columns(df.copy())

        min_required = rsi + 1
        if macd:
            min_required = max(min_required, 26)

        IndicatorCollector._validate_min_rows(
            out,
            min_required,
            "Momentum indicators (RSI/MACD)",
        )

        out.ta.rsi(length=rsi, append=True)

        if macd:
            out.ta.macd(append=True)

        output = IndicatorOutput(
            ticker=ticker,
            data=out,
            start=start,
            end=end,
            interval=interval,
        )

        if return_dict:
            return output.to_dict()
        return output

    @staticmethod
    def volatility(
        ticker: str,
        df: pd.DataFrame,
        bb_window: int = 20,
        bb_std: float = 2.0,
        atr: int = 14,
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: Optional[str] = None,
        return_dict: bool = True,
    ) -> Union[Dict, IndicatorOutput]:
        """
        Compute volatility indicators such as Bollinger Bands and ATR.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            bb_window (int): Bollinger Band window length
            bb_std (float): Standard deviation multiplier
            atr (int): ATR lookback period

        Returns:
            output (Dict, IndicatorOutput): volatility indicators data.
        """
        out = IndicatorCollector._normalize_columns(df.copy())

        min_required = max(bb_window, atr)
        IndicatorCollector._validate_min_rows(
            out,
            min_required,
            "Volatility indicators (BBANDS/ATR)",
        )

        out.ta.bbands(length=bb_window, std=bb_std, append=True)
        out.ta.atr(length=atr, append=True)

        output = IndicatorOutput(
            ticker=ticker,
            data=out,
            start=start,
            end=end,
            interval=interval,
        )

        if return_dict:
            return output.to_dict()
        return output

    @staticmethod
    def volume(
        ticker: str,
        df: pd.DataFrame,
        obv: bool = True,
        vma: int = 20,
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: Optional[str] = None,
        return_dict: bool = True,
    ) -> Union[Dict, IndicatorOutput]:
        """
        Compute volume-based indicators such as OBV and Volume Moving Average.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            obv (bool): Whether to compute On-Balance Volume
            vma (int): Volume moving average window size

        Returns:
            output (Dict, IndicatorOutput): volume indicators data.
        """
        out = IndicatorCollector._normalize_columns(df.copy())

        IndicatorCollector._validate_min_rows(
            out,
            vma,
            "Volume indicators (OBV/VMA)",
        )

        if obv:
            out.ta.obv(append=True)

        out.ta.sma(close="volume", length=vma, append=True)

        output = IndicatorOutput(
            ticker=ticker,
            data=out,
            start=start,
            end=end,
            interval=interval,
        )

        if return_dict:
            return output.to_dict()
        return output
