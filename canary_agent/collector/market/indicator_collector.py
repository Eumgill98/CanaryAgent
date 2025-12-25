from __future__ import annotations

import pandas as pd
import pandas_ta as ta
from typing import List

from canary_agent.core.base import BaseCollector


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
        df: pd.DataFrame,
        sma: List[int] = [20, 60],
        ema: List[int] = [12, 26],
    ) -> pd.DataFrame:
        """
        Compute trend indicators such as SMA and EMA.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            sma (List[int]): Window sizes for Simple Moving Averages
            ema (List[int]): Window sizes for Exponential Moving Averages

        Returns:
            pd.DataFrame: DataFrame with trend indicators appended
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

        return out

    @staticmethod
    def momentum(
        df: pd.DataFrame,
        rsi: int = 14,
        macd: bool = True,
    ) -> pd.DataFrame:
        """
        Compute momentum indicators such as RSI and MACD.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            rsi (int): RSI lookback period
            macd (bool): Whether to compute MACD

        Returns:
            pd.DataFrame: DataFrame with momentum indicators appended
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

        return out

    @staticmethod
    def volatility(
        df: pd.DataFrame,
        bb_window: int = 20,
        bb_std: float = 2.0,
        atr: int = 14,
    ) -> pd.DataFrame:
        """
        Compute volatility indicators such as Bollinger Bands and ATR.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            bb_window (int): Bollinger Band window length
            bb_std (float): Standard deviation multiplier
            atr (int): ATR lookback period

        Returns:
            pd.DataFrame: DataFrame with volatility indicators appended
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

        return out

    @staticmethod
    def volume(
        df: pd.DataFrame,
        obv: bool = True,
        vma: int = 20,
    ) -> pd.DataFrame:
        """
        Compute volume-based indicators such as OBV and Volume Moving Average.

        Args:
            df (pd.DataFrame): OHLCV time-series data
            obv (bool): Whether to compute On-Balance Volume
            vma (int): Volume moving average window size

        Returns:
            pd.DataFrame: DataFrame with volume indicators appended
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

        return out
