#########################################
# strategy/overseas_strategy.py
#########################################
import pandas as pd
import numpy as np


##########################
# (1) 지표 계산 함수들
##########################

def ema(series: pd.Series, window: int) -> pd.Series:
    """ Exponential Moving Average """
    return series.ewm(span=window, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    RSI (단순 버전)
    """
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0.0).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def momentum(series: pd.Series, period: int = 10) -> pd.Series:
    """
    Momentum(ROC) = ((현재가 - n분 전 가격) / n분 전 가격) * 100
    """
    shifted = series.shift(period)
    return (series - shifted) / shifted * 100


def bollinger_bands(series: pd.Series, window: int = 20, num_std: float = 2.0):
    """
    볼린저 밴드 (기본 20봉, 표준편차 * 2)
    """
    sma = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = sma + (num_std * std)
    lower = sma - (num_std * std)
    return sma, upper, lower


def macd(series: pd.Series, short=12, long=26, signal=9):
    """
    MACD = EMA(short) - EMA(long)
    Signal = EMA(MACD, signal)
    Hist = MACD - Signal
    """
    macd_line = ema(series, short) - ema(series, long)
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Average True Range (ATR)
    """
    df["prev_close"] = df["close"].shift(1)

    df["h_l"] = df["high"] - df["low"]
    df["h_pc"] = (df["high"] - df["prev_close"]).abs()
    df["l_pc"] = (df["low"] - df["prev_close"]).abs()

    df["TR"] = df[["h_l", "h_pc", "l_pc"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(window=period).mean()

    return df["ATR"]


##########################
# (2) 종합 지표 계산
##########################
def calculate_advanced_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    분봉 DataFrame에서 다음 컬럼을 추가:
      - EMA10, EMA20, EMA50
      - RSI14
      - MOM10 (Momentum)
      - BB_MID, BB_UPPER, BB_LOWER (볼린저 밴드)
      - MACD, MACD_SIGNAL, MACD_HIST
      - ATR14
    """
    df = df.copy()

    # 'last' -> 'close' (분봉에서는 'last'라는 컬럼을 종가로 사용)
    if "last" in df.columns and "close" not in df.columns:
        df.rename(columns={"last": "close"}, inplace=True)

    # 문자열→숫자 변환(혹시 타입이 object인 경우)
    for col in ["open", "high", "low", "close"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # EMA (10, 20, 50)
    df["EMA10"] = ema(df["close"], 10)
    df["EMA20"] = ema(df["close"], 20)
    df["EMA50"] = ema(df["close"], 50)

    # RSI(14)
    df["RSI14"] = rsi(df["close"], 14)

    # Momentum(10)
    df["MOM10"] = momentum(df["close"], 10)

    # 볼린저 밴드(20, 2)
    df["BB_MID"], df["BB_UPPER"], df["BB_LOWER"] = bollinger_bands(df["close"], 20, 2.0)

    # MACD(12, 26, 9)
    df["MACD"], df["MACD_SIGNAL"], df["MACD_HIST"] = macd(df["close"], 12, 26, 9)

    # ATR(14)
    df["ATR14"] = atr(df, 14)

    # 임시 컬럼 제거
    for tmp in ["prev_close", "h_l", "h_pc", "l_pc", "TR"]:
        if tmp in df.columns:
            df.drop(columns=[tmp], inplace=True)

    return df


##########################
# (3) 트레일링 스탑 & 자금 관리
##########################
def apply_trailing_stop(df: pd.DataFrame, atr_multiplier: float = 2.0):
    """
    ATR 기반의 트레일링 스탑
    """
    df = df.copy()
    df["trailing_stop"] = df["close"] - (df["ATR14"] * atr_multiplier)
    return df


def position_sizing(capital: float, current_price: float, atr_value: float, risk_per_trade: float = 0.01):
    """
    간단한 자금관리: 최대 손실(capital * risk_per_trade)을 기준으로 수량 계산
    - Stop Loss = current_price - (2 * ATR)
    """
    stop_loss = current_price - 2.0 * atr_value
    if stop_loss <= 0:
        return 1

    loss_per_share = current_price - stop_loss
    max_loss = capital * risk_per_trade
    qty = int(max_loss // loss_per_share)
    return max(qty, 1)


##########################
# (4) 매수/매도/보유 시그널 생성
##########################
def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    예시 규칙 (분봉 기반):
      - BUY: (EMA10 > EMA20) & (RSI14 < 70) & (MACD > MACD_SIGNAL) & (MOM10 > 0)
      - SELL: (EMA10 < EMA20) | (RSI14 > 70) | (MACD < MACD_SIGNAL) | (MOM10 < 0)
      - HOLD: 그 외
    """
    df = df.copy()
    df["signal"] = "HOLD"

    cond_buy = (
            (df["EMA10"] > df["EMA20"]) &
            (df["RSI14"] < 70) &
            (df["MACD"] > df["MACD_SIGNAL"]) &
            (df["MOM10"] > 0)
    )
    cond_sell = (
            (df["EMA10"] < df["EMA20"]) |
            (df["RSI14"] > 70) |
            (df["MACD"] < df["MACD_SIGNAL"]) |
            (df["MOM10"] < 0)
    )

    df.loc[cond_buy, "signal"] = "BUY"
    df.loc[cond_sell, "signal"] = "SELL"

    return df


##########################
# (5) 최신 시그널 가져오기
##########################
def get_latest_signal(df: pd.DataFrame) -> str:
    if df.empty:
        return "NO_DATA"
    return df.iloc[-1]["signal"]
