#############################################
# main_overseas.py
#############################################
import sys
import pandas as pd

from data.fetch_overseas_data import (
    get_access_token,
    fetch_and_save_data
)

from strategy.overseas_strategy import (
    calculate_advanced_indicators,
    apply_trailing_stop,
    generate_signals,
    get_latest_signal,
    position_sizing
)

def main():
    print("===== 해외주식 분봉 데이터 & 스윙(단기) 전략 =====")

    # 1) 토큰 발급
    access_token, _ = get_access_token()
    if not access_token:
        print("[오류] 토큰 발급 실패. 종료.")
        sys.exit(1)

    # 2) 사용자 입력
    exc_code = input("거래소 코드 (예: NAS): ")
    sym_code = input("종목 코드 (예: TSLA): ")
    nmin = int(input("분봉 주기를 입력 (예: 5): "))
    period = int(input("반복 조회할 횟수 (예: 4): "))

    # 3) 데이터 수집 & CSV 저장
    df = fetch_and_save_data(exc_code, sym_code, nmin, period, access_token)
    if df.empty:
        print("[주의] 조회 결과가 없습니다. 종료.")
        return

    print(f"[확인] 조회된 데이터: {len(df)} rows")

    # 4) 지표 계산
    df_ind = calculate_advanced_indicators(df)

    # 5) 트레일링 스탑
    df_trail = apply_trailing_stop(df_ind, atr_multiplier=2.0)

    # 6) 매수/매도/보유 시그널 생성
    df_signal = generate_signals(df_trail)

    # 7) 최신 시그널 확인
    latest_sig = get_latest_signal(df_signal)
    latest_time = df_signal.iloc[-1]["datetime"]
    print(f"[{latest_time}] 최신 시그널: {latest_sig}")

    # 8) 자금관리 예시
    if latest_sig == "BUY":
        capital = 10_000_000  # 천만원 예시 (일단 그냥 천만원으로 해둠)
        current_price = df_signal.iloc[-1]["close"]
        atr_val = df_signal.iloc[-1]["ATR14"]
        qty = position_sizing(capital, current_price, atr_val, risk_per_trade=0.01)
        print(f"[BUY 시그널] 추천 매수 수량: {qty} 주 (현재가: {current_price:.2f}, ATR14: {atr_val:.2f})")

    # 9) 결과 저장
    output_csv = f"{sym_code}_{nmin}min_signals.csv"
    df_signal.to_csv(output_csv, index=False)
    print(f"[저장] {output_csv} 파일로 저장했습니다.")


if __name__ == "__main__":
    main()
