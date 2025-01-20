#############################################
# data/fetch_overseas_data.py
#############################################
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import time

# yaml 대신 settings.py 사용
from config.settings import (
    APP_KEY,
    APP_SECRET,
    URL_BASE,
    ACCESS_TOKEN,
    ACCESS_TOKEN_EXPIRED,
    HTS_ID
)


###################################
# 1. 접근토큰 발급 함수
###################################
def get_access_token():
    """
    OAuth 인증 > 접근토큰발급 (client_credentials 방식)
    - 발급받은 토큰과 만료 시간을 반환
    - settings.py의 global 변수를 갱신하거나, 필요하면 호출자가 받아서 쓸 수도 있음
    """
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"

    time.sleep(0.1)  # 유량제한 예방 (REST: 1초에 20건)
    res = requests.post(URL, headers=headers, data=json.dumps(body))

    if res.status_code == 200:
        try:
            data = res.json()
            access_token = data.get("access_token")
            access_token_expired = data.get("access_token_expired")
            print("Access Token 발급 성공:", access_token)
            return access_token, access_token_expired
        except KeyError as e:
            print(f"[오류] 토큰 발급 중 키 에러: {e}, 응답: {res.json()}")
            return None, None
    else:
        print("[오류] 접근 토큰 발급 불가. 코드:", res.status_code)
        print("응답 내용:", res.json())
        return None, None


###################################
# 2. 해외 주식 분봉 조회 API
###################################
def call_overseas_minute_api(
        exc_code,  # 거래소 코드 (NAS, NYS, AMS 등)
        sym_code,  # 심볼(종목코드, 예: TSLA)
        nmin,  # 분봉 주기 (1, 5, 10 등)
        keyb="",  # 다음 조회용 시간 키
        next_value="",  # 다음 페이지를 조회하기 위한 next값
        access_token=""
):
    """
    해외주식 분봉 조회 API 호출
    - nmin: 분봉 간격
    - keyb, next_value: 페이징(과거 데이터) 조회 시 필요한 파라미터
    """
    PATH = "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
    URL = f"{URL_BASE}/{PATH}"

    params = {
        "AUTH": "",
        "EXCD": exc_code,  # 거래소 코드 (예: NAS)
        "SYMB": sym_code,  # 종목 코드 (예: TSLA)
        "NMIN": nmin,  # 분봉 주기
        "PINC": "1",
        "NEXT": next_value,  # 페이징
        "NREC": "120",  # 조회 레코드 개수 (최대 120개)
        "FILL": "",
        "KEYB": keyb
    }

    headers = {
        'content-type': 'application/json',
        'authorization': f'Bearer {access_token}',
        'appkey': APP_KEY,
        'appsecret': APP_SECRET,
        'tr_id': 'HHDFS76950200',  # 해외주식 시세 분봉 조회 TR
        'custtype': 'P'
    }

    time.sleep(0.1)  # 유량제한
    response = requests.get(URL, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        # print("API 호출 성공:", data)
        return data
    else:
        print(f"[오류] API 호출 실패, 상태 코드: {response.status_code}")
        try:
            print("오류 메시지:", response.json())
        except json.JSONDecodeError:
            print("JSON 디코딩 실패. 응답:", response.text)
        return None


###################################
# 3. 다음 조회용 keyb(시간) 계산
###################################
def get_next_keyb(output2, nmin):
    """
    output2: API 응답의 분봉 리스트
    nmin: 분봉 주기 (1,5,10 등)
    - 가장 마지막 레코드의 시간(xhms)와 날짜(xymd)를 합쳐서 datetime 변환
    - nmin 분 전으로 이동한 시점을 keyb로 반환
    """
    last_record = output2[-1]
    last_time_str = last_record["xymd"] + last_record["xhms"]  # YYYYMMDDHHMMSS 형태
    last_time = datetime.strptime(last_time_str, "%Y%m%d%H%M%S")
    next_keyb_time = last_time - timedelta(minutes=nmin)
    return next_keyb_time.strftime("%Y%m%d%H%M%S")


###################################
# 4. DataFrame 변환
###################################
def convert_to_dataframe(data):
    """
    한국투자증권 해외주식 API 응답(JSON)을 pandas DataFrame으로 변환
    """
    if "output2" in data:
        df = pd.DataFrame(data["output2"])

        # 필요한 열만 추출
        df = df[['tymd', 'xhms', 'open', 'high', 'low', 'last', 'evol', 'eamt']]

        # 문자열->숫자로 변환
        for col in ['open', 'high', 'low', 'last', 'evol', 'eamt']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # datetime 처리
        df['datetime'] = pd.to_datetime(df['tymd'] + df['xhms'], format='%Y%m%d%H%M%S')

        df = df.sort_values(by='datetime').reset_index(drop=True)
        df.drop(columns=['tymd', 'xhms'], inplace=True)

        return df
    else:
        return pd.DataFrame()


###################################
# 5. 반복 조회 + CSV 저장
###################################
def fetch_and_save_data(exc_code, sym_code, nmin, period, access_token):
    """
    exc_code : 'NAS', 'NYS', 'AMS' 등 거래소 코드
    sym_code : TSLA, AAPL 등 종목 코드
    nmin     : 분봉 (1,5,10 ...)
    period   : 몇 번 반복 조회할 것인가 (1회 조회당 최대 120봉씩)
    access_token: 발급받은 토큰

    반환: 최종 취합된 DataFrame
    저장: sym_code_fetched_data.csv 파일로 저장
    """
    all_data = pd.DataFrame()

    # 1) 첫 조회
    first_call = call_overseas_minute_api(exc_code, sym_code, nmin, access_token=access_token)
    if not first_call:
        print("[오류] 첫 API 호출 실패")
        return pd.DataFrame()

    df_first = convert_to_dataframe(first_call)
    all_data = pd.concat([all_data, df_first], ignore_index=True)

    next_value = first_call["output1"]["next"]
    keyb = get_next_keyb(first_call["output2"], nmin)

    # 2) 두 번째 ~ period 번까지 반복
    for i in range(period - 1):
        next_call = call_overseas_minute_api(
            exc_code,
            sym_code,
            nmin,
            keyb=keyb,
            next_value=next_value,
            access_token=access_token
        )
        if not next_call:
            print("[오류] 다음 API 호출 실패, 중단합니다.")
            break

        df_next = convert_to_dataframe(next_call)
        all_data = pd.concat([all_data, df_next], ignore_index=True)

        next_value = next_call["output1"]["next"]
        keyb = get_next_keyb(next_call["output2"], nmin)

    # 시간순 정렬 + 중복 제거
    all_data = all_data.sort_values(by='datetime').reset_index(drop=True)
    all_data.drop_duplicates(subset='datetime', inplace=True)

    # CSV로 저장
    filename = f"{sym_code}_fetched_data.csv"
    all_data.to_csv(filename, index=False, encoding="utf-8")
    print(f"[완료] {sym_code} 분봉 데이터가 '{filename}'로 저장되었습니다.")

    return all_data
