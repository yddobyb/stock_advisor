project/
├── config/
│   └── settings.py           # my info 
├── data/
│   └── fetch_overseas_data.py  # 해외주식 분봉 조회 로직
├── strategy/ 
│   └── fetch_overseas_data.py      
├── backtest/
│   └── backtester.py
└── main_overseas.py           # 해외주식 분봉 데이터 받아오는 메인 스크립트


for me - 

swing + ema50, rsi14 = nmin - 15, period -7 (5주-6주)
                        nmin - 30, period - 7 (3달)