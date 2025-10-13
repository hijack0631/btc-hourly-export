python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python get_btcusdt_hourly.py --months 10 --tz "Europe/Zurich" --out btcusdt_1h_10months.csv