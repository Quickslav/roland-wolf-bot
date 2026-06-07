import requests

KEY    = "PKIYGXQT3DGX7B6BDIZFZ6VQWU"
SECRET = "Ekaz5bQHUbbFUQvmidbBSU89wHMtgigik3TsyFD15NA3"

HEADERS = {
    "APCA-API-KEY-ID":     KEY,
    "APCA-API-SECRET-KEY": SECRET,
}

ROWS = [

    # June 3
    ["2026-06-03","XOS"],["2026-06-03","LASE"],["2026-06-03","PMI"],["2026-06-03","SVCO"],
    ["2026-06-03","SINT"],["2026-06-03","NEOV"],["2026-06-03","LFVN"],["2026-06-03","CNTB"],
    ["2026-06-03","BRUN"],["2026-06-03","BNKK"],["2026-06-03","APVO"],["2026-06-03","AIRJ"],
    ["2026-06-03","SOAR"],["2026-06-03","SDOT"],
    # June 4
    ["2026-06-04","BNKK"],["2026-06-04","GENK"],["2026-06-04","TWAV"],["2026-06-04","SBEV"],
    ["2026-06-04","ONFO"],["2026-06-04","TLYS"],["2026-06-04","MOBX"],["2026-06-04","ROLR"],
    ["2026-06-04","LGIH"],["2026-06-04","CAL"],
    # June 5
    ["2026-06-05","MRLN"],["2026-06-05","RMSG"],["2026-06-05","BKSY"],["2026-06-05","MCRB"],
    ["2026-06-05","MNTS"],["2026-06-05","STI"],["2026-06-05","DEVS"],
]

tickers = list(set(r[1] for r in ROWS))

resp = requests.get(
    "https://data.alpaca.markets/v2/stocks/bars",
    headers=HEADERS,
    params={
        "symbols":    ",".join(tickers),
        "timeframe":  "1Day",
        "start":      "2026-05-08",
        "end":        "2026-06-06",
        "limit":      10000,
        "feed":       "iex",
        "adjustment": "raw",
    }
)

bars = resp.json().get("bars", {})
data = {}
for ticker, bar_list in bars.items():
    for bar in bar_list:
        key = ticker + "|" + bar["t"][:10]
        data[key] = {"h": bar["h"], "l": bar["l"], "c": bar["c"]}

print("date,ticker,high,low,close")
for date, ticker in ROWS:
    key = ticker + "|" + date
    if key in data:
        d = data[key]
        print(f"{date},{ticker},{d['h']},{d['l']},{d['c']}")
    else:
        print(f"{date},{ticker},N/A,N/A,N/A")
