import requests, time
KEY    = "PKIYGXQT3DGX7B6BDIZFZ6VQWU"
SECRET = "Ekaz5bQHUbbFUQvmidbBSU89wHMtgigik3TsyFD15NA3"
HEADERS = {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SECRET}

rows = [
    ("2026-06-08","BKSY"),("2026-06-08","SKYQ"),("2026-06-08","LFVN"),
    ("2026-06-08","AEHR"),("2026-06-08","CGEM"),("2026-06-08","MRLN"),
    ("2026-06-08","NNE"),("2026-06-08","SUNE"),("2026-06-08","ELAB"),
    ("2026-06-08","RVI"),("2026-06-09","SUNE"),("2026-06-09","DBI"),
    ("2026-06-09","BNKK"),("2026-06-09","CPSH"),("2026-06-09","RNAC"),
    ("2026-06-09","CING"),("2026-06-09","AMPG"),("2026-06-09","AEHR"),
    ("2026-06-09","BNAI"),("2026-06-09","FEED"),("2026-06-09","CECO"),
    ("2026-06-10","XOS"),("2026-06-10","CBRL"),("2026-06-10","AMSS"),
    ("2026-06-10","LAKE"),("2026-06-10","BATL"),("2026-06-10","BNKK"),
    ("2026-06-11","ELAB"),("2026-06-11","SEGG"),("2026-06-11","RKTO"),
    ("2026-06-11","DXYZ"),("2026-06-11","ASTC"),("2026-06-11","HCWB"),
    ("2026-06-11","ELVN"),("2026-06-11","AMPG"),("2026-06-11","DAIC"),
    ("2026-06-11","VELO"),("2026-06-12","RKTO"),("2026-06-12","FWRD"),
    ("2026-06-12","ARTV"),("2026-06-12","BKKT"),("2026-06-12","FJET"),
    ("2026-06-12","FABC"),("2026-06-12","DXYZ"),("2026-06-12","SUNE"),
    ("2026-06-12","DFNS"),
]

for date, ticker in rows:
    r = requests.get(f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
        headers=HEADERS,
        params={"timeframe":"1Day","start":date,"end":date,"feed":"iex","adjustment":"raw"},
        timeout=10)
    bars = r.json().get("bars", [])
    close = bars[0]["c"] if bars else "N/A"
    print(f"{date},{ticker},{close}")
    time.sleep(0.2)
