import requests
resp = requests.get(
    "https://www.alphavantage.co/query",
    params={"function":"TIME_SERIES_DAILY","symbol":"AAPL","apikey":"0O97VR30NBJXS780"}
)
print(resp.json())
