import requests
import json


result = requests.request("GET", "https://ftx.com/api/markets")

data = result.json()


outData = []

file = open("symbols.json", "w")

usdMarket = {}

for entry in data["result"]:
    if ("/USD" in entry["name"]):
        usdMarket[entry["name"]] = entry["name"]

for entry in data["result"]:
    if ("PERP" in entry["name"]):
        usdName = entry["name"].replace("-PERP", "/USD")
        if (usdName in usdMarket):
            print(entry["name"], usdMarket[usdName])
            symbol = {}
            symbol["spot"] = usdName
            symbol["perp"] = entry["name"]
            outData.append(symbol)

json.dump(outData,file)
