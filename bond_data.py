import os

import requests
import pandas as pd
import json


def get_cn_us_10y():
    url = "http://datacenter.eastmoney.com/api/data/get"
    params = {
        "type": "RPTA_WEB_TREASURYYIELD",
        "sty": "ALL",
        "st": "SOLAR_DATE",
        "sr": "-1",
        "token": "894050c76af8597a853f5b408b759f5d",
        "p": 1,
        "ps": 5,
    }
    r = requests.get(url, params=params, timeout=15)
    print(f"bond data response status code: {r.status_code}")
    if r.status_code != 200:
        print("get_cn_us_10y exit")
        exit()
    data = r.json()["result"]["data"]
    df = pd.DataFrame(data)

    df = df.rename(columns={
        "SOLAR_DATE": "date",
        "EMM00166466": "cn_10y",
        "EMG00001310": "us_10y",
    })
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["cn_10y"] = pd.to_numeric(df["cn_10y"], errors="coerce")
    df["us_10y"] = pd.to_numeric(df["us_10y"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)
    df["cn_us_10y_spread"] = df["cn_10y"] - df["us_10y"]
    return df[["date", "cn_10y", "us_10y", "cn_us_10y_spread"]]


def get_latest_yield_data():
    df = get_cn_us_10y()

    # 获取最新一行数据
    latest_data = df.iloc[-1]

    # 转换为字典对象
    result = {
        "date": latest_data["date"].isoformat(),
        "cn_10y": float(latest_data["cn_10y"]),
        "us_10y": float(latest_data["us_10y"]),
        "cn_us_10y_spread": float(latest_data["cn_us_10y_spread"])
    }

    return result


# 获取最新数据
latest_data = get_latest_yield_data()

# 转换为JSON格式
json_data = json.dumps(latest_data, indent=2, ensure_ascii=False)

print("最新中美十年期国债收益率数据（JSON对象）：")
print(json_data)
json_path = './gen_report/'
if not os.path.exists(json_path):
    os.makedirs(json_path)
with open(f"{json_path}bond_data.json", "w") as file:
    json.dump(json_data, file)


