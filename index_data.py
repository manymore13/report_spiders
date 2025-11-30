import requests
import os
import json

"""
指数数据抓取
"""

gz_url = 'https://danjuanfunds.com/djapi/index_eva/dj'

# 设置请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36",
    "host":"danjuanfunds.com",
    "Cache-Control":"no-cache"
}
response = requests.get(gz_url, headers = headers)
print(response.status_code)
if response.status_code != 200:
    print("exit")
    exit()
json_data = response.json()
# print(json_data)
index_info_list = json_data['data']['items']
data_list = []
for index_info in index_info_list:
    print(index_info)
    data_list.append({
        "code":index_info["index_code"],
        "name":index_info["name"],
        "ttype":index_info["ttype"],
        "pe":index_info["pe"],
        "pb":index_info["pb"],
        "pePercentile":index_info["pe_percentile"],
        "pbPercentile":index_info["pb_percentile"],
        "roe":index_info["roe"],
        "yeild":index_info["yeild"],
        "ts":index_info["ts"],
        "evaType":index_info["eva_type"],
        "evaTypeInt":index_info["eva_type_int"],
        "bondYeild":index_info["bond_yeild"],
        "beginAt":index_info["begin_at"],
        "createdAt":index_info["created_at"],
        "updatedAt":index_info["updated_at"],
        "pbFlag":index_info["pb_flag"],
        "pbOverHistory":index_info["pb_over_history"],
        "peOverHistory":index_info["pe_over_history"],
        "date":index_info["date"],
    })
response.close()
print(data_list)
json_path = './gen_report/'
if not os.path.exists(json_path):
    os.makedirs(json_path)
with open(f"{json_path}index_data.json", "w") as file:
    json.dump(data_list, file)







