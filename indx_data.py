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
        "index_code":index_info["index_code"],
        "index_name":index_info["name"],
        "ttype":index_info["ttype"],
        "pe":index_info["pe"],
        "pb":index_info["pb"],
        "pe_percentile":index_info["pe_percentile"],
        "pb_percentile":index_info["pb_percentile"],
        "roe":index_info["roe"],
        "yeild":index_info["yeild"],
        "ts":index_info["ts"],
        "eva_type":index_info["eva_type"],
        "eva_type_int":index_info["eva_type_int"],
        "bond_yeild":index_info["bond_yeild"],
        "begin_at":index_info["begin_at"],
        "created_at":index_info["created_at"],
        "updated_at":index_info["updated_at"],
        "pb_flag":index_info["pb_flag"],
        "pb_over_history":index_info["pb_over_history"],
        "pe_over_history":index_info["pe_over_history"],
        "date":index_info["date"],
    })
response.close()
json_data = json.dumps(data_list)
print(json_data)
with open("indx_data.json", "w") as file:
    json.dump(json_data, file)



