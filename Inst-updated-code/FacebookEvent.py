from requests import Session
from lxml import html
import re
from bs4 import BeautifulSoup as bs
import json
import pandas as pd
import requests


data = {
    'av': '100012503265370',
    '__user': '100012503265370',
    '__a': '1',
    '__req': '2x',
    '__hs': '19507.HYP:comet_pkg.2.1..2.1',
    'dpr': '1',
    '__ccg': 'MODERATE',
    '__rev': '1007579915',
    '__s': 'ffilqp:1p38t9:3o892l',
    '__hsi': '7239042716392825596',
    '__dyn': '7AzHJ16U9ob8Cdg5K8G6EjBWo2nDwAxu13wFG1sgS3q2ibwNw9G2Saxa1NwJwpUe8hwaG0Z82_Cy8kwMwfm1-x278bbwto886C11xmfz81s8hwGxu782lwj8bU9kbxS2218wc61axe3S7Udo5qfK0zEkxe2GewGwkUtxGm2SUbElxm3y11xfxmu3W3y261eBx_wHwoE2mBwJCwLyES0Io5d08O321LwTwNxe6Uak1xwJwxyo6O',
    '__csr': 'gH69TbsWk-y9h4LeN2bnBTqtlJYLqkBYJkWlWYZlA9AH_9jsgIzfLBAXi9hqQQHqXJaXKPAhaKFlhRHnLLGu9J28xpaKaGvDAGmut5KKiUC6rhbxmajzFESGgjKbDCWXWAg-FeiFk2O9wFzedCy8yqmE-XyA68rxS1az8swYwFzEd89K1YwqU8Egwj8G1vwhQ0GEnweO4E8oG2a1rwTwau1vwopXw08AO00L6815U0niw4sg1GU3Xw56w8q05po08sE088U2yDQ0oLw4xw6UQ019pw3Zo0Di05vA1bU2Aw13Hw',
    '__comet_req': '15',
    'fb_dtsg': 'NAcOkPFm1M1m9Wp9UVaMMgD9N7vst0H22HRxXNHwuSlDGm1Aj7i0ZHQ:33:1684574714',
    'jazoest': '25239',
    'lsd': 'gg_qurR4kIk1YlR-aKdAM9',
    '__spin_r': '1007579915',
    '__spin_b': 'trunk',
    '__spin_t': '1685470975',
    'fb_api_caller_class': 'RelayModern',
    'fb_api_req_friendly_name': 'EventCometHomeDiscoverContentRefetchQuery',
    'variables': '{"count":9,"cursor":"eyJzdGFydF90aW1lIjoxNjg1NDcwOTc1LCJlbmRfdGltZSI6MTY5MzI0Njk3NSwiYnJvd3NlX2N1cnNvciI6IkFicFljX2xhWTV1b2pmbHRvZDhBeTJheGlpaHN4TDVQNEJ3NnlUbjd2Yks3eVR2dldwZUp2SFBvWnViRmdRVmRQQ2I3VWNwN3hUWk9Rc2UyRlZDcE9sY0VvZUhEU3ZOTU5vNm9LOWlJR3BBWGwyU04wZ05ZYzh2UVNZNTJCWlVycFhxWWk4Y3hrUXZ5VWFFMnluelVqTEptNEticm5fRlBHYzZseXp5NkpKbXkwLVBscGQ4MVlDeHI5cG1mc1RjNEFxZ1NIdW9kdlRPMXJNQTh5LUtEdThuME9PMWRWckNUSGozYkZlRHZFR2JVZ3RnaXhwUDIwSUZEUFZPbDQ4ZXNUOFJVbjZyWUtuZ1NTSy0tSnMzTVFsdllMaGNaTDFjekN1S1p1NFIzNWs1RHpwRWhoclVTWndHSVRudG9NRnEycjJDdTVjYUpPbE5tVWpzclVzaVdodEJ3RVQxZlJCblFBdjE0V1RaWEhhdkVpWXhnUlFxOUxndnlfRkcwZGthLUQ3S0piaFd0elBmdTN6SXFPYVlLRDdWSExoZlNSdUdEcFVvdHdJRXlXa3d4d3B2Q0ExX0FxdDhRb0ZzNFRWa2dNQUFEUy1NUk02aUhJNF8yaEJ0OGoxeWlWWE1hNlhfbXQyUlg1aG9XckRkc09RMzlXM1ZIS3VZUjFEbEtqaU8xeG5pT3JVaVZIWm5pampRWjZBVnFRRGd5X2U0dXZHN182eGZhMFN6VHNPamYxT2J0ak84TEZ6X080WWlDbC1aTUtsdTdfaHpsNUZXVklOeFZHc2o5UXRzaCJ9","end_timestamp":null,"event_flags":null,"location_id":"708405265952462","scale":1,"start_timestamp":null,"tab_type":"CUSTOM","id":"100012503265370"}',
    'server_timestamps': 'true',
    'doc_id': '6013802245406290',
}



cookies = {
    'datr': 'bpFoZN1-TJle5WB7h3fcHup0',
    'sb': 'kJFoZBvnloSq9LhrSyrHULfu',
    'c_user': '100012503265370',
    'xs': '33%3ATftwscn-xJVg4A%3A2%3A1684574714%3A-1%3A-1%3A%3AAcWzahFbLdzGjzSpcf42WVaOlyvdb-8C2AgwAhNwfw',
    'fr': '02KC0tTBz2VIdYRHC.AWX5pAFUSD08Q_kzKsUAmV_X1qQ.Bka7wZ.Ji.AAA.0.0.Bka7wZ.AWVkyjtbWJs',
    'presence': 'C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1684782196578%2C%22v%22%3A1%7D',
    'wd': '1225x270',
}

headers = {
    'authority': 'www.facebook.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en-GB;q=0.9,en;q=0.8,en-US;q=0.7',
    'cache-control': 'max-age=0',
    # 'cookie': 'datr=bpFoZN1-TJle5WB7h3fcHup0; sb=kJFoZBvnloSq9LhrSyrHULfu; c_user=100012503265370; xs=33%3ATftwscn-xJVg4A%3A2%3A1684574714%3A-1%3A-1%3A%3AAcWzahFbLdzGjzSpcf42WVaOlyvdb-8C2AgwAhNwfw; fr=02KC0tTBz2VIdYRHC.AWX5pAFUSD08Q_kzKsUAmV_X1qQ.Bka7wZ.Ji.AAA.0.0.Bka7wZ.AWVkyjtbWJs; presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1684782196578%2C%22v%22%3A1%7D; wd=1225x270',
    'sec-ch-prefers-color-scheme': 'dark',
    'sec-ch-ua': '"Microsoft Edge";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
    'sec-ch-ua-full-version-list': '"Microsoft Edge";v="113.0.1774.50", "Chromium";v="113.0.5672.127", "Not-A.Brand";v="24.0.0.0"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-ch-ua-platform-version': '"10.15.7"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.50',
}


key = "Classics"
HomeUrl = "https://www.facebook.com/events/search/?q={}".format(key)
response = requests.get(HomeUrl,headers=headers,cookies=cookies)
print(response.status_code)
# doc_url = re.findall(',"U2JuSrt":{"type":"js","src":"(.*?)",',response.text)[0].replace("\\","")
soup = bs(response.text,features="lxml")
# print("Data :- ",soup.find("link",{"as":"script"}))
# print("doc_url :- {}".format(doc_url))
doc_url = soup.find("script",{"data-bootloader-hash":"3cPU9WG"}).get("src")
# print(soup.find("script",{"data-bootloader-hash":"3cPU9WG"})).get("src")
# print(soup.find("script",{"data-bootloader-hash":"3cPU9WG"})).get("src")


d_res = requests.get(doc_url,headers=headers,cookies=cookies)
document_id = re.findall('\{e\.exports\="(.*?)"\}\)',d_res.text)

for doc in document_id:
    if "6084829694968465" == doc:
        print(doc)



# Finding the doc Id for events by locations. re.findall('\("EventCometHomeDiscoverContentRefetchQuery\_facebookRelayOperation"\,\[\]\,\(function\(a\,b\,c\,d\,e\,f\)\{e.exports\="(.*?)"\}\)\,',response.text)


"https://static.xx.fbcdn.net/rsrc.php/v3iEXu4/ym/l/makehaste_jhash/8ELS2mkU6LMHoOzm8-tO1SdVReyIPyLJkO35Cma2-nTuf3QiwYkFZ5EPiZ3qNVF061GK2R9nGWTT6BSvBVODhdzsjD6HzgssX4wbSieOePi63FkrYi80ymnskxihChwsIllVGL2sIz49CxQN12c0NX2SZBWtYKFANsgHuJ41h6kySuh_TuaLFj0-ZKnw8W-QpgstU_YLWyk33FyfqA8jdRtVpLP0TcfC2pX71KaEye8AHeBgTTJvjUkWIWSgpKSKdNWtah_f0Nfu80ub-jl1Q_jsPaQTn1.js?_nc_x=n3_TecVTLai"
"https://static.xx.fbcdn.net/rsrc.php/v3i8B24/ym/l/makehaste_jhash/OmJ_mLWZRCqrtxJqRIai6W88d-rIZNeH4ZlCVJZE8gztqwqf85EvcnBO1cLZtlERg4vS_lZXgB2V9_AW8AIPh0qVkvVDQNjfAGF2zeMZWRkZtIxTesf-1BNdl.js?_nc_x=n3_TecVTLai"




"6092853250763987"
"6092853250763987"
"6092853250763987"
"6092853250763987"
"6092853250763987"
"6092853250763987"
"6092853250763987"
"6088717934484513"




"6013802245406290"


"""
Given an expression string x. Examine whether the pairs and the orders of {,},(,),[,] are correct in exp.
For example, the function should return 'true' for exp = [()]{}{[()()]()} and 'false' for exp = [(]).

Note: The drive code prints "balanced" if function return true, otherwise it prints "not balanced".

"""