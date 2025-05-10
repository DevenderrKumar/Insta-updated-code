import requests
import pandas as pd

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:108.0) Gecko/20100101 Firefox/108.0',
 'Accept': 'application/json, text/plain, */*',
 'Accept-Language': 'en-US,en;q=0.5',
 'Ocp-Apim-Subscription-Key': 'e914eec9448c4d5eb672debf5011cf8f',
 'Connection': 'keep-alive',
 'Referer': 'https://www.albertsons.com/shop/search-results.html?q=Canadian',
 'Sec-Fetch-Dest': 'empty',
 'Sec-Fetch-Mode': 'cors',
 'Sec-Fetch-Site': 'same-origin'}

cookies = {'visid_incap_1980972': 'IP3+z/OKSRi6neBfGmcqC7GfxWMAAAAAQUIPAAAAAAAjMgq4ns6vVuN/CkE2QBmx',
 'nlbi_1980972': 'jzfJXu6jfCszMjTX3I3p5AAAAADfukiL0ChT068pywKWxpx6',
 'incap_ses_714_1980972': 'lz7uUZiOmTQjZOnGcaPoCbOfxWMAAAAAg9j+xQAOMHyhlDfQqR90QA==',
 'nlbi_1980972_2147483392': 'et3fGUx1eh+7XhMp3I3p5AAAAAD35erwHeDLfVjJ3N+nBOxb',
 'AMCV_A7BF3BC75245ADF20A490D4D%40AdobeOrg': '-1124106680%7CMCIDTS%7C19374%7CMCMID%7C00832931250624930321701436136087941393%7CMCAAMLH-1674500661%7C12%7CMCAAMB-1674500661%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1673903061s%7CNONE%7CMCSYNCSOP%7C411-19381%7CvVersion%7C5.2.0',
 'mbox': 'session#3595b8e47dcd4a97b2440de608eaba0f#1673898921|PC#3595b8e47dcd4a97b2440de608eaba0f.31_0#1737140831',
 'AMCVS_A7BF3BC75245ADF20A490D4D%40AdobeOrg': '1',
 'ECommBanner': 'albertsons',
 'abs_gsession': '%7B%22info%22%3A%7B%22COMMON%22%3A%7B%22Selection%22%3A%22user%22%2C%22preference%22%3A%22J4U%22%2C%22userType%22%3A%22G%22%2C%22zipcode%22%3A%2292677%22%2C%22banner%22%3A%22albertsons%22%7D%2C%22J4U%22%3A%7B%22zipcode%22%3A%2292677%22%2C%22storeId%22%3A%222556%22%7D%2C%22SHOP%22%3A%7B%22zipcode%22%3A%2292677%22%2C%22storeId%22%3A%222556%22%7D%7D%7D',
 'SWY_SHARED_SESSION_INFO': '%7B%22info%22%3A%7B%22COMMON%22%3A%7B%22userType%22%3A%22G%22%2C%22zipcode%22%3A%2292677%22%2C%22banner%22%3A%22albertsons%22%2C%22preference%22%3A%22J4U%22%2C%22Selection%22%3A%22user%22%2C%22userData%22%3A%7B%7D%7D%2C%22J4U%22%3A%7B%22storeId%22%3A%222556%22%2C%22zipcode%22%3A%2292677%22%2C%22userData%22%3A%7B%7D%7D%2C%22SHOP%22%3A%7B%22storeId%22%3A%222556%22%2C%22zipcode%22%3A%2292677%22%2C%22userData%22%3A%7B%7D%7D%7D%7D',
 'abs_previouslogin': '%7B%22info%22%3A%7B%22COMMON%22%3A%7B%22Selection%22%3A%22user%22%2C%22preference%22%3A%22J4U%22%2C%22userType%22%3A%22G%22%2C%22zipcode%22%3A%2292677%22%2C%22banner%22%3A%22albertsons%22%7D%2C%22J4U%22%3A%7B%22zipcode%22%3A%2292677%22%2C%22storeId%22%3A%222556%22%7D%2C%22SHOP%22%3A%7B%22zipcode%22%3A%2292677%22%2C%22storeId%22%3A%222556%22%7D%7D%7D',
 'SWY_SYND_USER_INFO': '%7B%22storeAddress%22%3A%22%22%2C%22storeZip%22%3A%2292677%22%2C%22storeId%22%3A%222556%22%2C%22preference%22%3A%22J4U%22%7D',
 'ECommSignInCount': '0',
 'at_check': 'true',
 'OptanonConsent': 'groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1%2CC0003%3A1&datestamp=Tue+Jan+17+2023+00%3A37%3A14+GMT%2B0530+(India+Standard+Time)&version=202211.2.0&hosts=&landingPath=NotLandingPage&isGpcEnabled=0&geolocation=%3B&isIABGlobal=false&AwaitingReconsent=false',
 'SAFEWAY_MODAL_LINK': '',
 's_cc': 'true',
 's_nr30': '1673896035235-New',
 's_vncm': '1675189799598%26vn%3D1',
 's_ivc': 'true',
 'gpv_Page': 'albertsons%3Adelivery%3Asearch-results',
 'reese84': '3:GqjpQtVFtrACSByqtUooyQ==:G9TYeDzJWVRlccWiH0WjUlrAG5scYiWwt9YlQX5sj+yg0YnbYCor683yPdKX6rzUsMJmXnrBpP5wV9A0O0/CgJkAWk0hVJ9PzNWhyDi94yNM1cou/wzA6uBgMIE9I0v4m1Kzl/sf5TKWP6wIi+sMcj8YIJXqdGxkKilaJZFiZaV8TTpb1iF18i1O46kcfNCbM/Wnyeb7XgTOAm69GrHXkFExLzWoVz4q1C6hKjEKqt3j5D/n7hzQBSQYJSeAxt91vcVCo/RM1kKYEQfplpd6zlJMoQhY2deiPHKwiJvoh2IRgizBP7JaMuLEhPcazOqVI1HH17Oq0zqF304EcDW3J4Bvz9FVlcBWbayqYTZrvN/h73AmqLQxQL8C/XdL4G6J/VEfeqajKySkc2cyicE1ZGqyqT6Od2jM8d43OvwamLTE7wch7dViVhJ/XSSziAH8Yeyw6y5RFH7E+HmjfIq8cl3z574t8zGQtuVq4P9RPfM=:YYCEdHBRjZJVovLkQ7Q1n4iNttvGdUdU33pAswyom10=',
 'aam_uuid': '08295781443472777722105465175877532597',
 '_gcl_au': '1.1.839664307.1673895868',
 '_pin_unauth': 'dWlkPU9UZ3dNRFEzWmpjdFpEWXpZeTAwTURoa0xXSmtNbUV0WmpObVkyWXdabVJtWW1ZNA',
 '_ga_XPZG6WVXDP': 'GS1.1.1673895868.1.1.1673897059.0.0.0',
 '_ga': 'GA1.1.360734999.1673895869',
 '_br_uid_2': 'uid%3D2189555772150%3Av%3D12.0%3Ats%3D1673895868843%3Ahc%3D2',
 '_fbp': 'fb.1.1673895870528.1171199181',
 '__gads': 'ID=fb8df5fb188ab86e:T=1673895870:S=ALNI_MY9N7NZhaoU5-hRuBlJnQTj5y23kA',
 '__gpi': 'UID=00000ba5f6d67610:T=1673895870:RT=1673895870:S=ALNI_MZsNofREUY2D0DQC5EHTsIxNf7fjQ',
 'OptanonAlertBoxClosed': '2023-01-16T19:04:37.528Z',
 's_sq': '%5B%5BB%5D%5D',
 '_uetsid': '98ec2d9095d011eda6851907df2d1b2d',
 '_uetvid': '98ec363095d011edb468a7beed33dd66',
 '__gsas': 'ID=45abfef5f1671373:T=1673896044:S=ALNI_MbblbQUzpKpoe_uHOyldBQ4B_Kppw'}

base_url = "https://www.albertsons.com/shop/product-details.{}.html"
data = []

def crawling_list(url):
    response = requests.get(url,headers=headers,cookies=cookies)
    json_data = response.json()
    products = json_data["response"].get("docs")
    prod_rank = 1
    for product in products:
        item = dict()
        product_url = product.get("pid")
        item["product_url"] = base_url.format(product_url) if product_url else ""
        item["Product Rank"] = prod_rank
        prod_rank+=1
        print(item)
        data.append(item)


crawling_list("https://www.albertsons.com/abs/pub/xapi/search/products?request-id=7751673897062173568&url=https://www.albertsons.com&pageurl=https://www.albertsons.com&pagename=search&rows=30&start=0&search-type=keyword&storeid=2556&featured=true&search-uid=uid%3D2189555772150%3Av%3D12.0%3Ats%3D1673895868843%3Ahc%3D2&q=Canadian&sort=&userid=&featuredsessionid=&screenwidth=1280,443&dvid=web-4.1search&channel=instore&banner=albertsons")
df = pd.DataFrame(data)
df.to_excel("albertsons_list_page.xlsx",index=False)

