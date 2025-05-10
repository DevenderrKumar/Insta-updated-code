import json
from requests import Session
import requests
import pandas as pd
from lxml import html
import time
from pymongo import MongoClient
import random
import pymongo
import json
import logging
import requests
import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup as bs
import time

flag = True


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0',
 'Accept': '*/*',
 'Accept-Language': 'en-US,en;q=0.5',
 'X-CSRFToken': 'FiNRRKhMpxDBSdptzIwEJ1JAynmDsuW3',
 'X-IG-App-ID': '936619743392459',
 'X-ASBD-ID': '129477',
 'X-IG-WWW-Claim': 'hmac.AR3j75b5XiHq6RpE4jyiLHDXPGp4ZW8PyOjqOFCcZAYCJB4Q',
 'X-Requested-With': 'XMLHttpRequest',
 'Connection': 'keep-alive',
 'Referer': 'https://www.instagram.com/martateresaxsaltyvibes/',
 'Sec-Fetch-Dest': 'empty',
 'Sec-Fetch-Mode': 'cors',
 'Sec-Fetch-Site': 'same-origin',
 'Pragma': 'no-cache',
 'Cache-Control': 'no-cache'}

new_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'X-CSRFToken': 'loaErFG0dIbM2XErdvS2mFTjrDGq4kjp',
    'X-IG-App-ID': '936619743392459',
    'X-ASBD-ID': '198387',
    'X-IG-WWW-Claim': 'hmac.AR0YGf2APW88ep4_LaZc9LodVWbWV1V6bYHmwfV4LnQm8y-E',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Referer': 'https://www.instagram.com/p/Cn6dk7nyIQd/',
    'Cookie': 'ig_did=E8E2C977-E665-4EEB-B128-6D8D92A85228; datr=xKmkYzg_H2hB218oV81-7V7g; mid=Y6SpzwAEAAFEyKlch75tbOOP3puO; ig_nrcb=1; fbm_124024574287414=base_domain=.instagram.com; rur="PRN\\05456853077686\\0541706429153:01f7fbb72bc59a78fbf8dbf6310a2afdf6b30048cc73f412d5056875b826858419bcfeb5"; fbsr_124024574287414=lVuK0xMAbTWtEDwnIXk_8f8YxjW65rvjUpWaZgnAw2A.eyJ1c2VyX2lkIjoiMTAwMDEyNTAzMjY1MzcwIiwiY29kZSI6IkFRQU5URXhDdkJXUy1FczcyWjVBQm52U3JhVzlwUGpuNGYwVFFCMnEyeEhFRkN1aXBxZ3NhSjN4M3lLeTlxbGpNVnlVUjdDZE01TUczM3hINkFRSVdyZ0x5c0J0cjkzenppZ1UzRkhNZGEyZ21aYWpTTFU0S1VYTnU2azZ5NUVERUd1YjFIWEdxLUwzblR6NkdEcEI2LUpTZ3hrQ3dPQkdlS2JtUFJkWkRXWnBsbTBVVURyUFVqYTlESC1vSE85RFpEVzRTTmdEblJZMnM2dlNoNXMzYXRUbTdlZ1cta3JVd21YNWhlVWJrRlJDNEdWaXBhMEpzUld4SGhkZHVzZ0hRYUVjWEtZa21jQU0zbnB4YV90RHdVV095bHd4Z0hTakYzRjdjMDlvVGR1emtfd3N0V3NJSy05ZUJubk5mRXZvZGpHczF6d0dpZHhST3NlUHNsMHZkSF9aIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCQUxaQml2ekhUazZWOTdXWkNuc0xDcFdHWkNjTDdIb3ZaQ1RESXEyTkVaQ0loY0drb1pBQmNicVFRdzlKQnlPeEhIaXZFNDQwelFINmxLY0NqdjR2R3pER2txUEI2SDNrNGlUVXBUNEwwdDlRbE5GTlpDQXhCb29ZRG1icTBDWkJSY05INTBsR0JaQ2w3RDlQS1NUZjZpeUxRUm01d1k1TW5jYkJ4dVVmWkNaQnF0NWx1UXczTFhGOFRzWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTY3NDg5MzEwNn0; csrftoken=loaErFG0dIbM2XErdvS2mFTjrDGq4kjp; ds_user_id=56853077686; sessionid=56853077686%3ALrdaBEnLQwm1wT%3A7%3AAYcpRlthQHl1Nd4Edrblr64YH7zQlHaYamaUMPoBlg; shbid="18949\\05456853077686\\0541706300671:01f78a2489b9ef37fb2de3ea1e412b59e5640243cb4c64c86dd9cc0c41061e7e49ffd3dc"; shbts="1674764671\\05456853077686\\0541706300671:01f7bcc698bdaef8eb07491b743346f2912e3213781b80e78e58ab2aea3cc139c5fdecb5"; fbsr_124024574287414=E09UCz0EsLCV21IW-N8dLgKCjV18J08FZxtShHK1Ab4.eyJ1c2VyX2lkIjoiMTAwMDEyNTAzMjY1MzcwIiwiY29kZSI6IkFRQlhuZmh6QU5EZXNTaUZFSFpOcVZFYjFRaFE3ZjJkWmlPcm5NSlZnLURfQ29mVUI0OWFmRmtQQTdZVFRMalVQV2g2SE9TUEplNXQzR3dXcDk4TnpIYWxWQ2lDSktoNGdnbGlOQnhWRFp4dGxRbWhDakhmWENiVmVpNEFRNTgtZVV3M2JNc0FEeVM0M0Y4cnY2NHBMbjJneTdDUzhfTVVUcnRsb29fcmM1ekI1eHpzdkVNX1RyMUZiOE5rVEtWSGd5aXZWWjhuS291RUJ3YXVLb1N2bDBFLWY2NjZKM1EySjNHcXZFVTVBdnZQY3pPdmI0OWZRSVRPNnhEUlczLU0taDhaMi14YkU5ZmN3MFQ4Z0xzZWZNTGI3Vk9NYWpzd082UnZZZjJIekV6cmQwX19ZUVcxakFmQ2F6ekFvWDYwS2lURl9uVlhQYjU0eWxnaGV0YjNmLU4wUk9VZTMzR052TjZOcDdRUlhaT25BZyIsIm9hdXRoX3Rva2VuIjoiRUFBQnd6TGl4bmpZQkFFMlJWZUNIYnBKNkY2NVN3TXVzUnZRbHhQMDJRM2Fva0tnc1dQQzBmUUNWRTRHcHV5TWh5bGRUcTFBelIwblE1alNZTFpCcEdROWxsV2F5R1RWREhaQzJwZnc4OWpHTnFrT2VvSWFMa3dLNmpDcnpZbHZpQmtIVFFiQ1pCVG1WVnlPZEZQSnBFQlh2YjJxTlFNQm5UNWlEQ0hyNnhQWXJ4Q2l5bnlaQmRFR2s2d25NTW5jWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTY3NDg5MjU4NH0',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    # Requests doesn't support trailers
    # 'TE': 'trailers',
}


GotComments = list()




class GetAllSuggessions:

    def __init__(self):
        self.suggession_data = []
        self.base_url = "https://www.instagram.com/{}"
        self.headers = headers
        self.new_headers = new_headers
        self.NextParams = {'can_support_threading': 'true','min_id': '{"cached_comments_cursor": "17981506852884848", "bifilter_token": "KHkAgXZAYGzlPwAikDiHS3w_AGKcowkqxj8AxHAfbU3JPwBGnnieXBVBAOhTDfMlzz8A12b9tQeMPwALlfiPnMY_AG53PWgqhT8ArreKnSjVPwAx7KwrAsc_ANMnck3v_D8A9QkzLsnBQABXEnc3RpI_ALlE5FS-3z8AAA=="}'}
        self.post_url = "https://www.instagram.com/p/{}/"
        self.profile_url = "https://www.instagram.com/{}/"
        self.user_pass = [{'username':'devender85068202522023','password':'Dev@1234'}]
        self.next_comments_params = {'can_support_threading': 'true','min_id': '{"server_cursor": "QVFDWDZGTW1UeTZFX3hyM2dzSG8wcjN5dDlVYmVKZzZ0MnVyaWsyWGh5TkQ4WW5qSkI1a0dZVHN4OVN3Ry1jcTlpNlBIQXFnelJHUmhMRGRpU1Y5RXV1OFFhYjNHaXpZMHFVQTdVUTA2VnNoSEE=", "is_server_cursor_inverse": true}',}
        self.comments_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                # 'Accept-Encoding': 'gzip, deflate, br',
                'X-CSRFToken': 'FiNRRKhMpxDBSdptzIwEJ1JAynmDsuW3',
                'X-IG-App-ID': '936619743392459',
                'X-ASBD-ID': '129477',
                'X-IG-WWW-Claim': 'hmac.AR3j75b5XiHq6RpE4jyiLHDXPGp4ZW8PyOjqOFCcZAYCJBBI',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
                'Referer': 'https://www.instagram.com/p/CCIcO6khRE3/',
                # 'Cookie': 'ig_did=E8E2C977-E665-4EEB-B128-6D8D92A85228; datr=xKmkYzg_H2hB218oV81-7V7g; mid=Y6SpzwAEAAFEyKlch75tbOOP3puO; ig_nrcb=1; fbm_124024574287414=base_domain=.instagram.com; shbid="16489\\05414816613506\\0541718183157:01f78423e95aebfda20f2b631032b72e8937638fdc5a420ca4e7327b8da2a349fcd45866"; shbts="1686647157\\05414816613506\\0541718183157:01f796dbfe56bdc11597d892cba4fe5dddbd8c5b3f1a909eb815de68a7351699d21b2cec"; rur="EAG\\05459910281066\\0541718350938:01f769c9e30baf96c8e47658a6e0b3f8391b22be378b088a9116a6bcb77360a45cb2c689"; sessionid=59910281066%3ARTpXo4Qu7Uvvwo%3A17%3AAYcCNWGy7_6dFP_9FrpD3nI-gln8I8KZk-IzScX80Q; ds_user_id=59910281066; csrftoken=FiNRRKhMpxDBSdptzIwEJ1JAynmDsuW3',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                # Requests doesn't support trailers
                # 'TE': 'trailers',
            }
        

    def login_insta(self,username,password):
        try:
            login_url = "https://www.instagram.com/api/v1/web/accounts/login/ajax/"
            ck = requests.get(login_url)
            crsf = ck.cookies['csrftoken']
            headers = {
                'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0",
                'x-requested-with': 'XMLHttpRequest',
                'referer': 'https://www.instagram.com/accounts/login/',
                'sec-ch-ua-platform':'Linux',
                'x-csrftoken': crsf}
            time_ = int(datetime.now().timestamp())
            payload = {
                'username': username,
                'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{time_}:{password}',  # <-- note the '0' - that means we want to use plain passwords
                'queryParams': {},
                'optIntoOneTap': 'false'}
            # res = requests.post(login_url,data=payload,headers=headers,proxies=proxieslt)
            random_number = random.randrange(1,5) 
            time.sleep(random_number)
            res = requests.post(login_url,data=payload,headers=headers)

            print(res)

            print("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
            return res
        except Exception as e:
            logging.error(f"Error in login_insta function :- {e}")


    def GetCommentNextPage(self,cursor,profile_id,post_url,LogingCookies,userpass):
        global flag
        while flag:
            try:
                
                time.sleep(3)
                print("Fetching Data For :- {}".format(post_url))
                self.NextParams["min_id"] = cursor
                random_time = random.randrange(1,8)
                # print("Requesting for Next page function for Comments ....")
                time.sleep(random_time)
                headers["Referer"] = post_url
                response = requests.get(f'https://www.instagram.com/api/v1/media/{profile_id}/comments/', params=self.NextParams, cookies=LogingCookies,headers=self.comments_headers)
                if response.status_code == 401:
                    print("Got 401 response So Sleep for 1-5 secends range....")
                    time.sleep(random.randrange(1,5))
                    self.new_headers["Referer"] = post_url
                    response = requests.get(f'https://www.instagram.com/api/v1/media/{profile_id}/comments/', params=self.NextParams, cookies=LogingCookies,headers=self.new_headers)  
                         
                if ',"require_login":true,"status":"fail"}' in response.text and response.status_code != 200:
                    res = self.login_insta(userpass["username"],userpass["password"])
                    LogingCookies = res.cookies.get_dict()
                    self.GetCommentNextPage(self,cursor,profile_id,post_url,LogingCookies,userpass)
                

                js = response.json()
                comments = js.get("comments")
                next_cursor = js.get("next_min_id")
                print(post_url)
                print("Number of comments For GetCommentNextPage is :- {}".format(len(comments)))
                # if comments:
                for comment in comments:
                    item = dict()
                    item["profile_url"] = self.profile_url.format(js["caption"]["user"]["username"])
                    item["post_url"] = post_url
                    commit_text  = comment.get("text")
                    item["commit_text"] = commit_text if commit_text else ""
                    pk_id = comment.get("pk")
                    item["pk_id"] = pk_id if pk_id else ""
                    comment_count = js.get("comment_count")
                    item["comment_count"] = comment_count if comment_count else ""
                    user_id = comment.get("user_id")
                    item["user_id"] = user_id if user_id else ""
                    status = comment.get("status")
                    item["status"] = status if status else ""
                    username = comment["user"].get("username")
                    item["comment_Profile_url"] = self.profile_url.format(username) if username else ""
                    item["comment_by_username"] = username if username else ""
                    is_verified = comment["user"].get("is_verified")
                    item["is_verified"] = is_verified if is_verified else False
                    is_private = comment["user"].get("is_private")
                    item["is_private"] = is_private if is_private else False
                    comment_on_username = js["caption"]["user"].get("full_name")
                    item["comment_on_username"] = comment_on_username if comment_on_username else ""
                    item["Date"] = time.strftime("%Y-%d-%m")
                    item["Time"] = time.strftime("%H:%M:%S")
                    print("Getting data and inserting to list")
                    GotComments.append(item)
                
                # print("Fetching Data for Comments for Next Pages .....")
                if next_cursor:
                    print("Next Cursor :- {}".format(next_cursor))
                    cursor = next_cursor
                else:
                    flag = False
            except Exception as e:
                print("Error in GetCommentNextPage function :- {}".format(e))
                print("sleeping for 10")
                time.sleep(10)
                continue


    def GetCommentFirstPage(self,product_id,post_url,LogingCookies,userpass):
        global flag
        try:
            random_time = random.randrange(1,5)
            time.sleep(random_time)
            params = {'can_support_threading': 'true','permalink_enabled': 'false',}
            self.comments_headers["Referer"] = post_url
            response = requests.get(f'https://www.instagram.com/api/v1/media/{product_id}/comments/', params=params, headers=self.comments_headers,cookies=LogingCookies)
            js = response.json()
            comments = js.get("comments")
            next_cursor = js.get("next_min_id")
            # print(post_url)
            print("Number of comments is :- {}".format(len(comments)))
            for comment in comments:
                item = dict()
                item["post_url"] = post_url
                commit_text  = comment.get("text")
                item["commit_text"] = commit_text if commit_text else ""
                pk_id = comment.get("pk")
                item["pk_id"] = pk_id if pk_id else ""
                comment_count = js.get("comment_count")
                item["comment_count"] = comment_count if comment_count else ""
                user_id = comment.get("user_id")
                item["user_id"] = user_id if user_id else ""
                status = comment.get("status")
                item["status"] = status if status else ""
                username = comment["user"].get("username")
                item["comment_Profile_url"] = self.profile_url.format(username) if username else ""
                item["comment_by_username"] = username if username else ""
                is_verified = comment["user"].get("is_verified")
                item["is_verified"] = is_verified if is_verified else False
                is_private = comment["user"].get("is_private")
                item["is_private"] = is_private if is_private else False
                comment_on_username = js["caption"]["user"].get("full_name")
                item["comment_on_username"] = comment_on_username if comment_on_username else ""
                item["Date"] = time.strftime("%Y-%d-%m")
                item["Time"] = time.strftime("%H:%M:%S")
                # print(item)
                print("Getting data and inserting to list")
                GotComments.append(item)

            if next_cursor:
                # print("Next Cursor Sending to crawling_commits function :- {}".format(next_cursor))
                time.sleep(random.randint(1,10))
                flag = True
                self.GetCommentNextPage(next_cursor,product_id,post_url,LogingCookies,userpass)
            else:
                print("Not found the next page cursor")
        except Exception as e:
            print("Error in crawling_first_page_comments :- {}".format(e))


    def GetHomePage(self,url,LogingCookies):
        try:
            random_number = random.randrange(1,5) 
            time.sleep(random_number)
            response = requests.get(url, cookies=LogingCookies, headers=headers)
            return response
        except Exception as e:
            logging.error(f"Error in GetHomePage function :- {e}")


    def StartProcess(self,ProfileCode_):  
        try:  
            for userpass in self.user_pass:
                res = self.login_insta(userpass["username"],userpass["password"])
                LogingCookies = res.cookies.get_dict()
                for _len in range(len(ProfileCode_)):
                    PrData = _Prfofile_urls[_len]
                    self.GetCommentFirstPage(PrData["PostID"],PrData["PostUrl"],LogingCookies,userpass)
        except Exception as e:
            logging.error("Error in StartProcess Function :- {}".format(e))


# _Prfofile_urls = [{"jayfashion":"https://www.instagram.com/p/DIGt2GCMa5c/", "PostID":"3125904244220949543"}]

_Prfofile_urls = [{"ProfileCode":"jayfashion","PostUrl":"https://www.instagram.com/p/DIGt2GCMa5c/","PostID":"3604770181180665436"}]

# Creating Objects for Class and calling the function For starting the PROGRAM
# _Prfofile_urls = [{"ProfileCode":"cristiano","PostUrl":"https://www.instagram.com/p/CthcVkurPQn/","PostID":"3125904244220949543"},
#                   {"ProfileCode":"cristiano","PostUrl":"https://www.instagram.com/p/CtepPkUMFd3/","PostID":"3125116581134227319"},
#                   {"ProfileCode":"cristiano","PostUrl":"https://www.instagram.com/p/CtchXQQoLT5/","PostID":"3124518975029949689"},
#                   {"ProfileCode":"cristiano","PostUrl":"https://www.instagram.com/p/CtUCrQqIA9a/","PostID":"3122132210591731546"},
#                   {"ProfileCode":"cristiano","PostUrl":"https://www.instagram.com/p/CtPmEemLiI4/","PostID":"3120880500289184312"}]


obj = GetAllSuggessions()
obj.StartProcess(_Prfofile_urls)





print("Generating The Files for CommentsScrape ........ And Length of Products :- {}".format(len(GotComments)))

df = pd.DataFrame(GotComments)
df.to_excel("GotCommentsExcelCristiano_Posts.xlsx",index=False)

print("Completed!.....")
time.sleep(5)