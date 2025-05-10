# !/usr/bin/python3.5
# -*- coding: utf-8 -*-
import time
import logging
import pymongo,datetime
from datetime import datetime
from datetime import timedelta
from collections import Counter

import argparse
import re,os,json,requests
import pymongo,datetime
from google.cloud import storage
import hashlib
import redis,sys,os
import dateutil.parser as dp

import cv2
import numpy as np
from warnings import filterwarnings
import random
import pdb
from datetime import datetime
from multiprocessing import Process
from fake_headers import Headers


comments_headers = {
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

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0',
 'Accept': '*/*',
 'Accept-Language': 'en-US,en;q=0.5',
 'Content-Type': 'application/x-www-form-urlencoded',
 'X-FB-Friendly-Name': 'PolarisPostCommentsPaginationQuery',
 'X-CSRFToken': 'Va85aED8CMoupgfWEViUiPZRPNowB8CO',
 'X-IG-App-ID': '936619743392459',
 'X-FB-LSD': '8b225Ry5ilHMod-7wxZbey',
 'X-ASBD-ID': '129477',
 'Origin': 'https://www.instagram.com',
 'Connection': 'keep-alive',
 'Referer': 'https://www.instagram.com/p/CCIcO6khRE3/',
 'Sec-Fetch-Dest': 'empty',
 'Sec-Fetch-Mode': 'cors',
 'Sec-Fetch-Site': 'same-origin',
 'Pragma': 'no-cache',
 'Cache-Control': 'no-cache'}


# pranavchikra incorrect password
# amirkhn91227 incorrect password


flag = True


flag = True

class InstagramScraper(object):
    def __init__(self, **kwargs):
        self.count_ = 0
        self.proxieslt =proxieslt = {"http":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321'),"https":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321')}
        self.MultipleUsers = [{'username':'pranavchikra','password':'Ajeet@123'},
                            {'username':'amirkhn91227','password':'Ajeet@123'},
                            {'username':'sayisid176','password':'Ajeet@123'},
                            {'username':'smritinayir','password':'Ajeet@123'},
                            {'username':'waldiyababita','password':'Ajeet@123'},
                            {'username':'wopajos544','password':'Ajeet@123'},
                            {'username':'shreadharry','password':'Ajeet@123'},
                            {'username':'deshwalsamira','password':'Ajeet@123'},
                            {"username":"modata1398","password":"Ajeet@123"},
                            {"username":"migado5632","password":"Ajeet@123"},
                            {"username":"lebih34093","password":"Ajeet@123"},
                            {"username":"momam43895","password":"Ajeet@123"},
                            {"username":"tikamo3778","password":"Ajeet@123"},
                            {"username":"cibana8004","password":"Ajeet@123"},
                            {'username':'ajeetkumar2oiiop','password':'Ajeet@10'}]

        # self.UserInfo = {"username":"ajeetkumar2oiiop","password":"Ajeet@10"}
        # self.MultipleUsers = cred = [{"username":"modata1398","password":"Ajeet@123"},{"username":"migado5632","password":"Ajeet@123"},{"username":"lebih34093","password":"Ajeet@123"},{"username":"momam43895","password":"Ajeet@123"},{"username":"tikamo3778","password":"Ajeet@123"},{"username":"cibana8004","password":"Ajeet@123"}]
        self.NextParams = {'can_support_threading': 'true','min_id': '{"cached_comments_cursor": "17981506852884848", "bifilter_token": "KHkAgXZAYGzlPwAikDiHS3w_AGKcowkqxj8AxHAfbU3JPwBGnnieXBVBAOhTDfMlzz8A12b9tQeMPwALlfiPnMY_AG53PWgqhT8ArreKnSjVPwAx7KwrAsc_ANMnck3v_D8A9QkzLsnBQABXEnc3RpI_ALlE5FS-3z8AAA=="}'}
        self.AllCommentsData = list()
        self.AllLikesData_ = list()
        self.CommentsHeaders = comments_headers
        self.flag = True
        self.GetLikes__ = "https://www.instagram.com/api/v1/media/{}/likers/"
        self.GetComments_ = "https://www.instagram.com/api/v1/media/{}/comments/"
        default_attr = dict(username='',multiimageId=[],idType=0,csv_id='',profiletype='', country=[],category=[]
            ,fstr=0,fend=10,skp=0,limit=0,gender='',profileview='',hasttagView='',mydb='',nanocat=[],ProfileFe=[],include_location=False,iamgeName='',userId='',reelscrape=0,PSID='',scrapePlive=0,postprofile=0,reelprofile=0)
        allowed_attr = list(default_attr.keys())
        default_attr.update(kwargs)

        for key in default_attr:
            if key in allowed_attr:
                self.__dict__[key] = default_attr.get(key)



    def GetNextComments(self,cursor,PostID,post_url,LogingCookies,product_id):

        MyclientConnect = self.ConnectLocalDB()

        mydbGet = MyclientConnect["influncer"]
        PostIdsStoreGet = mydbGet["UserIdsLikes"]

        mydb = MyclientConnect["influncer"]
        collection = mydb["InstaLikesTable"]

        header = Headers(browser="firfox",os="mac",headers=True)
        RandomHeader = header.generate()
        self.CommentsHeaders.update({'User-Agent':RandomHeader["User-Agent"]})
        self.CommentsHeaders["Referer"] = "https://www.instagram.com/p/{}/".format(PostID)


        while self.flag:
            try:
                self.NextParams["min_id"] = cursor
                random_time = random.randrange(1,5)
                time.sleep(random_time)
                self.CommentsHeaders["X-CSRFToken"] = LogingCookies["csrftoken"]

                # Without proxy
                response_without_proxy = requests.get(f'https://www.instagram.com/api/v1/media/{PostID}/comments/', params=self.NextParams, cookies=LogingCookies,headers=self.CommentsHeaders)
                if response_without_proxy.status_code == 200:
                    response = response_without_proxy
                elif response_without_proxy.status_code != 200:
                    # With Proxy
                    response = requests.get(f'https://www.instagram.com/api/v1/media/{PostID}/comments/', params=self.NextParams, cookies=LogingCookies,headers=self.CommentsHeaders,proxies=self.proxieslt)


                if response.status_code != 200:
                    try:
                        RandomNumber = random.randint(0,self.user_pass.__len__()-1)
                        user_name = self.user_pass[RandomNumber]["username"]
                        pass_word = self.user_pass[RandomNumber]["password"]
                    except Exception as e:
                        print("Error in RandomNumber and username/password :- {}".format(e))

                    header = Headers(browser="firfox",os="mac",headers=True)
                    RandomHeader = header.generate()
                    self.CommentsHeaders.update({'User-Agent':RandomHeader["User-Agent"]})
                    self.CommentsHeaders["Referer"] = "https://www.instagram.com/p/{}/".format(PostID)
                    res = self.login_insta(user_name,pass_word)
                    LogingCookies = res.cookies.get_dict()

                js = response.json()
                comments = js.get("comments")
                next_cursor = js.get("next_min_id")
                for comment in comments:
                    pk_id = comment.get("pk")
                    if not collection.find_one({"pk_id":"{}".format(pk_id)}) and pk_id:
                        item = dict()
                        item["UserId"] = product_id["UserId"]
                        item["PostId"] = product_id["PostId"]
                        item["pk_id"] = comment["user_id"]
                        item["PostIdUserID"] = "{}_{}".format(product_id["UserId"],item["pk_id"])
                        PostIdsStoreGet.update_one({"PostId":product_id["PostId"]},{"$set":{"CommentStatus":"1"}})
                        try:
                            item["username"] = comment.get("user").get("username") if comment.get("user") else ""
                        except Exception as e:
                            print("Error in username :- {}".format(e))
                        item["Date"] = time.strftime("%Y%d%m")
                        item["Time"] = time.strftime("%H%M%S")
                        # print(item)
                        collection.insert_one(item)
                        # print(item)
                        print("Next Page comments Inserting to DB......")
                    else:
                        PostIdsStoreGet.update_one({"PostId":product_id["PostId"]},{"$set":{"CommentStatus":"1"}})
                        print(f"For Comments Function :- This id :- {pk_id} is Already Scrap and Already has in DB PL check...")

                if next_cursor:
                    print("Next Cursor :- {}".format(next_cursor))
                    cursor = next_cursor
                else:
                    self.flag = False
            except Exception as e:
                print("Error in crawling_commits function :- {}".format(e))
                print("sleeping for 10")
                time.sleep(10)
                continue



    def GetComments(self,ProductIDS=[],LogingCookies=""):

        MyclientConnect = self.ConnectLocalDB()

        mydbGet = MyclientConnect["influncer"]
        PostIdsStoreGet = mydbGet["UserIdsLikes"]

        mydb = MyclientConnect["influncer"]
        collection = mydb["InstaLikesTable"]

        try:
            self.CommentsHeaders["X-CSRFToken"] = LogingCookies["csrftoken"]
            for product_id in ProductIDS:
                if ProductIDS.get("UserId"):
                    params = {'can_support_threading': 'true','permalink_enabled': 'false'}
                    header = Headers(browser="firfox",os="mac",headers=True)
                    RandomHeader = header.generate()
                    self.CommentsHeaders.update({'User-Agent':RandomHeader["User-Agent"]})
                    self.CommentsHeaders["Referer"] = "https://www.instagram.com/p/{}/".format(product_id["PostId"])
                    try:
                        # Without proxy
                        response = requests.post(self.GetComments_.format(product_id["PostId"]), cookies=LogingCookies, headers=self.CommentsHeaders, data=params)
                    except:
                        # With Proxy
                        response = requests.post(self.GetComments_.format(product_id["PostId"]), cookies=LogingCookies, headers=self.CommentsHeaders, data=params, proxies=self.proxieslt)
                    
                    if response.status_code != 200:
                        try:
                            RandomNumber = random.randint(0,self.MultipleUsers.__len__()-1)
                            user_name = self.MultipleUsers[RandomNumber]["username"]
                            pass_word = self.MultipleUsers[RandomNumber]["password"]
                        except Exception as e:
                            print("Error in RandomNumber and username/password :- {}".format(e))
                        
                        print(f"Got {response.status_code} Resposne So waiting for some seconds and requests again... ")
                        print("for GetComments_ Username :- {} \nPassword :- {}".format(user_name,pass_word))
                        res = self.login_insta(user_name,pass_word)
                        LogingCookies = res.cookies.get_dict()
                        random_time = random.randrange(1,5)
                        params = {'can_support_threading': 'true','permalink_enabled': 'false'}
                        self.headers["Referer"] = post_url
                        self.headers["X-CSRFToken"] = LogingCookies["csrftoken"]
                        response = requests.post(f'https://www.instagram.com/api/v1/media/{product_id}/comments/', cookies=LogingCookies, headers=self.headers, data=params,proxies=self.proxieslt)

                    js = response.json()
                    comments = js.get("comments")
                    next_cursor = js.get("next_min_id")
                    if comments:
                        for comment in comments:
                            try:
                                pk_id = comment.get("user_id")
                                if not collection.find_one({"pk_id":"{}".format(pk_id)}) and pk_id:
                                    item = dict()

                                    item["UserId"] = product_id["UserId"]
                                    try:
                                        item["PostId"] = product_id["PostId"]
                                    except Exception as e:
                                        print("Error in PostId :- {}".format(e))

                                    try:
                                        item["pk_id"] = comment.get("user_id")
                                    except Exception as e:
                                        print("Error in pk_id :- {}".format(e))

                                    try:
                                        item["PostIdUserID"] = "{}_{}".format(product_id["UserId"],item["pk_id"])
                                    except Exception as e:
                                        print("Error in PostIdUserID :- {}".format(e))

                                    PostIdsStoreGet.update_one({"PostId":product_id["PostId"]},{"$set":{"CommentStatus":"1"}})
                                    try:
                                        item["username"] = comment.get("user").get("username") if comment.get("user") else ""
                                    except Exception as e:
                                        print("Error in username :- {}".format(e))
                                    item["Date"] = time.strftime("%Y%d%m")
                                    item["Time"] = time.strftime("%H%M%S")
                                    collection.insert_one(item)
                                else:
                                    PostIdsStoreGet.update_one({"PostId":product_id["PostId"]},{"$set":{"CommentStatus":"1"}})
                                    print(f"For Comments Function :- This id :- {pk_id} is Already Scrap and Already has in DB PL check...")
                            except:
                                continue

                    if next_cursor:
                        self.flag = True
                        print("Next Cursor Sending to crawling_first_page_comments function :- {}".format(next_cursor))
                        self.GetNextComments(next_cursor,product_id["PostId"],self.CommentsHeaders["Referer"],LogingCookies,product_id)
                    else:
                        print("Not found the next page cursor")

                    time.sleep(3)
                    UserId_ = product_id["UserId"]
                    PostId_ = product_id["PostId"]
                    print(f"UserID :- {UserId_} and PostID :- {PostId_} It is been completed and store in DB PL check....")

        except Exception as e:
            print("Error in crawling_first_page_comments function :- {}".format(e))
            pass




                    
    def useAdd(self,data,hstype=0):
       
        add_info = ['insta_image_down','user_id','biography','hastags','followers_count','following_count','full_name','is_business_account',
        'is_joined_recently','is_private','posts_count' ,
        'etc1','profile_pic_url','etc2','profile_pic_url_hd','insta_user_name','has_channel','external_url',
        'external_url_linkshimmed','business_category_name','overall_category_name','update','is_verified','searchkey']
        add_infohistory = ['userid','followers_count','following_count','posts_count','insta_user_name','full_name','is_verified']
        userDict = {}
        for mv in add_info:
            if mv in data:
                userDict.update({mv : data[mv]})
            else:
                mstcount = ''
                if mv=='followers_count':
                    mstcount = 0
                if mv=='searchkey':
                    er =1
                else:
                    userDict.update({mv : mstcount})
                    
        url =''
        no = email = ''
       
        userDict.update({"gender":0})
        if 'biography' in data:
            url = self.Find(data['biography'])
            no = self.Findmobile(data['biography'])
            email = self.Findemail(data['biography'])
        f3 = round(time.time())    

        userDict.update({"email":email})
        userDict.update({"phone":no})
        userDict.update({"insta_check":0})
        userDict.update({"contact":no})
        userDict.update({"insta_category":''})
        userDict.update({"insta_category_check":0})
        userDict.update({"insta_category_hastag":''})
        userDict.update({"insta_eng":''})
        userDict.update({"insta_engchk":f3})
        userDict.update({"addurl":url})
        userDict.update({"dateadd":f3})

        #History start
        add_infohistory = ['user_id','followers_count','following_count','posts_count','insta_user_name','is_verified']
        userDictsht = {}
        if hstype==1:
            for mvh in add_infohistory:
                if mvh in data:
                    userDictsht.update({mvh : data[mvh]})
                else:
                    userDictsht.update({mvh : ''})
            userDictsht.update({"dateadd":f3})
        userId = data['user_id']

        return userDict,userDictsht,userId
    def allstrepalce(self,lkt):
        lkt = lkt.replace(".",'1w1d')
        lkt = lkt.replace("@", '2w1d')
        lkt = lkt.replace("+", '3w1d')
        lkt = lkt.replace("-", '4w1d')
        lkt = lkt.replace("!", '51w1d')
        lkt = lkt.replace("#", '6w1d')
        lkt = lkt.replace("%", '7w1d')
        lkt = lkt.replace("*", '8w1d')
        lkt = lkt.replace("&", '9w1d')
        lkt = lkt.replace(",", '10w1d')
        lkt = lkt.replace("_", '11w1d')
        lkt = lkt.replace("/", '12w1d')
        lkt = lkt.replace(":", '13w1d')
        lkt = lkt.replace(";", '14w1d')
        lkt = re.sub('\W+',' ',lkt)
        lkt = lkt.replace('1w1d', ".")
        lkt = lkt.replace('2w1d', "@")
        lkt = lkt.replace('3w1d',"+")
        lkt = lkt.replace( '4w1d', "-")
        lkt = lkt.replace('51w1d', "!")
        lkt = lkt.replace('6w1d', "#")
        lkt = lkt.replace('7w1d', "%")
        lkt = lkt.replace('8w1d', "*")
        lkt = lkt.replace('9w1d', "&")
        lkt = lkt.replace('10w1d', ",")
        lkt = lkt.replace('11w1d', "_")
        lkt = lkt.replace('12w1d', "/")
        lkt = lkt.replace('13w1d', ":")
        lkt = lkt.replace('14w1d', ";")
        return lkt

    def getBucketProfile(self, item):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/var/live/gtranslateapi-d9895ca275db.json"
        bucket_name = "liveprofiledata"
        storage_client = storage.Client()
        BUCKET = storage_client.bucket(bucket_name)
        try:
            filename = 'live_'+str(item)+'_live.json'
            #print(filename)
            blob = BUCKET.blob(filename)
            if blob:
                data = json.loads(blob.download_as_string(client=None))
        except Exception as er :
            opi=1
            data={}
        return data
               

    def BucketUserProfile(self, item,json_object):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/var/live/gtranslateapi-d9895ca275db.json"
        bucket_name = "liveprofiledata"
        storage_client = storage.Client()
        filename = "live_"+str(item)+'_live.json'
        ##print(filename)
        BUCKET = storage_client.bucket(bucket_name)
        blob = BUCKET.blob(filename)

            
        #print("----")    
        #print("&&&&&&&&&&&&&&&7---------------------------------------------------")
        # upload the blob 
        blob.upload_from_string(
        data=json.dumps(json_object),
        content_type='application/json'
        )
        #print("ddddddddddddddddddddddddddddddddddddddddddddddfdfd")
        result = filename + ' upload complete'
        print(result)
        return {'response' : result}


    def BucketUserLikes(self, item,json_object):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/var/live/gtranslateapi-d9895ca275db.json"
        bucket_name = "liveprofiledata"
        storage_client = storage.Client()
        filename = "live_"+str(item)+'_live.json'
        ##print(filename)
        BUCKET = storage_client.bucket(bucket_name)
        blob = BUCKET.blob(filename)

            
        #print("----")    
        #print("&&&&&&&&&&&&&&&7---------------------------------------------------")
        # upload the blob 
        blob.upload_from_string(
        data=json.dumps(json_object),
        content_type='application/json'
        )
        #print("ddddddddddddddddddddddddddddddddddddddddddddddfdfd")
        result = filename + ' upload complete'
        print(result)
        return {'response' : result}



    def extract_tags(self, item):
        """Extracts the hashtags from the caption text."""
        caption_text = ''
        if 'caption' in item and item['caption']:
            if isinstance(item['caption'], dict):
                caption_text = item['caption']['text']
            else:
                caption_text = item['caption']

        elif 'edge_media_to_caption' in item and item['edge_media_to_caption'] and item['edge_media_to_caption'][
            'edges']:
            caption_text = item['edge_media_to_caption']['edges'][0]['node']['text']

        if caption_text:
            # include words and emojis
            item['tags'] = re.findall(
                r"(?<!&)#(\w+|(?:[\xA9\xAE\u203C\u2049\u2122\u2139\u2194-\u2199\u21A9\u21AA\u231A\u231B\u2328\u2388\u23CF\u23E9-\u23F3\u23F8-\u23FA\u24C2\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE\u2600-\u2604\u260E\u2611\u2614\u2615\u2618\u261D\u2620\u2622\u2623\u2626\u262A\u262E\u262F\u2638-\u263A\u2648-\u2653\u2660\u2663\u2665\u2666\u2668\u267B\u267F\u2692-\u2694\u2696\u2697\u2699\u269B\u269C\u26A0\u26A1\u26AA\u26AB\u26B0\u26B1\u26BD\u26BE\u26C4\u26C5\u26C8\u26CE\u26CF\u26D1\u26D3\u26D4\u26E9\u26EA\u26F0-\u26F5\u26F7-\u26FA\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721\u2728\u2733\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763\u2764\u2795-\u2797\u27A1\u27B0\u27BF\u2934\u2935\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55\u3030\u303D\u3297\u3299]|\uD83C[\uDC04\uDCCF\uDD70\uDD71\uDD7E\uDD7F\uDD8E\uDD91-\uDD9A\uDE01\uDE02\uDE1A\uDE2F\uDE32-\uDE3A\uDE50\uDE51\uDF00-\uDF21\uDF24-\uDF93\uDF96\uDF97\uDF99-\uDF9B\uDF9E-\uDFF0\uDFF3-\uDFF5\uDFF7-\uDFFF]|\uD83D[\uDC00-\uDCFD\uDCFF-\uDD3D\uDD49-\uDD4E\uDD50-\uDD67\uDD6F\uDD70\uDD73-\uDD79\uDD87\uDD8A-\uDD8D\uDD90\uDD95\uDD96\uDDA5\uDDA8\uDDB1\uDDB2\uDDBC\uDDC2-\uDDC4\uDDD1-\uDDD3\uDDDC-\uDDDE\uDDE1\uDDE3\uDDEF\uDDF3\uDDFA-\uDE4F\uDE80-\uDEC5\uDECB-\uDED0\uDEE0-\uDEE5\uDEE9\uDEEB\uDEEC\uDEF0\uDEF3]|\uD83E[\uDD10-\uDD18\uDD80-\uDD84\uDDC0]|(?:0\u20E3|1\u20E3|2\u20E3|3\u20E3|4\u20E3|5\u20E3|6\u20E3|7\u20E3|8\u20E3|9\u20E3|#\u20E3|\\*\u20E3|\uD83C(?:\uDDE6\uD83C(?:\uDDEB|\uDDFD|\uDDF1|\uDDF8|\uDDE9|\uDDF4|\uDDEE|\uDDF6|\uDDEC|\uDDF7|\uDDF2|\uDDFC|\uDDE8|\uDDFA|\uDDF9|\uDDFF|\uDDEA)|\uDDE7\uD83C(?:\uDDF8|\uDDED|\uDDE9|\uDDE7|\uDDFE|\uDDEA|\uDDFF|\uDDEF|\uDDF2|\uDDF9|\uDDF4|\uDDE6|\uDDFC|\uDDFB|\uDDF7|\uDDF3|\uDDEC|\uDDEB|\uDDEE|\uDDF6|\uDDF1)|\uDDE8\uD83C(?:\uDDF2|\uDDE6|\uDDFB|\uDDEB|\uDDF1|\uDDF3|\uDDFD|\uDDF5|\uDDE8|\uDDF4|\uDDEC|\uDDE9|\uDDF0|\uDDF7|\uDDEE|\uDDFA|\uDDFC|\uDDFE|\uDDFF|\uDDED)|\uDDE9\uD83C(?:\uDDFF|\uDDF0|\uDDEC|\uDDEF|\uDDF2|\uDDF4|\uDDEA)|\uDDEA\uD83C(?:\uDDE6|\uDDE8|\uDDEC|\uDDF7|\uDDEA|\uDDF9|\uDDFA|\uDDF8|\uDDED)|\uDDEB\uD83C(?:\uDDF0|\uDDF4|\uDDEF|\uDDEE|\uDDF7|\uDDF2)|\uDDEC\uD83C(?:\uDDF6|\uDDEB|\uDDE6|\uDDF2|\uDDEA|\uDDED|\uDDEE|\uDDF7|\uDDF1|\uDDE9|\uDDF5|\uDDFA|\uDDF9|\uDDEC|\uDDF3|\uDDFC|\uDDFE|\uDDF8|\uDDE7)|\uDDED\uD83C(?:\uDDF7|\uDDF9|\uDDF2|\uDDF3|\uDDF0|\uDDFA)|\uDDEE\uD83C(?:\uDDF4|\uDDE8|\uDDF8|\uDDF3|\uDDE9|\uDDF7|\uDDF6|\uDDEA|\uDDF2|\uDDF1|\uDDF9)|\uDDEF\uD83C(?:\uDDF2|\uDDF5|\uDDEA|\uDDF4)|\uDDF0\uD83C(?:\uDDED|\uDDFE|\uDDF2|\uDDFF|\uDDEA|\uDDEE|\uDDFC|\uDDEC|\uDDF5|\uDDF7|\uDDF3)|\uDDF1\uD83C(?:\uDDE6|\uDDFB|\uDDE7|\uDDF8|\uDDF7|\uDDFE|\uDDEE|\uDDF9|\uDDFA|\uDDF0|\uDDE8)|\uDDF2\uD83C(?:\uDDF4|\uDDF0|\uDDEC|\uDDFC|\uDDFE|\uDDFB|\uDDF1|\uDDF9|\uDDED|\uDDF6|\uDDF7|\uDDFA|\uDDFD|\uDDE9|\uDDE8|\uDDF3|\uDDEA|\uDDF8|\uDDE6|\uDDFF|\uDDF2|\uDDF5|\uDDEB)|\uDDF3\uD83C(?:\uDDE6|\uDDF7|\uDDF5|\uDDF1|\uDDE8|\uDDFF|\uDDEE|\uDDEA|\uDDEC|\uDDFA|\uDDEB|\uDDF4)|\uDDF4\uD83C\uDDF2|\uDDF5\uD83C(?:\uDDEB|\uDDF0|\uDDFC|\uDDF8|\uDDE6|\uDDEC|\uDDFE|\uDDEA|\uDDED|\uDDF3|\uDDF1|\uDDF9|\uDDF7|\uDDF2)|\uDDF6\uD83C\uDDE6|\uDDF7\uD83C(?:\uDDEA|\uDDF4|\uDDFA|\uDDFC|\uDDF8)|\uDDF8\uD83C(?:\uDDFB|\uDDF2|\uDDF9|\uDDE6|\uDDF3|\uDDE8|\uDDF1|\uDDEC|\uDDFD|\uDDF0|\uDDEE|\uDDE7|\uDDF4|\uDDF8|\uDDED|\uDDE9|\uDDF7|\uDDEF|\uDDFF|\uDDEA|\uDDFE)|\uDDF9\uD83C(?:\uDDE9|\uDDEB|\uDDFC|\uDDEF|\uDDFF|\uDDED|\uDDF1|\uDDEC|\uDDF0|\uDDF4|\uDDF9|\uDDE6|\uDDF3|\uDDF7|\uDDF2|\uDDE8|\uDDFB)|\uDDFA\uD83C(?:\uDDEC|\uDDE6|\uDDF8|\uDDFE|\uDDF2|\uDDFF)|\uDDFB\uD83C(?:\uDDEC|\uDDE8|\uDDEE|\uDDFA|\uDDE6|\uDDEA|\uDDF3)|\uDDFC\uD83C(?:\uDDF8|\uDDEB)|\uDDFD\uD83C\uDDF0|\uDDFE\uD83C(?:\uDDF9|\uDDEA)|\uDDFF\uD83C(?:\uDDE6|\uDDF2|\uDDFC))))[\ufe00-\ufe0f\u200d]?)+",
                caption_text, re.UNICODE)
            item['tags'] = list(set(item['tags']))

        return item

    def extract_tags_profile(self, caption_text):
        """Extracts the hashtags from the caption text."""
        tagsall = ''
        if caption_text:
            # include words and emojis
            tags = re.findall(
                r"(?<!&)#(\w+|(?:[\xA9\xAE\u203C\u2049\u2122\u2139\u2194-\u2199\u21A9\u21AA\u231A\u231B\u2328\u2388\u23CF\u23E9-\u23F3\u23F8-\u23FA\u24C2\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE\u2600-\u2604\u260E\u2611\u2614\u2615\u2618\u261D\u2620\u2622\u2623\u2626\u262A\u262E\u262F\u2638-\u263A\u2648-\u2653\u2660\u2663\u2665\u2666\u2668\u267B\u267F\u2692-\u2694\u2696\u2697\u2699\u269B\u269C\u26A0\u26A1\u26AA\u26AB\u26B0\u26B1\u26BD\u26BE\u26C4\u26C5\u26C8\u26CE\u26CF\u26D1\u26D3\u26D4\u26E9\u26EA\u26F0-\u26F5\u26F7-\u26FA\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721\u2728\u2733\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763\u2764\u2795-\u2797\u27A1\u27B0\u27BF\u2934\u2935\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55\u3030\u303D\u3297\u3299]|\uD83C[\uDC04\uDCCF\uDD70\uDD71\uDD7E\uDD7F\uDD8E\uDD91-\uDD9A\uDE01\uDE02\uDE1A\uDE2F\uDE32-\uDE3A\uDE50\uDE51\uDF00-\uDF21\uDF24-\uDF93\uDF96\uDF97\uDF99-\uDF9B\uDF9E-\uDFF0\uDFF3-\uDFF5\uDFF7-\uDFFF]|\uD83D[\uDC00-\uDCFD\uDCFF-\uDD3D\uDD49-\uDD4E\uDD50-\uDD67\uDD6F\uDD70\uDD73-\uDD79\uDD87\uDD8A-\uDD8D\uDD90\uDD95\uDD96\uDDA5\uDDA8\uDDB1\uDDB2\uDDBC\uDDC2-\uDDC4\uDDD1-\uDDD3\uDDDC-\uDDDE\uDDE1\uDDE3\uDDEF\uDDF3\uDDFA-\uDE4F\uDE80-\uDEC5\uDECB-\uDED0\uDEE0-\uDEE5\uDEE9\uDEEB\uDEEC\uDEF0\uDEF3]|\uD83E[\uDD10-\uDD18\uDD80-\uDD84\uDDC0]|(?:0\u20E3|1\u20E3|2\u20E3|3\u20E3|4\u20E3|5\u20E3|6\u20E3|7\u20E3|8\u20E3|9\u20E3|#\u20E3|\\*\u20E3|\uD83C(?:\uDDE6\uD83C(?:\uDDEB|\uDDFD|\uDDF1|\uDDF8|\uDDE9|\uDDF4|\uDDEE|\uDDF6|\uDDEC|\uDDF7|\uDDF2|\uDDFC|\uDDE8|\uDDFA|\uDDF9|\uDDFF|\uDDEA)|\uDDE7\uD83C(?:\uDDF8|\uDDED|\uDDE9|\uDDE7|\uDDFE|\uDDEA|\uDDFF|\uDDEF|\uDDF2|\uDDF9|\uDDF4|\uDDE6|\uDDFC|\uDDFB|\uDDF7|\uDDF3|\uDDEC|\uDDEB|\uDDEE|\uDDF6|\uDDF1)|\uDDE8\uD83C(?:\uDDF2|\uDDE6|\uDDFB|\uDDEB|\uDDF1|\uDDF3|\uDDFD|\uDDF5|\uDDE8|\uDDF4|\uDDEC|\uDDE9|\uDDF0|\uDDF7|\uDDEE|\uDDFA|\uDDFC|\uDDFE|\uDDFF|\uDDED)|\uDDE9\uD83C(?:\uDDFF|\uDDF0|\uDDEC|\uDDEF|\uDDF2|\uDDF4|\uDDEA)|\uDDEA\uD83C(?:\uDDE6|\uDDE8|\uDDEC|\uDDF7|\uDDEA|\uDDF9|\uDDFA|\uDDF8|\uDDED)|\uDDEB\uD83C(?:\uDDF0|\uDDF4|\uDDEF|\uDDEE|\uDDF7|\uDDF2)|\uDDEC\uD83C(?:\uDDF6|\uDDEB|\uDDE6|\uDDF2|\uDDEA|\uDDED|\uDDEE|\uDDF7|\uDDF1|\uDDE9|\uDDF5|\uDDFA|\uDDF9|\uDDEC|\uDDF3|\uDDFC|\uDDFE|\uDDF8|\uDDE7)|\uDDED\uD83C(?:\uDDF7|\uDDF9|\uDDF2|\uDDF3|\uDDF0|\uDDFA)|\uDDEE\uD83C(?:\uDDF4|\uDDE8|\uDDF8|\uDDF3|\uDDE9|\uDDF7|\uDDF6|\uDDEA|\uDDF2|\uDDF1|\uDDF9)|\uDDEF\uD83C(?:\uDDF2|\uDDF5|\uDDEA|\uDDF4)|\uDDF0\uD83C(?:\uDDED|\uDDFE|\uDDF2|\uDDFF|\uDDEA|\uDDEE|\uDDFC|\uDDEC|\uDDF5|\uDDF7|\uDDF3)|\uDDF1\uD83C(?:\uDDE6|\uDDFB|\uDDE7|\uDDF8|\uDDF7|\uDDFE|\uDDEE|\uDDF9|\uDDFA|\uDDF0|\uDDE8)|\uDDF2\uD83C(?:\uDDF4|\uDDF0|\uDDEC|\uDDFC|\uDDFE|\uDDFB|\uDDF1|\uDDF9|\uDDED|\uDDF6|\uDDF7|\uDDFA|\uDDFD|\uDDE9|\uDDE8|\uDDF3|\uDDEA|\uDDF8|\uDDE6|\uDDFF|\uDDF2|\uDDF5|\uDDEB)|\uDDF3\uD83C(?:\uDDE6|\uDDF7|\uDDF5|\uDDF1|\uDDE8|\uDDFF|\uDDEE|\uDDEA|\uDDEC|\uDDFA|\uDDEB|\uDDF4)|\uDDF4\uD83C\uDDF2|\uDDF5\uD83C(?:\uDDEB|\uDDF0|\uDDFC|\uDDF8|\uDDE6|\uDDEC|\uDDFE|\uDDEA|\uDDED|\uDDF3|\uDDF1|\uDDF9|\uDDF7|\uDDF2)|\uDDF6\uD83C\uDDE6|\uDDF7\uD83C(?:\uDDEA|\uDDF4|\uDDFA|\uDDFC|\uDDF8)|\uDDF8\uD83C(?:\uDDFB|\uDDF2|\uDDF9|\uDDE6|\uDDF3|\uDDE8|\uDDF1|\uDDEC|\uDDFD|\uDDF0|\uDDEE|\uDDE7|\uDDF4|\uDDF8|\uDDED|\uDDE9|\uDDF7|\uDDEF|\uDDFF|\uDDEA|\uDDFE)|\uDDF9\uD83C(?:\uDDE9|\uDDEB|\uDDFC|\uDDEF|\uDDFF|\uDDED|\uDDF1|\uDDEC|\uDDF0|\uDDF4|\uDDF9|\uDDE6|\uDDF3|\uDDF7|\uDDF2|\uDDE8|\uDDFB)|\uDDFA\uD83C(?:\uDDEC|\uDDE6|\uDDF8|\uDDFE|\uDDF2|\uDDFF)|\uDDFB\uD83C(?:\uDDEC|\uDDE8|\uDDEE|\uDDFA|\uDDE6|\uDDEA|\uDDF3)|\uDDFC\uD83C(?:\uDDF8|\uDDEB)|\uDDFD\uD83C\uDDF0|\uDDFE\uD83C(?:\uDDF9|\uDDEA)|\uDDFF\uD83C(?:\uDDE6|\uDDF2|\uDDFC))))[\ufe00-\ufe0f\u200d]?)+",
                caption_text, re.UNICODE)
            tagsall = tags
        caption_text=tags=''
        return tagsall 
    def Find(self,string): 
        # findall() has been used  
        # with valid conditions for urls in string 
        url = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+] |[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', string) 
        return url
    def Findmobile(self,string):
        Phonenumber=re.compile(r'\d\d\d\d\d\d\d\d\d\d\d\d')
        m=Phonenumber.search(string)
        mw = ''
        if m:
            mw = self.allstrepalce(m.group())
        return mw
    def Findemail(self,string):
        emails = re.findall("([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", string)
        return emails  

    def get_profile_user(self, user={}, username='',sctype=0):
        usernamepr = username
        ####print("QQQQQQQQQQQQQQKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK!!!!!!!!!!!!!")
        user_id = biography = followers_count = following_count = full_name = is_business_account = is_joined_recently = is_private = posts_count = etc1 = profile_pic_url = etc2 =profile_pic_url_hd =insta_user_name  =hastagvalue = connected_fb_page = has_channel = external_url = external_url_linkshimmed = business_category_name = overall_category_name = is_verified = ''
        
        if 'id' in user:
            
            ####print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK!!!!!!!!!!!!!")

            user_id = user['id']
            if 'biography' in user:
                biography = self.allstrepalce(user['biography'])
                hastags= self.extract_tags_profile(biography)
                if hastags:
                    hastagvalue =",".join(hastags) 
            if 'edge_followed_by' in user:
                followers_count = user['edge_followed_by']['count']
            if 'edge_follow' in user:
                following_count = user['edge_follow']['count']
            if 'full_name' in user:
                full_name = self.allstrepalce(user['full_name'])
            if 'is_business_account' in user:
                is_business_account = user['is_business_account']
            if 'is_joined_recently' in user:
                is_joined_recently = user['is_joined_recently']
            if 'is_private' in user:
                is_private = user['is_private']
            if 'edge_owner_to_timeline_media' in user:
                posts_count = user['edge_owner_to_timeline_media']['count']
            if 'profile_pic_url' in user:
                etc1 = profile_pic_url = user['profile_pic_url']
            if 'profile_pic_url_hd' in user:
                etc2 = profile_pic_url_hd = user['profile_pic_url_hd']

            if 'connected_fb_page' in user:
                connected_fb_page = user['connected_fb_page']

            if 'has_channel' in user:
                has_channel = user['has_channel']

            if 'external_url' in user:
                external_url = user['external_url']

            if 'external_url_linkshimmed' in user:
                external_url_linkshimmed = user['external_url_linkshimmed']

            if 'business_category_name' in user:
                business_category_name = user['business_category_name']
            if 'overall_category_name' in user:
                overall_category_name = user['overall_category_name']

            if 'is_verified' in user:
                is_verified = user['is_verified']

            if followers_count:
                we=1
                ####print(followers_count)
            else:
                followers_count = 0
            scrapeimg = self.UserImageDownLoad(user_id,etc1)
            scrapeimg =''
                
                  

            profile_info = {"insta_image_down":scrapeimg,'user_id': user_id,'biography': biography,'hastags': hastagvalue,'followers_count': followers_count,'following_count': following_count,
            'full_name': full_name,'is_business_account': is_business_account,'is_joined_recently': is_joined_recently,'is_private': is_private,
            'posts_count': posts_count,'etc1': etc1,'profile_pic_url': profile_pic_url,'etc2': etc2,'profile_pic_url_hd': profile_pic_url_hd,
            'insta_user_name_oth': insta_user_name,'has_channel': has_channel,'external_url': external_url,
            'external_url_linkshimmed': external_url_linkshimmed,'business_category_name':business_category_name,
            'overall_category_name': overall_category_name,'is_verified':is_verified,'update': 4,'rst': 1,'insta_user_name':usernamepr.lower()}
            return self.useAdd(profile_info,sctype)

    def fulnamecheck(self,nameuser):
        userremoveName = ['sri','shri','shree','mrs.','miss.','mr','dr','ca','er','acp','dcp','irs','ias']
        mastername = UserNamefull = nameuser.split(" ")
        UserNamefull = UserNamefull[0].split(".")
        nameAdd =  UserNamefull[0]
        if nameAdd in userremoveName:
            if mastername[1]:
                nameAdd = mastername[1]
        return nameAdd.lower()

    def _get_nodes(self,container):
        #print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
        #print(container)
        return [self.augment_node(node['node']) for node in container['edges']]
    def get_original_image(self, url):
        """Gets the full-size image from the specified url."""
        # these path parts somehow prevent us from changing the rest of media url
        #url = re.sub(r'/vp/[0-9A-Fa-f]{32}/[0-9A-Fa-f]{8}/', '/', url)
        # remove dimensions to get largest image
        #url = re.sub(r'/[sp]\d{3,}x\d{3,}/', '/', url)
        # get non-square image if one exists
        #url = re.sub(r'/c\d{1,}.\d{1,}.\d{1,}.\d{1,}/', '/', url)
        return url    

    def augment_node(self, node):
        self.extract_tags(node)

        details = None
        if self.include_location and 'location' not in node:
            #details = self.__get_media_details(node['shortcode'])
            node['location'] = ''

        if 'urls' not in node:
            node['urls'] = []

        if node['is_video'] and 'video_url' in node:
            node['urls'] = [node['video_url']]
        elif '__typename' in node and node['__typename'] == 'GraphImage':
            node['urls'] = [self.get_original_image(node['display_url'])]
        else:
            return node
            if details is None:
                details = self.__get_media_details(node['shortcode'])

            if details:
                if '__typename' in details and details['__typename'] == 'GraphVideo':
                    node['urls'] = [details['video_url']]
                elif '__typename' in details and details['__typename'] == 'GraphSidecar':
                    urls = []
                    for carousel_item in details['edge_sidecar_to_children']['edges']:
                        urls += self.augment_node(carousel_item['node'])['urls']
                    node['urls'] = urls
                else:
                    node['urls'] = [self.get_original_image(details['display_url'])]

        return node

    def extract_tagsdd(self, item):
        """Extracts the hashtags from the caption text."""
        caption_text = ''
        if 'caption' in item and item['caption']:
            if isinstance(item['caption'], dict):
                caption_text = item['caption']['text']
            else:
                caption_text = item['caption']

        elif 'edge_media_to_caption' in item and item['edge_media_to_caption'] and item['edge_media_to_caption'][
            'edges']:
            caption_text = item['edge_media_to_caption']['edges'][0]['node']['text']

        if caption_text:
            # include words and emojis
            item['tags'] = re.findall(
                r"(?<!&)#(\w+|(?:[\xA9\xAE\u203C\u2049\u2122\u2139\u2194-\u2199\u21A9\u21AA\u231A\u231B\u2328\u2388\u23CF\u23E9-\u23F3\u23F8-\u23FA\u24C2\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE\u2600-\u2604\u260E\u2611\u2614\u2615\u2618\u261D\u2620\u2622\u2623\u2626\u262A\u262E\u262F\u2638-\u263A\u2648-\u2653\u2660\u2663\u2665\u2666\u2668\u267B\u267F\u2692-\u2694\u2696\u2697\u2699\u269B\u269C\u26A0\u26A1\u26AA\u26AB\u26B0\u26B1\u26BD\u26BE\u26C4\u26C5\u26C8\u26CE\u26CF\u26D1\u26D3\u26D4\u26E9\u26EA\u26F0-\u26F5\u26F7-\u26FA\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721\u2728\u2733\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763\u2764\u2795-\u2797\u27A1\u27B0\u27BF\u2934\u2935\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55\u3030\u303D\u3297\u3299]|\uD83C[\uDC04\uDCCF\uDD70\uDD71\uDD7E\uDD7F\uDD8E\uDD91-\uDD9A\uDE01\uDE02\uDE1A\uDE2F\uDE32-\uDE3A\uDE50\uDE51\uDF00-\uDF21\uDF24-\uDF93\uDF96\uDF97\uDF99-\uDF9B\uDF9E-\uDFF0\uDFF3-\uDFF5\uDFF7-\uDFFF]|\uD83D[\uDC00-\uDCFD\uDCFF-\uDD3D\uDD49-\uDD4E\uDD50-\uDD67\uDD6F\uDD70\uDD73-\uDD79\uDD87\uDD8A-\uDD8D\uDD90\uDD95\uDD96\uDDA5\uDDA8\uDDB1\uDDB2\uDDBC\uDDC2-\uDDC4\uDDD1-\uDDD3\uDDDC-\uDDDE\uDDE1\uDDE3\uDDEF\uDDF3\uDDFA-\uDE4F\uDE80-\uDEC5\uDECB-\uDED0\uDEE0-\uDEE5\uDEE9\uDEEB\uDEEC\uDEF0\uDEF3]|\uD83E[\uDD10-\uDD18\uDD80-\uDD84\uDDC0]|(?:0\u20E3|1\u20E3|2\u20E3|3\u20E3|4\u20E3|5\u20E3|6\u20E3|7\u20E3|8\u20E3|9\u20E3|#\u20E3|\\*\u20E3|\uD83C(?:\uDDE6\uD83C(?:\uDDEB|\uDDFD|\uDDF1|\uDDF8|\uDDE9|\uDDF4|\uDDEE|\uDDF6|\uDDEC|\uDDF7|\uDDF2|\uDDFC|\uDDE8|\uDDFA|\uDDF9|\uDDFF|\uDDEA)|\uDDE7\uD83C(?:\uDDF8|\uDDED|\uDDE9|\uDDE7|\uDDFE|\uDDEA|\uDDFF|\uDDEF|\uDDF2|\uDDF9|\uDDF4|\uDDE6|\uDDFC|\uDDFB|\uDDF7|\uDDF3|\uDDEC|\uDDEB|\uDDEE|\uDDF6|\uDDF1)|\uDDE8\uD83C(?:\uDDF2|\uDDE6|\uDDFB|\uDDEB|\uDDF1|\uDDF3|\uDDFD|\uDDF5|\uDDE8|\uDDF4|\uDDEC|\uDDE9|\uDDF0|\uDDF7|\uDDEE|\uDDFA|\uDDFC|\uDDFE|\uDDFF|\uDDED)|\uDDE9\uD83C(?:\uDDFF|\uDDF0|\uDDEC|\uDDEF|\uDDF2|\uDDF4|\uDDEA)|\uDDEA\uD83C(?:\uDDE6|\uDDE8|\uDDEC|\uDDF7|\uDDEA|\uDDF9|\uDDFA|\uDDF8|\uDDED)|\uDDEB\uD83C(?:\uDDF0|\uDDF4|\uDDEF|\uDDEE|\uDDF7|\uDDF2)|\uDDEC\uD83C(?:\uDDF6|\uDDEB|\uDDE6|\uDDF2|\uDDEA|\uDDED|\uDDEE|\uDDF7|\uDDF1|\uDDE9|\uDDF5|\uDDFA|\uDDF9|\uDDEC|\uDDF3|\uDDFC|\uDDFE|\uDDF8|\uDDE7)|\uDDED\uD83C(?:\uDDF7|\uDDF9|\uDDF2|\uDDF3|\uDDF0|\uDDFA)|\uDDEE\uD83C(?:\uDDF4|\uDDE8|\uDDF8|\uDDF3|\uDDE9|\uDDF7|\uDDF6|\uDDEA|\uDDF2|\uDDF1|\uDDF9)|\uDDEF\uD83C(?:\uDDF2|\uDDF5|\uDDEA|\uDDF4)|\uDDF0\uD83C(?:\uDDED|\uDDFE|\uDDF2|\uDDFF|\uDDEA|\uDDEE|\uDDFC|\uDDEC|\uDDF5|\uDDF7|\uDDF3)|\uDDF1\uD83C(?:\uDDE6|\uDDFB|\uDDE7|\uDDF8|\uDDF7|\uDDFE|\uDDEE|\uDDF9|\uDDFA|\uDDF0|\uDDE8)|\uDDF2\uD83C(?:\uDDF4|\uDDF0|\uDDEC|\uDDFC|\uDDFE|\uDDFB|\uDDF1|\uDDF9|\uDDED|\uDDF6|\uDDF7|\uDDFA|\uDDFD|\uDDE9|\uDDE8|\uDDF3|\uDDEA|\uDDF8|\uDDE6|\uDDFF|\uDDF2|\uDDF5|\uDDEB)|\uDDF3\uD83C(?:\uDDE6|\uDDF7|\uDDF5|\uDDF1|\uDDE8|\uDDFF|\uDDEE|\uDDEA|\uDDEC|\uDDFA|\uDDEB|\uDDF4)|\uDDF4\uD83C\uDDF2|\uDDF5\uD83C(?:\uDDEB|\uDDF0|\uDDFC|\uDDF8|\uDDE6|\uDDEC|\uDDFE|\uDDEA|\uDDED|\uDDF3|\uDDF1|\uDDF9|\uDDF7|\uDDF2)|\uDDF6\uD83C\uDDE6|\uDDF7\uD83C(?:\uDDEA|\uDDF4|\uDDFA|\uDDFC|\uDDF8)|\uDDF8\uD83C(?:\uDDFB|\uDDF2|\uDDF9|\uDDE6|\uDDF3|\uDDE8|\uDDF1|\uDDEC|\uDDFD|\uDDF0|\uDDEE|\uDDE7|\uDDF4|\uDDF8|\uDDED|\uDDE9|\uDDF7|\uDDEF|\uDDFF|\uDDEA|\uDDFE)|\uDDF9\uD83C(?:\uDDE9|\uDDEB|\uDDFC|\uDDEF|\uDDFF|\uDDED|\uDDF1|\uDDEC|\uDDF0|\uDDF4|\uDDF9|\uDDE6|\uDDF3|\uDDF7|\uDDF2|\uDDE8|\uDDFB)|\uDDFA\uD83C(?:\uDDEC|\uDDE6|\uDDF8|\uDDFE|\uDDF2|\uDDFF)|\uDDFB\uD83C(?:\uDDEC|\uDDE8|\uDDEE|\uDDFA|\uDDE6|\uDDEA|\uDDF3)|\uDDFC\uD83C(?:\uDDF8|\uDDEB)|\uDDFD\uD83C\uDDF0|\uDDFE\uD83C(?:\uDDF9|\uDDEA)|\uDDFF\uD83C(?:\uDDE6|\uDDF2|\uDDFC))))[\ufe00-\ufe0f\u200d]?)+",
                caption_text, re.UNICODE)
            item['tags'] = list(set(item['tags']))
        ##print(item)    

        ##print("***********5555555555555555**********************8")    

        return item    
    def augment_nodaae(self, node):
        ##print(node)
        ##print("*********************************8")
        return self.extract_tagsdd(node)    
    def _get_noccdes(self, container):
        #print("sssssssssssssss")
        #aaa=[self.augment_nodaae(node['node']) for node in container['edges']]
        ##print(aaa)
        return [self.augment_nodaae(node['node']) for node in container['edges']]
    def __get_media_details(self, shortcode):
        resp = self.get_json(VIEW_MEDIA_URL.format(shortcode))

        if resp is not None:
            try:
                return json.loads(resp)['graphql']['shortcode_media']
            except Exception as er:
                k=1

        else:
            h=1
            ##print('Failed to get media details for ' + shortcode)    
    def __query_media_scrape(self, payload, end_cursor=''):
        if payload:
            container = payload['edge_owner_to_timeline_media']
            ##print(container)
            ##print("UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU")
            nodes = self._get_noccdes(container)
            ##print("MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM")
            ##print(nodes)
            end_cursor = container['page_info']['end_cursor']
            return nodes, end_cursor

        return None, None 
    def paidUser(self,postrow,paidLi=[],is_paid_partnership=0):
        singlePost =[]
        if 'is_paid_partnership' in postrow:
            if postrow['is_paid_partnership']:
                is_paid_partnership=1
        if 'edge_media_to_sponsor_user' in postrow:
            if 'edges' in postrow['edge_media_to_sponsor_user']:
                for lr in postrow['edge_media_to_sponsor_user']['edges']:
                    if 'node' in lr:
                        if 'sponsor' in lr['node']:
                            taggedid = lr['node']['sponsor']['username']
                            if taggedid not in paidLi:
                                paidLi.append(taggedid)
        return paidLi,is_paid_partnership

    def tagegedUser(self,postrow,taggedL=[]):
        if 'edge_media_to_tagged_user' in postrow:
            if 'edges' in postrow['edge_media_to_tagged_user']:
                for lr in postrow['edge_media_to_tagged_user']['edges']:
                    if 'node' in lr:
                        if 'user' in lr['node']:
                            taggedid = lr['node']['user']['username']
                            if taggedid not in taggedL:
                                taggedL.append(taggedid)
        return taggedL

    def categoryTags(self,tagshash):
        tagshashv = tagshash.split(",")
        catlist = {'tech': '12', 'likes': '20', 'destiny': '19', 'dance': '18', 'basketball': '7', 'musician': '18', 'minecraft': '19', 'smartphone': '12', 'instagram': '20,18,17,16,15,12,7,6,4,3', 'techno': '12', 'cricket': '7', 'gameplay': '19', 'photoshoot': '20', 'photographer': '20,3', 'nikon': '20', 'crafting': '17', 'photooftheday': '20,16,15,12,8,6,4,3', 'technology': '12', 'crossfit': '7,7', 'insta': '20', 'instafit': '7,7', 'gadget': '12', 'shopping': '4', 'summer': '3', 'instadaily': '20,3', 'exercise': '7,7', 'xboxone': '19', 'photography': '20,19,18,17,16,15,7,6,4,3', 'streetphotography': '20', 'programming': '12', 'instaphoto': '20', 'travelgram': '20,3', 'instalike': '20', 'india': '20,18,12,3', 'fitnessmodel': '7,7', 'engineer': '12', 'google': '12', 'familia': '16', 'christmas': '17,16', 'sewing': '17', 'hiking': '3', 'etsy': '17', 'techie': '12', 'restaurant': '8', 'sportsphotography': '7', 'innovation': '12', 'wanderlust': '3', 'wood': '17', 'lunch': '8', 'sports': '7', 'coding': '12', 'paper': '17', 'nintendoswitch': '19', 'gamergirl': '19', 'entertainer': '18', 'technologynews': '12', 'likeforlikes': '20,15', 'gamingmemes': '19', 'nigeria': '18', 'cooking': '8', 'software': '12', 'interiordesign': '17', 'dinner': '8', 'hair': '6', 'hockey': '7', 'events': '18', 'automation': '12', 'handcraft': '17', 'artist': '20,18,17', 'handmade': '17,4', 'tecnologia': '12', 'tourism': '3', 'follow': '20,19,18,16,15,12,8,7,6,4,3', 'craftbeer': '17', 'instagaming': '19', 'repost': '18', 'handmadewithlove': '17', 'hobby': '17', 'security': '12', 'gamerlife': '19', 'electronics': '12', 'show': '18', 'homedecor': '17', 'fortnitememes': '19', 'decor': '17', 'livemusic': '18', 'celebrity': '18', 'bodybuilding': '7,7', 'portraitphotography': '20', 'wellness': '7,7', 'medicine': '7', 'beer': '17', 'lashes': '6', 'sport': '7,7,7', 'healing': '7', 'home': '16', 'gamer': '19', 'organic': '7', 'trip': '3', 'movie': '18', 'adventure': '3', 'gamers': '19', 'beerporn': '17', 'party': '18', 'electronic': '12', 'ootd': '4', 'artwork': '17', 'city': '3', 'healthyfood': '8,7', 'photo': '20,6,3', 'marketing': '12', 'traveler': '3', 'papercraft': '17', 'design': '17,12,4', 'like': '20,18,16,15,8,7,6,4,3', 'samsung': '12', 'happiness': '16,7', 'gaming': '19', 'pcgamer': '19', 'game': '19', 'football': '7', 'nutrition': '7', 'handcrafted': '17', 'yoga': '7,7', 'beautiful': '20,17,16,6,4,3', 'iphone': '12', 'memes': '19,18', 'cute': '20,16,6,4', 'nature': '20,16,15,6,3', 'explore': '3', 'travel': '20,16,15,3', 'lifestyle': '20,18,15,7,7,6,4,3', 'bhfyp': '20,20,19,19,18,17,16,15,12,12,8,8,7,7,7,7,6,4,3', 'twitchstreamer': '19', 'cybersecurity': '12', 'geek': '19,12', 'merrychristmas': '16', 'picoftheday': '20,16,15,8,6,4,3', 'architecture': '3', 'workout': '15,7,7,7', 'yummy': '8', 'artificialintelligence': '12', 'instatech': '12', 'podcast': '18', 'selflove': '7', 'informationtechnology': '12', 'healthylife': '7', 'accessories': '17', 'hollywood': '18', 'xbox': '19', 'personaltrainer': '7,7', 'pcgaming': '19', 'esports': '19', 'fashionblogger': '4', 'canon': '20', 'healthylifestyle': '7,7', 'mountains': '3', 'instafashion': '4', 'developer': '12', 'hiphop': '18', 'love': '20,18,17,16,15,12,8,7,7,7,6,4,3', 'diet': '7', 'computer': '12', 'foodie': '8', 'style': '20,17,15,6,4,3', 'travelphotography': '20,3', 'nightlife': '18', 'fashion': '20,18,17,16,15,7,6,4,3', 'technews': '12', 'live': '18', 'soccer': '7', 'pubg': '19', 'event': '18', 'nails': '6', 'comedy': '18', 'foodphotography': '8', 'photos': '20', 'fitspo': '7,7', 'foodlover': '8', 'streamer': '19', 'landscape': '20,3', 'apple': '12', 'computerscience': '12', 'running': '7', 'youtuber': '19', 'kerajinantangan': '17', 'education': '12', 'skincare': '7,6', 'beach': '3', 'future': '12', 'food': '18,16,8,7,3', 'callofduty': '19', 'crochet': '17', 'actor': '18', 'fitnessaddict': '7', 'luxury': '15', 'cosplay': '19', 'woodworking': '17', 'gamingcommunity': '19', 'smile': '16', 'athlete': '7', 'foto': '20', 'homemade': '17,8', 'creative': '17', 'actress': '18', 'weightloss': '7,7', 'gymlife': '7,7', 'music': '20,18', 'instapic': '20', 'italy': '3', 'chef': '8', 'giftideas': '17', 'beerstagram': '17', 'vacation': '3', 'youtube': '19,18', 'fotografia': '20', 'plus': '12', 'vintage': '17', 'robotics': '12', 'dancer': '18', 'business': '18,12', 'mentalhealth': '7', 'entertainment': '18', 'traveling': '3', 'internet': '12', 'craft': '17', 'foodporn': '8', 'makeup': '20,6,4', 'entrepreneur': '18,12', 'followforfollowback': '20', 'instagamer': '19', 'wellbeing': '7', 'delicious': '8', 'healthyeating': '7', 'producer': '18', 'makersgonnamake': '17', 'custom': '17', 'pubgmobile': '19', 'crafter': '17', 'jewelry': '17', 'media': '18', 'healthy': '15,8,7,7', 'picture': '20', 'traveltheworld': '3', 'bollywood': '18', 'diycrafts': '17', 'naturephotography': '20,3', 'gaminglife': '19', 'natural': '7', 'baby': '16', 'cosmetics': '6', 'funny': '19,18', 'healthcare': '7', 'crafts': '17', 'battleroyale': '19', 'makeupartist': '6', 'handmadegifts': '17', 'engineering': '12', 'amazing': '3', 'meme': '19', 'fitness': '15,7,7,7', 'smallbusiness': '17', 'winter': '20,3', 'doctor': '7', 'ceramics': '17', 'healthyliving': '7', 'instatravel': '3', 'inspiration': '15,7', 'tasty': '8', 'sunset': '3', 'playstation': '19', 'kids': '16', 'nintendo': '19', 'film': '18', 'anime': '19', 'motivation': '15,7,7,7', 'goals': '7', 'videogame': '19', 'singer': '18', 'family': '18,16,7', 'workshop': '17', 'viral': '18', 'vegan': '7', 'photographylovers': '20', 'landscapephotography': '20', 'friends': '16', 'europe': '3', 'fitnessmotivation': '15,7,7', 'strong': '7', 'fortnite': '19', 'mobile': '12', 'foodblogger': '8', 'videogames': '19', 'android': '12', 'twitch': '19', 'foodies': '8', 'followme': '20', 'wedding': '20,18', 'scrapbook': '17', 'foodgasm': '8', 'scrapbooking': '17', 'awstenknight': '18', 'life': '20,16,15,7,7,3', 'csgo': '19', 'news': '18,12', 'science': '12', 'crafty': '17', 'cardio': '7', 'clay': '17', 'create': '17', 'instabeer': '17', 'online': '19', 'muscle': '7,7', 'dress': '4', 'fashionista': '4', 'fitnessgirl': '7', 'gymmotivation': '7', 'strength': '7', 'portrait': '20', 'training': '7,7,7', 'roadtrip': '3', 'travelblogger': '3', 'beautifuldestinations': '3', 'holiday': '3', 'blackandwhite': '20', 'skin': '6', 'radio': '18', 'embroidery': '17', 'foodstagram': '8', 'traveller': '3', 'baseball': '7', 'gadgets': '12', 'beauty': '20,15,7,6,4', 'girl': '6,4', 'eatclean': '7', 'health': '15,7,7', 'familytime': '16', 'fitfam': '7,7', 'moda': '4', 'instagood': '20,18,17,16,15,12,8,7,7,7,6,4,3', 'happy': '20,16,15,7,6,4,3', 'retrogaming': '19', 'fortniteclips': '19', 'startup': '12', 'newyork': '18', 'games': '19', 'medical': '7', 'gifts': '17', 'travelling': '3', 'instafood': '8', 'outfit': '4', 'gift': '17', 'trending': '18', 'model': '20,18,15,6,4'}
        hashcategory = []
        hashcategorycn = ''
        for ms in tagshashv:
            ouths = ms.lower()

            if ouths in catlist:
                tgaslist = catlist[ouths].split(",")
                for lsd in tgaslist:
                    if lsd not in hashcategory:
                        hashcategory.append(lsd)
        hashcategorycn = ",".join(hashcategory)            
        return hashcategorycn

    def logcreate(self,mk='',kk=''):
        d=1
        ##print('d')

    def get_media_insert_new(self,postrow,username,mydb,locid='',locname='',instamediapost=0,tagedUser=''):
        intsta_media_user = ''
        intsta_post_comment = ''
        intsta_post_like = ''
        intsta_shortcode = ''
        insta_etc1 = ''
        intsta_media_location = ''
        intsta_media_location_id = ''
        insta_etc2 = ''
        insta_image = ''
        insta_image_large = ''
        intsta_tags = ''
        intsta_text = ''
        insta_user_name = username.lower()
        is_video = 0
        taken_at_timestamp = ''
        is_loction_get = 1
        tageeduser = ''
        video_view_count = 0
        video_play_count = 0
        product_type = 0
        product_type = 0
        edge_liked_by = 0
        is_paid_partnership=0
        edge_media_to_tagged_user=[]
        taggeduserAll = []
        clips_music_attribution_info=[]
        postId=''
        if 'shortcode' in postrow:
            intsta_shortcode = postrow['shortcode']
            if intsta_shortcode : 
                if locname:
                    is_loction_get =2

                if 'edge_media_preview_like' in postrow:
                    intsta_post_like = postrow['edge_media_preview_like']['count']
                if 'clips_music_attribution_info' in postrow:
                    clips_music_attribution_info = postrow['clips_music_attribution_info']    

                if 'id' in postrow:
                    postId = postrow['id']    
                if 'edge_liked_by' in postrow:
                    edge_liked_by = postrow['edge_liked_by']['count']    
                if 'edge_media_to_comment' in postrow:
                    intsta_post_comment = postrow['edge_media_to_comment']['count']
                if 'tags' in postrow:
                    intsta_tags = self.allstrepalce(",".join(postrow['tags']))
                if 'owner' in postrow:
                    intsta_media_user = postrow['owner']['id']
                if 'location' in postrow:
                    if postrow['location']:
                        intsta_media_location_id = postrow['location']['id']
                        intsta_media_location = postrow['location']['name']

                if 'urls' in postrow:
                    insta_etc2 = postrow['urls']
                if 'product_type' in postrow:
                    product_type = postrow['product_type']   
                video_duration = 0    
                if 'video_duration' in postrow:
                  video_duration = postrow['video_duration']    
                if 'is_video' in postrow:
                    is_videos = postrow['is_video']
                    if is_videos=='true' or is_videos==True:
                        is_video = 1
                        if 'video_view_count' in postrow:
                            video_view_count = postrow['video_view_count']
                        if 'video_play_count' in postrow:
                             video_play_count = postrow['video_play_count']


                if 'taken_at_timestamp' in postrow:
                    taken_at_timestamp = postrow['taken_at_timestamp']

                if 'thumbnail_src' in postrow:
                    insta_image = postrow['thumbnail_src']
                    insta_image_large = postrow['thumbnail_src']


                if 'edge_media_to_caption' in postrow:
                    if 'edges' in postrow['edge_media_to_caption']:
                        if postrow['edge_media_to_caption']['edges']:
                            if 'node' in postrow['edge_media_to_caption']['edges'][0]:
                                if 'text' in postrow['edge_media_to_caption']['edges'][0]['node']:
                                    intsta_text = postrow['edge_media_to_caption']['edges'][0]['node']['text']
                                    taggeduserAll = re.findall(r"@(\w+)", intsta_text)

                postcat = ''
                is_paid_d= self.paidUser(postrow,edge_media_to_tagged_user,0)
                is_paid_partnership= is_paid_d[1]
                edge_media_to_tagged_user= is_paid_d[0]
                taggeduserAll = self.tagegedUser(postrow,taggeduserAll)

                if taggeduserAll:
                    taggeduserAll=set(taggeduserAll)
                    tageeduser = ",".join(taggeduserAll)
                if intsta_tags:
                    postcat = self.categoryTags(intsta_tags)

                #imagesc = self.GetMediaCheck(intsta_shortcode,insta_image)
                imagesc = 0
                f3 = round(time.time())       
                            

        addpost = {'is_paid_partnership':is_paid_partnership,'edge_media_to_tagged_user':edge_media_to_tagged_user,'edge_liked_by':edge_liked_by,'video_duration':video_duration,'product_type':product_type,'video_view_count':video_view_count,'video_play_count':video_play_count,'intsta_media_user':intsta_media_user,
        'intsta_post_like':intsta_post_like,
        'intsta_shortcode':intsta_shortcode,
        'insta_etc1':insta_etc1,
        'insta_etc2':insta_etc2,
        'insta_image':insta_image,
        'postId':postId,
        'insta_image_large':insta_image_large,
        'intsta_tags':intsta_tags,
        'intsta_taguser':tageeduser,
        'intsta_tagged':tagedUser,
        'intsta_text':intsta_text,
        'insta_user_name':insta_user_name.lower(),
        'is_video':is_video,
        'clips_music_attribution_info':clips_music_attribution_info,
        'taken_at_timestamp':taken_at_timestamp,
        'intsta_post_comment':intsta_post_comment,
        'is_loction_get':is_loction_get,
        'insta_category':postcat,
        "dateadd":f3,
        "insta_image_down":imagesc}

        addposthis = {'intsta_media_user':intsta_media_user,
        'intsta_post_like':intsta_post_like,
        'intsta_shortcode':intsta_shortcode,
        'is_video':is_video,
        'intsta_post_comment':intsta_post_comment,
        "dateadd":f3}
        if locid:
            intsta_media_location_id =locid
            intsta_media_location = locname
        #valur1 = mydb.insta_media_scrap.find_one( { "intsta_shortcode": intsta_shortcode })
        if intsta_media_location_id:
            addpost.update({"intsta_media_location":intsta_media_location})
            addpost.update({"intsta_media_location_id":intsta_media_location_id})
        addpost.update({"city_name":''})
        addpost.update({"country_code":''})
        intsta_media_user = intsta_post_comment = intsta_post_like = intsta_shortcode = insta_etc1 = intsta_media_location = intsta_media_location_id = insta_etc2 = insta_image = insta_image_large = intsta_tags = intsta_text = insta_user_name = username = is_video = taken_at_timestamp = ''
        return addpost,addposthis        ###print('')       


    def search_User_Barnd(self,taggedUser,redis_dbw,SponserTageed=[]):
        redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
        tagedSpo =SponserTageed
        brand_data =redis_db.get('brand')
        brand_data = json.loads(brand_data)
        if taggedUser:
            for mtg in taggedUser:
                ###print(mtg)
                if mtg in brand_data:
                    SponserTageed.append(mtg)
                    if mtg in tagedSpo:
                        tagedSpo.remove(mtg)
        return SponserTageed,tagedSpo;                 



    def get_media_new_search(self,user,username,mydb='',tagedandhash = [],SponserTageed=[],ltcid=[],postIds=[],profileLocation={},insertLive=[],namef=0):
        #print(namef)
        if namef==0:
            ##print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
            username = user['username']
        ##print(user)
         
        userdta = self.__query_media_scrape(user,'')
        ##print("888888888888888888888888888888888888888888888")
        postcategory  =  ''
        historylist=[]
        ctlist=''
        totallike =0
        totalcomment =0
        totalpost = 0
        
        allImageText=[]
        sxdf=0
        for postrow in userdta[0]:
            sxdf=sxdf+1
            #print(sxdf)

            totalpost=totalpost+1
            print
            rtnytop = self.get_media_insert_new(postrow,username,mydb,'','',1)
            rtny = rtnytop[0]
            if rtny['postId']:
                postidV=rtny['postId']
                ##print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                if postidV not in postIds:
                    if 'postId' in rtny:
                        postIds.append(rtny['postId'])
                    ##print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")    
                    if 'intsta_post_comment' in rtny:
                        totalcomment=totalcomment+int(rtny['intsta_post_comment'])

                    if 'intsta_post_like' in rtny:
                        totallike=totallike+int(rtny['intsta_post_like'])
                    taken_at_timestamp = ''    
                    if 'taken_at_timestamp' in rtny:
                        taken_at_timestamp=taken_at_timestamp
                        
                    if 'insta_category' in rtny:
                        if rtny['insta_category']:
                            ctlist =ctlist+str(',')+str(rtny['insta_category'])
                    if 'intsta_media_location_id' in rtny:
                        if rtny['intsta_media_location_id']:
                            if rtny['intsta_media_location_id'] not in ltcid:
                                profileLocation.update({rtny['intsta_media_location_id']:{"count":1,"id":rtny['intsta_media_location']}})
                                ltcid.append(rtny['intsta_media_location_id'])
                            else:
                                locatod=profileLocation[rtny['intsta_media_location_id']]
                                countv=locatod['count']+1
                                profileLocation.update({rtny['intsta_media_location_id']:{"count":countv,"id":rtny['intsta_media_location']}})
                                
                    if self.PSID:
                        if rtny['intsta_shortcode']==self.PSID:
                            allImageText.append(str(rtny['intsta_shortcode'])+'-:::-'+str(rtny['insta_image_large']))
                        s=1
                    else:
                        allImageText.append(str(rtny['intsta_shortcode'])+'-:::-'+str(rtny['insta_image_large']))
                    ##print("dddddddddd--------22222244444444444444------------------------------------")
                        
                    rtnyhis = rtnytop[1]
                    historylist.append(rtnyhis)
                    insertLive.append(rtny)
                    ###print(rtny['intsta_taguser'])
                    if 'edge_media_to_tagged_user' in rtny:
                        for mu in rtny['edge_media_to_tagged_user']:
                            if mu not in SponserTageed:
                                SponserTageed.append(mu)

                    if rtny['intsta_taguser']:
                        tagedandhash.append({"taken_at_timestamp":rtny['taken_at_timestamp'],"intsta_shortcode":rtny['intsta_shortcode'],"intsta_taguser":rtny['intsta_taguser'],"intsta_tags":rtny['intsta_tags']})
                resultcat = ''

            if ctlist:
                taggeduserl = ctlist.split(",")
                dictOfElems = dict(Counter(taggeduserl))
                dictOfElemsD = {}
                if dictOfElems:
                    dictOfElemsD = sorted((value, key) for (key,value) in dictOfElems.items())
                finct = []
                if dictOfElemsD:
                    for m in list(reversed(list(dictOfElemsD)))[0:8]:
                        finct.append(m[1])
                resultcat = ",".join(finct)
                finct = []
                dictOfElemsD = {}
                taggeduserl = dictOfElems = ''
            #print("")    

 
        return  insertLive,tagedandhash,totalpost,totalcomment,totallike,SponserTageed,postIds,allImageText,profileLocation,ltcid

    def search_User_Tagged(self,taggedUser,mydb):
        valur1 = mydb.profile_tagged_post_new.find({"tagged":{"$in":taggedUser}})
        ###print(taggedUser)
        tageduser ={}
        tageduser22 ={}
        TagTime =[]
        finaltageed={}
        import  datetime
        try:
            for m in valur1:
                #TagTime =[]

                ####print(m)
                if 'taken_at_timestamp' in m:
                    postId =PostId = m['postid']

                    tTimeSt = m['taken_at_timestamp']
                    #tTimeSt = datetime.datetime.fromtimestamp(tTimeSt2).date()

                    d=t= m['tagged']
                    fr = d+'_'+str(tTimeSt)

                    if fr in finaltageed:
                        ko = finaltageed[fr]
                        for nj in postId:
                            if nj not in ko:
                                ko.append(nj)
                        finaltageed.update({fr:ko})        
                    else:
                        finaltageed.update({fr:postId})
                        ####print("Not p")
        except Exception as er:
            opi=1                       
        ####print(finaltageed)
        return finaltageed;

    def search_User_Scrape_Tagged_Post(self,userliveid,data,userName,mydb,SponserTageed=[]):
        import  datetime
        #####print("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ")
        #print(SponserTageed)
        #####print("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ")
        traeendUsr = []
        traeendUsrTime = {}

        traeendUsrAAAs= []
        tagedDict = {}
        tagedDictFinal = {}
        listTimeFinal = {}
        import time
        seconds = time.time()
        postData =[]
        #####print("8888888888888888888***********************")
        firstHit = int(round(time.time() * 1000))
        f1 = round(time.time() * 1000)
        if userliveid==1:
            for mt in data:
                postId = mt['intsta_shortcode']
                tageed = mt['intsta_taguser']
                printTy= mt['taken_at_timestamp']
                if printTy:
                    try:
                        date_time_string = str(datetime.datetime.fromtimestamp(printTy).date())
                        datetime_object = datetime.datetime.strptime(date_time_string, '%Y-%m-%d')

                        tTimeSt = taken_at_timestamp = int(time.mktime(datetime_object.timetuple()))
                    except:
                        we=1
                        ####print("-----------------------------------------------&&&&")
                        tTimeSt =taken_at_timestamp = datetime.datetime.fromtimestamp(printTy)                       
                    #####print("*********")
                    #####print(tTimeSt)
                    #####print("*********")
                    if tageed:
                        ###print(tageed)
                        tageedl = tageed.split(',')
                        for d in tageedl:
                            if d not in traeendUsr:
                                traeendUsr.append(d)
                            tmfr = d+"_"+str(tTimeSt)
                            listTimeFinal.update({tmfr:[tTimeSt,d]})
                            if tmfr in tagedDictFinal:
                                finalpst = tagedDictFinal[tmfr]
                                if postId not in finalpst:
                                    finalpst.append(postId)
                                tagedDictFinal.update({tmfr:finalpst})    
                                u=1
                            else:
                                TagTime=[postId]
                                tagedDictFinal.update({tmfr:TagTime})
                                

        f2 = round(time.time() * 1000)


        if traeendUsr:
            ##print(traeendUsr)

            fgy =0
            bulkinsert = []
            #####print("----------------------QQQQQQQQQQQQQQQQQQQQ")
            #####print(tagedDict)
            #####print("----------------------QQQQQQQQQQQQQQQQQQQQ")
            f3 = round(time.time() * 1000) 
            allPo = self.search_User_Tagged(traeendUsr,mydb)
            f4 = round(time.time() * 1000)
            fd4 = int(f2)-int(f1)
            fd5 = int(f3)-int(f4)

            messsagevar="Profile:"+str(userName)+' First Filter'+str(fd4)+' Query Excute'+str(fd5)+" Query "+str({"tagged":{"$in":traeendUsr}})
            ###print(listTimeFinal)
            self.logcreate(messsagevar,'queryTime')

            

            bulk = mydb.profile_tagged_post_new.initialize_unordered_bulk_op()
            #####print(kkkkkkkkkkkkkkk)
            SponserTageedL= self.search_User_Barnd(traeendUsr,mydb,SponserTageed)
            #print(SponserTageedL)

            #####print(kkkkkkkkkkkkkkk)
            SponserTageed=SponserTageedL[0]
            NewTageed=SponserTageedL[1]

            for kl in listTimeFinal:
                if kl in allPo:
                    postIdold=allPo[kl]
                    NewPost = tagedDictFinal[kl]
                    fLppst = postIdold + list(set(NewPost) - set(postIdold))
                    bulk.find( { 'tagged':listTimeFinal[kl][1],"taken_at_timestamp":listTimeFinal[kl][0]}).update({ "$set": {"postid" :fLppst}})
                    fgy =1
                    #####print(allPo[kl])
                    #####print(listTimeFinal[kl][1])
                else:
                    NewPost = tagedDictFinal[kl]
                    bulkinsert.append({"postid" : NewPost, "tagged" : listTimeFinal[kl][1], "inserttime" :firstHit,"taken_at_timestamp":listTimeFinal[kl][0]})
                
               

            ####print("----------------------")
            #####print(allPo)
            if fgy==1:
                ####print("999999999")
                bulk.execute()
            bulk = ''    
            if bulkinsert:
                ###print(bulkinsert)
                mydb.profile_tagged_post_new.insert(bulkinsert)
            if SponserTageed:
                bulk = mydb.profile_tagged_post_new.initialize_unordered_bulk_op()
                for ty in SponserTageed:
                     bulk.find( { 'tagged':ty}).update({ "$set": {"brand" :1}})
                bulk.execute()
                bulk =''
            if NewTageed:
                bulkne=[]
                #bulkp = mydb.inf_insta_profile.initialize_unordered_bulk_op()
                for e in NewTageed:
                    mydb.inf_insta_profile.update({"insta_user_name":e,"profileUp":1},{"$set":{"profileUp":1,'gender':'brand'}})
                    #bulkp.find({"insta_user_name":e}).update({ "$set": {"profileUp":1,'gender':'brand'}})
                    bulkne.append({"name" : e})
                mydb.brand.insert(bulkne)
                #bulkp.execute()           

    def userAddresfilter(self,useradd,userrenderlist):
        countryv = []
        localityv = []

        for ty in useradd:
            if ty in userrenderlist:

                userrender = userrenderlist[ty]
                #####print(userrender)
                #####print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                if 'geocountry' in userrender:
                    country = userrender['geocountry']
                    if country:
                        locality = ''
                        if userrender['geocity'] and userrender['geocity']!= locality:
                            if locality:
                                locality  = locality+str(',City:')+userrender['geocity']
                            else:
                                locality  = "City:"+userrender['geocity']
                               
                        if userrender['geostate']:
                            if locality:
                                locality  = locality+str(',State:')+userrender['geostate']
                            else:
                                locality  = "State:"+userrender['geostate']

                        if userrender['geocountry']:
                            if locality:
                                locality  = locality+str(',Country:')+userrender['geocountry']
                            else:
                                locality  = "Country:"+userrender['geocountry']



                               
                        if country not in countryv:
                            countryv.append(country)
                        if locality:
                            localityv.append(locality)

        countryv = ",".join(countryv)
        localityv = "-".join(localityv)
        return  countryv,localityv    
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

                
    def get_media_insertReels(self,postrow,db='',locname='',locid=''):
          ####print(postrow)####print("kkkkkkkkkkkkk")
          ##print(1)####print(instamediapost)
          ##print("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
          intsta_media_user = ''
          intsta_post_comment = ''
          intsta_post_like = ''
          intsta_shortcode = ''
          insta_etc1 = ''
          intsta_media_location = ''
          intsta_media_location_id = ''
          insta_etc2 = ''
          insta_image = ''
          insta_image_large = ''
          intsta_tags = ''
          intsta_text = ''
          insta_user_name = ''
          is_video = 0
          taken_at_timestamp = ''
          is_loction_get = 1
          tageeduser = ''
          intsta_tags = ''
          video_play = 0
          video_view = 0
          video_duration = 0
          is_paid_partnership = ''
          branded_content_tag_info = ''
          clips_metadata = {}
          PostId=0
          postIds=[]
          if locname:
              is_loction_get =2

          if 'like_count' in postrow:
              intsta_post_like = postrow['like_count']
              #print(intsta_post_like)

          if 'clips_metadata' in postrow:
              clips_metadata = postrow['clips_metadata']    
          if 'pk' in postrow:
              PostId = postrow['pk']
              postIds.append(PostId)  
          if 'comment_count' in postrow:
              intsta_post_comment = postrow['comment_count']
              #print(intsta_post_comment)
          if 'user' in postrow:
              intsta_media_user = postrow['user']['pk']
              insta_user_name = postrow['user']['username']
          if 'code' in postrow:
              intsta_shortcode = postrow['code']
          if 'play_count' in postrow:
              video_play = postrow['play_count']
              
          if 'view_count' in postrow:
              video_view = postrow['view_count']

          if 'video_duration' in postrow:
              video_duration = postrow['video_duration']    
          if 'is_paid_partnership' in postrow:
              is_paid_partnership = postrow['is_paid_partnership']
          if 'clips_metadata' in postrow:
              branded_content_tag_info = postrow['clips_metadata']['branded_content_tag_info']               
          if 'location' in postrow:
              if postrow['location']:
                  dd = 1

                  intsta_media_location_id = postrow['location']['pk']
                  intsta_media_location = postrow['location']['name']
          if 'urls' in postrow:
              insta_etc2 = postrow['urls']

          is_video = 1


          if 'taken_at' in postrow:
              taken_at_timestamp = postrow['taken_at']

          if 'image_versions2' in postrow:
              insta_image = postrow['image_versions2']['candidates'][0]['url']
              insta_image_large = postrow['image_versions2']['candidates'][0]['url']


          if 'caption' in postrow:
              ####print(postrow['caption'])
              if postrow['caption']:
                if 'text' in postrow['caption']:
                    intsta_text = postrow['caption']['text']
                    alltagshash = re.findall(r"(?<!&)#(\w+|(?:[\xA9\xAE\u203C\u2049\u2122\u2139\u2194-\u2199\u21A9\u21AA\u231A\u231B\u2328\u2388\u23CF\u23E9-\u23F3\u23F8-\u23FA\u24C2\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE\u2600-\u2604\u260E\u2611\u2614\u2615\u2618\u261D\u2620\u2622\u2623\u2626\u262A\u262E\u262F\u2638-\u263A\u2648-\u2653\u2660\u2663\u2665\u2666\u2668\u267B\u267F\u2692-\u2694\u2696\u2697\u2699\u269B\u269C\u26A0\u26A1\u26AA\u26AB\u26B0\u26B1\u26BD\u26BE\u26C4\u26C5\u26C8\u26CE\u26CF\u26D1\u26D3\u26D4\u26E9\u26EA\u26F0-\u26F5\u26F7-\u26FA\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721\u2728\u2733\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763\u2764\u2795-\u2797\u27A1\u27B0\u27BF\u2934\u2935\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55\u3030\u303D\u3297\u3299]|\uD83C[\uDC04\uDCCF\uDD70\uDD71\uDD7E\uDD7F\uDD8E\uDD91-\uDD9A\uDE01\uDE02\uDE1A\uDE2F\uDE32-\uDE3A\uDE50\uDE51\uDF00-\uDF21\uDF24-\uDF93\uDF96\uDF97\uDF99-\uDF9B\uDF9E-\uDFF0\uDFF3-\uDFF5\uDFF7-\uDFFF]|\uD83D[\uDC00-\uDCFD\uDCFF-\uDD3D\uDD49-\uDD4E\uDD50-\uDD67\uDD6F\uDD70\uDD73-\uDD79\uDD87\uDD8A-\uDD8D\uDD90\uDD95\uDD96\uDDA5\uDDA8\uDDB1\uDDB2\uDDBC\uDDC2-\uDDC4\uDDD1-\uDDD3\uDDDC-\uDDDE\uDDE1\uDDE3\uDDEF\uDDF3\uDDFA-\uDE4F\uDE80-\uDEC5\uDECB-\uDED0\uDEE0-\uDEE5\uDEE9\uDEEB\uDEEC\uDEF0\uDEF3]|\uD83E[\uDD10-\uDD18\uDD80-\uDD84\uDDC0]|(?:0\u20E3|1\u20E3|2\u20E3|3\u20E3|4\u20E3|5\u20E3|6\u20E3|7\u20E3|8\u20E3|9\u20E3|#\u20E3|\\*\u20E3|\uD83C(?:\uDDE6\uD83C(?:\uDDEB|\uDDFD|\uDDF1|\uDDF8|\uDDE9|\uDDF4|\uDDEE|\uDDF6|\uDDEC|\uDDF7|\uDDF2|\uDDFC|\uDDE8|\uDDFA|\uDDF9|\uDDFF|\uDDEA)|\uDDE7\uD83C(?:\uDDF8|\uDDED|\uDDE9|\uDDE7|\uDDFE|\uDDEA|\uDDFF|\uDDEF|\uDDF2|\uDDF9|\uDDF4|\uDDE6|\uDDFC|\uDDFB|\uDDF7|\uDDF3|\uDDEC|\uDDEB|\uDDEE|\uDDF6|\uDDF1)|\uDDE8\uD83C(?:\uDDF2|\uDDE6|\uDDFB|\uDDEB|\uDDF1|\uDDF3|\uDDFD|\uDDF5|\uDDE8|\uDDF4|\uDDEC|\uDDE9|\uDDF0|\uDDF7|\uDDEE|\uDDFA|\uDDFC|\uDDFE|\uDDFF|\uDDED)|\uDDE9\uD83C(?:\uDDFF|\uDDF0|\uDDEC|\uDDEF|\uDDF2|\uDDF4|\uDDEA)|\uDDEA\uD83C(?:\uDDE6|\uDDE8|\uDDEC|\uDDF7|\uDDEA|\uDDF9|\uDDFA|\uDDF8|\uDDED)|\uDDEB\uD83C(?:\uDDF0|\uDDF4|\uDDEF|\uDDEE|\uDDF7|\uDDF2)|\uDDEC\uD83C(?:\uDDF6|\uDDEB|\uDDE6|\uDDF2|\uDDEA|\uDDED|\uDDEE|\uDDF7|\uDDF1|\uDDE9|\uDDF5|\uDDFA|\uDDF9|\uDDEC|\uDDF3|\uDDFC|\uDDFE|\uDDF8|\uDDE7)|\uDDED\uD83C(?:\uDDF7|\uDDF9|\uDDF2|\uDDF3|\uDDF0|\uDDFA)|\uDDEE\uD83C(?:\uDDF4|\uDDE8|\uDDF8|\uDDF3|\uDDE9|\uDDF7|\uDDF6|\uDDEA|\uDDF2|\uDDF1|\uDDF9)|\uDDEF\uD83C(?:\uDDF2|\uDDF5|\uDDEA|\uDDF4)|\uDDF0\uD83C(?:\uDDED|\uDDFE|\uDDF2|\uDDFF|\uDDEA|\uDDEE|\uDDFC|\uDDEC|\uDDF5|\uDDF7|\uDDF3)|\uDDF1\uD83C(?:\uDDE6|\uDDFB|\uDDE7|\uDDF8|\uDDF7|\uDDFE|\uDDEE|\uDDF9|\uDDFA|\uDDF0|\uDDE8)|\uDDF2\uD83C(?:\uDDF4|\uDDF0|\uDDEC|\uDDFC|\uDDFE|\uDDFB|\uDDF1|\uDDF9|\uDDED|\uDDF6|\uDDF7|\uDDFA|\uDDFD|\uDDE9|\uDDE8|\uDDF3|\uDDEA|\uDDF8|\uDDE6|\uDDFF|\uDDF2|\uDDF5|\uDDEB)|\uDDF3\uD83C(?:\uDDE6|\uDDF7|\uDDF5|\uDDF1|\uDDE8|\uDDFF|\uDDEE|\uDDEA|\uDDEC|\uDDFA|\uDDEB|\uDDF4)|\uDDF4\uD83C\uDDF2|\uDDF5\uD83C(?:\uDDEB|\uDDF0|\uDDFC|\uDDF8|\uDDE6|\uDDEC|\uDDFE|\uDDEA|\uDDED|\uDDF3|\uDDF1|\uDDF9|\uDDF7|\uDDF2)|\uDDF6\uD83C\uDDE6|\uDDF7\uD83C(?:\uDDEA|\uDDF4|\uDDFA|\uDDFC|\uDDF8)|\uDDF8\uD83C(?:\uDDFB|\uDDF2|\uDDF9|\uDDE6|\uDDF3|\uDDE8|\uDDF1|\uDDEC|\uDDFD|\uDDF0|\uDDEE|\uDDE7|\uDDF4|\uDDF8|\uDDED|\uDDE9|\uDDF7|\uDDEF|\uDDFF|\uDDEA|\uDDFE)|\uDDF9\uD83C(?:\uDDE9|\uDDEB|\uDDFC|\uDDEF|\uDDFF|\uDDED|\uDDF1|\uDDEC|\uDDF0|\uDDF4|\uDDF9|\uDDE6|\uDDF3|\uDDF7|\uDDF2|\uDDE8|\uDDFB)|\uDDFA\uD83C(?:\uDDEC|\uDDE6|\uDDF8|\uDDFE|\uDDF2|\uDDFF)|\uDDFB\uD83C(?:\uDDEC|\uDDE8|\uDDEE|\uDDFA|\uDDE6|\uDDEA|\uDDF3)|\uDDFC\uD83C(?:\uDDF8|\uDDEB)|\uDDFD\uD83C\uDDF0|\uDDFE\uD83C(?:\uDDF9|\uDDEA)|\uDDFF\uD83C(?:\uDDE6|\uDDF2|\uDDFC))))[\ufe00-\ufe0f\u200d]?)+",intsta_text, re.UNICODE)
                    taggeduserAll = re.findall(r"@(\w+)", intsta_text)
                    if taggeduserAll:
                        tageeduser = ",".join(taggeduserAll)
                    if alltagshash:
                        intsta_tags = self.allstrepalce(",".join(alltagshash))
                      

          postcat = ''
          tagfinv = 1
          if tageeduser:
              tagfinv = 0

          if intsta_tags:
            postcat = ''
            #postcat = self.categoryTags(intsta_tags)
            #print(intsta_shortcode)

            #print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
                              

          addpost = {'intsta_media_user':str(intsta_media_user),
          'intsta_post_like':intsta_post_like,
          'clips_music_attribution_info':clips_metadata,
          'intsta_shortcode':intsta_shortcode,
          'insta_etc1':insta_etc1,
          'insta_etc2':insta_etc2,
          'PostId':PostId,
          'insta_image':insta_image,
          'insta_image_large':insta_image_large,
          'intsta_tags':intsta_tags,
          'intsta_taguser':tageeduser,
          'intsta_text':intsta_text,
          'insta_user_name':insta_user_name,
          'is_video':is_video,
          'taken_at_timestamp':taken_at_timestamp,
          'intsta_post_comment':intsta_post_comment,
          'is_loction_get':is_loction_get,
          'insta_category':postcat,
          'tagfind':tagfinv, 
          'product_type':'clips',
          'video_view':video_view,
          'video_play':video_play,
          'video_view_count':video_view,
          'video_play_count':video_play,
          'isdvtype':1,
          'is_paid_partnership':is_paid_partnership,
          'video_duration':video_duration,
          'branded_content_tag_info':branded_content_tag_info,      
          "dateadd":round(time.time()),
          "insta_image_down":0}


          addposthis = {'intsta_media_user':intsta_media_user,
          'intsta_post_like':intsta_post_like,
          'intsta_shortcode':intsta_shortcode,
          'is_video':is_video,
          'intsta_post_comment':intsta_post_comment,
          "dateadd":round(time.time())}
          if locid:
              intsta_media_location_id =locid
              intsta_media_location = locname
          #valur1 = mydb.insta_media_scrap.find_one( { "intsta_shortcode": intsta_shortcode })
          if intsta_media_location_id:
              addpost.update({"intsta_media_location":intsta_media_location})
              addpost.update({"intsta_media_location_id":intsta_media_location_id})
          addpost.update({"city_name":''})
          addpost.update({"country_code":''})
          intsta_media_user = intsta_post_comment = intsta_post_like = intsta_shortcode = insta_etc1 = intsta_media_location = intsta_media_location_id = insta_etc2 = insta_image = insta_image_large = intsta_tags = intsta_text = insta_user_name = username = is_video = taken_at_timestamp = ''
          ##print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
          return addpost,addposthis,postIds


    def profilePostWithLike(self):
        my_list = self.username

        returnData=self.scrape_profile_simmler(my_list)
        return returnData

    def reelscrapeLive(self,erd,max_id='',tagedandhash = [],SponserTageed=[],ltcid=[],postIds=[],profileLocation={},insrtlist=[]):
        #print("ddddddddddddwwwwwwwwwwwwwwwwwwddd")
        url = "https://i.instagram.com/api/v1/clips/user/"
        import string  
        S = 2
        ran = ''.join(random.choices(string.ascii_uppercase + string.digits, k = S))
        proxieslt =proxieslt = {"http":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321'),"https":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321')}
        
        headers = {}
        headers2 = {}
        agentlist=[{"User-Agent":'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0','csrf':'B5CSul4A1ClgmhPd86NqtKrL1BL9AFtJ'},
        {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36','csrf':'4yFg9F2J3yYqiEcdp5JdPY2a9r8bFURT'},
        {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36','csrf':'mtxeoQBgpZqhvVoxaZec4Kwdysqnhga4T'},
        {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57','csrf':'qaahJCTzaUwtNyxrbh9RBkBNUUuQruihs'}]
       
        rand_idx = random.randrange(len(agentlist))
        random_num = agentlist[rand_idx]
        headers.update({'User-Agent':random_num['User-Agent']})

        login_url = "https://www.instagram.com/api/v1/web/accounts/login/ajax/"
        ck = requests.get(login_url,headers=headers)
        crsf = ck.cookies['csrftoken']
        print(crsf)

        headers.update({'User-Agent':random_num['User-Agent']})
        headers.update({'X-CSRFToken':crsf})
        headers.update({'X-Instagram-AJAX':'1007579283'})
        headers.update({'X-IG-App-ID':'936619743392459'})
        ###print(erd)
        finlst=[]
        finlstNd=[]
        allImageText=[]
        insrtlist=[]
        allPostId =[]
        totallike=0
        totalcomment=0
        totalview=0
        totalplay=0
        postcmnt = 0
        postlike = 0
        vdplay = 0
        totalply = 0
        totalpost=0

        profileIds=[]
        #bulk = db.allmail.initialize_unordered_bulk_op()
        blk=0
        allImageText=[]

        for idfg in erd:
          ##print({'target_user_id':idfg,"page_size":12,"max_id":''})
          try:
            firstHit = round(time.time())
            r = requests.post(url,headers=headers, timeout=10,proxies=proxieslt,data = {'target_user_id':idfg,"page_size":12,"max_id":max_id})
            #print(r.text)
            try:
              erdata = json.loads(r.text)
              if 'message' in erdata:
                print(erdata)
              totalpost=0
              totallike=0
              totalcomment=0
              totalview=0
              totalplay=0
              postcmnt = 0
              pdata = ''
              liveeng = 0
              rtny=[]
              profileIds=[]
              
              allPostId =[]
              UserName=''
              if erdata['items']:

                max_id=''
                if 'max_id' in erdata['paging_info']:
                  max_id =erdata['paging_info']['max_id']
                ###print(max_id)
                profiledate=[]
                for postrow in erdata['items']:
                    ##print(postrow)

                    UserName=postrow['media']['user']['username']
                    ##print(postrow)

                    #insrtlist.append(rtny)
                    sdf = self.get_media_insertReels(postrow['media'])
                    ##print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                    postData = sdf[0]
                    profileIds.append(postData['PostId'])
                    ##print(sdf[2])
                    ##print(postData)
                  
                    insrtlist.append(postData)
                    totalpost = int(totalpost)+1
                    postcmnt = 0
                    postlike = 0
                    vdplay = 0
                    totalply = 0
                    ###print(str(postData['intsta_shortcode'])+'-:::-'+str(postData['insta_image_large']))
                    allPostId.append(postData['intsta_shortcode'])
                    allImageText.append(str(postData['intsta_shortcode'])+'-:::-'+str(postData['insta_image_large']))
                    if postData['intsta_post_like']:
                        totallike =int(totallike)+int(postData['intsta_post_like'])
                        postlike =int(postData['intsta_post_like'])

                    if 'intsta_media_location_id' in postData:
                        if postData['intsta_media_location_id']:
                            if postData['intsta_media_location_id'] not in ltcid:
                                profileLocation.update({postData['intsta_media_location_id']:{"count":1,"id":postData['intsta_media_location']}})
                                ltcid.append(postData['intsta_media_location_id'])
                            else:
                                locatod=profileLocation[postData['intsta_media_location_id']]
                                countv=locatod['count']+1
                                profileLocation.update({postData['intsta_media_location_id']:{"count":countv,"id":postData['intsta_media_location']}})

                                

                    if 'edge_media_to_tagged_user' in postData:
                        for mu in postData['edge_media_to_tagged_user']:
                            if mu not in SponserTageed:
                                SponserTageed.append(mu)

                    if postData['intsta_taguser']:
                        ##print(postData['intsta_taguser'])
                        tagedandhash.append({"taken_at_timestamp":postData['taken_at_timestamp'],"intsta_shortcode":postData['intsta_shortcode'],"intsta_taguser":postData['intsta_taguser'],"intsta_tags":postData['intsta_tags']})             



                    if postData['intsta_post_comment']:
                        totalcomment =int(totalcomment)+int(postData['intsta_post_comment'])
                        postcmnt =int(postData['intsta_post_comment'])
                    if postData['video_play']:
                        totalview =int(totalview)+int(postData['video_play'])
                        vdplay = int(postData['video_play'])
                    if postData['video_view']:
                        totalplay =int(totalplay)+int(postData['video_view'])
                        totalvew  = postData['video_view']
                    pdata=str(pdata)+str(postData['insta_image'])+"-k-k-k-k-k-"+str(vdplay)+"-k-k-k-k-k-"+str(postcmnt)+"-k-k-k-k-k-";

                    firstHitw = round(time.time())
                    postimage=''     
                    if 'insta_image_large' in postData:
                        postimage=postData['insta_image_large'].split("?")
                        postimage=postimage[0].split(".")[-1]
                    profiledate.append({"v":postData['video_view'],"pl":postData['video_play'],"p":postData['intsta_shortcode'],'l':postData['intsta_post_like'],'c':postData['intsta_post_comment'],'i':postimage,'t':postData['taken_at_timestamp']})
                    blk=1
                #print(totalview)
                #print(totalplay) 
                #print("UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU")   

            except Exception as er:
                l=1
                opi=1
              ###print("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
              ###print(idfg['cnt_user'])
              #opi=1
              ###print("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
          except Exception as er:
            m=1
            ##print("-----------3333333333333333333333333")
            opi=1
            m=1
        if blk==1:
            op=1
            #bulk.execute()


        
        finalstats={"totallike":totallike,"postcmnt":totalcomment,'totalview':totalview,'totalplay':totalplay,'totalpost':totalpost}                
            

  
        return insrtlist,profileIds,finalstats,tagedandhash,SponserTageed,profileLocation,ltcid,allImageText
    
    def profilepostData(self,uname,end_cursor,tagedandhash,SponserTageed,ltcid,postUserId,profileLocation,insertPost,pstuname=1,totalpost=0,totalcomment=0,totallike=0,allImageText=''):
    
        try:
            proxieslt =proxieslt = {"http":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321'),"https":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321')}
            import requests

            headers = {}
            headers2 = {}
            agentlist=[{"User-Agent":'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0','csrf':'B5CSul4A1ClgmhPd86NqtKrL1BL9AFtJ'},
            {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36','csrf':'4yFg9F2J3yYqiEcdp5JdPY2a9r8bFURT'},
            {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36','csrf':'mtxeoQBgpZqhvVoxaZec4Kwdysqnhga4T'},
            {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57','csrf':'qaahJCTzaUwtNyxrbh9RBkBNUUuQruihs'}]
           
            rand_idx = random.randrange(len(agentlist))
            random_num = agentlist[rand_idx]
            headers.update({'User-Agent':random_num['User-Agent']})

            login_url = "https://www.instagram.com/api/v1/web/accounts/login/ajax/"
            ck = requests.get(login_url,headers=headers)
            crsf = ck.cookies['csrftoken']

            headers = {
            'authority': 'www.instagram.com',
            'cache-control': 'max-age=0',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Cookie': f'ig_did=D2AEADB8-4FE2-4A77-813C-5825B9DA198B; csrftoken={crsf}; mid=YCu8eQAEAAF_M7sbdCHbFUXixn3e; ig_nrcb=1',
            'Content-Type': 'text/plain'
            }

            # login_url = "https://www.instagram.com/api/v1/web/accounts/login/ajax/"
            # ck = requests.get(login_url,headers=headers)
            # crsf = ck.cookies['csrftoken']

            if end_cursor:
                nameUrl = 'https://www.instagram.com/graphql/query/?query_hash=ea4baf885b60cbf664b34ee760397549&variables={"id"%3A"'+uname+'"%2C"first"%3A50%2C"after"%3A"'+end_cursor+'"}'
            else:
                nameUrl = 'https://www.instagram.com/graphql/query/?query_hash=ea4baf885b60cbf664b34ee760397549&variables={"id"%3A"'+uname+'"%2C"first"%3A100}'
            
            # pdb.set_trace()

            response = requests.request("GET", nameUrl, headers=headers,proxies=proxieslt, timeout=30)
            ##print(response.text)
            user_info = json.loads(response.text)['data']['user']
            end_cursor = user_info['edge_owner_to_timeline_media']['page_info']['end_cursor']

            ##print(user_info)
            ##print(user_infoq)
            if user_info:
                ##print("ddddddddddddddddddddddddddddddddddddddd")
              
                livedatapost = self.get_media_new_search(user_info,uname,'',tagedandhash,SponserTageed,ltcid,postUserId,profileLocation,insertPost,pstuname)
                ##print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$--------------------------------------------------")


                insertPost=livedatapost[0]
                tagedandhash=livedatapost[1]
                totalpost=totalpost+livedatapost[2]
                totalcomment=totalcomment+livedatapost[3]
                totallike=totallike+livedatapost[4]
                SponserTageed=livedatapost[5]

                postUserId=livedatapost[6]

                profileLocation=livedatapost[8]
                ltcid=livedatapost[9]
                allImageText=livedatapost[7]
                postsetIncache=1
                if end_cursor and totalpost<200:
                    return self.profilepostData(uname,end_cursor,tagedandhash,SponserTageed,ltcid,postUserId,profileLocation,insertPost,1,totalpost,totalcomment,totallike,allImageText)

                #   #print("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYY666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666")
                                               

        except Exception as er:
            print(er)
            opi=1
        return profileLocation,insertPost,tagedandhash,totalpost,totalcomment,totallike,SponserTageed,postUserId,ltcid,allImageText   


    def login_insta(self,username,password):
        from datetime import datetime
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
            random_number = random.randrange(1,5) 
            time.sleep(random_number)
            # proxieslt =proxieslt = {"http":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321'),"https":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321')}
            res = requests.post(login_url,data=payload,headers=headers,proxies=self.proxieslt)
            # res = requests.post(login_url,data=payload,headers=headers)

            # print(res)

            print("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
            return res
        except Exception as e:
            logging.error(f"Error in login_insta function :- {e}")



    def ConnectLocalDB(self):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        return client



    def GetLIkes(self,AllPostIDS=[],LogingCookies=""):


        MyclientConnect = self.ConnectLocalDB()
        mydbGet = MyclientConnect["influncer"]
        PostIdsStoreGet = mydbGet["UserIdsLikes"]

        mydb = MyclientConnect["influncer"]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'X-CSRFToken': "",
            'X-IG-App-ID': '936619743392459',
            'X-ASBD-ID': '129477',
            'X-IG-WWW-Claim': 'hmac.AR3j75b5XiHq6RpE4jyiLHDXPGp4ZW8PyOjqOFCcZAYCJFMN',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            # 'Referer': f'{PostUrl}',
            'Referer':"https://www.instagram.com/p/CtrNo7At_OL/",
            # 'Cookie': 'ig_did=E8E2C977-E665-4EEB-B128-6D8D92A85228; datr=xKmkYzg_H2hB218oV81-7V7g; mid=Y6SpzwAEAAFEyKlch75tbOOP3puO; ig_nrcb=1; fbm_124024574287414=base_domain=.instagram.com; shbid="16489\\05414816613506\\0541718183157:01f78423e95aebfda20f2b631032b72e8937638fdc5a420ca4e7327b8da2a349fcd45866"; shbts="1686647157\\05414816613506\\0541718183157:01f796dbfe56bdc11597d892cba4fe5dddbd8c5b3f1a909eb815de68a7351699d21b2cec"; rur="NAO\\05459910281066\\0541718344060:01f77ec9dddea911366c76e43ec99f35c54883325777ec2b4ee9190da7b06ad68a575594"; sessionid=59910281066%3ARTpXo4Qu7Uvvwo%3A17%3AAYcCNWGy7_6dFP_9FrpD3nI-gln8I8KZk-IzScX80Q; ds_user_id=59910281066; csrftoken=FiNRRKhMpxDBSdptzIwEJ1JAynmDsuW3',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            # Requests doesn't support trailers
            # 'TE': 'trailers',
        }

        for AllPostid in AllPostIDS:
            if AllPostid.get("UserId"):
                FetchUserId  = AllPostid["UserId"]
                PostId = AllPostid["PostId"]
                collection = mydb["InstaLikesTable"]
                try:
                    header = Headers(browser="firfox",os="mac",headers=True)
                    RandomHeader = header.generate()
                    headers.update({'User-Agent':RandomHeader["User-Agent"]})
                    headers["X-CSRFToken"] = LogingCookies["csrftoken"]
                    headers["Referer"] = "https://www.instagram.com/p/{}/".format(PostId)
                    try:
                        # Without proxy 
                        response = requests.get(self.GetLIkes_.format(PostId), cookies=LogingCookies, headers=headers)
                    except:
                        # With Proxy
                        response = requests.get(self.GetLIkes_.format(PostId), cookies=LogingCookies, headers=headers,proxies=self.proxieslt)

                    if response.status_code == 200:
                        json_data = response.json()
                        LinksCount = json_data.get("users")
                        print("Number of counts Likes :- {}".format(LinksCount.__len__()))
                        if LinksCount:
                            for links_count in LinksCount:
                                pk_id = links_count.get("pk_id")
                                if not collection.find_one({"pk_id":"{}".format(pk_id)}) and pk_id:
                                    item = dict()
                                    PostIdsStoreGet.update_one({"PostId":PostId},{"$set":{"Status":"1"}})
                                    try:
                                        item["pk_id"] = links_count.get("pk_id") if links_count.get("pk_id") else ""
                                    except Exception as e:
                                        print("Error in pk_id :- {}".format(e))

                                    item["UserId"] = FetchUserId
                                    item["PostId"] = PostId
                                    item["PostIdUserID"] = "{}_{}".format(FetchUserId,item["pk_id"])
                                    try:
                                        item["username"] = links_count.get("username") if links_count.get("username") else ""
                                    except Exception as e:
                                        print("Error in username :- {}".format(e))

                                    item["Date"] = time.strftime("%Y%d%m")
                                    item["Time"] = time.strftime("%H%M%S")
                                    # print(item)
                                    print("Inserting Data in DB For GetLIkes()")
                                    collection.insert_one(item)

                                else:
                                    PostIdsStoreGet.update_one({"PostId":PostId},{"$set":{"Status":"1"}})
                                    # print(f"This id :- {pk_id} is Already Scrap and Already has in DB PL check...")

                except Exception as e:
                    pass


    def scrape_like_comment(self):
        try:
            myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            mydb = myclient["influncer"]
            PostIdsStore = mydb["UserIdsLikes"]


            print("scrape_like_comment Function is Workking printing IT :- {}".format(self.count_))
            self.count_+=1
            for GetUserId in self.userId:
                getbucketProfile = self.getBucketProfile(GetUserId)
                if getbucketProfile:
                    if not PostIdsStore.find_one({"UserId":"{}".format(GetUserId)}):
                        if getbucketProfile["postUserId"].__len__() >= 12:
                            for getbucketProfileNum in range(0,12):
                                item = dict()
                                item["UserId"] = GetUserId
                                item["PostId"] = getbucketProfile["postUserId"][getbucketProfileNum]
                                item["Status"] = "0"
                                item["Date"] = time.strftime("%Y%d%m")
                                item["Time"] = time.strftime("%H%M%S")
                                item["CommentStatus"] = "0"
                                PostIdsStore.insert_one(item)
                        elif getbucketProfile.get("postUserId"):
                            for getbucketProfileID_ in getbucketProfile["postUserId"]:
                                item = dict()
                                item["UserId"] = GetUserId
                                item["PostId"] = getbucketProfileID_
                                item["Status"] = "0"
                                item["Date"] = time.strftime("%Y%d%m")
                                item["Time"] = time.strftime("%H%M%S")
                                item["CommentStatus"] = "0"
                                PostIdsStore.insert_one(item)
                    else:
                        print("This UserID is already exist in DB :- {}".format(GetUserId))
                else:
                    print("This is no Post for this ID :- {}".format(GetUserId))


            # It is used to fetch the Post ids from DB for Likes and Comments Functions...
            MyclientConnect = self.ConnectLocalDB()
            mydb = MyclientConnect["influncer"]
            PostIdsStoreGet = mydb["UserIdsLikes"]
            data = []
            for AllPostIDS in PostIdsStoreGet.find({"Status":"0"}):
                data.append(AllPostIDS)

            CommentsData = []
            for AllPostIDS in PostIdsStoreGet.find({"CommentStatus":"0"}):
                CommentsData.append(AllPostIDS)

            if data:
                print("Calling GetLikes Function for Fetch data for Likes and Count is :- {}".format(data.__len__()))
                time.sleep(5)
                try:
                    RandomNumber = random.randint(0,self.MultipleUsers.__len__()-1)
                    user_name = self.MultipleUsers[RandomNumber]["username"]
                    pass_word = self.MultipleUsers[RandomNumber]["password"]
                except Exception as e:
                    print("Error in RandomNumber and username/password :- {}".format(e))

                res = self.login_insta(user_name,pass_word)
                LogingCookies = res.cookies.get_dict()

                n=800;              
                final = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]
                procs = []
                for ms in final:
                    procs = []
                    proc = Process(target=self.GetLIkes, args=([ms,LogingCookies]))
                    procs.append(proc)
                    dsui=proc.start()

            
            if CommentsData:
                print("Calling ScrapComments Function for Fetch data for Comments and Count is :- {}".format(CommentsData.__len__()))
                time.sleep(5)
                # It is a function which calling the Comments for every Posts
                self.ScrapComments(CommentsData)
        except Exception as e:
            print("Error in scrape_like_comment Function :- {}".format(e))




    def ScrapComments(self,CommentsData):
        try:
            try:
                RandomNumber = random.randint(0,self.MultipleUsers.__len__()-1)
                user_name = self.MultipleUsers[RandomNumber]["username"]
                pass_word = self.MultipleUsers[RandomNumber]["password"]
            except Exception as e:
                print("Error in RandomNumber and username/password :- {}".format(e))

            res = self.login_insta(user_name,pass_word)
            LogingCookies = res.cookies.get_dict()
            n=800;
            final = [CommentsData[i * n:(i + 1) * n] for i in range((len(CommentsData) + n - 1) // n )]
            procs = []
            for ms in final:
                procs = []
                proc = Process(target=self.GetComments, args=([ms,LogingCookies]))
                procs.append(proc)
                dsui=proc.start()
        except Exception as e:
            print("Error in ScrapComments Function.... :- {}".format(e))








    def InsertLikesData(self):
        print("Total Numbers of Likes :- {}".format(self.AllLikesData_.__len__()))
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["influncer"]
        Likes_ = mydb["PostLikesTable"]
        Likes_.insert_many(self.AllLikesData_, ordered=False)
        print("Data is Inserted to DB and Table name is PostLikesTable")

    def scrape_profile_post(self):
        useralldata={}
        finadata={}
        instpostbulk = []
        instpostbulkhis = []
        usergenderList = {}
        profileDisplay = []
        profileLocation = {}
        profileDisplay = []
        tagedandhash = []
        sponserDict=[]
        taggedDict=[]
        usernameDRT = self.username
        returnData=[]
        postUserId=[]
        userrender=[]
        ltcid=[]
        insertPost=[]
        reelsstats={}
        poststats={}
        cnttop=[]
        statetop=[]
        localitytop=[]
        citytop=[]
        ziptop=[]
        SponserTageed=[]
        postsetIncache=0

        for uname in usernameDRT:
            UserIds=uname
            ##print(UserIds)
            uslcid={}
            scrape=1
            # pdb.set_trace()

            getbucketProfile=self.getBucketProfile(UserIds)
            print("****&&&&&&&**********************")
            print("reelprofile :- {}".format(self.reelprofile))
            print("postprofile :- {}".format(self.postprofile))
            ##print(getbucketProfile['reelsstats'])

            ##print(kkkkllllllllllllllllllllllllllllllllllllllppppppppppppppppppppppppp)
            if self.reelprofile==1:

                try:
                    rlscp=0
                    if 'reelsstats' in getbucketProfile:
                        if getbucketProfile['reelsstats']['totallike']>0:
                            rlscp=1
                            print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                        
                    if rlscp==1:
                        print("rlscp :- @@@@@@@@@@@@@@@@@@@@@ :- {}".format(rlscp))
                        insertPost=getbucketProfile['insertPost']
                        postUserId=getbucketProfile['postUserId']
                        reelsstats=getbucketProfile['reelsstats']
                        print(reelsstats)
                        tagedandhash=getbucketProfile['tagedandhash']
                        SponserTageed=getbucketProfile['SponserTageed']
                        profileLocation=getbucketProfile['profileLocation']
                        ltcid=getbucketProfile['ltcid']
                        allImageText=getbucketProfile['allImageText']

                        #print("HKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
                    else:
                        print("Not rlscp @@@@@@@@@@@@@@@@@@@@@ :- {}".format(rlscp))
                        time.sleep(2)    
                        udata=self.reelscrapeLive([UserIds],'',tagedandhash,SponserTageed,ltcid,postUserId,profileLocation,insertPost)
                        ##print(udata[2])
                        ##print(udata)
                        ##print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                        insertPost=udata[0]

                        postUserId=udata[1]
                        reelsstats=udata[2]
                        tagedandhash=udata[3]
                        SponserTageed=udata[4]
                        profileLocation=udata[5]
                        ltcid=udata[6]
                        allImageText=udata[7]
                        postsetIncache=1
                except Exception as er:
                    print(er)
                    opi=1
                    




            if self.postprofile==1:
                time.sleep(2) 
   
                ghklist=[]
                rtnyhis=[]
                userId=''
                edge_related_profiles=[]
                profile_live=0
                psscp=0
                ##print(getbucketProfile['poststats'])

                if 'poststats' in getbucketProfile:
                    #print(getbucketProfile['poststats']['totalpost'])
                   
                    if int(getbucketProfile['poststats']['totalpost'])>0:
                        #print(getbucketProfile['poststats']['totalpost'])
                        #print("------------------------------------------------------------")
                        psscp=1
                ##print(uuuuuuuuuuuuu)        
                if psscp==1:
                    insertPost=getbucketProfile['insertPost']
                    postUserId=getbucketProfile['postUserId']
                    poststats=getbucketProfile['poststats']
                    tagedandhash=getbucketProfile['tagedandhash']
                    SponserTageed=getbucketProfile['SponserTageed']
                    profileLocation=getbucketProfile['profileLocation']
                    ltcid=getbucketProfile['ltcid']
                    allImageText=getbucketProfile['allImageText']
                    #print("UUUU BBBBBBBBBBBBBBBBBBBB")
                else:    
                        

                    try:  
                        postUserdata=self.profilepostData(uname,'',tagedandhash,SponserTageed,ltcid,postUserId,profileLocation,insertPost,1,0,0,0,allImageText)
                        #print("UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU")
                        #print(postUserdata[4])


                        insertPost=postUserdata[1]
                        tagedandhash=postUserdata[2]
                        totalpost=postUserdata[3]
                        totalcomment=postUserdata[4]
                        totallike=postUserdata[5]
                        SponserTageed=postUserdata[6]

                        postUserId=postUserdata[7]

                        profileLocation=postUserdata[0]
                        ltcid=postUserdata[8]
                        allImageText=postUserdata[9]
                        postsetIncache=1
                        poststats={"totalpost":totalpost,'totalcomment':totalcomment,"totallike":totallike}
                        #print(poststats)

                        
                                                           

                    except Exception as er:
                        opi=1

                        sdsds=1
            try:
                ##print(tagedandhash)
                #postsetIncache=1
                if profileLocation and postsetIncache==1:
                    ##print(profileLocation)
                    ##print(ltcid)
                    ##print(sssssssssss)
                    userrender = self.locationscrap(profileLocation)
                    ##print(userrender)
                    if userrender:
                        for nn in userrender:
                            locationiddata=userrender[nn]
                            ##print(locationiddata)
                            geocnt=locationiddata['geocountry'].lower()
                            if geocnt not in cnttop and geocnt:
                                cnttop.append(geocnt)
                            geostate=locationiddata['geostate'].lower()
                            if geostate not in statetop and geostate :
                                statetop.append(geostate)

                            geolocality=locationiddata['geolocality'].lower()
                            if geolocality not in localitytop and geolocality :
                                localitytop.append(geolocality)

                            geocity=locationiddata['geocity'].lower()
                            if geocity not in citytop and geocity :
                                citytop.append(geocity)
                            geopostcode=locationiddata['geopostcode'].lower()
                            if geopostcode not in ziptop and geopostcode :
                                ziptop.append(geopostcode)
            except Exception as er:
                opi=1
                asasa=1
                                
                    
            try:
                postlive={'ltcid':ltcid,'allImageText':allImageText,"locationgender":userrender,"userId":UserIds,'profileLocation':profileLocation,"insertPost":insertPost,"tagedandhash":tagedandhash,'postUserId':postUserId,'SponserTageed':SponserTageed,'poststats':poststats,'reelsstats':reelsstats}

                if postsetIncache==1:
                    print("BBBBBBBBBBBBBBBBBMMMMMMMMMMMMMMMMMMMMMUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU")
                    self.BucketUserProfile(UserIds,postlive)

            except Exception as er:
                kk=1
                        
                    
                        
                        

        return {"profile":''}        

    def scrape_profile(self):
        useralldata={}
        finadata={}
        instpostbulk = []
        instpostbulkhis = []
        usergenderList = {}
        profileDisplay = []
        profileLocation = {}
        profileDisplay = []
        tagedandhash = []
        sponserDict=[]
        taggedDict=[]
        username = self.username
        returnData=[]
        postUserId=[]
        userrender=[]

        for uname in username:
            uslcid={}
            scrape=1
            if scrape==1:
   
                ghklist=[]
                rtnyhis=[]
                userId=''
                edge_related_profiles=[]
                profile_live=0
                try:
                    proxieslt =proxieslt = {"http":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321'),"https":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321')}
                    import requests
                    headers = {
                    'authority': 'i.instagram.com',
                    'accept': '*/*',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7',
                    # Requests sorts cookies= alphabetically
                    # 'cookie': 'mid=YR-xcwAEAAGZ7rpVv5Lb75f1M6wY; ig_did=01F3BE4F-F405-4973-8D72-FE0A6F7CE8C4; ig_nrcb=1; fbm_124024574287414=base_domain=.instagram.com; datr=QFyKYebyWFAxzcGHf7aTKCq9; csrftoken=kJKW2FyxeBd12b1qAZPKuoyFyKVOafyE; ds_user_id=8802916640; shbid="7509\\0548802916640\\0541692171858:01f7197046295bfcfa1dd84cdb8d0eaacba3f5af66a7767e1c95143fbee91d401d5bfc50"; shbts="1660635858\\0548802916640\\0541692171858:01f789dffe12f2c6aaf3bc3d9dd6420aaf7856ce54f0f6a5423c3beedec68fde7a407861"; sessionid=8802916640%3AeVqkdlYqNmHs7J%3A19%3AAYdNoZ2y_VesT6qxi_kctO1_bANn5cMRs56kc8sb0g; fbsr_124024574287414=eNUCh9HR3wj82zwmJD7cHFXpAwQB1v-EyEsU3F4x5SU.eyJ1c2VyX2lkIjoiMTAwMDI0NzcwNjkwMDQzIiwiY29kZSI6IkFRQTZWM3VsMHh4Z005RV9GVjlqTjNWVmEzbkJySEdTa19hcThoN193S1Byd1NiZGR5T3NFLWEyZGs2UHFTWUZHeEJSZlpxOTBqNHAydmlmRnBNN0hqRV93WUVzMGd3dHQxbExScjZJQ3M3dDFUZ2Z5V1E2amFOQ1RqdW5IcTdPT1JkWV90U1h4RG9RZ1VWWTZHTTA2cUxoSFVoajdZLU5EeXdoLXl5Sm81a1gtaHVyb2ZMZTVYcWtZRUptbnlTOUl6VW5DODl5MkxaUC01QkFjVmxlMXdlRVpjQmJPbnBzVHJDNzFka2puNEZLWUhwVG90cW5YTGtMVmxwNTZvOG4weERxVm54YVhGU3llNFJfdXVLb0FPS2xGZnZxVllYV2M4RnJ1VjNpN2wzN0Q3aTJfSnBVYlBBamp3Zl9idDhreEIxSnp0ZkFNWGNoeWZXTFR6eDlJX2xBSV95U1RGQnVaS000aGhYNzVYMkljZyIsIm9hdXRoX3Rva2VuIjoiRUFBQnd6TGl4bmpZQkFCd2F3eVMyVkM2U0ZvV3hnNEg1bzdaQlpDY0x0dUJCalNpejJqWkIzNEVpWkMyaXllYkZETERKcVc2V0oySFRMVUJLZ1ZVdUVMV2IzakpTaXY3WDlQMDk3TVNZaWl5TDhZTUdtMUo5UnJDV2REeVpBa01MVmNnVXRwWkNaQUUzRmttWWhnYW1hclpCeUs3eW8zZnlwZE5lQ2JGY3dzckhmNUtuV3hqc0xQOTUiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTY2MDgxOTU5M30; rur="EAG\\0548802916640\\0541692355784:01f71e33e93967c5e4e3b2095a09dd9d40ad63ef7c9de1a2a6bfe02501adf6bdd9c45105"',
                    'origin': 'https://www.instagram.com',
                    'referer': 'https://www.instagram.com/',
                    'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
                    'x-asbd-id': '198387',
                    'x-csrftoken': 'kJKW2FyxeBd12b1qAZPaKuoyFyKVOafyE',
                    'x-ig-app-id': '936619743392459',
                    'x-ig-www-claim': 'hmac.AR37LQZhDGQsGYLcR-dXO7wYwZvs750FwzK_h4RHcWGlyGIq',
                    }
                    params = {'username': str(uname),}
                    response = requests.request("GET", 'https://i.instagram.com/api/v1/users/web_profile_info/', params=params, headers=headers,proxies=proxieslt,timeout=30)
                    ##print(response.text)
                    user_info = json.loads(response.text)['data']['user']
                    #print(user_info)
                    ##print(user_infoq)
                    if user_info:
                        try:
                            profile_live=1
                            try:
                                if 'edge_related_profiles' in user_info:
                                    edge_related_profiless=user_info['edge_related_profiles']['edges']
                                    for rt in edge_related_profiless:
                                        if 'node' in rt:
                                            edge_related_profiles.append(rt['node'])
                            except:
                                kop=1
                                            
                            profileExit=0
                            topuser = self.get_profile_user(user_info, uname,profileExit)
                            rtny = topuser[0]
                            rtnyhis = topuser[1]
                            userId = topuser[2]
                            UserName = UserNamefull = ''
                            returnData.append({uname:userId})

                            if rtny['full_name']:
                                 UserName = self.fulnamecheck(rtny['full_name'])

                            if UserName:
                                usergender = self.userGendr(UserName)
                                if usergender:
                                    rtny.update({"gender":usergender})

                            livedatapost = self.get_media_new_search(user_info,uname,'',instpostbulk,instpostbulkhis,profileLocation,tagedandhash)
                            ##print(livedatapost)

                            instpostbulk = livedatapost[0]
                            instpostbulkhis = livedatapost[1]
                            instcatgory = livedatapost[2]
                            profileLocation = livedatapost[3]

                            tagedandhash = livedatapost[5]
                            SponserTageed = livedatapost[9]
                            postUserId=livedatapost[10]

                            totalpostCnt = livedatapost[6]
                            totalcommentcnt = livedatapost[7]
                            totallike = livedatapost[8]

                            if livedatapost[4]:
                                uslcid.update({userId:set(livedatapost[4])})
                            if SponserTageed:
                                sponserDict=SponserTageed
                            profileTag=1
                            if profileTag==1:
                                taggedDict=tagedandhash
                                jj=1

                            if rtny['followers_count']:
                                totaleng = self.engcalc(rtny['followers_count'],totalpostCnt,totallike,totalcommentcnt)
                                rtny.update({"totallike":totallike})
                                rtny.update({"totalcmnt":totalcommentcnt})
                                rtny.update({"postcnt":totalpostCnt})    

                            
                            profileDisplay.append({userId:rtny})
                            ##print(rtny)

                        except Exception as er:
                            opi=1
                            b=1
                    try:    
                        genderarr ={}
                        if usergenderList:
                            try:
                                genderarr = self.userGendrscrp(usergenderList)
                            except:
                                we=1 
                        if profileLocation:
                            userrender = self.locationscrap(profileLocation)


                        if profileDisplay:
                        
                            for llv in profileDisplay:
                                profileAdmin=1

                               
                                for llvkey in llv:
                                    ####print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$QQQQQQQQQQQQQQQQQQQQQQQQQQQQ");
                                    ghklist = llv[llvkey]
                                    ####print(ghklist)
                                    #####print(userrender)
                                    if llvkey in genderarr:
                                        ghklist.update({"gender":fndvalue.lower()})
                                            
                                    if llvkey in  uslcid:
                                        userAddres = self.userAddresfilter(uslcid[llvkey],userrender)
                                        #####print(userAddres)
                                        cntry = userAddres[0]
                                        stateloc = userAddres[1]
                                        profileCountery={}
                                        if llvkey in profileCountery:
                                            counteryold = profileCountery[llvkey]['country']
                                            cityold = profileCountery[llvkey]['city']
                                            if counteryold :
                                                cntry = str(cntry)+str(',')+str(counteryold)
                                            if cityold :
                                                stateloc = str(stateloc)+str('-')+str(cityold)
                                        country = set(cntry.split(","))
                                        ctym = stateloc.split("-")[:10]
                                        city = set(ctym)
                                        #####print(city)
                                        #city = city[:6]
                                        country =",".join(country)
                                        city ="-".join(city)
                                        if profileAdmin==1:
                                            ghklist.update({"country_new":country.lower()})
                                            ghklist.update({"city_new":city.lower()})
                                        else:
                                            ghklist.update({"country":country.lower()})
                                            ghklist.update({"city":city.lower()})
                                    #####print(ghklist)
                                    finallist= ghklist
                                    if profileAdmin == 1:
                                        if 'gender' in ghklist:
                                            ghklist.pop("gender")
                                        if 'country' in ghklist:
                                            ghklist.pop("country")
                                        if 'city' in ghklist:
                                            ghklist.pop("city")
                                        if 'insta_category' in ghklist:
                                            ghklist.pop("insta_category")
                    except Exception as er:
                        opi=1
                        kl=1

                except Exception as er:

                    opi=1
            if userId:

                if self.scrapePlive==1:
                    if userrender:
                        #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                        #print(userrender)
                        self.mydb.locationLive.insert_one({"usseId":userId,"data":userrender})

                    ghklist.update({"related_profile":edge_related_profiles})
                    self.mydb.profileLive.insert_one(ghklist)
                    self.mydb.profilePost_sub.insert_one({"usseId":userId,'data':livedatapost,'reeldata':{}})

                else:
                    try:
                        #print(ghklist)
                        userRells=[]
                        userRells.append(str(userId))
                        #print(userRells)
                        postids=[]
                        rellsdata=[]
                        try:
                            udata=self.reelscrapeLive(userRells)
                            postids=udata[1]
                            rellsdata=udata[0]
                            ghklist.update({"reel_like":udata[2]})
                            ghklist.update({"reel_comment":udata[3]})
                            ghklist.update({"reel_view":udata[4]})
                            ghklist.update({"reel_play":udata[5]})
                        except:
                            ghklist.update({"reel_like":0})
                            ghklist.update({"reel_comment":0})
                            ghklist.update({"reel_view":0})
                            ghklist.update({"reel_play":0})
                            ddd=1
                             
                        
                        postlikecomment=[]
                        finalk=[]
                        resultList= list(set(postUserId) | set(postids))
                        if resultList:
                            for mmk in resultList:
                                if mmk not in finalk:
                                    postlikecomment.append({'userData': int(userId),"scrape":0,"scrapecmnt":0,"postid":int(mmk)})
                        if postlikecomment:
                            try:
                                self.mydb.profileUserd.insert_many(postlikecomment, ordered=False) 
                            except:
                                bl=1    

                        ghklist.update({"related_profile":edge_related_profiles})
                        self.mydb.profileLive.insert_one(ghklist)
                        self.mydb.profilePost.insert_one({"usseId":userId,'data':livedatapost,'reeldata':rellsdata})
                        #print("^^^^^^^^^^^^^^^^^^^^^^^^%%%%%%%%%%%%5555555555555555555555555555")
                    except Exception as er:
                        opi=1
                     

        return {"profile":''}
                    
    def locationscrap(self,nct,mydb=''):
        #print("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
        livest = []
        newnct = {}
        postlocation ={}
        for kl in nct:
            #print(kl)
            livest.append(kl)
            newnct.update({kl:nct[kl]['id']})

        insertrw = []
        for listid in livest:
            try:
                urv= newnct[listid].replace(" ",'+')
                urlf = 'https://maps.googleapis.com/maps/api/geocode/json?address='+str(urv)+'&key=AIzaSyA0b_YvofuH2axlX7_WWBHvno0ski2O-TA'

                resp = requests.get(urlf)
                user_info = json.loads(resp.text)

                postcode = geocountry = geocity = geostate= geolocality = geocountryfull =''
                geologoogle = 1
                if 'results' in user_info:
                    if user_info['status']=='OK':
                        rtm = user_info['results'][0]['address_components']
                        for rsv in rtm:
                            if rsv['types'][0]=='postal_code':
                                postcode=rsv['short_name']
                            if rsv['types'][0]=='country':
                                geocountry=rsv['short_name']
                                geocountryfull=rsv['long_name']
                            if rsv['types'][0]=='administrative_area_level_1':
                                geostate=rsv['long_name']
                            if rsv['types'][0]=='administrative_area_level_2':
                                geocity=rsv['long_name']
                            if rsv['types'][0]=='locality':
                                geolocality=rsv['long_name']
                        geologoogle = 2     
                ftcity = {"geocountryfull":geocountryfull,"geocountry":geocountry,"geocity":geocity,"geostate":geostate,"geolocality":geolocality,'geopostcode':postcode,'intsta_media_location_id':listid,'geologoogle':geologoogle}
                postlocation.update({listid:ftcity})
             
            except:
                we=1
                ###print(1)
        return postlocation;
    def userGendr(self, username):
        gender = ''
        dirName = "/var/live/namelog/"+str(username)
        jty = dirName+"/"+str(username)+'.json'
        if os.path.exists(jty):
            with open(jty) as f:
                data = json.load(f)
                gender = data[username]['gender']
        return gender

    def mediaScrapeInsert(self,mydb,instpostbulk,mediatype=0):
        ###print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")
        from datetime import datetime
        from datetime import timedelta
        deletepost=[]
        bulk=[]
        bulk2=[]
        allpost=[]
        m=0
        instamedia=[]
        for ff in instpostbulk:
            try:
                ####print(ff)
                hastaguser=[]
                taguser=[]
                viraltakentime=round(time.time() * 1000)
                viraltakentimeat=round(time.time())
                intsta_text=''
                if 'intsta_shortcode' in ff:
                    
                    if 'intsta_text' in ff:
                        intsta_text= ff['intsta_text']
                        if ff['intsta_text']:
                            hastaguser = re.findall(r"#(\w+)", ff['intsta_text'])
                            hastaguser=list(map(str.lower,hastaguser))

                    if 'intsta_taguser' in ff:
                        if ff['intsta_taguser']:
                            taguser = ff['intsta_taguser'].split()
                            taguser=list(map(str.lower,taguser))
                    if 'dateadd' in ff:
                        viraltakentime=ff['dateadd']


                    lastimage=ff['insta_etc1']
                    if ff['insta_image'] and  lastimage=='':
                         lastimage=ff['insta_image']

                    lastimagev = lastimage.split("?")
                    lastimagevs = lastimagev[0].split("/")
                    destination_blob_name = lastimagevs[-1].split(".")[-1]
                    ####print(destination_blob_name)
                    tagner = ','.join(hastaguser)
                    taguserner = ','.join(taguser)
                    ff3=ff

                    insta_etc2 =''
                    if 'insta_etc2' in ff:
                        insta_etc2=ff['insta_etc2']

                    insta_image_large =''
                    if 'insta_image_large' in ff:
                        insta_image_large=ff['insta_image_large']    


                    ff.update({"insta_etc1":ff['insta_etc1'],"insta_image":destination_blob_name,"intsta_tags":hastaguser,"intsta_taguser":taguser,'dateadd':viraltakentime})
                    ff3.update({"insta_etc1":ff['insta_etc1'],"insta_image":destination_blob_name,"intsta_tags":tagner,"intsta_taguser":taguserner,'dateadd':viraltakentime})
                    ###print(ff)
                    allpost.append(ff3)
                    newArru= {"intsta_text":intsta_text,'insta_image':lastimage,'intsta_shortcode':ff['intsta_shortcode'],'insta_etc2':insta_etc2,'insta_image_large':insta_image_large}
                    ####print(newArru)
                    if 'insta_etc2' in ff:
                        ff.pop('insta_etc2')

                    if 'insta_image_large' in ff:
                        ff.pop('insta_image_large')
                    if 'intsta_text' in ff:
                        ff.pop('intsta_text')
                    instamedia.append({"taken":viraltakentimeat,'postid':ff['intsta_shortcode'],'scrape':0,'insta_user_name':ff['insta_user_name']})

                    m=1
                    ####print(ff)
                    ####print(intsta_text)
                    bulk.append(ff)
                    bulk2.append(newArru)
                    intsta_shortcode= ff['intsta_shortcode']
                    deletepost.append(intsta_shortcode)
            except Exception as er:
                opi=1
                ###print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")
                s=1
        if deletepost:
            ee=1
            #mydb.insta_media_scrap_new.remove({"intsta_shortcode":{"$in":deletepost}})
           
        if m==1:
            if mediatype==1:
                if instamedia:
                    try:

                        mydb.insta_media_post_data.insert_many(instamedia,ordered=False);
                    except  Exception as bwe:
                        print(1)
                        
            try:
                mydb.insta_media_scrap.insert(allpost)
            except Exception as er:
                print(er )
                

            try:
                mydb.insta_media_scrap_data.insert_many(bulk2,ordered=False);
            except  Exception as bwe:

                ds=1
                ##print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK11111")
            try:
                mydb.insta_media_scrap_new.insert_many(bulk,ordered=False);
            except  Exception as bwe:
                asa=1 
                ##print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK22222222222")
        deletepost=[]
        bulk=[]
        bulk2=[]
                
    def userGendrscrp(self, genderlist,mydb=''):
        navest =''
        retrungender = {}
        bulksetup = []
        #####print(genderlist)
        for username in genderlist:
            #####print(username)
            if navest=='':
                navest = '?name[]='+str(username)
            else:
                navest = str(navest)+'&name[]='+str(username)

        if navest:
            proxieslt = {"http":"http://"+str('sp852061412:Tyagi1986Ra@gate.dc.smartproxy.com:20000'),"https":"https://"+str('sp852061412:Tyagi1986Ra@gate.dc.smartproxy.com:20000')}
            url1 = 'https://api.nationalize.io/'+str(navest)
            url2 = 'https://api.agify.io/'+str(navest)
            url3 = 'https://api.genderize.io/'+str(navest)
            try:
                response = requests.get(url3, proxies=proxieslt)    
                userdictmediagen = {}
                cntlist = ''
                name = ''
                for sds in response.json():
                    age = ''
                    name = ''
                    if 'name' in sds:
                        name = sds['name']
                        age = sds['gender']
                        userdictmediagen.update({name:age}) 
                
                for mg in userdictmediagen:
                    gnd = ''
                    if mg in userdictmediagen:
                        gnd = userdictmediagen[mg]
                    ager = ''


                    bulksetup.append({"name":mg,"gender":gnd,"age":ager,"nation":''})
                    #####print(genderlist)
                    #####print(mg)
                    retrungender.update({genderlist[mg]:gnd})
            except:
                 retrungender = {}
                 bulksetup = []
                   

        if(bulksetup):
            m=1
            #mydb.profile_gender_name.insert(bulksetup)
        return retrungender

    def updatedata(self,redis_db):
        storekey= 'insta_data'
        insta_data =redis_db.get(storekey)
        insta_data = json.loads(insta_data)
        ###print(insta_data)
        finallist=[]
        sponserDict=[]
        postdata=[]
        historylist=[]
        profilelist=[]

        if len(insta_data)>=10:
            
            userIddata= []
            userIddataAll=[]
            for mmp in insta_data:
               
                userIddata.append(mmp)
                userIddataAll.append(int(mmp))
                ###print(insta_data[mmp])
                ###print(000)
                ###print(userIddata)
                ####print(insta_data[mmp]['profileExit'])
                ###print(insta_data[mmp]['profile'])
                ####print(insta_data[mmp]['taggedDict'])
                finallist=finallist+insta_data[mmp]['taggedDict']
                ###print(insta_data[mmp]['taggedDict'])
                ###print(insta_data[mmp]['sponserDict'])
                sponserDict=sponserDict+insta_data[mmp]['sponserDict']
                postdata=postdata+insta_data[mmp]['post']
                historylist=historylist+insta_data[mmp]['posthis']
                profilelist.append(insta_data[mmp]['profilehis'])
            ##print("ddddddddddd111111111111111111111111111111----------------------------------------------222222222222222222222222222222dddddddddddddddddddddddddddddddddddddddddddddddddddd")
            ##print({"user_id":{"$in":userIddata}})

          
            

            try:
                

                
                

                myclient = pymongo.MongoClient("mongodb://localhost:27017/")
                mydb = myclient["influncer"]
                valur1 = mydb.inf_insta_profile.find({"user_id":{"$in":userIddata}},{'user_id': 1,"insta_user_name":1,'country': 1,'city': 1,'profileUp':1,"viralprofile":1})
                bulkp = mydb.inf_insta_profile.initialize_unordered_bulk_op()
                ###print(userIddata)
                exitdata=[]
                for mi in valur1:
                    ##print("dddddddddddddddddddddddddddddddddddddddddddd")
                    if mi['user_id'] in userIddata:
                        userIdN=mi['user_id']
                        exitdata.append(userIdN)
                        updatedata= insta_data[str(userIdN)]['profile_final']
                        counteryold =''
                        cityold =''
                        if 'country' in mi:
                            counteryold = mi['country']
                        if 'city' in mi:
                            cityold = mi['city']
                        cntry=''
                        stateloc =''
                        if 'country' in  updatedata:
                            cntry=updatedata['country'];
                        if 'city' in  updatedata:
                            stateloc=updatedata['city'];

                        if counteryold and cntry:
                             cntry = str(cntry)+str(',')+str(counteryold)
                        if cityold and stateloc:
                            stateloc = str(stateloc)+str('-')+str(cityold)

                        country = set(cntry.split(","))
                        ctym = stateloc.split("-")[:10]
                        city = set(ctym)
                        ###print(mi)
                        if 'profileUp' in mi:
                            if mi['profileUp']==1:
                                if 'gender' in updatedata:
                                    updatedata.pop("gender")
                                if 'country' in updatedata:
                                    updatedata.pop("country")
                                if 'city' in updatedata:
                                    updatedata.pop("city")
                                if 'insta_category' in updatedata:
                                    updatedata.pop("insta_category")
                        updatedata['user_id']=updatedata['user_id']
                        updatedata['dateadd']=datetime.datetime.utcnow()
                        bulkp.find({"_id":mi['_id']}).update({ "$set": updatedata})
                        ###print(updatedata)
                ###print(userIddata)
                ###print(exitdata)        
                allremovedata=[]
                for mmp in insta_data:
                    allremovedata.append(mmp)
                    if mmp in exitdata:
                        m=1
                        ##print("update")
                    else:
                        allremovedata.append(mmp)
                        updatedatay=insta_data[mmp]['profile_final']
                        updatedatay['user_id']=updatedatay['user_id']
                        updatedatay['dateadd']=datetime.datetime.utcnow()
                        ##print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                        ##print(updatedatay)
                        ##print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                        bulkp.insert(updatedatay)
                        ##print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                        ##print(updatedatay)

                        ###print(insta_data[mmp]['profile_final'])
                       

                    
                if finallist:
                    ###print(finallist)
                    self.search_User_Scrape_Tagged_Post(1,finallist,'redis data',mydb,sponserDict)
                if postdata:
                    self.mediaScrapeInsert(mydb,postdata)
                if historylist:
                    mydb.insta_media_scrap_history.insert(historylist)
                if profilelist:
                    ###print("ddddddddddddddddddddddddddddddddddddddddd")
                    mydb.inf_insta_profile_history.insert(profilelist)
                from pymongo.errors import BulkWriteError    
                try:
                    bulkp.execute()
                    for mmr in allremovedata:
                        if mmr in insta_data:
                            ##print("---")
                            insta_data.pop(mmr)
                    redis_db.set(storekey,json.dumps(insta_data))   
                except BulkWriteError as bwe:
                    ds=1
                    ###print(bwe.details)
                    



            except Exception as er:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                ##print(exc_type, fname, exc_tb.tb_lineno)
                ##print("22222222--------------------------------------------------------------------------")
                opi=1
                m=1
    def profile(self):

        redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
        my_list = self.username
        n=10
        returnData=[]
        if len(my_list)>10:
            ###print(len(my_list))
            ###print("dddddddddddddddddddddddd");
            final = [my_list[i * n:(i + 1) * n] for i in range((len(my_list) + n - 1) // n )]
            for ms in final:
                try:
                    ####print(ms)
                    returnData=self.scrape_profile(ms,redis_db,returnData)
                    #iplist = self.scrapeproxies(mydb,0)
                except Exception as er:
                    we=1
                  
                    #opi=1
                   
        else:
            try:
                returnData=self.scrape_profile(my_list,redis_db,returnData)
            except Exception as er:
                we=1
                #opi=1
        ###print("AllData__________________________________________________________________________________________") 
       
        ###print("dddddddddd");
        my_list=[]
        return returnData


    def profile_new(self):

        redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
        my_list = self.username
        #print(my_list)
        n=10
        returnData=[]
        #print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
        if len(my_list)>10:
            ###print(len(my_list))
            ###print("dddddddddddddddddddddddd");
            final = [my_list[i * n:(i + 1) * n] for i in range((len(my_list) + n - 1) // n )]
            for ms in final:
                try:
                    ####print(ms)
                    returnData=self.scrape_profile_simmler(ms,redis_db,returnData)
                    #iplist = self.scrapeproxies(mydb,0)
                except Exception as er:
                    we=1
                  
                    #opi=1
                   
        else:
            try:
                returnData=self.scrape_profile_simmler(my_list)
            except Exception as er:
                we=1
                opi=1
        ###print("AllData__________________________________________________________________________________________") 
       
        ###print("dddddddddd");
        my_list=[]
        return returnData    


    def _get_nodes(self, container):
        return [self.augment_node(node['node']) for node in container['edges']]

    def augment_node(self, node):
        self.extract_tags(node)
    
    def __query_media_hashtagscrapeNew(self, payload, end_cursor=''):

        if payload:
            if 'data' in payload:
                if 'hashtag' in payload['data']:
                    container = payload['data']['hashtag']['edge_hashtag_to_media']
                    nodes = self._get_nodes(container)
                    end_cursor = container['page_info']['end_cursor']
                    return nodes, end_cursor    


    def profileDataWithPost(self,userdata,userid,redis_db,end_cursor='',instpostbulk=[],instpostbulkhis=[],profileLocation={},tagedandhash=[],userprofile=1,SponserTageed=[],page=0):
        finadata={}
        postdata=0;
        postId={}
        try:

            proxieslt =proxieslt = {"http":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321'),"https":"http://"+str('gauravtygai:Beyondlab123456_streaming-1@geo.iproyal.com:12321')}
            if end_cursor:
                nameUrl = 'https://www.instagram.com/graphql/query/?query_hash=ea4baf885b60cbf664b34ee760397549&variables={"id"%3A"'+userid+'"%2C"first"%3A50%2C"after"%3A"'+end_cursor+'"}'
            else:
                nameUrl = 'https://www.instagram.com/graphql/query/?query_hash=ea4baf885b60cbf664b34ee760397549&variables={"id"%3A"'+userid+'"%2C"first"%3A50}'
            

            headers = {
            'authority': 'www.instagram.com',
            'cache-control': 'max-age=0',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Cookie': 'ig_did=D2AEADB8-4FE2-4A77-813C-5825B9DA198B; csrftoken=4Q9BApcoMW5TomWnqIcCE4a1L6HQ2p5o; mid=YCu8eQAEAAF_M7sbdCHbFUXixn3e; ig_nrcb=1',
            'Content-Type': 'text/plain'
            }

            response = requests.request("GET", nameUrl, headers=headers,proxies=proxieslt, timeout=30)
            ##print(nameUrl)
            user_info = json.loads(response.text)['data']['user']
            ##print(user_info)

            end_cursor = user_info['edge_owner_to_timeline_media']['page_info']['end_cursor']               

        except Exception as er:
            opi=1
            #print("ssssssss2222222222222")
            return postId;
           
        try:
            self.posts = []
            self.last_scraped_filemtime = 0
            greatest_timestamp = 0
            future_to_item = {}
            user = user_info
            dst = ''
            future_to_item = ''
            livedatapost = self.get_media_new_search(user_info,userid,'',instpostbulk,instpostbulkhis,profileLocation,tagedandhash,userprofile,SponserTageed,page)
            instpostbulk = livedatapost[0]
            newpagesc=0;
            
            for yps in instpostbulk:
                if yps['intsta_shortcode']==self.PSID:
                    newpagesc=1
                    postId['shortcode']=yps['intsta_shortcode']
                    selectdata=['insta_image_large','taken_at_timestamp','intsta_tags','intsta_taguser','product_type','video_view_count','video_play_count','intsta_media_user','intsta_post_like','postId','is_video','intsta_post_comment']
                    
                    for tu in selectdata:
                        if tu in yps:
                            postId[tu]=yps[tu]
                        else:
                            postId[tu]=0
                    postId['post_scrape']=round(time.time())
                            
                          

                    postdata=1

            ####print(instpostbulk)
            instpostbulkhis = livedatapost[1]
            instcatgory = livedatapost[2]
            profileLocation = livedatapost[3]
            tagedandhash = livedatapost[5]
            SponserTageed = livedatapost[9]
            page=page+1
           
            if page>5 or newpagesc==1:
                end_cursor=''

            if end_cursor:
                return self.profileDataWithPost(userdata,userid,redis_db,end_cursor,instpostbulk,instpostbulkhis,profileLocation,tagedandhash,userprofile,SponserTageed,page)
            ##print(instpostbulk)
        except Exception as et:
            
            #print(et)
            derr=1    
        try:
            if instpostbulk:
                myclient = pymongo.MongoClient("mongodb://localhost:27017/")
                mydb = myclient["influncer"]

                if tagedandhash:
                    ###print(finallist)
                    self.search_User_Scrape_Tagged_Post(1,tagedandhash,'redis data',mydb,SponserTageed)
                if instpostbulk:
                    self.mediaScrapeInsert(mydb,instpostbulk,1)
                if instpostbulkhis:
                    mydb.insta_media_scrap_history.insert(instpostbulkhis)
        except:
            f=1
                    

        ##print(postId)
        return postId; 
                    





    def profileData(self,userdata,userid,redis_db):
        finadata={}
        try:

            proxieslt =proxieslt = {"http":"http://"+str('sp852061412:Tyagi1986Ra@gate.smartproxy.com:7000'),"https":"https://"+str('sp852061412:Tyagi1986Ra@gate.smartproxy.com:7000')}

            nameUrl = 'https://www.instagram.com/graphql/query/?query_hash=ea4baf885b60cbf664b34ee760397549&variables={"id"%3A"'+userid+'"%2C"first"%3A50}'
            headers = {
            'authority': 'www.instagram.com',
            'cache-control': 'max-age=0',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Cookie': 'ig_did=D2AEADB8-4FE2-4A77-813C-5825B9DA198B; csrftoken=4Q9BApcoMW5TomWnqIcCE4a1L6HQ2p5o; mid=YCu8eQAEAAF_M7sbdCHbFUXixn3e; ig_nrcb=1',
            'Content-Type': 'text/plain'
            }

            response = requests.request("GET", nameUrl, headers=headers,proxies=proxieslt, timeout=30)
            #print(response.text)
            user_info = json.loads(response.text)['data']['user']                        

        except Exception as er:
            opi=1
            return 1;

        self.posts = []
        self.last_scraped_filemtime = 0
        greatest_timestamp = 0
        future_to_item = {}
        user = user_info
        dst = ''
        future_to_item = ''
        


        livedatapost = self.get_media_new_search(user_info,userdata[userid]['userName'],'',[],[],{},[],1)
        instpostbulk = livedatapost[0]
        ####print(instpostbulk)
        instpostbulkhis = livedatapost[1]
        instcatgory = livedatapost[2]
        profileLocation = livedatapost[3]
        tagedandhash = livedatapost[5]
        SponserTageed = livedatapost[9]
        finadata[userid]={'post':instpostbulk,'posthis':instpostbulkhis,'sponserDict':SponserTageed,'taggedDict':tagedandhash}
        ##print(userid)
        try:
            storekey= 'insta_data_live_post'
            #insta_data =redis_db.delete(storekey)
            insta_data =redis_db.get(storekey)
            if insta_data:
                ##print("NNNNNNNNNNNNNNNNNNNN")
                insta_data = json.loads(insta_data)


                for ii in finadata:
                    ##print("UUUUUUUUUUUUUUUUUUUUUUUUUUUUGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
                    ##print(ii)
                    ##print("UUUUUUUUUUUUUUUUUUUUUUUUUUUUGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
                    insta_data[ii]=finadata[ii]
                ##print(len(insta_data))
                redis_db.set(storekey,json.dumps(insta_data))
               
                ###print("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
                ####print(insta_data)
                ###print("**************************************************************************")
                els=1
            else:
                storekey= 'insta_data_live_post'
                ##print("HIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIi")
                insta_data =redis_db.set(storekey,json.dumps(finadata))
        except Exception as er:
            opi=1
        insta_data =redis_db.get(storekey)
        insta_data = json.loads(insta_data)
        for iin in insta_data:
            dsds=1

    def isoToUnixt(self,t):
        utc_dt = dp.parse(t)
        t_in_seconds = utc_dt.timestamp()
        return round(t_in_seconds)
    def userPostScr(self):
        redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
        idp=self.profileDataWithPost(self.userId,self.userId,redis_db)
        return idp


    def profilescrapePost(self,data,after=''):
        from datetime import timezone
        #data='shilpakhatwani'
        my_list=[]
        final_list=[]
        #datakeysc()
        firstHitw = round(time.time())
        profileimage=[]
        profileUserData=[]
        profileimage=[]
        #bulk = db.allmailnew.initialize_unordered_bulk_op()
        blk=0
        userdata=[]
        profiledate=[]
        if data:
            try:
                #print('https://graph.facebook.com/17841406049620406?fields=business_discovery.username('+data+'){username,biography,followers_count,follows_count,media_count,profile_picture_url,name,website,media{media_product_type,media_type,permalink,timestamp,caption,comments_count,like_count,media_url}}&limit=50&access_token=EAADFlhpLfOgBAPjWX1ZC6jIBN1yByiwRdZCdy2d94pOFbQsQjRZClIsBJY44uJOdt5fkb6mLiJkV9LyU35caK6duiFJMHZBRk8YK00whCTlo69GBIveOZBRLzIeM2oQ88L9DMlRed0pYHXi5y04niQHW0jE2XrCLHdT3VFc3aCgZDZD')
                if after:
                    #print("ssssssssssssssssssssssssssss")
                    response = requests.get('https://graph.facebook.com/17841406049620406?fields=business_discovery.username('+data+'){username,biography,followers_count,follows_count,media_count,profile_picture_url,name,website,media{media_product_type,media_type,permalink,timestamp,caption,comments_count,like_count,media_url}}&limit=25&access_token=EAADFlhpLfOgBAPjWX1ZC6jIBN1yByiwRdZCdy2d94pOFbQsQjRZClIsBJY44uJOdt5fkb6mLiJkV9LyU35caK6duiFJMHZBRk8YK00whCTlo69GBIveOZBRLzIeM2oQ88L9DMlRed0pYHXi5y04niQHW0jE2XrCLHdT3VFc3aCgZDZD&after='+str(after))
                else:
                    response = requests.get('https://graph.facebook.com/17841406049620406?fields=business_discovery.username('+data+'){username,biography,followers_count,follows_count,media_count,profile_picture_url,name,website,media{media_product_type,media_type,permalink,timestamp,caption,comments_count,like_count,media_url}}&limit=25&access_token=EAADFlhpLfOgBAPjWX1ZC6jIBN1yByiwRdZCdy2d94pOFbQsQjRZClIsBJY44uJOdt5fkb6mLiJkV9LyU35caK6duiFJMHZBRk8YK00whCTlo69GBIveOZBRLzIeM2oQ88L9DMlRed0pYHXi5y04niQHW0jE2XrCLHdT3VFc3aCgZDZD')
                ##print(response.text);
                ggh = json.loads(response.text)
                response=''
                idhash=ggh['business_discovery']
                ggh=''
                #print(idhash['media']['paging']['cursors']['after'])
                mediadata=idhash['media']['data']
                afterf=idhash['media']['paging']['cursors']['after']
                ##print(mediadata)
                rl=0
                rc=0
                vl=0
                vc=0
                pl=0
                pc=0
                rn=0
                vn=0
                pn=0
                username=idhash['username']
                ername=idhash['name']
                biography=''
                if 'biography' in idhash:
                    biography=idhash['biography']
                followers_count=idhash['followers_count']
                follows_count=idhash['follows_count']
                media_count=idhash['media_count']
                profileimage.append(idhash['username']+'-:::-'+idhash['profile_picture_url'])
                ##print(idhash['username']+'-:::-'+idhash['profile_picture_url'])
                profileext=idhash['profile_picture_url'].split("?")
                profileext=profileext[0].split(".")[-1]
                profiledate=[]
                piddata=[]
                idhash=''
                mpots=1
                for er in mediadata:
                    postId1=er['permalink'].split("/");
                    postId=(postId1[4])
                    #print(postId)
                    datetm=self.isoToUnixt(er['timestamp']);
                    #print(datetm)
                    if self.PSID!=postId:
                        tageeduser=[]
                        intsta_tags=[]
                        if 'caption' in er:
                            intsta_text=er['caption']
                            ##print(intsta_text)
                            alltagshash = re.findall(r"(?<!&)#(\w+|(?:[\xA9\xAE\u203C\u2049\u2122\u2139\u2194-\u2199\u21A9\u21AA\u231A\u231B\u2328\u2388\u23CF\u23E9-\u23F3\u23F8-\u23FA\u24C2\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE\u2600-\u2604\u260E\u2611\u2614\u2615\u2618\u261D\u2620\u2622\u2623\u2626\u262A\u262E\u262F\u2638-\u263A\u2648-\u2653\u2660\u2663\u2665\u2666\u2668\u267B\u267F\u2692-\u2694\u2696\u2697\u2699\u269B\u269C\u26A0\u26A1\u26AA\u26AB\u26B0\u26B1\u26BD\u26BE\u26C4\u26C5\u26C8\u26CE\u26CF\u26D1\u26D3\u26D4\u26E9\u26EA\u26F0-\u26F5\u26F7-\u26FA\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721\u2728\u2733\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763\u2764\u2795-\u2797\u27A1\u27B0\u27BF\u2934\u2935\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55\u3030\u303D\u3297\u3299]|\uD83C[\uDC04\uDCCF\uDD70\uDD71\uDD7E\uDD7F\uDD8E\uDD91-\uDD9A\uDE01\uDE02\uDE1A\uDE2F\uDE32-\uDE3A\uDE50\uDE51\uDF00-\uDF21\uDF24-\uDF93\uDF96\uDF97\uDF99-\uDF9B\uDF9E-\uDFF0\uDFF3-\uDFF5\uDFF7-\uDFFF]|\uD83D[\uDC00-\uDCFD\uDCFF-\uDD3D\uDD49-\uDD4E\uDD50-\uDD67\uDD6F\uDD70\uDD73-\uDD79\uDD87\uDD8A-\uDD8D\uDD90\uDD95\uDD96\uDDA5\uDDA8\uDDB1\uDDB2\uDDBC\uDDC2-\uDDC4\uDDD1-\uDDD3\uDDDC-\uDDDE\uDDE1\uDDE3\uDDEF\uDDF3\uDDFA-\uDE4F\uDE80-\uDEC5\uDECB-\uDED0\uDEE0-\uDEE5\uDEE9\uDEEB\uDEEC\uDEF0\uDEF3]|\uD83E[\uDD10-\uDD18\uDD80-\uDD84\uDDC0]|(?:0\u20E3|1\u20E3|2\u20E3|3\u20E3|4\u20E3|5\u20E3|6\u20E3|7\u20E3|8\u20E3|9\u20E3|#\u20E3|\\*\u20E3|\uD83C(?:\uDDE6\uD83C(?:\uDDEB|\uDDFD|\uDDF1|\uDDF8|\uDDE9|\uDDF4|\uDDEE|\uDDF6|\uDDEC|\uDDF7|\uDDF2|\uDDFC|\uDDE8|\uDDFA|\uDDF9|\uDDFF|\uDDEA)|\uDDE7\uD83C(?:\uDDF8|\uDDED|\uDDE9|\uDDE7|\uDDFE|\uDDEA|\uDDFF|\uDDEF|\uDDF2|\uDDF9|\uDDF4|\uDDE6|\uDDFC|\uDDFB|\uDDF7|\uDDF3|\uDDEC|\uDDEB|\uDDEE|\uDDF6|\uDDF1)|\uDDE8\uD83C(?:\uDDF2|\uDDE6|\uDDFB|\uDDEB|\uDDF1|\uDDF3|\uDDFD|\uDDF5|\uDDE8|\uDDF4|\uDDEC|\uDDE9|\uDDF0|\uDDF7|\uDDEE|\uDDFA|\uDDFC|\uDDFE|\uDDFF|\uDDED)|\uDDE9\uD83C(?:\uDDFF|\uDDF0|\uDDEC|\uDDEF|\uDDF2|\uDDF4|\uDDEA)|\uDDEA\uD83C(?:\uDDE6|\uDDE8|\uDDEC|\uDDF7|\uDDEA|\uDDF9|\uDDFA|\uDDF8|\uDDED)|\uDDEB\uD83C(?:\uDDF0|\uDDF4|\uDDEF|\uDDEE|\uDDF7|\uDDF2)|\uDDEC\uD83C(?:\uDDF6|\uDDEB|\uDDE6|\uDDF2|\uDDEA|\uDDED|\uDDEE|\uDDF7|\uDDF1|\uDDE9|\uDDF5|\uDDFA|\uDDF9|\uDDEC|\uDDF3|\uDDFC|\uDDFE|\uDDF8|\uDDE7)|\uDDED\uD83C(?:\uDDF7|\uDDF9|\uDDF2|\uDDF3|\uDDF0|\uDDFA)|\uDDEE\uD83C(?:\uDDF4|\uDDE8|\uDDF8|\uDDF3|\uDDE9|\uDDF7|\uDDF6|\uDDEA|\uDDF2|\uDDF1|\uDDF9)|\uDDEF\uD83C(?:\uDDF2|\uDDF5|\uDDEA|\uDDF4)|\uDDF0\uD83C(?:\uDDED|\uDDFE|\uDDF2|\uDDFF|\uDDEA|\uDDEE|\uDDFC|\uDDEC|\uDDF5|\uDDF7|\uDDF3)|\uDDF1\uD83C(?:\uDDE6|\uDDFB|\uDDE7|\uDDF8|\uDDF7|\uDDFE|\uDDEE|\uDDF9|\uDDFA|\uDDF0|\uDDE8)|\uDDF2\uD83C(?:\uDDF4|\uDDF0|\uDDEC|\uDDFC|\uDDFE|\uDDFB|\uDDF1|\uDDF9|\uDDED|\uDDF6|\uDDF7|\uDDFA|\uDDFD|\uDDE9|\uDDE8|\uDDF3|\uDDEA|\uDDF8|\uDDE6|\uDDFF|\uDDF2|\uDDF5|\uDDEB)|\uDDF3\uD83C(?:\uDDE6|\uDDF7|\uDDF5|\uDDF1|\uDDE8|\uDDFF|\uDDEE|\uDDEA|\uDDEC|\uDDFA|\uDDEB|\uDDF4)|\uDDF4\uD83C\uDDF2|\uDDF5\uD83C(?:\uDDEB|\uDDF0|\uDDFC|\uDDF8|\uDDE6|\uDDEC|\uDDFE|\uDDEA|\uDDED|\uDDF3|\uDDF1|\uDDF9|\uDDF7|\uDDF2)|\uDDF6\uD83C\uDDE6|\uDDF7\uD83C(?:\uDDEA|\uDDF4|\uDDFA|\uDDFC|\uDDF8)|\uDDF8\uD83C(?:\uDDFB|\uDDF2|\uDDF9|\uDDE6|\uDDF3|\uDDE8|\uDDF1|\uDDEC|\uDDFD|\uDDF0|\uDDEE|\uDDE7|\uDDF4|\uDDF8|\uDDED|\uDDE9|\uDDF7|\uDDEF|\uDDFF|\uDDEA|\uDDFE)|\uDDF9\uD83C(?:\uDDE9|\uDDEB|\uDDFC|\uDDEF|\uDDFF|\uDDED|\uDDF1|\uDDEC|\uDDF0|\uDDF4|\uDDF9|\uDDE6|\uDDF3|\uDDF7|\uDDF2|\uDDE8|\uDDFB)|\uDDFA\uD83C(?:\uDDEC|\uDDE6|\uDDF8|\uDDFE|\uDDF2|\uDDFF)|\uDDFB\uD83C(?:\uDDEC|\uDDE8|\uDDEE|\uDDFA|\uDDE6|\uDDEA|\uDDF3)|\uDDFC\uD83C(?:\uDDF8|\uDDEB)|\uDDFD\uD83C\uDDF0|\uDDFE\uD83C(?:\uDDF9|\uDDEA)|\uDDFF\uD83C(?:\uDDE6|\uDDF2|\uDDFC))))[\ufe00-\ufe0f\u200d]?)+",intsta_text, re.UNICODE)
                            taggeduserAll = re.findall(r"@(\w+)", intsta_text)
                            if taggeduserAll:
                                tageeduser = taggeduserAll
                            if alltagshash:
                                intsta_tags = alltagshash
                        #print(intsta_tags)
                        #print(tageeduser)
                        selectdata=['post_scrape','insta_image_large','taken_at_timestamp','intsta_tags','intsta_taguser','product_type','video_view_count','video_play_count','intsta_media_user','intsta_post_like','postId','is_video','intsta_post_comment']
                        profileDt={}
                        profileDt.update({"post_scrape":round(time.time())})
                        piddata.append(profileDt)


                        #print(postId)
                        mpots=0

                if mpots==1:
                    sasa=1
                    s#elf.profilescrapePost(data,afterf)


                    

            except:
                print(1)
        mediadata=[]        
        try: 
            if profiledate:
                r=1
                ##print(piddata)
                #db.postlive.remove({"shortcode":{"$in":piddata}})
                #db.postlive.insert(profiledate)
            profiledate=[]
            piddata=''    

            if profileimage:
                j=1
                #self.multipleIamge(profileimage) 

            
            er=[]
            profiledate=[]
            mediadata=[]
            ggh=''
            response=idhash=allreadyAdd=UserId=postId1=piddata=''
            rl=0
            rc=0
            vl=0
            vc=0
            pl=0
            pc=0
            rn=0
            vn=0
            pn=0
            mediadata=''
        except Exception as er:
            opi=1
            m=1
            mediadata=''
            
            
        return  userdata;    

    def userPostScrWithApi(self):
        
        idp=self.profilescrapePost(self.username,'')
        return idp    

    def profilehash(self):

        redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
        storekey= 'insta_data_brandreport'
        insta_data=redis_db.get('storekey')
        insta_data = json.loads(insta_data)
        ###print(insta_data)
        m=0
        finalkey=[]
        profilescrape=[]
        for mm in insta_data:
            datain=0
            #print(mm)
            for nn in mm:
                finalD= mm[nn]
                viraltakentimeat=round(time.time())
                varsc=0
                if finalD['update']==0:
                    varsc=1
                else:
                    ##print(viraltakentimeat)
                    ##print(finalD['update'])
                    takelast =viraltakentimeat-finalD['update']
                    takelast =takelast
                    ##print(takelast)
                    if takelast>172800:
                        ###print("dddddddddddddddd")
                        varsc=1

                if varsc==1  and m<10:
                    datain=1
                    m=m+1
                    ###print(finalD)
                    viraltakentime=round(time.time())
                    finalD['update']=viraltakentime
                    updata={nn:finalD}
                    try:
                        #self.profileData(mm,nn,redis_db)
                        profilescrape.append(mm)
                        finalkey.append(updata)
                    except:
                        finalkey.append(mm)
                else:
                    finalkey.append(mm)

        if finalkey:            
            storekey= 'insta_data_brandreport'
            redis_db.set('storekey',json.dumps(finalkey))
        if profilescrape:
            for lk in profilescrape:
                for nnk in lk:
                    try:
                        self.profileData(lk,nnk,redis_db)
                    except:
                        dsds=1
                        
            
 
        insta_data1 =redis_db.get('insta_data_live_post')
        insta_data1 = json.loads(insta_data1)
        userIddata= []
        #print(len(insta_data1))
 
        if len(insta_data1)>=20:
            try:
                myclient = pymongo.MongoClient("mongodb://localhost:27017/")
                mydb = myclient["influncer"]
                finallist=[]
                sponserDict=[]
                postdata=[]
                historylist=[]
                #print("222222222222222222222222222222dddddddddddddddddddddddddddddddddddddddddddddddddddd")
                userIddata= []
                for mmp in insta_data1:
                    userIddata.append(mmp)
                    finallist=finallist+insta_data1[mmp]['taggedDict']
                    ###print(insta_data1[mmp]['sponserDict'])
                    sponserDict=sponserDict+insta_data1[mmp]['sponserDict']
                    postdata=postdata+insta_data1[mmp]['post']
                    historylist=historylist+insta_data1[mmp]['posthis']
                if finallist:
                    ###print(finallist)
                    self.search_User_Scrape_Tagged_Post(1,finallist,'redis data',mydb,sponserDict)
                if postdata:
                    self.mediaScrapeInsert(mydb,postdata,1)
                if historylist:
                    mydb.insta_media_scrap_history.insert(historylist)    
                        
            except:
                print("ERT")
            if userIddata:
                insta_data1 =redis_db.get('insta_data_live_post')
                insta_data1 = json.loads(insta_data1)
                for ii in userIddata:
                    try:
                        insta_data1.pop(ii)
                    except:
                        g=1
                        
                redis_db.set('insta_data_live_post',json.dumps(insta_data1))

            insta_data1=''
            finallist=[]
            sponserDict=[]
            postdata=[]
            historylist=[]
            userIddata=[]
            mmp=''
            insta_data1=''
            profilescrape=[]
            finalkey=''
            updata='' 
            insta_data=''

        return insta_data


    def profileIamge(self):
        import hashlib
        userId = self.userId
        lastimage=iamgeName = self.iamgeName
        lastimagev = lastimage.split("?")
        lastimagevs = lastimagev[0].split("/")
        imagewxt = lastimagevs[-1]
        a = hashlib.md5((userId).encode())
        hashvt = a.hexdigest()
        destination_blob_name = hashvt+'.'+lastimagevs[-1].split(".")[-1]
        resp = requests.get('https://storage.googleapis.com/viralimage/'+str(destination_blob_name))
        
        if resp.status_code == 200:
            #print('https://storage.googleapis.com/viralimage/'+str(destination_blob_name))
            print("iiiiii")
        else:
             self.UserImageDownLoad(userId,iamgeName)
        resp=''     
        return 1

    def multipleIamge(self):
        import hashlib
        multiimageId = self.multiimageId
        #print("--------------------------------")
        #print(multiimageId)
        self.allmediaDownload(multiimageId)
        multiimageId=''
        #print("-------------------------------------");
        return 1;