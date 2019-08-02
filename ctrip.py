import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from urllib.parse import quote
import json
import datetime
from decimal import Decimal, ROUND_DOWN
from pypinyin import pinyin, Style
import re
import uuid
import db


class CtripScraper():

    def __init__(self, pages, placename):
        self.page_indices = list(range(1, pages))
        self.placename = placename
        self.comments = []
        self.star_levels = []
        self.sight_names = []
        self.site_names = []
        self.detail_id = []
        self.info_id = []

    # 提取评论，传入参数为该景点url
    def get_comment(self, urls, page):

        try:
            # 发送HTTP请求
            # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 \
            #                      (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'}
            headers = {
                        # ':path' : '/ sight / chengdu28 / 4227 - dianping - p1.html',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
                        'cookie': '_abtest_userid=313d78e4-58e9-4344-9283-fbf03857acb3; ibu_wws_c=1566617224131%7Czh-cn; MKT_Pagesource=PC; _RSG=JdK33h5IpKCoI3CDI4r3.A; _RDG=28aa73fd8f37c823f63fff34f394d7fa95; _RGUID=c533485a-c30d-49d3-af83-d4448d3034b2; Session=smartlinkcode=U135371&smartlinklanguage=zh&SmartLinkKeyWord=&SmartLinkQuary=&SmartLinkHost=; Union=AllianceID=4899&SID=135371&OUID=&Expires=1564709636256; TicketSiteID=SiteID=1004; ASP.NET_SessionSvc=MTAuOC4xODkuNTh8OTA5MHxqaW5xaWFvfGRlZmF1bHR8MTU1NzgxMzQxNDE3Ng; _RF1=35.197.1.135; _bfa=1.1564025219452.1t9llu.1.1564111258853.1564114246290.7.40.10650014170; _bfs=1.5; _jzqco=%7C%7C%7C%7C%7C1.33664493.1564025236757.1564114438999.1564114450305.1564114438999.1564114450305.0.0.0.33.33; __zpspc=9.8.1564114288.1564114450.4%233%7Cwww.google.com%7C%7C%7C%7C%23; appFloatCnt=23'
            }
            r = requests.get(url=urls[page], headers=headers)

            # 解析网页，定位到评论部分
            soup = BeautifulSoup(r.text, 'lxml')
            main_content = soup.find_all('div', class_='comment_single')

            # 提取评论
            for para in main_content:
                comment = para.find('span', class_='heightbox')
                self.comments.append(comment.text.replace('&quot', ''))
                stars = para.find('span', class_='starlist')
                stars = stars.find('span')['style']
                stars = re.sub('[%,;]', '', stars) # remove last two chars
                s = int(int(stars[6:])/20)

                self.star_levels.append(s)

            self.page_indices.remove(page)

        except Exception as err:
            print(err)

    def get_id(self):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        decoded_str = quote(self.placename)

        url = "http://m.ctrip.com/restapi/h5api/globalsearch/search?action=online&source=globalonline&keyword=" + decoded_str + "&t=" + str(ts)

        headers = {
                    'Origin' : 'https://www.ctrip.com',
                    'Referer' : 'https://www.ctrip.com/',
                    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
                  }
        r = requests.get(url, headers)
        clean_r = r.content.decode('utf-8')
        dict_r = json.loads(clean_r)

        for x in range(0, len(dict_r['data'])):
            # url = dict_r['matchList'][x]['url']
            # print(url)
            # try finding the - to separate the id
            # only extract id if type is sight, otherwise move onto next in the list
            if(dict_r['data'][x]['type'] == 'sight'):
                try:
                    # id_dict = re.split('\-+', url)  # split the url into two parts, second part is the id we want
                    # id = id_dict[1]
                    id = dict_r['data'][x]['id']
                    district_name = dict_r['data'][x]['districtName']
                    city_id = dict_r['data'][x]['cityId']
                    break # found id, exit for loop

                # didn't find id, try the next match
                except Exception:
                    # print("no luck, next match")
                    continue
            else:
                # print("no match found")
                return

        # print(id)
        if(id == None or district_name == None or city_id == None):
            return

        return id, district_name, city_id


    def get_district_id(self, district_name):
        list = pinyin(district_name, style=Style.NORMAL)
        result = []
        for l in list:
            result.append(l[0])

        return ''.join(result)

    def scrappy(self):
        # comments = []
        # star_levels = []
        urls = {}
        page_indices = list(range(1, 11))

        # deals with when no match of the demanded place was found
        if self.get_id() is None:
            print("No match for " + self.placename + " was found")
            return

        sight_id, district_name, city_id = self.get_id()
        district_id = self.get_district_id(district_name)

        # generate list of urls
        for p in self.page_indices:
            # creates url for a new page
            new_url = "https://you.ctrip.com/sight/" + district_id + str(city_id) + "/" + str(sight_id) + "-dianping-p%d.html" %p
            # adds a new pagenumber-url pair to the urls dict
            urls.update({p : new_url})

        # print(urls)
        # 利用concurrent.futures模块中的多线程来加速爬取评论
        executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
        # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
        future_tasks = []
        # for page in self.page_indices:
        #     future_tasks.append(executor.submit(self.get_comment, urls, page))
        # future_tasks = [executor.submit(self.get_comment, urls, ) for url in urls]
        while len(self.page_indices) != 0:
            # print(self.page_indices)
           # list of future tasks to be executed
            future_tasks = []
            # try scrapping all indices left in the list
            for page in self.page_indices:
                self.get_comment(urls, page)
        # 等待所有的线程完成，才进入后续的执行
        wait(future_tasks, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(self.placename)
            self.site_names.append('携程')

        if not self.comments:
            self.quit_scraping()
        else:
            self.save_to_database() # change to save_to_database() if want to write into data base

    def save_to_database(self):
        id_scrapy_info = str(uuid.uuid4())
        db.scrapy_info(id_scrapy_info, self.placename, "携程")

        for x in range(0, len(self.comments)):
            self.detail_id.append(str(uuid.uuid4()))
            self.info_id.append(id_scrapy_info)

        # insert into scrapy_detail
        db.scrapy_detail(self.detail_id, self.star_levels, self.comments, self.info_id)

        # 创建DataFrame并保存到csv文件
    def save_to_csv(self):
        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_levels' : self.star_levels})

        comments_table.to_csv("data/ctrip-" + self.placename + r".csv", index=False)

        print("Done scraping " + self.placename + " from ctrip")

    def quit_scraping(self):
        print('No record of ' + self.placename + ' found on qunar')
        return


if __name__ == "__main__":
    # scrappy("西湖", "ctrip/xh")
    # scrappy("武侯祠", "whc")
    # scrappy("紫禁城", "ctrip/zjc")
    # scrappy("华山", "ctrip/hs")
    # scrappy("中山陵", "ctrip/zsl")
    # scrappy("瘦西湖", "ctrip/sxh")
    # scrappy("北京大学", "ctrip/bjdx")
    # scrappy("南京路", "ctrip/njl")
    # s1 = CtripScraper(10)
    # s1.scrappy("石路")
    # s2 = CtripScraper(10)
    # s2.scrappy("故宫")
    placename = "故宫"
    s3 = CtripScraper(10, placename)
    s3.scrappy()
    
