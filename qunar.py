import pandas as pd
import requests
import datetime
from decimal import Decimal, ROUND_DOWN
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from urllib.parse import quote
import json
import uuid
import db

class QunarScraper():
    def __init__(self, pages, placename):
        self.pages = pages
        self.placename = placename
        self.comments = []
        self.star_levels = []
        self.sight_names = []
        self.site_names = []
        self.detail_id = []
        self.info_id = []

    def get_comment(self, url):

        try:
            # 发送HTTP请求
            headers = {
                'User - Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
            }
            r = requests.get(url=url, headers=headers)
            decoded_r = r.json()

            comment_list = decoded_r['data']['commentList']

            for l in comment_list:
                comment = l['content']
                self.comments.append(comment.replace('&quot', ''))

                star_level = l['score']
                self.star_levels.append(star_level)

        except Exception as err:
            print(err.with_traceback())

    def get_id(self):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        decoded_str = quote(self.placename)

        url = "https://search.piao.qunar.com/sight/suggestWithId.jsonp?callback=jQuery17205683114522839412_" + str(ts) + "&key=" + decoded_str +"&region=%E5%8C%97%E4%BA%AC&_=" + str(ts)

        headers = {
                    'Referer': 'https://piao.qunar.com/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
                }

        r = requests.get(url, headers)
        clean_r = r.content[41:len(r.content)-2].decode('utf-8')
        dict_r = json.loads(clean_r)

        for x in range(0, len(dict_r['data']['s'])):
            try:
                s = dict_r['data']['s'][x]
                id = s.split(',')[2]
                break
            except Exception:
                print("no match next one")
                continue

        if(id == None):
            return

        # print(id)
        return id

    def quit_scraping(self):
        print('No record of ' + self.placename + ' found on qunar')
        return

    def scrappy(self):

        sight_id = self.get_id()
        # 请求网址
        if self.get_id() is None:
            print("No match for " + self.placename + " was found")
            return

        urls = [
            "https://piao.qunar.com/ticket/detailLight/sightCommentList.json?sightId=" + str(
                sight_id) + "&index=1&page=%d&pageSize" \
                            "=50&tagType=0&tagName=%%E5%%85%%A8%%E9%%83%%A8" % x
            for x in
            range(1, 11)]
        # print(urls)
        urls[0] = urls[0].replace('page=1', '')

        # 利用concurrent.futures模块中的多线程来加速爬取评论
        executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
        # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
        future_tasks = [executor.submit(self.get_comment, url) for url in urls]
        # 等待所有的线程完成，才进入后续的执行
        wait(future_tasks, return_when=ALL_COMPLETED)

        if not self.comments:
            # scrape again if comments empty
            # 利用concurrent.futures模块中的多线程来加速爬取评论
            executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
            # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
            future_tasks = [executor.submit(self.get_comment, url) for url in urls]
            # 等待所有的线程完成，才进入后续的执行
            wait(future_tasks, return_when=ALL_COMPLETED)

            if not self.comments:
                # quit if empty again
                self.quit_scraping()
            else:
                self.save_to_database() # change to save_to_database as needed

        else:
            self.save_to_database() # change to save_to_database as needed

    def save_to_database(self):
        id_scrapy_info = str(uuid.uuid4())
        db.scrapy_info(id_scrapy_info, self.placename, "去哪儿")

        for x in range(0, len(self.comments)):
            self.detail_id.append(str(uuid.uuid4()))
            self.info_id.append(id_scrapy_info)

    def save_to_csv(self):
        for x in range(0, len(self.comments)):
            self.sight_names.append(self.placename)
            self.site_names.append('去哪儿')

        # 创建DataFrame并保存到csv文件
        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_levels': self.star_levels})

        comments_table.to_csv("data/qunar-" + self.placename + ".csv", index=False)

        print('done scrapping ' + self.placename + ' from qunar')

if __name__ == "__main__":
    # scrappy(13212, 'changlong')
    # scrappy("故宫", "qunar/gg")
    placename = "故宫"
    s1 = QunarScraper(10, placename)
    s1.scrappy()
    # scrappy("东方明珠", "qunar/dfmz")
    # scrappy("中山陵", "qunar/zsl")