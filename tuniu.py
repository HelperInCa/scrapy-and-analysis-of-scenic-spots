import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
from decimal import Decimal, ROUND_DOWN
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import uuid
import db

'''
途牛评论特别少
西湖只有29条,而且比较旧
'''

class TuniuScraper():
    def __init__(self, pages, placename):
        self.pages = pages
        self.placename = placename
        self.comments = []
        self.star_levels = []
        self.check_max_page = True
        self.urls = []
        self.site_names = []
        self.sight_names = []
        self.detail_id = []
        self.info_id = []

    # 提取评论，传入参数为网址url
    def get_comment(self, url):
        try:
            # 发送HTTP请求
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
            }
            r = requests.get(url=url, headers=headers)

            decode_r = r.json()['data']

            # 解析网页，定位到评论部分
            soup = BeautifulSoup(decode_r, "lxml")
            main_content = soup.find_all('div', class_='item')

            # max_page = soup.find('div', class_='page-info').contents[0][1]
            #     if max_page is None:
            #         self.urls = self.urls[0]
            #         print("only 1 page")
            #     else:
            #         self.urls = self.urls[0:max_page-1]
            #         print("max # pages: " + max_page)
            #     # set flag to false so won't check again
            #     self.check_max_page = False
            # if self.check_max_page is True:


            # 提取评论
            for para in main_content:
                comment = para.find('div', class_='content')
                self.comments.append(comment.text.replace('&quot', ''))

                top_list = para.find('div', class_='top')
                star_level_raw = top_list.find_all('span')[3]['class'][1]
                star_level = star_level_raw[-1:]
                self.star_levels.append(star_level)


        except Exception as err:
            print(err.with_traceback())

    def get_id(self):

        driver = webdriver.Chrome()
        driver.get('https://www.tuniu.com')
        search = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'keyword-input')))
        search.clear()
        search.send_keys(self.placename)
        result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='resultbox an_mo']")))
        id = result.get_attribute('data-id')

        driver.close()

        if id is None:
            self.quit_scraping()

        # else:
        #     # print(id)
        #

            return id


    def scrappy(self):
        id = self.get_id()
        # 请求网址
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        self.urls = [
            r"http://www.tuniu.com/newguide/api/widget/render/?widget=ask.AskAndCommentWidget&params%5BpoiId%5D=" + str(
                id) + "&params%5Bpage%5D=" + str(page) + r"&_=" + str(ts) for page in range(1, self.pages+1)]
        # print(self.urls)

        # self.urls[0] = self.urls[0].replace('page1', '')

        # 利用concurrent.futures模块中的多线程来加速爬取评论
        executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
        # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
        future_tasks = [executor.submit(self.get_comment, url) for url in self.urls]
        # 等待所有的线程完成，才进入后续的执行
        wait(future_tasks, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(self.placename)
            self.site_names.append('途牛')

        if not self.comments:
            self.quit_scraping()
        else:
            self.save_to_csv()

    def save_to_database(self):
        id_scrapy_info = str(uuid.uuid4())
        db.scrapy_info(id_scrapy_info, self.placename, "途牛")

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

        comments_table.to_csv("tuniu-" + self.placename + ".csv", index=False)

        # save into the data base
        # save_mysql.save_mysql_detail(self.comments, self.star_levels)

        print('done scrapping ' + self.placename + ' from tuniu')

    def quit_scraping(self):
        print('No record of ' + self.placename + ' found on tuniu')
        return

if __name__ == "__main__":
    # 示例: 西湖共29条评论
    # scrapy_tuniu(26, 1563412058482, 'xihu')
    # s1 = TuniuScraper(10)
    # s1.scrappy("西湖")
    # s2 = TuniuScraper(10)
    # s2.scrappy("西湖")
    placename = "故宫"
    s3 = TuniuScraper(10, placename)
    s3.scrappy()





# import pandas as pd
# import requests
# from bs4 import BeautifulSoup
# import datetime
# from decimal import Decimal, ROUND_DOWN
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.wait import WebDriverWait
# from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
# import uuid
# import db
#
# '''
# 途牛评论特别少
# 西湖只有29条,而且比较旧
# '''
#
# class TuniuScraper():
#     def __init__(self, pages, placename):
#         self.pages = pages
#         self.placename = placename
#         self.comments = []
#         self.star_levels = []
#         self.check_max_page = True
#         self.urls = []
#         self.site_names = []
#         self.sight_names = []
#         self.detail_id = []
#         self.info_id = []
#
#     # 提取评论，传入参数为网址url
#     def get_comment(self, url):
#         try:
#             # 发送HTTP请求
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
#             }
#             r = requests.get(url=url, headers=headers)
#
#             decode_r = r.json()['data']
#
#             # 解析网页，定位到评论部分
#             soup = BeautifulSoup(decode_r, "lxml")
#             main_content = soup.find_all('div', class_='item')
#
#             # max_page = soup.find('div', class_='page-info').contents[0][1]
#             #     if max_page is None:
#             #         self.urls = self.urls[0]
#             #         print("only 1 page")
#             #     else:
#             #         self.urls = self.urls[0:max_page-1]
#             #         print("max # pages: " + max_page)
#             #     # set flag to false so won't check again
#             #     self.check_max_page = False
#             # if self.check_max_page is True:
#
#
#             # 提取评论
#             for para in main_content:
#                 comment = para.find('div', class_='content')
#                 self.comments.append(comment.text.replace('&quot', ''))
#
#                 top_list = para.find('div', class_='top')
#                 star_level_raw = top_list.find_all('span')[3]['class'][1]
#                 star_level = star_level_raw[-1:]
#                 self.star_levels.append(star_level)
#
#
#         except Exception as err:
#             print(err.with_traceback())
#
#     def get_id(self):
#
#         driver = webdriver.Chrome()
#         driver.get('https://www.tuniu.com')
#         search = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'keyword-input')))
#         search.clear()
#         search.send_keys(self.placename)
#         result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='resultbox an_mo']")))
#         id = result.get_attribute('data-id')
#
#         driver.close()
#
#         if id is None:
#             self.quit_scraping()
#
#         else:
#             # print(id)
#
#             return id
#
#
#     def scrappy(self):
#         id = self.get_id()
#         # 请求网址
#         # create timestamp (len=13) for the current time, cast to decimal
#         ts = Decimal(datetime.datetime.now().timestamp())
#         # round down ts to 3 decimal places
#         ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
#         # get rid of the decimal point
#         ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)
#
#         self.urls = [
#             r"http://www.tuniu.com/newguide/api/widget/render/?widget=ask.AskAndCommentWidget&params%5BpoiId%5D=" + str(
#                 id) + "&params%5Bpage%5D=" + str(page) + r"&_=" + str(ts) for page in range(1, self.pages+1)]
#         # print(self.urls)
#
#         # self.urls[0] = self.urls[0].replace('page1', '')
#
#         # 利用concurrent.futures模块中的多线程来加速爬取评论
#         executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
#         # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
#         future_tasks = [executor.submit(self.get_comment, url) for url in self.urls]
#         # 等待所有的线程完成，才进入后续的执行
#         wait(future_tasks, return_when=ALL_COMPLETED)
#
#         for x in range(0, len(self.comments)):
#             self.sight_names.append(self.placename)
#             self.site_names.append('途牛')
#
#         if not self.comments:
#             self.quit_scraping()
#         else:
#             self.save_to_csv()
#
#     def save_to_database(self):
#         id_scrapy_info = str(uuid.uuid4())
#         db.scrapy_info(id_scrapy_info, self.placename, "途牛")
#
#         for x in range(0, len(self.comments)):
#             self.detail_id.append(str(uuid.uuid4()))
#             self.info_id.append(id_scrapy_info)
#
#         db.scrapy_detail(self.detail_id, self.star_levels, self.comments, self.info_id)
#
#     # 创建DataFrame并保存到csv文件
#     def save_to_csv(self):
#         comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
#                                        'comments': self.comments,
#                                        'star_levels' : self.star_levels})
#
#         comments_table.to_csv("data/tuniu-" + self.placename + ".csv", index=False)
#
#         # save into the data base
#         # save_mysql.save_mysql_detail(self.comments, self.star_levels)
#
#         print('done scrapping ' + self.placename + ' from tuniu')
#
#     def quit_scraping(self):
#         print('No record of ' + self.placename + ' found on tuniu')
#         return
#
# if __name__ == "__main__":
#     # 示例: 西湖共29条评论
#     # scrapy_tuniu(26, 1563412058482, 'xihu')
#     # s1 = TuniuScraper(10)
#     # s1.scrappy("西湖")
#     # s2 = TuniuScraper(10)
#     # s2.scrappy("西湖")
#     placename = "故宫"
#     s3 = TuniuScraper(10, placename)
#     s3.scrappy()
