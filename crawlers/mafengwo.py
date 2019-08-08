import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import datetime
from decimal import Decimal, ROUND_DOWN
import json
import re
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import uuid
from crawlers import db


class MafengwoScraper():
    def __init__(self, pages, placename):
        self.pages = pages
        self.placename = placename
        self.comments = []
        self.star_levels = []
        self.sight_names = []
        self.site_names = []
        self.info_id = []
        self.detail_id = []

    # 提取评论，传入参数为该景点url
    def get_comment(self, url, id):
        try:
            # 发送HTTP请求
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
                       'Referer': "http://www.mafengwo.cn/poi/" + str(id) + ".html"
                       }
            try:
                r = requests.get(url=url, headers=headers)
            except Exception:
                pass
            # print(url)

            decoded_r = r.content.decode('utf-8')
            dict_r = json.loads(decoded_r)
            html = dict_r['data']['html']
            soup = BeautifulSoup(html, 'lxml')
            # clean_soup = soup.prettify()
            # print(clean_soup)
            main_content = soup.find_all('li', class_='rev-item comment-item clearfix')
            # reviews = clean_soup.find_all('div', class_='rev-list')
            # p = main_content.find_all('p', class_='rev-txt')
            for para in main_content:
                comment = para.find_all('p', class_='rev-txt')
                c = comment[0].contents[0]
                self.comments.append(c)

                star_level_raw = para.find_all('span', limit=3)[2]['class'][1]
                star_level = star_level_raw[-1:]
                self.star_levels.append(star_level)

        except Exception as err:
            print(err.with_traceback())


    def get_id(self):
        driver = webdriver.Chrome()
        driver.get('http://www.mafengwo.cn/mdd/')
        # time.sleep(5)
        search = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, '_j_head_search_input')))
        search.send_keys(self.placename)
        # search.send_keys(Keys.ENTER)
        # click = driver.find_element_by_xpath("//a[@title= '蜂蜂点评']")
        # click.click()
        # tag = driver.find_element_by_class_name('mss-item _j_listitem')
        # result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//li[@class='mss-item _j_listitem']")))
        try:
            result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//li[@data-type='pois']")))
            url = result.get_attribute('data-url')
            if url is None:
                result = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//li[@class='mss-item _j_listitem active']")))
                url = result.get_attribute('data-url')
                if url is None:
                    # close browser and stop scrapping if no such url is found
                    driver.close()
                    self.quit_scraping()

        except Exception:
            try:
                result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//li[@data-type='mdd']")))
                result.click()
                result = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//a[@data-cs-p='景点']")))
                result.click()
                try:
                    x = "[@title='" + self.placename + "']"
                    tag = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//a[@target='_blank']" + x)))
                    # url = result.get_attribute('href')
                    # tag = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, placename)))
                    url = tag.get_attribute('href')
                except Exception:
                    driver.close()
                    self.quit_scraping()

                if url is None:
                    # close browser and stop scrapping if no such url is found
                    driver.close()
                    self.quit_scraping()

            except Exception:
                driver.close()
                self.quit_scraping()



        # url = driver.current_url
        match = re.findall('(\/[0-9]*\.)', url)
        if not match:
            match = re.findall('(\=[0-9]*\&)', url)
            if not match:
                driver.close()
                self.quit_scraping()

        id = match[0][1:][:-1]
        print(id)

        driver.close()

        return id

    def scrappy(self):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        id = self.get_id()

        urls = [r"http://pagelet.mafengwo.cn/poi/pagelet/poiCommentListApi?"
                + r"&params=%7B%22poi_id%22%3A%22" + str(id) + r"%22%2C%22page%22%3A" + str(page)
                + r"%2C%22just_comment%22%3A1%7D&_ts=" + str(ts) + "&_=" + str(ts) for page in range(1, self.pages + 1)]

        # 利用concurrent.futures模块中的多线程来加速爬取评论
        executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
        # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
        future_tasks = [executor.submit(self.get_comment, url, id) for url in urls]
        # 等待所有的线程完成，才进入后续的执行
        wait(future_tasks, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(self.placename)
            self.site_names.append('马蜂窝')

        # no comments found quit scraping for this place
        if not self.comments:
            self.quit_scraping()
        else:
            self.save_to_csv() # change to save_to_database as needed

    def save_to_database(self):
        id_scrapy_info = str(uuid.uuid4())
        db.scrapy_info(id_scrapy_info, self.placename, "马蜂窝")

        for x in range(0, len(self.comments)):
            self.detail_id.append(str(uuid.uuid4()))
            self.info_id.append(id_scrapy_info)

        # insert into scrapy_detail
        db.scrapy_detail(self.detail_id, self.star_levels, self.comments, self.info_id)

    # 创建DataFrame并保存到csv文件
    def save_to_csv(self):
        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_levels': self.star_levels})

        comments_table.to_csv('data/mfw-' + self.placename + r".csv", index=False)

        print('done scrapping' + self.placename + ' from mafengwo')

    def quit_scraping(self):
        print('No record of ' + self.placename + ' found on mafengwo')
        return


if __name__ == "__main__":
    # 示例: 西湖
    # scrappy('18103771465138821999_1563874257353', '1093', 'xihu')
    # s1 = MafengwoScraper(10)
    # s1.scrappy('1093', 'mfw-xihu')
    # s2 = MafengwoScraper(10)
    # s2.scrappy("故宫")
    # s3 = MafengwoScraper(10)
    # s3.scrappy("网师园")
    placename = "故宫"
    s5 = MafengwoScraper(10, placename)
    s5.scrappy()
    # s3 = MafengwoScraper(10)
    # s3.scrappy("泰山")