import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import datetime
from decimal import Decimal, ROUND_DOWN
import json
import uuid
from crawlers import db


class LvmamaScraper():

    def __init__(self, pages, placename):
        self.page_indices = list(range(1, pages + 1))
        self.placename = placename
        self.comments = []
        self.star_levels = []
        self.sight_names = []
        self.site_names = []
        self.detail_id = []
        self.info_id = []

    def get_comments(self, page, id):

        # print(page_indices)
        try:
            # create new urls to be scrapped from
            params = {
                      'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
                       }

            r = requests.post('http://ticket.lvmama.com/vst_front/comment/newPaginationOfComments', data={
                'type' : 'all',
                'currentPage' : str(page),
                'totalCount' : '1741',
                'placeId' : str(id),
                'productId' : '',
                'placeIdType' : 'PLACE',
                'isPicture' : '',
                'isBest' : '',
                'isPOI' : 'Y',
                'isELong' : 'N',
                }, params=params)

            # print(r.text)
            # lock.acquire()
            soup = BeautifulSoup(r.text, 'html.parser')
            texts = soup.find_all('div', class_='comment-li')

            for t in texts:
                temp = t.find('div', class_='ufeed-content')
                # remove all tabs/newlines/whitespaces
                x = "".join(temp.text.split())
                self.comments.append(x)
                # y = t.find('span', class_='ufeed-level')
                # print(y)
                y = t.find('span', class_='ufeed-level')
                y = y.find('i')["data-level"]
                self.star_levels.append(y)
                # print(y)

            # page scrapped successfully, remove from page_indices
            # print("page " + str(page) + " is scrapped")
            self.page_indices.remove(page)


        # throws error if page was scrapped unsuccessfully
        except Exception as err:
            print(err)
        # lock.release()

    # function that scraps the place id from entering keyword in search box
    def get_id(self):
        # create timestamp (len=13) for the current time, cast to decimal
        ts = Decimal(datetime.datetime.now().timestamp())
        # round down ts to 3 decimal places
        ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
        # get rid of the decimal point
        ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)

        params = {'callback' : 'recive',
                  'keyword' : self.placename,
                  'type' : "TICKET",
                  '_' : str(ts)
                 }
        # request_url = "https://www.lvmama.com/slvmamacomhttps/autocomplete/getNewAllCompleteData.do"
        request_url = "http://s.lvmama.com/autocomplete/autoCompleteNew.do"
        r = requests.get(request_url, params)
        # clean up r to become json format
        clean_r = r.content[7:len(r.content)-2].decode('utf-8')
        dict_r = json.loads(clean_r)

        if dict_r['totalResultSize'] is 0:
            self.quit_scraping()

        else:

            for x in range(0, len(dict_r['matchList'])):
                # url = dict_r['matchList'][x]['url']
                # print(url)
                # try finding the - to separate the id
                try:
                    # id_dict = re.split('\-+', url)  # split the url into two parts, second part is the id we want
                    # id = id_dict[1]
                    id = dict_r['matchList'][x]['urlId']
                    break # found id, exit for loop

                # didn't find id, try the next match
                except Exception:
                    # print("no luck, next match")
                    continue

            if id is None:
                return

            # print(id)

            return id

    def quit_scraping(self):
        print('No record of ' + self.placename + ' found on lvmama')
        return


    def scrappy(self):

        id = self.get_id()
        # fetch the reviews for all desired pages
        # for page in page_indices:
        #     get_reviews(page, id, page_indices, reviews)

        # adjust # max workers as needed
        executor = ThreadPoolExecutor(max_workers=1)

        # lock = threading.RLock()

        # keep scraping unscrapped pages until index list is empty (all page scrapped)
        while len(self.page_indices) != 0:
            # print(page_indices)
           # list of future tasks to be executed
            future_tasks = []
            # try scrapping all indices left in the list
            for page in self.page_indices:
                self.get_comments(page, id)
                # future_tasks.append(executor.submit(get_reviews, page, id, page_indices, reviews, star_levels, lock))
                # wait for this thread to be completed, move on to the next one
            # wait(future_tasks, 5, return_when=ALL_COMPLETED)

        for x in range(0, len(self.comments)):
            self.sight_names.append(self.placename)
            self.site_names.append('驴妈妈')

        if not self.comments:
            self.quit_scraping()
        else:
            self.save_to_csv() # change to save_to_database if needed

    def save_to_database(self):
        id_scrapy_info = str(uuid.uuid4())
        db.scrapy_info(id_scrapy_info, self.placename, "驴妈妈")

        for x in range(0, len(self.comments)):
            self.detail_id.append(str(uuid.uuid4()))
            self.info_id.append(id_scrapy_info)

        # insert into scrapy_detail
        db.scrapy_detail(self.detail_id, self.star_levels, self.comments, self.info_id)

    # create dataframe, save to .csv
    def save_to_csv(self):
        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star levels': self.star_levels})

        comments_table.to_csv("data/lmm-" + self.placename + r".csv", index=False)
        print("done scraping " + self.placename + " from lvmama")


if __name__ == "__main__":
    # scrappy("西湖", r"hangzhou-xihu.csv")
    # scrappy("千岛湖", r"hangzhou-qiandaohu.csv")
    # scrappy("杭州海洋公园", r"hangzhou-haiyanggongyuan.csv")
    # scrappy("灵隐寺", r"lingyinsi.csv")
    # scrappy("上海迪士尼乐园", r"shanghai-disney.csv")
    # scrappy("东方明珠", r"shanghai-dongfangmingzhu.csv")
    # scrappy("上海环球", r"shanghai-huanqiu.csv")
    # scrappy("上海野生动物园", r"shanghai-zoo.csv")
    # scrappy("上海海洋公园", r"shanghai-haiyanggongyuan.csv")
    # scrappy("上海科技馆", r"shanghai-kejiguan.csv")

    # get_id("西湖")
    placename = "故宫"
    s1 = LvmamaScraper(10, placename) # lmm_scraper instance to scrap 10 pages of reviews
    s1.scrappy()
    # s2 = LvmamaScraper(10)
    # s2.scrappy("西湖")
    # scrappy("都江堰", "dujiangyan.csv")
    # scrappy("网师园", "wangshiyuan.csv")
    # scrappy("莫干山", "moganshan.csv")
