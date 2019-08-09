import pandas as pd
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import json
import uuid
import db

class TongchengScraper():
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
            headers = {
                'User - Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
            }
            r = requests.get(url=url, headers=headers)
            decoded_r = r.json()

            dpList = decoded_r['dpList']

            for l in dpList:
                comment = l['dpContent']
                self.comments.append(comment.replace('&quot', ''))

                star_level = l['lineAccess']
                if(star_level == "好评"):
                    self.star_levels.append('5')
                elif(star_level == "中评"):
                    self.star_levels.append('3')
                else:
                    self.star_levels.append('0')

        except Exception as err:
            print(err)

    def get_id(self):
        decoded_str = quote(self.placename)
        url = "https://www.ly.com/commonajax/SearchBoxAjaxHandler/AutoCompleteSearch?_dAjax=callback&selectCity=226&keyword=" + decoded_str + "&fchannel=scenery&fpage=scenery-index"

        headers = {
                    'Cookie': 'NewProvinceId=16; NCid=226; NewProvinceName=%E6%B1%9F%E8%8B%8F; NCName=%E8%8B%8F%E5%B7%9E; 17uCNRefId=RefId=14211860&SEFrom=google&SEKeyWords=; TicketSEInfo=RefId=14211860&SEFrom=google&SEKeyWords=; CNSEInfo=RefId=14211860&tcbdkeyid=&SEFrom=google&SEKeyWords=&RefUrl=https%3A%2F%2Fwww.google.com%2F; qdid=35294|1|14211860|be6ca5; __tctma=144323752.1564389177825149.1564389177180.1564389177180.1564389177180.1; __tctmu=144323752.0.0; __tctmz=144323752.1564389177180.1.1.utmccn=(referral)|utmcsr=google.com|utmcct=|utmcmd=referral; longKey=1564389177825149; __tctrack=0; ASP.NET_SessionId=2i3meftmyidapy3lhpfiqa4u; route=f0004d02a0ba0e34264e5ff621ad2c06; wwwscenery=ba16fe144ace5eec5add6474f197674c; __tctmc=144323752.30665273; __tctmd=144323752.205791637; __tctmb=144323752.2097235406146154.1564389177180.1564389190551.2',
                    'Referer': 'https://www.ly.com/scenery/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
        }
        r = requests.get(url, headers)
        clean_r = r.content.decode('utf-8')
        dict_r = json.loads(clean_r)

        list = dict_r['ReturnValue']['records']

        for x in range(0, len(list)):
            try:
                y = list[x]['resourceId']

                if list[x]['resourceId']:
                    id = list[x]['resourceId']
                    break
                continue

            except Exception:
                print(Exception)
                continue

        # print(id)

        if id is None:
            return

        else:
            return id

    def quit_scraping(self):
        print('No record of ' + self.placename + ' found on qunar')
        return

    def scrappy(self):
        sight_id = self.get_id()

        urls = ["https://www.ly.com/scenery/AjaxHelper/DianPingAjax.aspx?action=GetDianPingList&sid=" + str(sight_id) + "&page=" + str(page) + "&pageSize=10&labId=1&sort=0" for page in range(1, 11)]
        urls[0] = urls[0].replace('page=1', '')

        # 利用concurrent.futures模块中的多线程来加速爬取评论
        executor = ThreadPoolExecutor(max_workers=20)  # 可以自己调整max_workers,即线程的个数
        # submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
        future_tasks = [executor.submit(self.get_comment, url) for url in urls]
        # 等待所有的线程完成，才进入后续的执行
        wait(future_tasks, return_when=ALL_COMPLETED)

        # for x in range(0, len(self.comments)):
        #     self.sight_names.append(self.placename)
        #     self.site_names.append('同城')

        if not self.comments:
            self.quit_scraping()
        else:
            self.save_to_database() # change to save_to_database as needed

    def save_to_database(self):
        id_scrapy_info = str(uuid.uuid4())
        db.scrapy_info(id_scrapy_info, self.placename, "同程")

        for x in range(0, len(self.comments)):
            self.detail_id.append(str(uuid.uuid4()))
            self.info_id.append(id_scrapy_info)

        db.scrapy_detail(self.detail_id, self.star_levels, self.comments, self.info_id)

    def save_to_csv(self):
        # 创建DataFrame并保存到csv文件
        comments_table = pd.DataFrame({'id': range(1, len(self.comments) + 1),
                                       'comments': self.comments,
                                       'star_level' : self.star_levels})

        comments_table.to_csv("tc-" + self.placename + ".csv", index=False)

        print('done scrapping ' + self.placename + ' from tongcheng')


if __name__ == "__main__":
    # scrapy(1390, 0.4341402146658353, 'shouxihu')
    placename = "故宫"
    s1 = TongchengScraper(10, placename)
    s1.scrappy()