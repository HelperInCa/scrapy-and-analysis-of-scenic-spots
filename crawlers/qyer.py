import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import datetime
from decimal import Decimal, ROUND_DOWN
from urllib.parse import quote
import re
import selenium
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC


# function to scrape comments from website
from selenium.webdriver.support.wait import WebDriverWait


def get_reviews(page, comments, star_levels, page_indices, urls):
    # global comments

    try:
        # send HTTP requests to the websites
        headers = {'Accept': '*/*',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en',
                   'Connection': 'keep-alive',
                   'Content-Length': '0',
                   'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                   'Cookie': 'PHPSESSID=67660c3d44a14e15b8c11cea2d9e490e; _guid=R246046d-7562-3791-8f1c-f6fa086359af; new_uv=1; new_session=1; _qyeruid=CgIBGV0ueHOUCX9CGOKYAg==; __utmc=253397513; __utmz=253397513.1563326581.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); ql_guid=QL69842d-16fe-4dfc-919a-37fe9071a2f1; source_url=https%3A//place.qyer.com/hong-kong/review/; isnew=1563326597273; __utma=253397513.464125237.1563326581.1563326581.1563329597.2; __utmt=1; __utmb=253397513.4.10.1563329597; ql_seq=4; ql_created_session=1; ql_stt=1563331682349; ql_vts=3',
                   'Host': 'place.qyer.com',
                   'Origin': 'https://place.qyer.com',
                   'Referer': 'https://place.qyer.com/poi/V2EJalFmBzFTZA/',
                   'User - Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'
                   }
        r = requests.get(url=urls[page], headers=headers)
        decoded_r = r.json()

        # list of comments on this page to be collected
        lists = decoded_r['data']['lists']

        # comment = decoded_r['data']['lists'][0]['content']
        for list in lists:
            comment = list['content']
            comments.append(comment.replace('&quot', ''))

            star_level = list['starlevel']
            star_levels.append(star_level)

        # done scraping this page, remove from index list to prevent from being scrapped again
        page_indices.remove(page)
        # print('page %d is scrapped'%page)

    except Exception as err:
        print(err)

def get_id(placename):
    # # create timestamp (len=13) for the current time, cast to decimal
    # ts = Decimal(datetime.datetime.now().timestamp())
    # # round down ts to 3 decimal places
    # ts = 1000 * ts.quantize(Decimal('.001'), rounding=ROUND_DOWN)
    # # get rid of the decimal point
    # ts = ts.quantize(Decimal('0'), rounding=ROUND_DOWN)
    #
    # decoded_str = quote(placename)
    #
    # decoded_str = re.sub('%', '%%', decoded_str)
    #
    # url = "https://www.qyer.com/qcross/home/ajax?action=search&keyword=" + decoded_str + "&timer=" + str(ts) + "&ajaxID=59a3d4c0cebeb65c18823987"
    # print(url)
    #
    # params = {'action' : 'search',
    #           'keyword' : placename,
    #           'timer' : str(ts),
    #           'ajaxID': '59a3d4c0cebeb65c18823987'
    #          }
    #
    # headers = {
    #             # 'GET' : '/qcross/home/ajax?action=search&keyword=%E9%A6%99%E6%B8%AF%E6%B5%B7%E6%B4%8B%E9%A6%86&timer=1563976782454&ajaxID=59a3d4c0cebeb65c18823987 HTTP/1.1',
    #             # 'Host' : 'www.qyer.com',
    #             # 'Connection' : 'keep-alive',
    #             'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    #             # 'Accept' : '*/*',················
    #             'Referer' : 'https://www.qyer.com/',
    #             'Accept-Encoding' : 'gzip, deflate, br',
    #             # 'Accept-Language' : 'en',
    #             'Cookie' : 'guid=R246046d-7562-3791-8f1c-f6fa086359af; new_uv=1; new_session=1; _qyeruid=CgIBGV0ueHOUCX9CGOKYAg==; ql_guid=QL69842d-16fe-4dfc-919a-37fe9071a2f1; source_url=https%3A//place.qyer.com/hong-kong/review/; isnew=1563326597273; PHPSESSID=2afdcab93295926df29fc2504f805cdd; __utma=253397513.464125237.1563326581.1563520438.1563976633.7; __utmc=253397513; __utmz=253397513.1563976633.7.3.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmt=1; __utmb=253397513.1.10.1563976633; ql_seq=1; ql_created_session=1; ql_stt=1563976632647; ql_vts=8'
    #           }
    # print(url)
    #
    # r = requests.get(url, headers)
    # # r = requests.get(url, headers)
    # decoded_r = r.json()

    driver = webdriver.Chrome()
    driver.get('https://place.qyer.com/')
    search = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'search js-search-input')))
    search.send_keys(placename)



    return id

# creates urls for desired number of pages of a destination, store as a dict
# param: poiid for the attraction
def scrappy(placename, savefilename):
    # list of comments to be appended to
    comments = []
    # list of star levels
    star_levels = []
    # list of pages to be scraped, starting from page1
    page_indices = list(range(1, 11))
    # create a dictionary of urls have yet to be visited
    urls = {}  # key - page number (int), value - url for the corresponding page (string) e.g. {1 : 'url1'}

    poiid = get_id(placename)

    for p in page_indices:
        # creates url for a new page
        new_url = "https://place.qyer.com/poi.php?action=comment&page=" + str(p) + "&order=5&poiid=" + str(poiid) + "&starLevel=all"
        # adds a new pagenumber-url pair to the urls dict
        urls.update({p : new_url})

    urls[1] = urls[1].replace('-p1', '')  # urls now index by page number

    # Executor object to execute functions asynchronous for a speedup
    executor = ThreadPoolExecutor(max_workers=20)  # max_workers to be adjusted as needed

    # keep scraping unscrapped pages
    while len(page_indices) != 0:
        # list of future tasks to be executed
        future_tasks = []
        # print(page_indices)
        for page in page_indices:
            future_tasks.append(executor.submit(get_reviews, page, comments, star_levels, page_indices, urls))
            # wait for this thread to be completed, move on to the next one
        wait(future_tasks, 5, return_when=ALL_COMPLETED)

    # create dataframe, save to .csv
    comments_table = pd.DataFrame({'id': range(1, len(comments) + 1),
                                   'comments': comments,
                                   'star levels': star_levels})

    comments_table.to_csv(savefilename, index=False)
    print("done scraping qyer")


if __name__ == "__main__":
    # stores all the url for the dest with certain ids
    # uncomment to start scraping

    # scrappy(59070, r"hongkong/hongkong-ocean-park.csv")
    # scrappy(59068, r"hongkong/hongkong-disneyland.csv")
    # scrappy(89891, r"hongkong/hongkong-golden-bauhinia-square.csv")
    # scrappy(83130, r"hongkong/hongkong-victoria-peak.csv")
    # scrappy(37686, r"hongkong/hongkong-nathan-road.csv")
    # scrappy(56501, r"hongkong/hongkong-the-stars-avenue.csv")
    # scrappy(94404, r"hongkong/hongkong-lan-kuai-fong.csv")
    # scrappy(94436, r"hongkong/hongkong-madame-tussauds.csv")
    # scrappy(94785, r"hongkong/hongkong-wan-chai.csv")
    # scrappy(246297, r"hongkong/hongkong-mankok.csv")
    # scrappy("香港海洋公园", "hongkong-seapark.csv")
    get_id("富士山")