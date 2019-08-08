import pymysql
import crawlers


def scrapy_detail(id, star_levels, reviews, info_id):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    # 拼接sql, 批量插入表 scrapy_detail
    sql = r"insert into scrapy_detail(id, star_levels, comments, info_id) values "
    for i in range(0, len(reviews)):
        # (id, 星级, '评论', 'info_id'),
        sql += r"(" + id[i] + r"," + star_levels[i] + r", '" + reviews[i] + r"', '" + info_id[i] + r"'),"
    sql = sql[:-1] + ";"

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as err:
        db.rollback()
        print(err)

    db.close()


def scrapy_info(id, sight_name, site_name):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()

    # 拼接sql, 批量插入表scrapy_info
    # insert into scrapy_info(id, sight_name, site_name) values ('id', 'sight', 'site');
    sql = r"insert into scrapy_info(id, sight_name, site_name) values ('" + id + r"', '" + sight_name + r"', '" + site_name + r"');"

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as err:
        db.rollback()
        print(err)

    db.close()

def segmentation_fetch():
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "select id, comments from scrapy_detail"
    cursor.execute(sql)
    raw_tuple = cursor.fetchall()  # raw_tuple: (('id', 'comment'), (), ())
    db.close()
    return raw_tuple


def pick_scraper(sitename, pages, placename):
    if sitename is '携程':
        s = crawlers.ctrip.CtripScraper(pages, placename)
        return s
    elif sitename is '驴妈妈':
        s = crawlers.lvmama.LvmamaScraper(pages, placename)
        return s
    elif sitename is '马蜂窝':
        s = crawlers.mafengwo.MafengwoScraper(pages, placename)
        return s
    elif sitename is '去哪儿':
        s = crawlers.qunar.QunarScraper(pages, placename)
        return s
    elif sitename is '同城':
        s = crawlers.tongcheng.TongchengScraper(pages, placename)
        return s
    elif sitename is '途牛':
        s = crawlers.tuniu.TuniuScraper(pages, placename)
        return s


def fetch_or_scrape(x, placename, sitename, pages):
    if x is '1':
        db = pymysql.connect("localhost", "root", "00000000", "scrapy")
        cursor = db.cursor()
        sql = "SELECT COUNT(1) FROM scrapy_info WHERE sight_name = '" + placename + "' AND site_name = '" + sitename + "'"
        cursor.execute(sql)
        has_sight = cursor.fetchone()[0]

        # scrapy_detail已经有携程的这个景点了,直接从里面取
        if has_sight >= 1:
            star_seg = []
            review_seg = []

            sql = "SELECT star_levels, comments FROM scrapy_detail"
            cursor.execute(sql)
            res = cursor.fetchall()
            for n in range(len(res)):
                star_seg.append(res[n][0])
                review_seg.append(res[n][1])

            cursor.close()
            db.close()

        # scrapy_detail没有携程的这个景点
        elif has_sight == 0:
            print("\n开始爬取" + sitename)
            s = pick_scraper(sitename, pages, placename)
            try:
                s.scrappy()
            # scrape again if exception happens
            except Exception:
                try:
                    s.scrappy()
                except Exception:
                    pass