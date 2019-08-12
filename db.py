import pymysql

import ctrip
import lvmama
import mafengwo
import qunar
import tongcheng
import tuniu


def scrapy_detail(id, star_levels, reviews, info_id):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    # 拼接sql, 批量插入表 scrapy_detail
    sql = r"insert into scrapy_detail(id, star_levels, comments, info_id) values "
    for i in range(0, len(reviews)):
        # (id, 星级, '评论', 'info_id'),
        sql += r"('" + id[i] + r"'," + star_levels[i] + r", '" + reviews[i] + r"', '" + info_id[i] + r"'),"
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


def fetch_detailid_comment():
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "select id, comments from scrapy_detail"
    cursor.execute(sql)
    raw_tuple = cursor.fetchall()  # raw_tuple: (('id', 'comment'), (), ())
    db.close()
    return raw_tuple


def fetch_infoid_comment():
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "select info_id, comments from scrapy_detail"
    cursor.execute(sql)
    info_comments = cursor.fetchall() # info_comments: (('info_id', 'comment'), (), ())
    db.close()
    return info_comments


def segmentation_insert(res_tuple):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "insert into segmentation(word, word_character, id_scrapy_detail) values "
    for i in range(0, len(res_tuple)):
        # ('word', 'word_character', 'id_scrapy_detail'),
        sql += "('" + res_tuple[i][0] + "', '" + res_tuple[i][1] + "', '" + res_tuple[i][2] + "'),"
    sql = sql[:-1] + ";"
    try:
        cursor.execute(sql)
        db.commit()

    except Exception as err:
        db.rollback()
        print(err)

    db.close()


def frequency_insert(res_freq):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "insert into word_frequency(word, frequency, scrapy_detail_id) values "
    for i in range(0, len(res_freq)):
        # ('word', freq, 'scrapy_detail_id'),
        sql += "('" + res_freq[i][0] + "', '" + str(res_freq[i][1]) + "', '" + res_freq[i][2] + "'),"
    sql = sql[:-1] + ";"
    try:
        cursor.execute(sql)
        db.commit()
        print("segmentation insert!")
    except Exception as err:
        db.rollback()
        print(err)

    db.close()


def opinion_insert(reponse):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "insert into opinion(id_detail, label, negative, medium, positive) values "
    for i in range(0, len(reponse)):
        # (id_detail, label, negative, medium, positive),
        sql += "('" + str(reponse[i][0]) + "', '" + str(reponse[i][1]) + "', '" + str(reponse[i][2]) + "', '" + str(
            reponse[i][3]) + "', '" + str(reponse[i][4]) + "'),"
    sql = sql[:-1] + ";"
    try:
        cursor.execute(sql)
        db.commit()

        print("opinion db inserted!")
    except Exception as err:
        db.rollback()
        print(err)

    db.close()


def pick_scraper(sitename, pages, placename):
    if sitename == '携程':
        s = ctrip.CtripScraper(pages, placename)
        return s
    elif sitename == '驴妈妈':
        s = lvmama.LvmamaScraper(pages, placename)
        return s
    elif sitename == '马蜂窝':
        s = mafengwo.MafengwoScraper(pages, placename)
        return s
    elif sitename == '去哪儿':
        s = qunar.QunarScraper(pages, placename)
        return s
    elif sitename == '同程':
        s = tongcheng.TongchengScraper(pages, placename)
        return s
    elif sitename == '途牛':
        s = tuniu.TuniuScraper(pages, placename)
        return s


def fetch_or_scrape(x, placename, sitename, pages):
    site_num = {'携程': '1', '驴妈妈' : '2', '马蜂窝' : '3', '去哪儿' : '4', '同程' : '5', '途牛' : '6'}
    if x is site_num[sitename]:
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