import pymysql


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


def segmentation_fetch():
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    sql = "select id, comments from scrapy_detail"
    cursor.execute(sql)
    raw_tuple = cursor.fetchall()  # raw_tuple: (('id', 'comment'), (), ())
    db.close()
    return raw_tuple


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
