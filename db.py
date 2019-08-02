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
