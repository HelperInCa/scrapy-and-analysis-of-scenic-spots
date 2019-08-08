import pymysql

def save_mysql_detail(self, reviews, star_levels):
    db = pymysql.connect("localhost", "root", "00000000", "scrapy")
    cursor = db.cursor()
    # 拼接sql, 批量插入表 scrapy_detail
    sql = r"insert into scrapy_detail(star_levels, comments) values "
    for i in range(0, len(reviews)):
        sql += r"(" + star_levels[i] + r", '" + reviews[i] + r"'),"  # (星级, '评论'),
    sql = sql[:-1] + ";"

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as err:
        db.rollback()
        print(err.with_traceback())

    db.close()