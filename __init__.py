import crawlers
from crawlers import db


if __name__ == "__main__":
    websites = []
    num = input('请输入一共需要获取评论的景点个数\n')
    num = int(num)
    # dict = {'1' : '携程', '2' : '驴妈妈', '3' : '马蜂窝', '4' : '去哪儿', '5' : '同城', '6' : '途牛'}

    for i in range(0, num):
        placename = input('\n请输入需查询的第' + str(i+1) + '个景点名称：\n')
        websites = input('从下列哪些网站查询？（请输入对应编号，如："123456"）\n 1 - 携程 2 - 驴妈妈 3 - 马蜂窝 4 - 去哪儿 5 - 同城 6 - 途牛 \n')
        # 1 - 携程 2 - 驴妈妈 3 - 马蜂窝 4 - 去哪儿 5 - 同城 6 - 途牛
        websites = list(websites)  # list of string
        page_num = input('请输入同一个网站所需爬取此景点相关的评论页数：\n')
        pages = int(page_num)

        for x in websites:
            if x is '1':
                db.fetch_or_scrape(x, placename, '携程', pages)

            if x is '2':
                db.fetch_or_scrape(x, placename, '驴妈妈', pages)

            if x is '3':
                db.fetch_or_scrape(x, placename, '马蜂窝', pages)

            if x is '4':
                db.fetch_or_scrape(x, placename, '去哪儿', pages)

            if x is '5':
                db.fetch_or_scrape(x, placename, '同城', pages)

            if x is '6':
                db.fetch_or_scrape(x, placename, '途牛', pages)

        print("\n" + placename + "相关评论爬取结束")

