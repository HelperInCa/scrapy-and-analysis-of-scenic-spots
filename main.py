import db
import ctrip
import lvmama
import mafengwo
import qunar
import tongcheng
import tuniu
from process import sentiment_analysis, frequency_analysis


def get_comments():
    websites = []
    num = input('请输入一共需要获取评论的景点个数\n')
    num = int(num)
    # dict = {'1' : '携程', '2' : '驴妈妈', '3' : '马蜂窝', '4' : '去哪儿', '5' : '同城', '6' : '途牛'}

    for i in range(0, num):
        placename = input('\n请输入需查询的第' + str(i + 1) + '个景点名称：\n')
        websites = input('从下列哪些网站查询？（请输入对应编号，如："123456"）\n 1 - 携程 2 - 驴妈妈 3 - 马蜂窝 4 - 去哪儿 5 - 同程 6 - 途牛 \n')
        # 1 - 携程 2 - 驴妈妈 3 - 马蜂窝 4 - 去哪儿 5 - 同城 6 - 途牛
        websites = list(websites)  # list of string
        page_num = input('请输入同一个网站所需爬取此景点相关的评论页数：\n')
        pages = int(page_num)

        for x in websites:
            if x is '1':
                db.fetch_or_scrape(x, placename, '携程', pages)

            elif x is '2':
                db.fetch_or_scrape(x, placename, '驴妈妈', pages)

            elif x is '3':
                db.fetch_or_scrape(x, placename, '马蜂窝', pages)

            elif x is '4':
                db.fetch_or_scrape(x, placename, '去哪儿', pages)

            elif x is '5':
                db.fetch_or_scrape(x, placename, '同程', pages)

            elif x is '6':
                db.fetch_or_scrape(x, placename, '途牛', pages)

        print("\n" + placename + "相关评论爬取结束")

def get_frequency():
    f1 = frequency_analysis.FreqAnaly()
    f1.analyse()

def get_sentiment_analysis():
    s1 = sentiment_analysis.SenAnaly()
    s1.analyse()

if __name__ == "__main__":
    # 查看数据库中某景区评论， 如没有，爬取评论
    get_comments()
    # 分词 + 词频分析
    # get_frequency()
    # #舆情分析
    # get_sentiment_analysis()