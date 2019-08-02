import lvmama
import ctrip
import mafengwo
import qunar
import tongcheng
import tuniu


if __name__ == "__main__":
    websites = []
    num = input('请输入一共需要爬取的景点个数\n')
    num = int(num)
    # dict = {'1' : '携程', '2' : '驴妈妈', '3' : '马蜂窝', '4' : '去哪儿', '5' : '同城', '6' : '途牛'}

    for i in range(0, num):
        placename = input('\n请输入需查询的第' + str(i+1) + '个景点名称：\n')
        websites = input('从下列哪些网站爬取？（请输入对应编号，如："123456"）\n 1 - 携程 2 - 驴妈妈 3 - 马蜂窝 4 - 去哪儿 5 - 同城 6 - 途牛 \n')
        # 1 - 携程 2 - 驴妈妈 3 - 马蜂窝 4 - 去哪儿 5 - 同城 6 - 途牛
        websites = list(websites)  # list of string
        page_num = input('请输入同一个网站所需爬取此景点相关的评论页数：\n')
        pages = int(page_num)

        for x in websites:
            if x is '1':
                print("\n开始爬取携程")
                s = ctrip.CtripScraper(pages, placename)
                try:
                    s.scrappy()
                # scrape again if exception happens
                except Exception:
                    try:
                        s.scrappy()
                    except Exception:
                        pass

            if x is '2':
                print("\n开始爬取驴妈妈")
                s = lvmama.LvmamaScraper(pages)
                try:
                    s.scrappy(placename)
                # scrape again if exception happens
                except Exception:
                    try:
                        s.scrappy(placename)
                    except Exception:
                        pass

            if x is '3':
                print("\n开始爬取马蜂窝")
                s = mafengwo.MafengwoScraper(pages)
                try:
                    s.scrappy(placename)
                # scrape again if exception happens
                except Exception:
                    try:
                        s.scrappy(placename)
                    except Exception:
                        pass

            if x is '4':
                print("\n开始爬取去哪儿")
                s = qunar.QunarScraper(pages)
                try:
                    s.scrappy(placename)
                except Exception:
                    pass

            if x is '5':
                print("\n开始爬取同城")
                s = tongcheng.TongchengScraper(pages)
                try:
                    s.scrappy(placename)
                # scrape again if exception happens
                except Exception:
                    try:
                        s.scrappy(placename)
                    except Exception:
                        pass

            if x is '6':
                print("\n开始爬取途牛")
                s = tuniu.TuniuScraper(pages)
                try:
                    s.scrappy(placename)
                # scrape again if exception happens
                except Exception:
                    try:
                        s.scrappy(placename)
                    except Exception:
                        pass

        print("\n" + placename + "相关评论爬取结束")

