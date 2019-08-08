import thulac
import nltk
from crawlers import db

# thulac(user_dict=None, model_path=None, T2S=False, seg_only=False, filt=False)
# user_dict           设置用户词典，用户词典中的词会被打上uw标签。词典中每一个词一行，UTF8编码
# T2S                 默认False, 是否将句子从繁体转化为简体
# seg_only            默认False, 时候只进行分词，不进行词性标注
# filt                默认False, 是否使用过滤器去除一些没有意义的词语，例如“可以”。
# model_path          设置模型文件所在文件夹，默认为models/

# n/名词 np/人名 ns/地名 ni/机构名 nz/其它专名
# m/数词 q/量词 mq/数量词 t/时间词 f/方位词 s/处所词
# v/动词 a/形容词 d/副词 h/前接成分 k/后接成分 i/习语
# j/简称 r/代词 c/连词 p/介词 u/助词 y/语气助词
# e/叹词 o/拟声词 g/语素 w/标点 x/其它

class FreqAnaly:
    def __init__(self):
        self.id_comments = ()
        self.tagged_comments = [] # e.g. [[['拙政园', 'n'], ['真', 'g'], ['好看', 'a']][第二条评论的分词及tag]]
        self.cleaned_tagged_comments = []
        self.common_words = []
        self.wordlist = []
        self.dict = {}
        # self.result = [] # a list of words from comments with stopwords removed

    # read comments from the database into a list
    def read_from_database(self):
        self.id_comments = db.segmentation_fetch() # raw_tuple: (('id', 'comment'), (), ())
        # raw_tuple = db.segmentation_fetch() # raw_tuple: (('id', 'comment'), (), ())
        # comments = []
        # ids = []
        # for x in raw_tuple:
        #     comments.append(x[1])
        #     ids.append(x[0])
        #
        # return comments, ids

    # def read_from_txt(self):
    #     file = open("data/comments.txt", 'r', encoding='utf-8')
    #     comments = file.readlines()
    #     file.close()
    #
    #     return comments
    #
    # # generates a list of individual comments from txt file
    # def clean(self):
    #     comments, ids = self.read_from_database() # change to read_from_database
    #
    #     for x in comments:
    #         if x is '\n':
    #             del x
    #
    #     return comments

    # split and cut each comment from the comments
    def tagging(self):
        # self.id_comments = (('1', '床前明月光'), ('2', '人家人家人家尽枕河'), ('3', '拙政园拙政园真好看'), ('4', '范仲淹故居'), ('5', '白毛浮绿水'))
        # self.read_from_database()
        # filters out stopwords
        thu = thulac.thulac(filt=True)

        for id_comment in self.id_comments:
            comment = id_comment[1]
            tagged_comment = thu.cut(id_comment[1])
            self.tagged_comments.append(tagged_comment)


    # leaves out stopwords in the tagged_comments list, save to
    def clean(self):
        tags_to_be_left_out = ['', 'm', 't', 'v', 'h', 'k', 'i', 'r', 'c', 'p', 'u', 'y', 'e', 'o', 'g', 'w']

        for i in range(0, len(self.tagged_comments)):
            comment = self.tagged_comments[i]
            comment_id = self.id_comments[i][0]
            # cleaned_comment = []

            for j in range(0, len(self.tagged_comments[i])):
                word_tag = self.tagged_comments[i][j]
                # leaves out spaces and punctuations in the result
                if(word_tag[0] != [] and word_tag[1] not in tags_to_be_left_out):
                    word_tag.append(comment_id)
                    # cleaned_comment.append(word_tag)
                    self.cleaned_tagged_comments.append(word_tag)
                    self.wordlist.append(word_tag[0])
                    self.dict.update({word_tag[0]: comment_id})
            # only append none empty cleaned_comment
            # if cleaned_comment:
            # self.cleaned_tagged_comments.append(cleaned_comment)
        return


    # tag the words from comments and remove stopwords
    def process_comments(self):
        self.tagging()
        self.clean()

    def analyse(self):
        self.process_comments()
        frequency = nltk.FreqDist(self.wordlist)

        total_count = frequency.B()
        self.common_words = frequency.most_common(total_count)

        for i in range(0, total_count):
            word = self.common_words[i][0]
            comment_id = self.dict[word]
            self.common_words[i] += (comment_id,)

        print(self.common_words)


if __name__ == "__main__":
    p1 = FreqAnaly()
    p1.analyse()