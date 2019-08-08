import requests
from crawlers import db

# makes a post request to the web and gets the result of analysis as response
# @ requestId - int
# @ logId - int
# @ text - a string of a single comment to be analysed (no longer than 600 chars)
class SenAnaly():
    def __init__(self, requestId, logId):
        self.requestId = requestId
        self.logId = logId
        self.comments = []
        self.result = [] # [[0, 0.3, 0.6, 0.1], [...], ...]

    # reads comments from database into a list
    def read_comments_from_database(self):
        self.comments = []

    # save the result of logId (index), label, prob into database
    def save_results_into_database(self):
        return

    def read_comments_from_txt(self):
        file = open("data/comments.txt", 'r', encoding='utf-8')
        self.comments = file.readlines()
        file.close()

    # make request and get response for a single comment
    def make_request(self, text):
        url = "http://10.154.9.14:20003//nlp-service/sentiment/wb"
        json_data = {
            "requestId": self.requestId,
            "items": [
                {
                    "logId": self.logId,
                    "text": text
                }
            ]
        }
        params = {
            'content-type': 'application/json',
            'accept': 'application/json'
        }
        r = requests.post(url=url, json=json_data, params=params)
        response = r.json()

        return response

    def analyse(self):
        id_comments = db.segmentation_fetch()

        for id_comment in id_comments:
            comment = id_comment[1]
            self.comments.append(comment)

        for comment in self.comments:
            response = self.make_request(comment)
            label = response['items'][0]['label']
            prob_neg = response['items'][0]['prob'][0]
            prob_neu = response['items'][0]['prob'][1]
            prob_pos = response['items'][0]['prob'][2]
            single_comment_result = [label, prob_neg, prob_neu, prob_pos]
            self.result.append(single_comment_result)

if __name__ == "__main__":
    s1 = SenAnaly('123', '123')
    s1.analyse()