import threading
import queue
from data2SQL import docIds_from_mysql
from WY_reply import next_urls
from WY_reply import parse_news
from data2SQL import data2mysql
import requests

class spyder():
    def __init__(self):
        self.url_dateset = docIds_from_mysql()
        self.url_q = queue.Queue()
        self.count = 0

    def urls_in(self, url):
        # 1.得到url
        urls = next_urls(url)
        urls_docid = [url.split('/')[-1].replace('.html', '') for url in urls]
        # 2.过滤并升级set
        urls_waiting = set([url for url in urls if url.split('/')[-1].replace('.html', '') not in self.url_dateset])
        self.url_dateset.update(urls_docid)
        # 入队操作
        c = 0
        for url in urls_waiting:
            self.url_q.put(url)
            c += 1
            self.count += 1
        if self.count % 100 == 0:
            print('本次入队{}次，当前队列总数{}个, 解析到url个数{}，待入队个数{}，set个数{}，入队总个数{}'.format(
                c, self.url_q.qsize(), len(urls), len(urls_waiting), len(self.url_dateset), self.count))


spy = spyder()


def url_out():
    url = "https://news.163.com"
    c = 0
    while True:
        enq = threading.Thread(target=spy.urls_in, args=[url])
        enq.start()
        url = spy.url_q.get()
        if requests.get(url).status_code == 404:
            continue
        # 解析并保存
        response = requests.get(url)
        content_item, comment_items = next(
            parse_news(response=response)
        )
        data2mysql(content_item, comment_items)
        print('processing')
        c += 1
        if c % 100 == 0:
            print("出对总{}次，url：{}".format(c, url))
url_out()