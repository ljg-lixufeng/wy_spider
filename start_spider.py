import threading
import queue
from data2SQL import statistics_from_mysql
from WY_reply import next_urls
from WY_reply import parse_news
from data2SQL import data2mysql
import requests
from log_func import my_log
from WY_reply import class_urls
import random

class spyder():
    def __init__(self):
        self.ids_dateset, self.url_dateset, self.category_count = statistics_from_mysql()
        self.url_q = queue.Queue()
        self.count = 0
        self.log = my_log("WY_spyder")

    # def statistics_class_num(self):



    def urls_in(self, url):
        # 1.得到url
        urls = next_urls(url)
        urls_docid = [url.split('/')[-1].replace('.html', '') for url in urls]
        # 2.过滤并升级set
        urls_waiting = set([url for url in urls if url.split('/')[-1].replace('.html', '') not in self.ids_dateset])
        self.ids_dateset.update(urls_docid)
        self.url_dateset.update(urls)
        # 入队操作
        c = 0
        for url in urls_waiting:
            self.url_q.put(url)
            c += 1
            self.count += 1
        # 如果队列大小为0，从dataset中随机挑一个给队列
        if self.url_q.qsize() == 0:
            print('qsize == 0 ，get random url from urls')
            rand = random.randint(0, len(urls))
            self.url_q.put(urls[rand])
            c += 1
            self.count += 1
        
        if self.count % 100 == 0:
            self.log.info(
                'In message: 本次入队{}次，当前队列总数{}个, 解析到url个数{}，待入队个数{}，set个数{}，入队总个数{}'
                    .format(c, self.url_q.qsize(), len(urls), len(urls_waiting), len(self.ids_dateset), self.count))


def url_out(father_url):
    c = 0
    enq0 = threading.Thread(target=wy.urls_in, args=[father_url])
    enq0.start()
    while True:
        url = wy.url_q.get()
        # 保证所爬url属于输入url的类别,http://sports.163.com
        if father_url.split('/')[2].split('.')[0] != url.split('/')[2].split('.')[0]:
            if wy.url_q.qsize() == 0:
                enq = threading.Thread(target=wy.urls_in, args=[url])
                enq.start()
            continue
        # 入队操作
        enq = threading.Thread(target=wy.urls_in, args=[url])
        enq.start()
        # 页面是404
        try:
            if requests.get(url).status_code == 404:
                continue
        except:
            print('connection error {}'.format(url))
            continue
        # 解析并保存
        response = requests.get(url)
        try:
            content_item, comment_items, tcount = next(parse_news(response=response))
        except:
            wy.log.info('parse error, url:{}'.format(url))
            continue
        # 跟帖小于30的，不保存
        if tcount < 100:
            continue
        try:
            data2mysql(content_item, comment_items)
        except:
            wy.log.info('dateset error, url:{}'.format(url))
            continue
        if c % 2 == 0:
            wy.log.info("out message: 出队第{}次，跟帖：{}个，url：{}".format(c,tcount,url))
        c += 1


if __name__ == '__main__':
    wy = spyder()
    wy.log.info(str(wy.category_count))
    category_urls = class_urls()
    while True:
        for url in category_urls[5:]:
            print('father category:%s '%(url[0]))
            url_out(url[0])
            # 下面这两句代码根本无法执行，上面的rul_out 是个无线循环的
            # if wy.count % 100 == 0:
            #     continue