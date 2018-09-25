import threading
import queue
from data2SQL import statistics_from_mysql
from WY_reply import next_urls
from WY_reply import parse_news
from data2SQL import data2mysql
import requests
from log_func import my_log
from WY_reply import father_urls
import random

class spyder():
    def __init__(self):
        self.ids_dateset, self.url_dateset, _ = statistics_from_mysql()
        self.url_q = queue.LifoQueue()
        self.in_count = 0
        self.out_count = 1
        self.log = my_log("WY_spyder")
        self.category_count = self.statistics_class_num()

    def statistics_class_num(self):
        url_category = [url.split('/')[2].split('.')[0] for url in self.url_dateset]
        category_count_dict = {}
        for x in url_category:
            if x not in category_count_dict.keys():
                category_count_dict[x] = 1
            else: category_count_dict[x] = category_count_dict[x] + 1
        return category_count_dict

    def urls_in(self, url):
        # 1.得到url
        urls = next_urls(url)
        urls_docid = [url.split('/')[-1].replace('.html', '') for url in urls]
        # 2.过滤并升级set
        urls_waiting = set(
            [url for url in urls
             if url.split('/')[-1].replace('.html', '') not in self.ids_dateset])
        self.ids_dateset.update(urls_docid)

        # 入队操作
        c = 0
        for url in urls_waiting:
            self.url_q.put(url)
            c += 1
            self.in_count += 1
        # 如果队列大小为0，从dataset中随机挑一个给队列
        if self.url_q.qsize() == 0:
            print('qsize == 0 ，get random url from urls')
            rand = random.randint(0, len(urls))
            self.url_q.put(urls[rand])
            c += 1
            self.in_count += 1
        
        if self.in_count % 100 == 0:
            self.log.info(
                'In message: 本次入队{}次，当前队列总数{}个, '
                '解析到url个数{}，待入队个数{}，set个数{}，入队总个数{}'
                    .format(
                    c, self.url_q.qsize(), len(urls), len(urls_waiting),
                    len(self.ids_dateset), self.in_count))

    def father_category(self):
        self.category_count.pop('2018')
        self.category_count.pop('hea')
        self.category_count.pop('data')
        min_value = min(self.category_count.values())
        for k,v in self.category_count.items():
            if min_value == v:
                return k


def url_out(father_url):
    enq0 = threading.Thread(target=wy.urls_in, args=[father_url])
    enq0.start()
    while wy.out_count%1000 != 0:
        url = wy.url_q.get()
        # 保证所爬url属于输入url的类别,http://sports.163.com
        if father_url.split('/')[2].split('.')[0] != \
                url.split('/')[2].split('.')[0]:
            if wy.url_q.qsize() == 0:
                enq = threading.Thread(target=wy.urls_in, args=[url])
                enq.start()
            continue
        # 页面是404
        try:
            if requests.get(url).status_code == 404:
                wy.log.info('404 error')
                continue
        except:
            print('connection error {}'.format(url))
            continue

        # 解析并保存
        response = requests.get(url)
        try:
            content_item, comment_items, tcount = next(
                parse_news(response=response))
        except:
            wy.log.info('parse error, url:{}'.format(url))
            continue
        # 跟帖小于30的，不保存
        if tcount < 100:
            wy.log.info(
                'tcount error: tcount:{} url: {}'.format(tcount,url))
            continue
        # 入队操作
        enq = threading.Thread(target=wy.urls_in, args=[url])
        enq.start()
        # 服务器操作
        try:
            data2mysql(content_item, comment_items)
        except:
            wy.log.info('dateset error, url:{}'.format(url))
            continue
        if wy.out_count % 2 == 0:
            wy.log.info(
                "out message: 出队第{}次，跟帖：{}个，url：{}".format(
                    wy.out_count,tcount,url))
        wy.category_count[url.split('/')[2].split('.')[0]] = \
            wy.category_count[url.split('/')[2].split('.')[0]] + 1
        wy.out_count += 1


if __name__ == '__main__':
    wy = spyder()
    wy.log.info(str(wy.category_count))
    father_urls = father_urls()
    while True:
        father_category = wy.father_category()
        print("father category {}".format(father_category))
        father_url = father_urls[father_category]
        print('father category:%s '%(father_url[0]))
        url_out(father_url[0])
