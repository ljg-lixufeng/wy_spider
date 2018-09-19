import threading
import queue
from data2SQL import docIds_from_mysql
from WY_reply import next_urls


def test():
    url_q = queue.Queue(maxsize=2)

    def url_in(url):
        url_q.put(url)

    def url_out():
        while True:
            url = url_q.get()
            if url == 4:
                break
            print(url_q.qsize())
            print('出对操作', url)

    enq = threading.Thread(
        target=url_in, args=[4]
    )

    deq = threading.Thread(
        target=url_out
    )

    enq.start()
    deq.start()


def url_manager(url, function):
    return function(url)

def urls_(url, url_q):
    # 1.得到url
    urls = next_urls(url)
    url_set = docIds_from_mysql()
    urls_waiting = [url for url in urls if url not in url_set]
    url_set.update(urls)

    def enq(urls):
        for url in urls:
            url_q.put(url)

    url_in = threading.Thread(target=enq, args=[urls])
    url_in.start()
    #url_out = threading.Thread(target=deq)

def main():
    url_q = queue.Queue()
    url = "url"
    def deq():
        return url_q.get()

    while True:
        urls_(url, url_q)
        url_out = threading.Thread(target=deq)
        url = url_out