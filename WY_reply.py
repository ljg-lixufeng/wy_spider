import re
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup
from scrapy.selector import Selector



def father_urls():
    urlstr=r'<a href="http://sports.163.com/">体育</a>' \
        r'<a href="http://sports.163.com/nba/">NBA</a>' \
        r'<a href="http://war.163.com/">军事</a>' \
        r'<a href="http://ent.163.com/">娱乐</a>' \
        r'<a href="http://money.163.com/">财经</a>' \
        r'<a href="http://money.163.com/stock/">股票</a>' \
        r'<a href="http://auto.163.com/">汽车</a>' \
        r'<a href="http://tech.163.com/">科技</a>' \
        r'<a href="http://mobile.163.com/">手机</a>' \
        r'<a href="http://digi.163.com/">数码</a>' \
        r'<a href="http://lady.163.com/">女人</a>' \
        r'<a href="http://travel.163.com/">旅游</a>' \
        r'<a href="http://house.163.com/">房产</a>' \
        r'<a href="http://home.163.com/">家居</a>' \
        r'<a href="http://edu.163.com/">教育</a>' \
        r'<a href="http://book.163.com/">读书</a>' \
        r'<a href="http://jiankang.163.com/">健康</a>' \
        r'<a href="http://art.163.com/">艺术</a>'
    r1 = re.findall('<a href="(.*?)">(.{2,3}?)</a>', urlstr)
    father_urls = {url[0].split('/')[2].split('.')[0]: url for url in r1}
    return father_urls

def next_urls(url):
    res = requests.get(url).text
    urls = re.findall('http://(.{2,9}).163.com/(..)/(....)/(..)/(.*?).html', res)
    next_urls = [ 'http://'+item[0]+'.163.com'+'/'+'/'.join(item[1:])+'.html' for item in urls]
    return next_urls


def comment_url_v1(url):
    reply_id = url.split('/')[-1]
    return 'http://comment.tie.163.com/'+ reply_id


def comment_call_back(productKey, docId):
    List_type = {'hotList_0': {'name': 'hotList','limit': '40', 'offset': '0'},
                 'newList': {'name': 'newList', 'limit': '30', 'offset': '30'},}
                 #'hotList_5': {'name': 'hotList','limit': '35', 'offset': '5'}}
    is_final_page = False
    false = False
    true = True
    comment_ids = []
    comments_dict = {}
    for item in List_type.values():
        if item['name'] == 'hotList':
            url = 'http://comment.api.163.com/api/v1/products/' + productKey + \
                  '/threads/' + docId + \
                  '/comments/' + item['name'] + \
                  '?ibc=newspc&limit=' + item['limit'] + \
                  '&showLevelThreshold=72&headLimit=1&tailLimit=2&offset=' + item['offset'] + \
                  '&callback'
            res = requests.get(url, 'parse.html').text
            comment_back = eval(res)
            comment_ids.extend(comment_back['commentIds'])
            for it in comment_back['comments'].items():
                if it[0] not in comments_dict.keys():
                    comments_dict[it[0]] = it[1]

        elif item['name'] == 'newList':
            page_num = 0
            while not is_final_page:
                offset = int(item['offset']) * page_num
                page_num += 1
                url = 'http://comment.api.163.com/api/v1/products/' + productKey + \
                      '/threads/' + docId + \
                      '/comments/' + item['name'] + \
                      '?ibc=newspc&limit=' + item['limit'] + \
                      '&showLevelThreshold=72&headLimit=1&tailLimit=2&offset=' + str(offset) + \
                      '&callback'

                res = requests.get(url, 'parse.html').text
                comment_back = eval(res)
                is_final_page = not(len(comment_back['commentIds']))

                comment_ids.extend(comment_back['commentIds'])
                for it in comment_back['comments'].items():
                    if it[0] not in comments_dict.keys() :
                        comments_dict[it[0]] = it[1]
    return comment_ids, comments_dict


def ListCombiner(contents):
    s = ''
    for content_ in contents:
        content = content_
        filter_span = re.finditer('<.*?>', str(content))
        for a in filter_span:
            span = a.span()
            content = content.replace(content_[span[0]:span[1]], '')
        content = content+'\n'
        s = s + content
    return s


def parse_news(response):
    #response = requests.get(url)
    item = dict()
    url = response.url
    selector = Selector(response)
    newsId, productKey, comment_url, creat_time, tcount = doc_info(response)
    comment_list, comments = comment_call_back(productKey, newsId)

    tlayer_num = len(comment_list)
    item['source'] = 'netease'
    item['comment'] \
        = {'link': comment_url, 'comment_list':comment_list}
    item['content'] = {'link': str(url), 'title': u'', 'docid': newsId,
                       'productKey': productKey, 'category': u'',
                       'time': u'', 'passage': u''}
    item['content']['title'] = selector.xpath(
        '//*[@id="epContentLeft"]/h1/text()').extract()
    item['content']['category'] = '_'.join(
        selector.xpath("//div[@class='post_crumb']/a/text()").extract())
    item['content']['time'] = creat_time
    item['content']['passage'] \
        = ListCombiner(selector.xpath('//*[@id="endText"]/p/text()').extract())
    yield item, comments, tlayer_num


def doc_info(response):

    url = response.url
    newsId = url.split('/')[-1].replace('.html', '')

    productKey = re.findall(re.compile(r'"productKey" : "(\w+)"'), response.text)[0]

    comments_api = 'http://comment.news.163.com/api/v1/products/' + productKey + '/threads/' + newsId
    boardId = re.findall(r'"boardId":"(\w+)"', str(urlopen(comments_api).read()))[0]
    false = False
    true = True
    info = eval(urlopen(comments_api).read())
    comments_url = 'http://comment.'+ url.split('/')[2] + '/' + info['boardId'] + '/' + newsId + '.html'
    creat_time = info['createTime']
    tcount = info['tcount']
    # comments_url = ('http://comment.'+ url.split('/')[2] + '/' + boardId + '/' + newsId + '.html')
    return newsId, productKey, comments_url, creat_time, tcount


def build_spyder():
    cla_urls = father_urls()
    for (_, url) in cla_urls.items():
        urls = next_urls(requests.get(url))
        for url in urls:
            response = requests.get(url)
            content_item = next(parse_news(response=response))
            print(content_item)
