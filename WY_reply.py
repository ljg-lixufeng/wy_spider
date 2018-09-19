import re
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup
from scrapy.selector import Selector


def class_urls():
    url = 'https://news.163.com/'
    res = requests.get(url).text
    soup = BeautifulSoup(res, 'html.parser')
    r = soup.find('div','newsdata_nav','clearfix',)
    r1 = re.findall('<a href="(.*?)" ne-role="tab-nav">(..)</a>', str(r))
    r2 = re.findall('<a class="nav_name" href="(.*?)" ne-role="tab-nav">\n(.*)\n', str(r))
    channel_dict = {}
    for lis in [r1, r2]:
        for item in lis:
            channel_dict[item[1].strip()]=item[0]
    channel_dict.pop('房产')
    channel_dict.pop('健康')
    return channel_dict


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
    newsId, productKey, comment_url, creat_time = doc_info(response)
    comment_list, comments = comment_call_back(productKey, newsId)

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
    yield item, comments


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
    # comments_url = ('http://comment.'+ url.split('/')[2] + '/' + boardId + '/' + newsId + '.html')
    return newsId, productKey, comments_url, creat_time


def build_spyder():
    cla_urls = class_urls()
    for (_, url) in cla_urls.items():
        urls = next_urls(requests.get(url))
        for url in urls:
            response = requests.get(url)
            content_item = next(parse_news(response=response))
            print(content_item)
