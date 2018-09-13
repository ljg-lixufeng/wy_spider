import json
import re
from urllib.request import urlopen
import scrapy
from crawl.items import NeteaseItem, SinaItem, TencentItem
from crawl.jsphlim.tool import ListCombiner
from scrapy.http import Request
from scrapy.selector import Selector


class NeteaseNewsSpider(scrapy.Spider):
    name = 'netease_news_spider'  # 最后要调用的名字
    start_urls = ['https://news.163.com']
    allowed_domains = ['news.163.com']

    url_pattern = r'(https://news\.163\.com)/(\d{2})/(\d{4})/(\d+)/(\w+)\.html'

    def parse(self, response):  # response即网页数据
        pat = re.compile(self.url_pattern)
        next_urls = re.findall(pat, str(response.body))

        # debug
        # article = next_urls[0][0]+'/'+next_urls[0][1]+'/'+next_urls[0][2]+'/'+next_urls[0][3]+'/'+next_urls[0][4]+'.html'
        # yield Request(article, callback=self.parse_news)
        # debug

        for next_url in next_urls:
            article = next_url[0] + '/' + next_url[1] + '/' + next_url[2] + '/' + next_url[3] + '/' + next_url[
                4] + '.html'
            print(article)
            yield Request(article, callback=self.parse_news)

    def parse_news(self, response):
        item = NeteaseItem()
        selector = Selector(response)
        pattern = re.match(self.url_pattern, response.url)

        source = 'netease'
        date = '20' + pattern.group(2) + pattern.group(3)
        newsId = pattern.group(5)
        cmtId = pattern.group(5)

        productKey = re.findall(re.compile(r'"productKey" : "(\w+)"'), str(response.body))[0]
        comments_api = 'https://comment.news.163.com/api/v1/products/' + productKey + '/threads/' + newsId
        boardId = re.findall(r'"boardId":"(\w+)"', str(urlopen(comments_api).read()))[0]
        comments = ('https://comment.news.163.com/' + boardId + '/' + newsId + '.html')

        item['source'] = 'netease'
        item['date'] = date
        item['newsId'] = newsId
        item['cmtId'] = cmtId
        # item['boardId'] = boardId
        item['comments'] = {'link': comments}
        item['contents'] = {'link': str(response.url), 'title': u'', 'passage': u''}
        item['contents']['title'] = selector.xpath('//*[@id="epContentLeft"]/h1/text()').extract()
        item['contents']['passage'] = ListCombiner(selector.xpath('//*[@id="endText"]/p').extract())
        yield item
