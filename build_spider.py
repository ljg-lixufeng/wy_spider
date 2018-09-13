import requests
from data2SQL import data2mysql, docIds_from_mysql
from WY_reply import parse_news
from WY_reply import class_urls, next_urls


def build_spyder():
    temp_docIds = docIds_from_mysql()
    cla_urls = class_urls()
    for (_, url) in cla_urls.items():
        urls = next_urls(requests.get(url))
        for url in urls:
            doc_id = url.split('/')[-1].replace('.html', '')
            if doc_id not in temp_docIds:
                temp_docIds.add(doc_id)
                response = requests.get(url)
                content_item, comment_items = next(
                    parse_news(response=response)
                    )
                data2mysql(content_item, comment_items)
                print('processing')


if __name__ == '__main__':
    build_spyder()



