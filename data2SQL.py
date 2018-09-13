import MySQLdb
import time as tt
# con = MySQLdb.connect(host='localhost', user='root', db='wy', passwd='123321', port=3306, charset='utf8')
# cur = con.cursor() # 创建游标对象，用来执行sql语句，获取数据；
# cur.execute('create table person (id int, name varchar(5), age int)') # 创建表
# cur.execute('insert into person (name, age) values (%s, %s)', ('qiye',20)) # 单条执行
# # 多条执行，效率比单条循环执行单条语句效率快很多
# cur.executemany('insert into person (name, age) values (%s, %s)', [('marry', 20), ('jack', 29)])
# # 插入数据操作不会立即生效，需要提交。（像极了git的操作逻辑）
# con.commit()


def value_or_null(d, key):
    if key not in d.keys():
        return 'NULL'
    elif isinstance(d[key], list) and len(d[key]) == 0:
        return 'NULL'
    else: 
        return d[key]


def data2mysql(content_item, comment_items):
    # content_item
    source= content_item['source']
    docId = content_item['content']['docid']
    category = content_item['content']['category']
    title = str(content_item['content']['title'])
    time = content_item['content']['time']
    #time = tt.strptime(time,'%Y-%m-%d %H:%M')
    passage = content_item['content']['passage']
    productKey = content_item['content']['productKey']
    content_link = content_item['content']['link']
    comment_link = content_item['comment']['link']
    comment_list = str(content_item['comment']['comment_list'])
#time.strptime('2018-03-09 01:18:57','%Y-%m-%d %H:%M:%S')
    con = MySQLdb.connect(host='localhost', user='root', db='wy', passwd='123321', port=3306, charset='utf8mb4')
    cur = con.cursor()  # 创建游标对象，用来执行sql语句，获取数据；
    # cur.execute('create table content '
    #             '('
    #             'source varchar(10),'
    #             'docId varchar(20) unique,'
    #             'category text,'
    #             'title text,'
    #             'time datetime,'
    #             'passage text,'
    #             'productKey varchar(50),'
    #             'content_link text,'
    #             'comment_link text,'
    #             'comment_list mediumtext'
    #             ')'
    #             )
    cur.execute(
        'replace into content '
        '(source, docId, category, title, time, passage, productKey, content_link, comment_link, comment_list)'
        ' values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
        (source, docId, category, title, time, passage, productKey, content_link, comment_link, comment_list)
                )

    # cur.execute('create table comment'
    #             '('
    #             'commentId varchar(100) unique,'
    #             'buildLevel int,'
    #             'createTime datetime,'
    #             'content text,'
    #             'favCount int,'
    #             'isDel varchar(6),'
    #             'postId varchar(30),'
    #             'productKey text,'
    #             'shareCount int,'
    #             'against int,'
    #             'siteName char(2),'
    #             'source varchar(10),'
    #             'user_avatar text,'
    #             'user_incentiveInfoList text,'
    #             'user_location varchar(50),'
    #             'user_nickname varchar(50),'
    #             'user_redNameInfo text,'
    #             'user_Id int,'
    #             'user_vipInfo varchar(20),'
    #             'vote int'
    #             ')')
    cur.executemany('replace into comment (commentId, buildLevel, createTime, content, favCount, isDel, postId,'
                    'productKey, shareCount, against, siteName, source, user_avatar, user_incentiveInfoList, user_location,'
                    'user_nickname, user_redNameInfo, user_Id, user_vipInfo, vote) '
                    'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ',
                    [(str(item['commentId']), item['buildLevel'], item['createTime'], item['content'], item['favCount'],
                      item['isDel'], item['postId'], item['productKey'], item['shareCount'], item['against'],
                      item['siteName'],item['source'],
                      value_or_null(item['user'], 'avatar'),
                      value_or_null(item['user'], 'incentiveInfoList'),
                      value_or_null(item['user'], 'location'),
                      value_or_null(item['user'], 'nickname'),
                      value_or_null(item['user'], 'redNameInfo'),
                      value_or_null(item['user'], 'userId'),
                      value_or_null(item['user'], 'vipInfo'),
                      item['vote'])
                     for item in comment_items.values()])
    con.commit()


def docIds_from_mysql():
    con = MySQLdb.connect(
        host='localhost', user='root', 
        db='wy', passwd='123321', 
        port=3306, charset='utf8mb4')
    cur = con.cursor()  # 创建游标对象，用来执行sql语句，获取数据；
    cur.execute('select docId from content c;')
    docIds = {o[0] for o in cur.fetchall()}
    cur.close()
    con.close()
    return docIds
