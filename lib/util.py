#! /bin/env python
#! -*coding:GBK-*-

__author__ = 'lvleibing01'

import hashlib


def get_novel_cluster_table_id(book_name):
    """
    """

    CLUSTER_TABLES_NUM = 256
    m = hashlib.md5()
    m.update(book_name.encode("GBK", "ignore"))
    table_id = int(m.hexdigest(), 16) % CLUSTER_TABLES_NUM

    return table_id


def fetch_object_key(url):
    """
    """

    m = hashlib.md5()

    rindex = url.rfind('.')
    suffix = url[rindex: ][1: ]

    m.update(url)
    object_prefix = m.hexdigest()
    object_suffix = suffix

    key = '.'.join([object_prefix, object_suffix])

    return key

if __name__ == '__main__':

    url = 'http://image.cmfu.com/books/2750457/2750457.jpg'

    print fetch_object_key(url)
