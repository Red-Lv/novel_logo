#! /bin/env python
#! -*coding:GBK-*-

__author__ = 'lvleibing01'

import hashlib


def get_novel_cluster_table_id(self, book_name):
    """
    """

    CLUSTER_TABLES_NUM = 256
    m = hashlib.md5()
    m.update(book_name.encode("GBK", "ignore"))
    table_id = int(m.hexdigest(), 16) % CLUSTER_TABLES_NUM

    return table_id


def fetch_object_key(self, url):
    """
    """

    m = hashlib.md5()

    rindex = url.rfind('.')
    prefix = url[: rindex]
    suffix = url[rindex: ][1: ]

    m.update(prefix)
    object_prefix = m.hexdigest()
    object_suffix = suffix

    key = '.'.join([object_prefix, object_suffix])

    return key

