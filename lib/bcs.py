#! /bin/env python
#! -*coding:GBK-*-

__author__ = 'lvleibing01'


import hashlib

import pybcs


class BCSExtended(object):

    def __init__(self):
        pass

    def init(self, config):

        self.bcs_host = config.get('bcs', 'host')
        self.bcs_ak = config.get('bcs', 'ak')
        self.bcs_sk = config.get('bcs', 'sk')

        self.bcs_bucket = config.get('bcs', 'bucket')

        self.bcs = pybcs.BCS(self.bcs_host, self.bcs_ak, self.bcs_sk, pybcs.HttplibHTTPC)
        self.bucket = self.bcs.bucket(self.bcs_bucket)

        return True

    def __del__(self):
        pass

    def exit(self):

        return True
