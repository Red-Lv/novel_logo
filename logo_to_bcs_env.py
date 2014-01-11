#! /bin/env python
#! -*coding:GBK-*-

__author__ = 'lvleibing01'

from logo2bcs.logo_to_bcs import *


if __name__ == '__main__':

    logo2bcs = Logo2BCS()
    logo2bcs.init('./conf/config.txt')

    logo2bcs.run()
