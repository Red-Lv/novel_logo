#! /bin/env python
#! -*coding:GBK-*-

__author__ = 'lvleibing01'

from logo_check.logo_check import *


if __name__ == '__main__':

    logo_check = LogoCheck()
    logo_check.init('./conf/config.txt')

    logo_check.run()
