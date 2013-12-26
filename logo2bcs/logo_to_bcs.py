#! /bin/env python
#!-*-coding:GBK-*-

__author__ = 'lvleibing01'

import sys
import time
import pybcs
import hashlib
import MySQLdb
import requests

import urlparse
import urllib
import ConfigParser
import subprocess


class Logo2BCS(object):

    def __init__(self):

        pass

    def init(self, config_file):

        config_parser = ConfigParser.ConfigParser()
        config = config_parser(config_file)

        self.module_num = config.getint('logo2bcs', 'module_num')
        self.module_index = config.getint('logo2bcs', 'module_index')
        self.module_index = config.get('logo2bcs', 'image_dir')

        self.timg_noexpired_key = config.get('timg', 'timg_noexpired_key')

        self.bcs_host = config.get('bcs', 'host')
        self.bcs_ak = config.get('bcs', 'ak')
        self.bcs_sk = config.get('bcs', 'sk')

        self.bcs_bucket = config.get('bcs', 'bucket')

        self.bcs = pybcs.BCS(self.bcs_host, self.bcs_ak, self.bcs_sk, pybcs.HttplibHTTPC)

        return True

    def run(self):

        authority_logo_info_list = self.fetch_authority_logo_info_list()

        self.fetch_ori_logo_from_web(authority_logo_info_list)

        self.to_bcs()

        self.substitute_authority_logo(authority_logo_info_list)

        return True

    def fetch_authority_logo_info_list(self):
        """
        """

        print 'start fetching authority logo info list'

        authority_logo_list = []

        try:
            conn = MySQLdb.connect(host='10.46.7.171', port=4198, user='wise_novelclu_w', passwd='C9l3U4n6M2e1',
                                   db='novels_new')
            conn.set_character_set('GBK')
            conn.autocommit(True)
        except Exception as e:
            print 'fail to connect to the db cluster. err: {0}'.format(e)
            return authority_logo_list

        query_sql = 'SELECT rid, book_name, logo FROM novel_authority_info WHERE rid % {0} = {1}' \
                    ''.format(self.module_num, self.module_index)
        delete_sql = ''.format()
        update_sql = ''.format()

        cursor = conn.cursor()

        cursor.execute(query_sql)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        for rid, book_name, logo in rows:

            logo = logo.replace('&amp;', '&')
            authority_logo_list.append((rid, book_name, logo))

        print 'finish fetching authority logo info list. len: {0}'.format(len(authority_logo_list))

        return authority_logo_list

    def fetch_ori_logo_from_web(self, authority_logo_info_list):
        """
        """

        print 'start fetching ori logo from the web'

        tot = len(authority_logo_info_list)
        cur = 0
        for rid, book_name, authority_logo in authority_logo_info_list:

            cur += 1
            print 'start fetching ori logo. cur: {0}, tot: {1}'.format(cur, tot)

            url_parse = urlparse.urlparse(authority_logo)
            query = urlparse.parse_qs(url_parse.query, keep_blank_values=True)
            ori_logo = query.get('src')

            if not ori_logo:
                continue

            ori_logo = ori_logo[0]

            headers = {'referer': query.get('ref', [''])[0]}
            r = requests.get(ori_logo, headers=headers)

            if r.status_code != requests.codes.ok:
                print 'fail to fetch ori logo. rid: {0}, book_name: {1}. ori_logo: {2}. status_code: {3}' \
                      ''.format(rid, book_name, ori_logo, r.status_code)
                continue

            bcs_object_name = self.fetch_object_name(ori_logo)

            with open(self.image_dir + '/' + bcs_object_name, 'w') as fp:

                fp.write(r.text)
                fp.close()

            print 'success in fetching ori logo. rid: {0}, book_name: {1}, ori_logo: {2}, bcs_object_name: {3}' \
                  ''.format(rid, book_name, ori_logo, bcs_object_name)

        print 'stop fetching ori logo from the web'

    def to_bcs(self):
        """
        """

        print 'start uploading ori logo to bcs'

        sub_process = subprocess.Popen('bcsh.py upload -r {0} {1}'.format(self.image_dir, self.bcs_logo_bucket),
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sub_process.wait()

        if sub_process.returncode != 0:
            print 'fail to upload ori logo to bcs. err: {0}'.format(sub_process.stderr.read())
            sys.exit(1)

        print 'finish uploading ori logo to bcs'

    def substitute_authority_logo(self, authority_logo_list):
        """
        """

        print 'start substituting authority logo'

        try:
            conn = MySQLdb.connect(host='10.46.7.171', port=4198, user='wise_novelclu_w', passwd='C9l3U4n6M2e1',
                                   db='novels_new')
            conn.set_character_set('GBK')
            conn.autocommit(True)
        except Exception as e:
            print 'fail to connect to the db cluster. error: {0}'.format(e)
            return False

        query_sql = ''.format()
        delete_sql = ''.format()
        update_sql = 'UPDATE novel_authority_info SET logo = %s WHERE rid = %s'.format()

        cursor = conn.cursor()

        tot = len(authority_logo_list)
        cur = 0
        for rid, book_name, authority_logo in authority_logo:

            cur += 1
            print 'start substituting authority logo. cur: {0}, tot: {1}'.format(cur, tot)

            url_parse = urlparse.urlparse(authority_logo)
            query = urlparse.parse_qs(url_parse.query, keep_blank_values=True)
            ori_logo = query.get('src')

            if not ori_logo:
                continue

            ori_logo = ori_logo[0]
            bcs_object_name = self.fetch_object_name(ori_logo)

            object = self.bcs_buckte(bcs_object_name)
            ori_logo_substitution = object.get_url
            r = requests.get(ori_logo_substitution)

            if r.status_code != requests.codes.ok:
                print 'fail to substitute authority logo. rid: {0}, ori: {1}, substitution: {2}, err: not exists in bcs' \
                      ''.format(rid, ori_logo, ori_logo_substitution)
                continue

            query_string = self.construct_query(query, ori_logo_substitution)
            url_parse.query = query_string
            authority_logo_substitution = urlparse.urlunparse(url_parse)

            authority_logo_substitution = authority_logo_substitution.replace('&', '&amp;')

            cursor.execute(update_sql, (authority_logo_substitution, rid))

            print 'success substituting authority logo. rid: {0}, ori: {1}, substitution: {2}' \
                  ''.format(rid, ori_logo, object.get_url)

        cursor.close()
        conn.close()

        print 'finish substituting authority logo'

    def construct_query(self, query, src_substitution):
        """
        """

        if 'ref' in query:
            del query['ref']

        del query['src']

        timestamp = str(int(time.time()))

        m = hashlib.md5()
        m.update(self.timg_noexpired_key)
        m.update(timestamp)
        m.update(src_substitution)
        di = m.hexdigest()

        query['sec'] = [timestamp]
        query['di'] = [di]

        query_string = urllib.urlencode(query) + '&' + urllib.urlencode({'src': src_substitution})
        return query_string

    def fetch_object_name(self, url):
        """
        """

        rindex = file.rfind('.')

        prefix = ''
        suffix = url

        if rindex != -1:
            prefix = url[: rindex]
            suffix = url[rindex + 1:]

        m = hashlib.md5()
        m.update(prefix)
        name = m.hexdigest()

        return '{0}.{1}'.format(name, suffix)

if __name__ == '__main__':

    logo_to_bcs = Logo2BCS()
    logo_to_bcs.init('../conf/config.txt')

    logo_to_bcs.run()
