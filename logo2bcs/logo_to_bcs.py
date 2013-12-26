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

        config = ConfigParser.ConfigParser()
        config.read(config_file)
        
        self.module_num = config.getint('logo2bcs', 'module_num')
        self.module_index = config.getint('logo2bcs', 'module_index')
        self.image_dir = config.get('logo2bcs', 'image_dir')

        self.timg_noexpired_key = config.get('timg', 'timg_noexpired_key')

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

        sub_process = subprocess.Popen('cd {0} && rm -rf *'.format(self.image_dir), shell=True)
        sub_process.wait()

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
        #query_sql = 'SELECT rid, book_name, logo FROM novel_authority_info WHERE rid = 1695774947'.format(self.module_num, self.module_index)
        delete_sql = ''.format()
        update_sql = ''.format()

        cursor = conn.cursor()

        cursor.execute(query_sql)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        for rid, book_name, logo in rows:

            logo = logo.replace('&amp;', '&')

            url_parse = urlparse.urlparse(logo)
            query = urlparse.parse_qs(url_parse.query, keep_blank_values=True)
            ori_logo = query.get('src')

            if not ori_logo:
                continue

            ori_logo = ori_logo[0]
            if ori_logo[:23] == 'http://bj.bs.baidu.com/':
                continue

            authority_logo_list.append((rid, book_name, logo))

        print 'finish fetching authority logo info list. len: {0}'.format(len(authority_logo_list))

        return authority_logo_list

    def fetch_ori_logo_from_web(self, authority_logo_info_list):
        """
        """

        print 'start fetching ori logo from the web'

        ori_logo_dict = {}
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

            if ori_logo in ori_logo_dict:
                continue

            #for debugging
            '''
            print authority_logo
            print ori_logo
            '''

            headers = {'referer': query.get('ref', [''])[0]}
            for retry_index in xrange(3):
                try:
                    r = requests.get(ori_logo, headers=headers, timeout=5)
                except Exception as e:
                    continue
                else:
                    break
            else:
                continue

            if r.status_code != requests.codes.ok:
                print 'fail to fetch ori logo. rid: {0}, book_name: {1}. ori_logo: {2}. status_code: {3}' \
                      ''.format(rid, book_name, ori_logo, r.status_code)
                continue

            ori_logo_dict[ori_logo] = 1
            bcs_object_name = self.fetch_object_name(ori_logo)

            with open(self.image_dir + '/' + bcs_object_name, 'wb') as fp:

                fp.write(r.content)
                fp.close()

            print 'success in fetching ori logo. rid: {0}, book_name: {1}, ori_logo: {2}, bcs_object_name: {3}' \
                  ''.format(rid, book_name, ori_logo, bcs_object_name)

        print 'stop fetching ori logo from the web'

    def to_bcs(self):
        """
        """

        print 'start uploading ori logo to bcs'

        #print 'python /home/work/tools/Baidu-BCS-SDK-Python-1.3.2/tools/bcsh.py upload -r {0} {1}'.format(self.image_dir, self.bcs_host + '/' + self.bcs_bucket)
        sub_process = subprocess.Popen('python /home/work/tools/Baidu-BCS-SDK-Python-1.3.2/tools/bcsh.py '
                                       'upload -r {0} {1}'
                                       ''.format(self.image_dir, self.bcs_host + '/' + self.bcs_bucket + '/'),
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
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
            print 'fail to connect to the db cluster. err: {0}'.format(e)
            return False

        query_sql = ''.format()
        delete_sql = ''.format()
        update_sql = 'UPDATE novel_authority_info SET logo = %s WHERE rid = %s'.format()

        cursor = conn.cursor()

        tot = len(authority_logo_list)
        cur = 0
        for rid, book_name, authority_logo in authority_logo_list:

            cur += 1
            print 'start substituting authority logo. cur: {0}, tot: {1}'.format(cur, tot)

            url_parse = urlparse.urlparse(authority_logo)
            query = urlparse.parse_qs(url_parse.query, keep_blank_values=True)
            ori_logo = query.get('src')

            if not ori_logo:
                continue

            ori_logo = ori_logo[0]
            bcs_object_name = self.fetch_object_name(ori_logo)

            object = self.bucket.object('/' + bcs_object_name)
            ori_logo_substitution = object.get_url
            for retry_index in xrange(3):
                try:
                    r = requests.get(ori_logo_substitution, timeout=5)
                except Exception as e:
                    continue
                else:
                    break
            else:
                continue

            if r.status_code != requests.codes.ok:
                print 'fail to substitute authority logo. rid: {0}, ori: {1}, substitution: {2}, err: not exists in bcs' \
                      ''.format(rid, ori_logo, ori_logo_substitution)
                continue

            query_string = self.construct_query(query, ori_logo_substitution)
            authority_logo_substitution = urlparse.urlunparse((url_parse.scheme, url_parse.netloc, url_parse.path,
                                                               url_parse.params, query_string, url_parse.fragment))

            #print authority_logo_substitution
            authority_logo_substitution = authority_logo_substitution.replace('&', '&amp;')

            cursor.execute(update_sql, (authority_logo_substitution, rid))

            print 'success substituting authority logo. rid: {0}, ori: {1}, substitution: {2}' \
                  ''.format(rid, authority_logo, authority_logo_substitution)
            
        cursor.close()
        conn.close()

        print 'finish substituting authority logo'

    def construct_query(self, query, src_substitution):
        """
        """

        if 'ref' in query:
            del query['ref']

        del query['src']
        del query['pa']

        timestamp = str(int(time.time()))

        m = hashlib.md5()
        m.update(self.timg_noexpired_key)
        m.update(timestamp)
        m.update(src_substitution)
        di = m.hexdigest()

        query['sec'] = [timestamp]
        query['di'] = [di]

        query_string = 'pa' + '&' + urllib.urlencode(query, doseq=True) + '&' + \
                       urllib.urlencode({'src': src_substitution})
        return query_string

    def fetch_object_name(self, url):
        """
        """

        rindex = url.rfind('.')

        prefix = url
        suffix = ''

        if rindex != -1:
            prefix = url[: rindex]
            suffix = url[rindex + 1:]

        m = hashlib.md5()
        m.update(prefix)
        name = m.hexdigest()

        return '{0}.{1}'.format(name, suffix)

if __name__ == '__main__':

    logo_to_bcs = Logo2BCS()
    logo_to_bcs.init('./conf/config.txt')

    logo_to_bcs.run()

    logo_to_bcs.exit()
