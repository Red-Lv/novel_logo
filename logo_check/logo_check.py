#! /usr/bin/env python2.7
#! -*-coding:GBK-*-

__author__ = 'lvleibing01'

import MySQLdb
import requests

import hashlib
import urlparse
import ConfigParser


class LogoCheck(object):

    def __init__(self):
        pass

    def init(self, config_file):

        config = ConfigParser.ConfigParser()
        config.read(config_file)

        self.module_num = config.getint('logo_check', 'module_num')
        self.module_index = config.getint('logo_check', 'module_index')

        self.default_logo_file = config.get('logo_check', 'default_logo_file')

        self.default_logo_dict = {}
        try:
            with open(self.default_logo_file) as fp:
                for line in fp:

                    line = line.strip()
                    if not line:
                        continue

                    line = line.split('\t')
                    if len(line) < 3:
                        continue

                    site_id, site, default_logo = line[: 3]
                    self.default_logo_dict[default_logo] = int(site_id)
                    self.default_logo_dict[self.fetch_object_name(default_logo)] = int(site_id)
        except Exception as e:
            print 'fail to read default logo file. err: {0}'.format(e)

        return True

    def __del__(self):
        pass

    def exit(self):

        return True

    def run(self):
        """
        """

        authority_logo_info_list = self.fetch_authority_logo_info_list()

        self.check_logo_validity(authority_logo_info_list)

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

        query_sql = 'SELECT rid, book_name, logo FROM novel_authority_info WHERE rid %% %s = %s' \
                    ''.format()
        delete_sql = ''.format()
        update_sql = ''.format()

        cursor = conn.cursor()

        cursor.execute(query_sql, (self.module_num, self.module_index))
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

                url_parse = urlparse.urlparse(ori_logo)
                path = url_parse.path
                ori_logo_object_name = path.split('/')[-1]
                if ori_logo_object_name not in self.default_logo_dict:
                    continue

            authority_logo_list.append((rid, book_name, logo))

        print 'finish fetching authority logo info list. len: {0}'.format(len(authority_logo_list))

        return authority_logo_list

    def check_logo_validity(self, authority_logo_info_list):
        """
        """

        print 'start checking logo validity'

        tot = len(authority_logo_info_list)
        cur = 0

        for rid, book_name, authority_logo in authority_logo_info_list:

            cur += 1
            print 'start checking logo validity. rid: {0}, cur: {1}, tot: {2}'.format(rid, cur, tot)

            is_optimal = self.is_optimal_logo(authority_logo)

            if is_optimal:
                continue

            substitution_logo = self.fetch_substitution_logo(rid, book_name)
            if not substitution_logo:
                substitution_logo = ''

            if authority_logo == substitution_logo:
                continue

            #For debugging
            print 'rid: {0}, book_name: {1}, ori_logo: {2}, substitution_logo: {3}' \
                  ''.format(rid, book_name, authority_logo, substitution_logo)

            #self.update_authority_logo(rid, substitution_logo)

        print 'finish checking logo validity.'

        return True

    def update_authority_logo(self, rid, authority_logo):
        """
        """

        print 'start updating authority logo. rid: {0}, authority_logo: {1}'.format(rid, authority_logo)

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

        cursor.execute(update_sql, (authority_logo, rid))

        cursor.close()
        conn.close()

        print 'finish updating authority logo. rid: {0}, authority_logo: {1}'.format(rid, authority_logo)

        return True

    def is_optimal_logo(self, logo):
        """
        """

        url_parse = urlparse.urlparse(logo)
        query = urlparse.parse_qs(url_parse.query, keep_blank_values=True)
        ori_logo = query.get('src')

        if not ori_logo:
            return False

        ori_logo = ori_logo[0]

        if ori_logo in self.default_logo_dict:
            return False

        return self.is_valid_logo(logo)

    def is_valid_logo(self, logo):
        """
        """

        print 'start verifying the validity of the logo. logo: {0}'.format(logo)

        is_valid = False

        url = logo.replace('/timg?', '/timg?er&')

        try:
            r = requests.get(url, timeout=10)
            if r.status_code == requests.codes.ok:
                is_valid = True
        except Exception as e:
            print 'fail to fetch the logo. logo: {0}'.format(logo)

        print 'finish verifying the validity of the logo. logo: {0}, is_valid: {1}'.format(logo, is_valid)

        return is_valid

    def fetch_substitution_logo(self, rid, book_name):
        """
        """

        print 'start fetching substitution logo. rid: {0}, book_name: {1}'.format(rid, book_name)

        substitution_logo = None

        try:
            conn = MySQLdb.connect(host='10.46.7.171', port=4198, user='wise_novelclu_w', passwd='C9l3U4n6M2e1',
                                   db='novels_new')
            conn.set_character_set('GBK')
            conn.autocommit(True)
        except Exception as e:
            print 'fail to connect to the db cluster. err: {0}'.format(e)
            return substitution_logo

        table_id = self.get_novel_cluster_table_id(book_name.decode('GBK', 'ignore'))

        query_sql = 'SELECT site_id, dir_id, dir_url FROM novel_cluster_info%s WHERE cluster_id = %s' \
                    ''.format()
        delete_sql = ''.format()
        update_sql = ''.format()

        cursor = conn.cursor()

        cursor.execute(query_sql, (table_id, rid))
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        dir_info_list = rows
        potential_logo_list = self.fetch_potential_logo_list(dir_info_list)

        for potential_logo in potential_logo_list:
            if self.is_valid_logo(potential_logo):
                substitution_logo = potential_logo
                break

        print 'finish fetching substitution logo. rid: {0}, potential_logo_num: {1}, is_substituted:{2}' \
              ''.format(rid, len(potential_logo_list), (substitution_logo is not None))

        return substitution_logo

    def fetch_potential_logo_list(self, dir_info_list):
        """
        """

        print 'start fetching potential logo list'

        potential_logo_list = []

        try:
            conn = MySQLdb.connect(host='10.46.7.172', port=4195, user='wise_novelfmt_w', passwd='H4k3D8v9X2y5',
                                   db='novels')
            conn.set_character_set('GBK')
            conn.autocommit(True)
        except Exception as e:
            print 'fail to connect to the db format. err: {0}'.format(e)
            return potential_logo_list

        query_sql = 'SELECT site_status, logo FROM dir_fmt_info%s WHERE dir_id = %s'
        delete_sql = ''.format()
        update_sql = ''.format()

        cursor = conn.cursor()

        default_logo_list = []
        non_default_logo_list = []

        for site_id, dir_id, dir_url in dir_info_list:

            cursor.execute(query_sql, (site_id, dir_id))
            row = cursor.fetchone()

            if not row:
                continue

            site_status, logo = row
            logo = logo.replace('&amp;', '&')

            url_parse = urlparse.urlparse(logo)
            query = urlparse.parse_qs(url_parse.query, keep_blank_values=True)
            ori_logo = query.get('src')

            if not ori_logo:
                continue

            ori_logo = ori_logo[0]
            if ori_logo in self.default_logo_dict:
                default_logo_list.append((site_status, logo))
            else:
                non_default_logo_list.append((site_status, logo))

        default_logo_list = sorted(default_logo_list, key=lambda item: item[0])
        non_default_logo_list = sorted(non_default_logo_list, key=lambda item: item[0])

        potential_logo_list = non_default_logo_list + default_logo_list
        potential_logo_list = [logo for site_status, logo in potential_logo_list]

        cursor.close()
        conn.close()

        print 'finish fetching potential logo list'

        return potential_logo_list

    def get_novel_cluster_table_id(self, book_name):
        """
        """

        CLUSTER_TABLES_NUM = 256
        m = hashlib.md5()
        m.update(book_name.encode("GBK", "ignore"))
        table_id = int(m.hexdigest(), 16) % CLUSTER_TABLES_NUM

        return table_id

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

    logo_check = LogoCheck()
    logo_check.init('./conf/config.txt')

    logo_check.run()

    logo_check.exit()
