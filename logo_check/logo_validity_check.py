#! /usr/bin/env python2.7
#! -*-coding:GBK-*-
__author__ = 'lvleibing01'

import urllib2

from DataFrame.basic.LogicModule import *
from DataFrame.public.FrameLog import *
from DataFrame.public.FrameUtils import *


class LogoValidityCheck(LogicModule):

    def __init__(self, name):
        LogicModule.__init__(self, name)

    def init(self, frame):
        LogicModule.init(self, frame)

        self.module_no = self.module_conf.getint('mo_logo_validity_check', 'module_no')
        self.current_module_index =  self.module_conf.getint('mo_logo_validity_check', 'current_module_index')

        return True

    def __del__(self):
        pass

    def exit(self):
        LogicModule.exit(self)

        return True

    def run(self):
        """
        """

        LOG_NOTICE('LogoValidityCheck start')

        self.process()

        LOG_NOTICE('LogoValidityCheck finish')

        return True

    def process(self):
        """
        """

        LOG_NOTICE('Start processing the LogoValidityCheck')

        self.check_logo_validity()

        LOG_NOTICE('Stop processing the LogoValidityCheck')

        return True

    def check_logo_validity(self):
        """
        """

        LOG_NOTICE('Start checking the logo validity')

        conn = self.frame.noveldb.fetch_cluster_dbhandler()

        if not conn:
            LOG_WARNING('Fail to connect to the db')
            return False

        query_sql = 'SELECT rid, book_name, logo FROM novel_authority_info WHERE rid % {0} = {1}' \
                    ''.format(self.module_no, self.current_module_index)
        delete_sql = ''
        update_sql = 'UPDATE novel_authority_info SET logo = "%s", update_time = unix_timestamp() WHERE rid = %s'

        cursor = conn.cursor()
        cursor.execute(query_sql)
        rows = cursor.fetchall()

        cursor.close()

        cur_logo_no = 0
        tot_logo_no = len(rows)
        substitution_logo_no = 0

        for rid, book_name, logo in rows:

            cur_logo_no += 1
            LOG_NOTICE('Start checking the validity of rid[{0}]. cur:{1} tot:{2}'.format(rid, cur_logo_no, tot_logo_no))

            is_valid = self.is_valid_logo(logo)

            if is_valid:
                continue

            substitution_logo_no += 1

            substitution_logo = self.fetch_substitution_logo(rid, book_name)
            if not substitution_logo:
                substitution_logo = ''

            #For debugging
            print 'rid:{0}\tbook_name:{1}'.format(rid, book_name)
            print 'ori_logo:{0}'.format(logo.replace('&amp;', '&'))
            print 'sub_logo:{0}'.format(substitution_logo.replace('&amp;', '&'))

            cursor = conn.cursor()
            cursor.execute(update_sql % (substitution_logo, rid))

            cursor.close()

        self.frame.noveldb.push_back_dbhandler(conn)

        LOG_NOTICE('Finish checking the logo validity. tot_logo_no:{0} substitution_logo_no:{1}'
                   ''.format(tot_logo_no, substitution_logo_no))

        return True

    def is_valid_logo(self, logo):
        """
        """

        LOG_NOTICE('Start verifying the validity of the log[{0}]'.format(logo))

        is_valid = False

        url = logo.replace('&amp;', '&')
        url = url.replace('/timg?', '/timg?er&')

        if url:
            try:
                response = urllib2.urlopen(url)
            except urllib2.HTTPError as e:
                LOG_WARNING('Fail to request url[{0}]. Return Code:{1}'.format(url, e.code))
            except urllib2.URLError as e:
                LOG_WARNING('Fail to request url[{0}]. ErrorInfo:{1}'.format(url, e))
            except:
                LOG_WARNING('Fail to request url[{0}]'.format(url))
            else:
                is_valid = True

        LOG_NOTICE('Finishing verifying the validity of the logo[{0}]. Is_valid:{1}'.format(logo, is_valid))

        return is_valid

    def fetch_substitution_logo(self, rid, book_name):
        """
        """

        LOG_NOTICE('Start fetching the substitution logo')

        substitution_logo = None

        conn = self.frame.noveldb.fetch_cluster_dbhandler()

        if not conn:
            LOG_WARNING('Fail to connect to the db')
            return substitution_logo

        table_id = get_novel_cluster_table_id(book_name.decode('GBK', 'ignore'))

        query_sql = 'SELECT site_id, dir_id, dir_url FROM novel_cluster_info{0} WHERE cluster_id = {1}' \
                    ''.format(table_id, rid)
        delete_sql = ''.format(table_id, rid)
        update_sql = ''.format(table_id, rid)

        cursor = conn.cursor()

        cursor.execute(query_sql)
        rows = cursor.fetchall()

        cursor.close()

        site_dir_dict = {}
        for site_id, dir_id, dir_url in rows:
            site_dir_dict.setdefault(site_id, [])
            site_dir_dict[site_id].append((dir_id, dir_url))

        potential_logo_list = self.fetch_potential_logo_list(site_dir_dict)

        for potential_logo in potential_logo_list:
            if self.is_valid_logo(potential_logo):
                substitution_logo = potential_logo
                break

        self.frame.noveldb.push_back_dbhandler(conn)

        LOG_NOTICE('Finishing fetching the substitution logo. rid:{0} potential_logo_list:{1} substitution_logo:{2}'
                   ''.format(rid, len(potential_logo_list), 1 if substitution_logo else 0))

        return substitution_logo

    def fetch_potential_logo_list(self, site_dir_dict):
        """
        """

        LOG_NOTICE('Start fetching potential logo list')

        potential_logo_list = []

        conn = self.frame.noveldb.fetch_format_dbhandler()

        if not conn:
            LOG_WARNING('Fail to connect to the db')
            return potential_logo_list

        query_sql = 'SELECT site_status, logo FROM dir_fmt_info%s WHERE dir_id in (%s)'
        delete_sql = ''.format()
        update_sql = ''.format()

        cursor = conn.cursor()

        for site_id in site_dir_dict:

            cursor.execute(query_sql % (site_id,
                                        ','.join(['{0}'.format(dir_id) for dir_id, dir_url in site_dir_dict[site_id]])))
            rows = cursor.fetchall()

            for site_status, logo in rows:
                potential_logo_list.append((site_status, logo))

        potential_logo_list = sorted(potential_logo_list, key=lambda item: item[0])
        potential_logo_list = [logo for site_status, logo in potential_logo_list]

        cursor.close()
        self.frame.noveldb.push_back_dbhandler(conn)

        LOG_NOTICE('Finish fetching potential logo list')

        return potential_logo_list
