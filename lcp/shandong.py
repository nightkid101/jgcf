import re
import os
import subprocess

from xlrd import open_workbook, xldate_as_tuple
from init import config_init
from oss_utils import oss_add_file, init_ali_oss
from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin

ali_bucket = init_ali_oss()
config = config_init()


def shandong_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '山东保监局', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('山东保监局 ' + 'Url to parse: %s' % announcement_url)

        r = request_site_page(announcement_url)
        if r is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_soup = bs(r.content, 'lxml') if r else bs('', 'lxml')

        table_content = content_soup.find(id='tab_content')
        if not table_content:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_text = get_content_text(table_content.find_all('tr')[3])
        if content_text == '':
            continue
        title = table_content.find_all('tr')[0].text.strip()

        if '行政处罚事项' in title:
            if len([each_link.attrs['href'] for each_link in table_content.find_all('a')
                    if '.xls' in each_link.attrs['href'].strip()]) > 0:
                xlsx_link = [urljoin(announcement_url, each_link.attrs['href'])
                             for each_link in table_content.find_all('a')
                             if '.xls' in each_link.attrs['href'].strip()][0]

                response = request_site_page(xlsx_link)
                with open('./test/tmp.xlsx', 'wb') as f:
                    f.write(response.content)

                with open('./test/tmp.xlsx', 'rb') as xlsx_file:
                    xlsx_content = xlsx_file.read()

                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': xlsx_link}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': xlsx_link,
                        'origin_url_id': each_circ_data['_id'],
                        'oss_file_type': 'xlsx',
                        'oss_file_name': announcement_title,
                        'oss_file_content': xlsx_content,
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.xlsx', xlsx_content)
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': xlsx_link})['_id']

                excel_data = open_workbook('./test/tmp.xlsx')
                logger.info('删除tmp文件')
                os.remove('./test/tmp.xlsx')

                sheet = excel_data.sheets()[0]
                if title == '山东保监局2013年行政处罚事项（七）' or title == '山东保监局2012年行政处罚事项（一）' \
                        or title == '山东保监局2011年行政处罚事项（九）' or title == '山东保监局2011年行政处罚事项（八）' \
                        or title == '山东保监局2011年行政处罚事项（六）' or title == '山东保监局2011年行政处罚事项（五）' \
                        or title == '山东保监局2011年行政处罚事项（四）' or title == '山东保监局2011年行政处罚事项（三）' \
                        or title == '山东保监局2011年行政处罚事项（二）':
                    sheet = excel_data.sheets()[-1]
                if title == '山东保监局2012年行政处罚事项（四）':
                    sheet = excel_data.sheets()[-2]
                if title == '山东保监局2012年行政处罚事项（三）':
                    sheet = excel_data.sheets()[-3]
                if title == '山东保监局2010年行政处罚事项（九）' or title == '山东保监局2010年行政处罚事项（八）' \
                        or title == '山东保监局2009年9月行政处罚事项' or title == '山东保监局2011年行政处罚事项（一）':
                    sheet = excel_data.sheets()[1]
                result_map_list = []
                if title == '山东保监局2010年行政处罚事项(六)' or title == '山东保监局2010年行政处罚事项(五)':
                    for i in range(sheet.nrows):
                        if i > 3:
                            if sheet.ncols == 8:
                                document_code = sheet.cell(i, 2).value
                                real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                litigant = sheet.cell(i, 3).value
                                if sheet.cell(i, 1).ctype == 3:
                                    publish_date = xldate_as_tuple(sheet.cell_value(i, 1), excel_data.datemode)
                                    publish_date = str(publish_date[0]) + '年' + str(publish_date[1]) + '月' + str(
                                        publish_date[2]) + '日'
                                else:
                                    publish_date = str(sheet.cell(i, 1).value.year) + '年' + str(
                                        sheet.cell(i, 1).value.month) + '月' + str(sheet.cell(i, 1).value.day) + '日'
                                truth = litigant + sheet.cell(i, 4).value
                                defense = defense_response = ''

                                punishment_decision = '依据' + sheet.cell(i, 6).value + '，我局对' + \
                                                      litigant + '作出以下处罚：' + \
                                                      sheet.cell(i, 7).value
                                punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 5).value
                            else:
                                document_code = sheet.cell(i, 1).value
                                real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                litigant = sheet.cell(i, 2).value
                                if sheet.cell(i, 0).ctype == 3:
                                    publish_date = xldate_as_tuple(sheet.cell_value(i, 0), excel_data.datemode)
                                    publish_date = str(publish_date[0]) + '年' + str(publish_date[1]) + '月' + str(
                                        publish_date[2]) + '日'
                                else:
                                    publish_date = str(sheet.cell(i, 0).value.year) + '年' + str(
                                        sheet.cell(i, 0).value.month) + '月' + str(sheet.cell(i, 0).value.day) + '日'
                                truth = litigant + sheet.cell(i, 3).value
                                defense = defense_response = ''
                                punishment_decision = '依据' + sheet.cell(i, 5).value + '，我局对' + \
                                                      litigant + '作出以下处罚：' + \
                                                      sheet.cell(i, 6).value
                                punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 4).value

                            each_map = {
                                'announcementTitle': real_title,
                                'announcementOrg': '山东保监局',
                                'announcementDate': publish_date,
                                'announcementCode': document_code,
                                'facts': truth,
                                'defenseOpinion': defense,
                                'defenseResponse': defense_response,
                                'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                'punishmentBasement': punishment_basis,
                                'punishmentDecision': punishment_decision,
                                'type': '行政处罚决定',
                                'oss_file_id': file_id,
                                'status': 'not checked'
                            }
                            if db.announcement.find(
                                    {'announcementTitle': real_title,
                                     'oss_file_id': file_id,
                                     'litigant': each_map['litigant']}).count() == 0:
                                db.announcement.insert_one(each_map)
                                logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                            else:
                                logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                            result_map_list.append(each_map)
                    if len(result_map_list) > 0:
                        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                        logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                    else:
                        logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
                else:
                    if title == '山东保监局2010年行政处罚事项（二）' or title == '山东保监局2010年行政处罚事项（一）' \
                            or title == '山东保监局2009年12月行政处罚事项':
                        for i in range(sheet.nrows):
                            if i > 1:
                                document_code = sheet.cell(i, 1).value
                                real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                litigant = sheet.cell(i, 0).value
                                if sheet.cell(i, 2).ctype == 3:
                                    publish_date = xldate_as_tuple(sheet.cell_value(i, 2), excel_data.datemode)
                                    publish_date = str(publish_date[0]) + '年' + str(publish_date[1]) + '月' + str(
                                        publish_date[2]) + '日'
                                else:
                                    publish_date = str(sheet.cell(i, 2).value.year) + '年' + str(
                                        sheet.cell(i, 2).value.month) + '月' + str(sheet.cell(i, 2).value.day) + '日'
                                truth = litigant + sheet.cell(i, 6).value
                                defense = defense_response = ''
                                punishment_decision = '依据' + sheet.cell(i, 5).value + '，我局对' + \
                                                      litigant + '作出以下处罚：' + \
                                                      sheet.cell(i, 3).value
                                punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 4).value

                                each_map = {
                                    'announcementTitle': real_title,
                                    'announcementOrg': '山东保监局',
                                    'announcementDate': publish_date,
                                    'announcementCode': document_code,
                                    'facts': truth,
                                    'defenseOpinion': defense,
                                    'defenseResponse': defense_response,
                                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                    'punishmentBasement': punishment_basis,
                                    'punishmentDecision': punishment_decision,
                                    'type': '行政处罚决定',
                                    'oss_file_id': file_id,
                                    'status': 'not checked'
                                }
                                if db.announcement.find(
                                        {'announcementTitle': real_title,
                                         'oss_file_id': file_id,
                                         'litigant': each_map['litigant']}).count() == 0:
                                    db.announcement.insert_one(each_map)
                                    logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                else:
                                    logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                result_map_list.append(each_map)
                        if len(result_map_list) > 0:
                            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                            logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                        else:
                            logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
                    else:
                        if title == '山东保监局2009年11月行政处罚事项':
                            for i in range(sheet.nrows):
                                if i > 3:
                                    document_code = sheet.cell(i, 0).value
                                    real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                    litigant = sheet.cell(i, 1).value
                                    if sheet.cell(i, 4).ctype == 3:
                                        publish_date = xldate_as_tuple(sheet.cell_value(i, 4), excel_data.datemode)
                                        publish_date = str(publish_date[0]) + '年' + str(publish_date[1]) + '月' + str(
                                            publish_date[2]) + '日'
                                    else:
                                        publish_date = str(sheet.cell(i, 4).value.year) + '年' + str(
                                            sheet.cell(i, 4).value.month) + '月' + str(sheet.cell(i, 4).value.day) + '日'
                                    truth = litigant + sheet.cell(i, 6).value
                                    defense = defense_response = ''
                                    punishment_decision = '依据' + sheet.cell(i, 3).value + '，我局对' + \
                                                          litigant + '作出以下处罚：' + \
                                                          sheet.cell(i, 5).value
                                    punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 2).value

                                    each_map = {
                                        'announcementTitle': real_title,
                                        'announcementOrg': '山东保监局',
                                        'announcementDate': publish_date,
                                        'announcementCode': document_code,
                                        'facts': truth,
                                        'defenseOpinion': defense,
                                        'defenseResponse': defense_response,
                                        'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                        'punishmentBasement': punishment_basis,
                                        'punishmentDecision': punishment_decision,
                                        'type': '行政处罚决定',
                                        'oss_file_id': file_id,
                                        'status': 'not checked'
                                    }
                                    if db.announcement.find(
                                            {'announcementTitle': real_title,
                                             'oss_file_id': file_id,
                                             'litigant': each_map['litigant']}).count() == 0:
                                        db.announcement.insert_one(each_map)
                                        logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                    else:
                                        logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                    result_map_list.append(each_map)
                            if len(result_map_list) > 0:
                                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                                logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                            else:
                                logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
                        else:
                            if title == '山东保监局2009年10月行政处罚事项':
                                for i in range(sheet.nrows):
                                    if i > 4:
                                        document_code = sheet.cell(i, 1).value
                                        real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                        litigant = sheet.cell(i, 2).value
                                        if sheet.cell(i, 0).ctype == 3:
                                            publish_date = xldate_as_tuple(sheet.cell_value(i, 0), excel_data.datemode)
                                            publish_date = str(publish_date[0]) + '年' + str(
                                                publish_date[1]) + '月' + str(
                                                publish_date[2]) + '日'
                                        else:
                                            publish_date = str(sheet.cell(i, 0).value.year) + '年' + str(
                                                sheet.cell(i, 0).value.month) + '月' + str(
                                                sheet.cell(i, 0).value.day) + '日'
                                        truth = litigant + sheet.cell(i, 6).value
                                        defense = defense_response = ''
                                        punishment_decision = '依据' + sheet.cell(i, 4).value + '，我局对' + \
                                                              litigant + '作出以下处罚：' + \
                                                              sheet.cell(i, 5).value
                                        punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 3).value

                                        each_map = {
                                            'announcementTitle': real_title,
                                            'announcementOrg': '山东保监局',
                                            'announcementDate': publish_date,
                                            'announcementCode': document_code,
                                            'facts': truth,
                                            'defenseOpinion': defense,
                                            'defenseResponse': defense_response,
                                            'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                            'punishmentBasement': punishment_basis,
                                            'punishmentDecision': punishment_decision,
                                            'type': '行政处罚决定',
                                            'oss_file_id': file_id,
                                            'status': 'not checked'
                                        }
                                        if db.announcement.find(
                                                {'announcementTitle': real_title,
                                                 'oss_file_id': file_id,
                                                 'litigant': each_map['litigant']}).count() == 0:
                                            db.announcement.insert_one(each_map)
                                            logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                        else:
                                            logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                        result_map_list.append(each_map)
                                if len(result_map_list) > 0:
                                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                                    logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                                else:
                                    logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
                            else:
                                if title == '山东保监局2009年9月行政处罚事项' or title == '山东保监局2009年8月行政处罚事项':
                                    for i in range(sheet.nrows):
                                        if title == '山东保监局2009年9月行政处罚事项':
                                            if i > 2:
                                                document_code = sheet.cell(i, 1).value
                                                real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                                litigant = sheet.cell(i, 2).value
                                                if sheet.cell(i, 0).ctype == 3:
                                                    publish_date = xldate_as_tuple(sheet.cell_value(i, 0),
                                                                                   excel_data.datemode)
                                                    publish_date = str(publish_date[0]) + '年' + str(
                                                        publish_date[1]) + '月' + str(
                                                        publish_date[2]) + '日'
                                                else:
                                                    publish_date = str(sheet.cell(i, 0).value.year) + '年' + str(
                                                        sheet.cell(i, 0).value.month) + '月' + str(
                                                        sheet.cell(i, 0).value.day) + '日'
                                                truth = litigant + sheet.cell(i, 3).value
                                                defense = defense_response = ''
                                                punishment_decision = '依据' + sheet.cell(i, 5).value + '，我局对' + \
                                                                      litigant + '作出以下处罚：' + \
                                                                      sheet.cell(i, 6).value
                                                punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 4).value

                                                each_map = {
                                                    'announcementTitle': real_title,
                                                    'announcementOrg': '山东保监局',
                                                    'announcementDate': publish_date,
                                                    'announcementCode': document_code,
                                                    'facts': truth,
                                                    'defenseOpinion': defense,
                                                    'defenseResponse': defense_response,
                                                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                                    'punishmentBasement': punishment_basis,
                                                    'punishmentDecision': punishment_decision,
                                                    'type': '行政处罚决定',
                                                    'oss_file_id': file_id,
                                                    'status': 'not checked'
                                                }
                                                if db.announcement.find(
                                                        {'announcementTitle': real_title,
                                                         'oss_file_id': file_id,
                                                         'litigant': each_map['litigant']}).count() == 0:
                                                    db.announcement.insert_one(each_map)
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                                else:
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                                result_map_list.append(each_map)
                                        else:
                                            if i > 3:
                                                document_code = sheet.cell(i, 1).value
                                                real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                                litigant = sheet.cell(i, 2).value
                                                if sheet.cell(i, 0).ctype == 3:
                                                    publish_date = xldate_as_tuple(sheet.cell_value(i, 0),
                                                                                   excel_data.datemode)
                                                    publish_date = str(publish_date[0]) + '年' + str(
                                                        publish_date[1]) + '月' + str(
                                                        publish_date[2]) + '日'
                                                else:
                                                    publish_date = str(sheet.cell(i, 0).value.year) + '年' + str(
                                                        sheet.cell(i, 0).value.month) + '月' + str(
                                                        sheet.cell(i, 0).value.day) + '日'
                                                truth = litigant + sheet.cell(i, 3).value
                                                defense = defense_response = ''
                                                punishment_decision = '依据' + sheet.cell(i, 5).value + '，我局对' + \
                                                                      litigant + '作出以下处罚：' + \
                                                                      sheet.cell(i, 6).value
                                                punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 4).value

                                                each_map = {
                                                    'announcementTitle': real_title,
                                                    'announcementOrg': '山东保监局',
                                                    'announcementDate': publish_date,
                                                    'announcementCode': document_code,
                                                    'facts': truth,
                                                    'defenseOpinion': defense,
                                                    'defenseResponse': defense_response,
                                                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                                    'punishmentBasement': punishment_basis,
                                                    'punishmentDecision': punishment_decision,
                                                    'type': '行政处罚决定',
                                                    'oss_file_id': file_id,
                                                    'status': 'not checked'
                                                }
                                                if db.announcement.find(
                                                        {'announcementTitle': real_title,
                                                         'oss_file_id': file_id,
                                                         'litigant': each_map['litigant']}).count() == 0:
                                                    db.announcement.insert_one(each_map)
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                                else:
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                                result_map_list.append(each_map)
                                    if len(result_map_list) > 0:
                                        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                                        logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                                    else:
                                        logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
                                else:
                                    if title == '山东保监局2009年7月行政处罚事项':
                                        for i in range(sheet.nrows):
                                            if i > 3 and sheet.cell(i, 0).value != '':
                                                document_code = sheet.cell(i, 1).value
                                                real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                                litigant = sheet.cell(i, 2).value
                                                if sheet.cell(i, 0).ctype == 3:
                                                    publish_date = xldate_as_tuple(sheet.cell_value(i, 0),
                                                                                   excel_data.datemode)
                                                    publish_date = str(publish_date[0]) + '年' + str(
                                                        publish_date[1]) + '月' + str(
                                                        publish_date[2]) + '日'
                                                else:
                                                    publish_date = str(sheet.cell(i, 0).value.year) + '年' + str(
                                                        sheet.cell(i, 0).value.month) + '月' + str(
                                                        sheet.cell(i, 0).value.day) + '日'
                                                truth = litigant + sheet.cell(i, 3).value
                                                defense = defense_response = ''
                                                punishment_decision = '依据' + sheet.cell(i, 5).value + '，我局对' + \
                                                                      litigant + '作出以下处罚：' + \
                                                                      sheet.cell(i, 6).value
                                                punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 4).value

                                                each_map = {
                                                    'announcementTitle': real_title,
                                                    'announcementOrg': '山东保监局',
                                                    'announcementDate': publish_date,
                                                    'announcementCode': document_code,
                                                    'facts': truth,
                                                    'defenseOpinion': defense,
                                                    'defenseResponse': defense_response,
                                                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                                    'punishmentBasement': punishment_basis,
                                                    'punishmentDecision': punishment_decision,
                                                    'type': '行政处罚决定',
                                                    'oss_file_id': file_id,
                                                    'status': 'not checked'
                                                }
                                                if db.announcement.find(
                                                        {'announcementTitle': real_title,
                                                         'oss_file_id': file_id,
                                                         'litigant': each_map['litigant']}).count() == 0:
                                                    db.announcement.insert_one(each_map)
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                                else:
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                                result_map_list.append(each_map)
                                        if len(result_map_list) > 0:
                                            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                                            logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                                        else:
                                            logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
                                    else:
                                        for i in range(sheet.nrows):
                                            if i > 1 and sheet.cell(i, 0).value != '':
                                                if sheet.ncols == 8 or title == '山东保监局2013年行政处罚事项（七）':
                                                    document_code = sheet.cell(i, 0).value
                                                    real_title = '山东保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                                    litigant = sheet.cell(i, 1).value + '\n' + sheet.cell(i, 7).value
                                                    if sheet.cell(i, 2).ctype == 3:
                                                        publish_date = xldate_as_tuple(sheet.cell_value(i, 2),
                                                                                       excel_data.datemode)
                                                        publish_date = str(publish_date[0]) + '年' + str(
                                                            publish_date[1]) + '月' + str(
                                                            publish_date[2]) + '日'
                                                    else:
                                                        publish_date = str(sheet.cell(i, 2).value.year) + '年' + str(
                                                            sheet.cell(i, 2).value.month) + '月' + str(
                                                            sheet.cell(i, 2).value.day) + '日'
                                                    truth = litigant + sheet.cell(i, 6).value
                                                    defense = defense_response = ''
                                                    punishment_decision = '依据' + sheet.cell(i, 5).value + '，我局对' + \
                                                                          litigant + '作出以下处罚：' + \
                                                                          sheet.cell(i, 3).value
                                                    punishment_basis = litigant + '上述行为违反了' + sheet.cell(i, 4).value
                                                else:
                                                    document_code = sheet.cell(i, 0).value
                                                    real_title = '山东银保监局行政处罚决定书' + '(' + str(document_code) + ')'
                                                    litigant = sheet.cell(i, 1).value + '\n' + sheet.cell(i, 6).value
                                                    if sheet.cell(i, 2).ctype == 3:
                                                        publish_date = xldate_as_tuple(sheet.cell_value(i, 2),
                                                                                       excel_data.datemode)
                                                        publish_date = str(publish_date[0]) + '年' + str(
                                                            publish_date[1]) + '月' + str(
                                                            publish_date[2]) + '日'
                                                    else:
                                                        publish_date = str(sheet.cell(i, 2).value.year) + '年' + str(
                                                            sheet.cell(i, 2).value.month) + '月' + str(
                                                            sheet.cell(i, 2).value.day) + '日'
                                                    truth = litigant + sheet.cell(i, 5).value
                                                    defense = defense_response = ''
                                                    punishment_decision = '依据' + sheet.cell(i, 4).value + '，我局对' + \
                                                                          litigant + '作出以下处罚：' + \
                                                                          sheet.cell(i, 3).value
                                                    punishment_basis = ''
                                                each_map = {
                                                    'announcementTitle': real_title,
                                                    'announcementOrg': '山东银保监局',
                                                    'announcementDate': publish_date,
                                                    'announcementCode': document_code,
                                                    'facts': truth,
                                                    'defenseOpinion': defense,
                                                    'defenseResponse': defense_response,
                                                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                                                    'punishmentBasement': punishment_basis,
                                                    'punishmentDecision': punishment_decision,
                                                    'type': '行政处罚决定',
                                                    'oss_file_id': file_id,
                                                    'status': 'not checked'
                                                }
                                                if db.announcement.find(
                                                        {'announcementTitle': real_title,
                                                         'oss_file_id': file_id,
                                                         'litigant': each_map['litigant']}).count() == 0:
                                                    db.announcement.insert_one(each_map)
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                                                else:
                                                    logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                                                result_map_list.append(each_map)
                                        if len(result_map_list) > 0:
                                            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                                            logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                                        else:
                                            logger.warning('山东保监局 数据解析 ' + ' -- 无数据')

            else:
                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': announcement_url,
                        'origin_url_id': each_circ_data['_id'],
                        'oss_file_type': 'html',
                        'oss_file_name': announcement_title,
                        'oss_file_content': r.text.encode(r.encoding).decode('utf-8'),
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                                 r.text.encode(r.encoding).decode('utf-8'))
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                table = table_content.find_all('tr')[3].find('table')
                result_map_list = []
                for tr in table.find_all('tr')[1:]:
                    td_list = tr.find_all('td')
                    document_code = td_list[0].text.strip()
                    real_title = '山东银保监局行政处罚决定书' + '(' + str(document_code) + ')'
                    litigant = td_list[1].text.strip() + '\n' + td_list[7].text.strip()
                    publish_date = str(td_list[2].text.strip()).split('-')[0] + '年' + \
                                   str(td_list[2].text.strip()).split('-')[1] + '月' + \
                                   str(td_list[2].text.strip()).split('-')[2] + '日'
                    truth = litigant + td_list[6].text.strip()
                    defense = defense_response = ''
                    punishment_decision = '依据' + td_list[5].text.strip() + '，我局对' + \
                                          litigant + '作出以下处罚：' + \
                                          td_list[3].text
                    punishment_basis = litigant + '上述行为违反了' + td_list[4].text.strip()
                    each_map = {
                        'announcementTitle': real_title,
                        'announcementOrg': '山东银保监局',
                        'announcementDate': publish_date,
                        'announcementCode': document_code,
                        'facts': truth,
                        'defenseOpinion': defense,
                        'defenseResponse': defense_response,
                        'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                        'punishmentBasement': punishment_basis,
                        'punishmentDecision': punishment_decision,
                        'type': '行政处罚决定',
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                    if db.announcement.find(
                            {'announcementTitle': real_title,
                             'oss_file_id': file_id,
                             'litigant': each_map['litigant']}).count() == 0:
                        db.announcement.insert_one(each_map)
                        logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
                    else:
                        logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
                    result_map_list.append(each_map)
                if len(result_map_list) > 0:
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
                else:
                    logger.warning('山东保监局 数据解析 ' + ' -- 无数据')
        else:
            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': announcement_url,
                    'origin_url_id': each_circ_data['_id'],
                    'oss_file_type': 'html',
                    'oss_file_name': announcement_title,
                    'oss_file_content': r.text.encode(r.encoding).decode('utf-8'),
                    'parsed': False
                }
                insert_response = db.parsed_data.insert_one(oss_file_map)
                file_id = insert_response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                             r.text.encode(r.encoding).decode('utf-8'))
                db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url})['_id']

            document_code_compiler = re.compile(r'(鲁(保监罚|银保监筹?).\n?\d{4}\n?.\n?\d+\n?号)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n'
                    + r'(经查|经检查|依据.*?的有关规定|'
                      r'你(公司|单位).*?经营过程中|'
                      r'你.*?任.*?期间|'
                      r'.*?现场检查|'
                      r'你公司自2006年以来在从事保险营销活动的过程中|'
                      r'你公司在2007年1至2月份，存在|'
                      r'你公司于2005年1月至2006年6月期间，在从事保险代理业务过程中，存在：|'
                      r'你公司在从事保险代理业务过程中，存在：|'
                      r'你公司于2006年1月至8月期间，从事保险经营业务过程中，存在)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                document_code = document_code_compiler.search(title).group(1).strip()
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|'
                                                                  r'你(公司|单位).*?经营过程中|'
                                                                  r'你.*?任.*?期间|'
                                                                  r'.*?现场检查|'
                                                                  r'你公司自2006年以来在从事保险营销活动的过程中|'
                                                                  r'你公司在2007年1至2月份，存在|'
                                                                  r'你公司于2005年1月至2006年6月期间，在从事保险代理业务过程中，存在：|'
                                                                  r'你公司在从事保险代理业务过程中，存在：|'
                                                                  r'你公司于2006年1月至8月期间，从事保险经营业务过程中，存在)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|你.*?经营过程中，存在以下行为：)' \
                             r'([\s\S]*?)' \
                             r'(上述(事实|行为)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                             r'(上述|以上|该)(事实)?行为违反了.*?第.*?条((的|等)规定)?|依据《保险营销员管理规定》第五十七条(的)?规定|' \
                             r'(上述事实)?违反了.*?第.*?条(的规定)?|依据《保险法》第一百四十七条)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(2).strip()
            else:
                truth_text_str = litigant + r'([\s\S]*?)' \
                                            r'(上述(事实|行为)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                            r'(上述|以上|该)(事实)?行为违反了.*?第.*?条((的|等)规定)?|依据《保险营销员管理规定》第五十七条(的)?规定|' \
                                            r'(上述事实)?违反了.*?第.*?条(的规定)?|依据《保险法》第一百四十七条)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?行为.*?申辩意见|当事人[^，。,；\n]*?提出陈述申辩(意见)?|' \
                                   r'[^，。,；\n]*?向我局(报送|递交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：)' \
                                   r'([\s\S]*?))' \
                                   r'(因此，我局决定|' \
                                   r'我局经复核认为|' \
                                   r'本案现已审理终结|' \
                                   r'我局经复查[^，。,；\n]*?情况|' \
                                   r'我局[^，。,；\n]*?复核|' \
                                   r'经研究，对[^，。,；\n]*?予以采纳。|' \
                                   r'我局认为.*?申辩理由|' \
                                   r'依据.*?我局认为.*?的申辩理由|' \
                                   r'经研究，我局认为.*?申辩意见)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0]
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                           + r'((.*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                         r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                         r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(我局经复核认为.|我局决定作出如下处罚：|我局决定作出如下行政处罚：)' \
                                           r'([\s\S]*?)' \
                                           r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                           r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                           r'请.*?在接到本处罚决定书之日)'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
            if punishment_decision_compiler.search(content_text):
                punishment_decision = punishment_decision_compiler.search(content_text).group(2).strip()
            else:
                punishment_decision_text_str = r'(((依据|根据).*?第.*?条.*?(的|之)?规定|' \
                                               r'(依据|根据).*?第.*?条.*?，我局决定)' \
                                               r'([\s\S]*?))' \
                                               r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                               r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                               r'请.*?在接到本处罚决定书之日)'
                punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
                punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                r'经查，你于2006年度在中国太平洋财产保险股份有限公司临邑营销服务部工作期间，该营销服务部存在印制假保单行为。违反了《保险法》第一百二十二条的规定',
                r'经查，你于2004年12月—2007年4月在中国太平洋财产保险股份有限公司德州中心支公司工作期间，公司私设账户并将部分款项转入以你个人名义开具的存折。违反了《保险法》第一百二十二条的规定',
                r'上述违反了《保险公司管理规定》第六十二条、第六十三条、《人身保险新型产品信息披露管理暂行办法》第十四条、第二十二条等规定',
                r'经查，你公司2005年－2007年4月期间，未经山东保监局批准，在东阿设立分支机构并从事保险业务经营活动，违反了《中华人民共和国保险法》第八十条的规定',
                r'经查，你于2005年－2007年4月任天安保险股份有限公司聊城中心支公司总经理期间，公司未经山东保监局批准，在东阿设立分支机构并从事保险业务经营活动，违反了《中华人民共和国保险法》第八十条的规定',
                r'经查，你于2006年2月份任中华联合财产保险股份有限公司临沂中心支公司郯城营销服务部经理期间，公司出具了3份车险阴阳保单，违反了《中华人民共和国保险法》第一百二十二条的规定',
                r'经查，你公司2006年2月出具了3份车险阴阳保单，违反了《中华人民共和国保险法》第一百二十二条的规定',
                r'经查，你于2007年1月－3月任安华农业保险股份有限公司潍坊中心支公司总经理期间，公司未经山东保监局批准，在安丘等地设立分支机构并从事保险业务经营活动，违反了《中华人民共和国保险法》第八十条的规定',
                r'经查，你公司2007年1月－3月期间，未经山东保监局批准，在安丘等地设立分支机构并从事保险业务经营活动，违反了《中华人民共和国保险法》第八十条的规定',
                r'一、产品说明会中对相关产品分红方式的介绍存在错误，违反了《保险法》第一百一十六条第一项的规定',
                r'二、委托无执业证人员展业并支付佣金，违反了《保险销售从业人员监管办法》第十六条的规定',
                r'三、未在犹豫期内完成回访，违反了《人身保险新型产品信息披露管理办法》第十条的规定',
                r'一、财务业务资料不真实，违反了《保险法》第八十六条第二款的规定',
                r'二、妨碍依法监督检查，违反了《保险法》第一百五十五条的规定',
                r'一、财务业务资料不真实，违反了《保险法》第八十六条第二款的规定',
                r'二、妨碍依法监督检查，违反了《保险法》第一百五十五条的规定',
                r'经查，你于2006年11月至2008年在中国人民财产保险股份有限公司平阴支公司工作期间，该机构存在出具阴阳保单的行为, '
                r'涉及企业财产险业务6笔、产品责任保险业务2笔、团体人身意外伤害保险业务1笔，保单正、副本差额合计335508.16元，违反了《中华人民共和国保险法》第一百二十二条的规定'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   r'.(\n?依据|\n?根据|\n?鉴于)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
            if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1]
                m = re.match("([0-9零一二两三四五六七八九十〇○ＯO]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
                real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            else:
                publish_date_text = table_content.find_all('tr')[1].text
                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                    int(publish_date.split('-')[2])) + '日'

            result_map = {
                'announcementTitle': title,
                'announcementOrg': '山东银保监局',
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '行政处罚决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': announcement_title,
                                     'oss_file_id': file_id,
                                     'litigant': result_map['litigant']}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('山东保监局 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('山东保监局 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('山东保监局 数据解析 ' + ' -- 修改parsed完成')
