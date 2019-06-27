from pymongo import MongoClient
from init import logger_init, config_init
import re
import xlwt

logger = logger_init('处罚决定 法律法规导出')
config = config_init()
db = MongoClient(
    host=config['mongodb']['mongodb_host'],
    port=int(config['mongodb']['mongodb_port']),
    username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
    password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
    config['mongodb']['mongodb_db_name']]

workbook = xlwt.Workbook(encoding='ascii')

worksheet = workbook.add_sheet('保监|银监|人行')
worksheet.write(0, 0, label='法律法规')

all_law_list = []
# {'announcementOrg': {'$regex': '.*?(保监|银监|人行).*?'}}
for each_announcement in db.punishAnnouncement.find():
    punishment_decision = each_announcement['punishmentBasement'] + '\n' + each_announcement['punishmentDecision']
    punishment_decision = punishment_decision.replace('\r', '').replace('\n', '').replace('\r\n', '')
    law_list = re.findall('(《.*?》(（.*?）)?)', punishment_decision)
    for each_law in law_list:
        if each_law not in all_law_list:
            all_law_list.append(each_law)

count = 1
for each_law in all_law_list:
    worksheet.write(count, 0, label=each_law)
    count += 1

#
workbook.save('./punishment_laws2.xls')


from pymongo import MongoClient
from init import logger_init, config_init
import re
import xlwt

logger = logger_init('处罚决定 法律法规导出')
config = config_init()
db = MongoClient(
    host=config['mongodb']['mongodb_host'],
    port=int(config['mongodb']['mongodb_port']),
    username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
    password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
    'laws_regulations']

laws_list = []
with open('./laws.txt', 'r', encoding='utf-8') as laws_file:
    for line in laws_file:
        laws_list.append(line.strip())

workbook = xlwt.Workbook(encoding='ascii')
worksheet = workbook.add_sheet('对应法规')

count = 0
for each_law in laws_list:
    worksheet.write(count, 0, label=each_law)
    # count += 1
    for each_data in db.laws_data.find({'name': {'$regex': '.*' + each_law + '.*'}}):
        if re.search(r'^《?' + each_law + ' *?(（.*?）)?' + '》?$', each_data['name']) or \
                re.search(r'^《?' + each_law + ' *?(（.*?）)?' + '(、).*?》?$', each_data['name']) or \
                re.search(r'^《?.*?(、)' + each_law + ' *?(（.*?）)?' + '》?$', each_data['name']) or \
                re.search(r'关于发布《' + each_law + ' *?(（.*?）)?' + '》的(公告|通知)$', each_data['name']):
            worksheet.write(count, 1,
                            label=each_data['name'])
            worksheet.write(count, 2,
                            label=str(each_data['_id']))
            worksheet.write(count, 3,
                            label=each_data.get('release_date', ''))
            worksheet.write(count, 4,
                            label=each_data.get('effective_date', ''))
            count += 1
    count += 1

workbook.save('/Users/austinzy/Desktop/laws.xls')
