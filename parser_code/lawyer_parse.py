from xlrd import open_workbook, xldate_as_tuple
import xlwt
from pymongo import MongoClient
import re

lawyer_db = MongoClient(host='192.168.50.130')['xuyc']

excel_data = open_workbook('./律师处罚信息.xlsx')
sheet = excel_data.sheets()[0]

workbook = xlwt.Workbook(encoding='ascii')

worksheet = workbook.add_sheet('全国中小企股份转让系统公司')
worksheet.write(0, 0, label='发文名称')
worksheet.write(0, 1, label='文号')
worksheet.write(0, 2, label='处罚日期')
worksheet.write(0, 3, label='当事人')
worksheet.write(0, 4, label='违法违规事实')
worksheet.write(0, 5, label='申辩意见')
worksheet.write(0, 6, label='申辩意见反馈')
worksheet.write(0, 7, label='监管机构认定意见')
worksheet.write(0, 8, label='处罚决定')
worksheet.write(0, 9, label='发布机构')
worksheet.write(0, 10, label='url')

count = 1
for i in range(sheet.nrows):
    if str(sheet.cell(i, 3).value) != '当事人':
        litigant = str(sheet.cell(i, 3).value)
        lawyer_name = re.split('，|,', litigant)[0]
        lawyer_firm_name = re.split('，|,', litigant)[1]
        lawyer_info = lawyer_db.law_person.find_one({'person_name': lawyer_name, 'firm_name': lawyer_firm_name})
        lawyer_info_map = {}

        litigant = '姓名：' + lawyer_name + '\n' + \
                   '性别：' + lawyer_info['sex']

        if lawyer_info['birth_date'] != '':
            lawyer_info_map['出生日期'] = lawyer_info['birth_date']
        if lawyer_info['firm_name'] != '':
            lawyer_info_map['律师事务所'] = lawyer_info['firm_name']
        if lawyer_info['address'] != '':
            lawyer_info_map['地址'] = lawyer_info['address']
        if lawyer_info['post_in_firm'] != '':
            lawyer_info_map['职务'] = lawyer_info['post_in_firm']
        if lawyer_info['technical_title'] != '':
            lawyer_info_map['职称'] = lawyer_info['technical_title']
        if lawyer_info['practice_category'] != '':
            lawyer_info_map['执业类别'] = lawyer_info['practice_category']
        if lawyer_info['qual_cert_code'] != '':
            lawyer_info_map['资格证号'] = lawyer_info['qual_cert_code']

        for each_info in lawyer_info_map:
            litigant += '\n' + each_info + '：' + lawyer_info_map[each_info]

        print(litigant + '\n')

        if sheet.cell(i, 2).ctype == 3:
            publish_date = xldate_as_tuple(sheet.cell_value(i, 2), excel_data.datemode)
            real_publish_date = str(publish_date[0]) + '年' + str(publish_date[1]) + '月' + str(
                publish_date[2]) + '日'
        else:
            real_publish_date = str(sheet.cell(i, 2).value)
        worksheet.write(count, 0, label=str(sheet.cell(i, 0).value))
        worksheet.write(count, 1, label=str(sheet.cell(i, 1).value))
        worksheet.write(count, 2, label=real_publish_date)
        worksheet.write(count, 3, label=litigant)
        worksheet.write(count, 4, label=str(sheet.cell(i, 4).value))
        worksheet.write(count, 5, label=str(sheet.cell(i, 5).value))
        worksheet.write(count, 6, label=str(sheet.cell(i, 6).value))
        worksheet.write(count, 7, label=str(sheet.cell(i, 7).value))
        worksheet.write(count, 8, label=str(sheet.cell(i, 8).value))
        worksheet.write(count, 9, label=str(sheet.cell(i, 9).value))
        worksheet.write(count, 10, label=str(sheet.cell(i, 10).value))
        count += 1

workbook.save('./lawyer_punishment.xls')