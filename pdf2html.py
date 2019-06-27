import copy
import datetime as dt
from decimal import Decimal
import os
import re
from statistics import mean
import string
import subprocess
import json
import math
from collections import defaultdict
from utility import iter_2d_dict
import time

import pdfplumber  # imagemagick -> https://github.com/dahlia/wand/issues/327
from PIL import Image, ImageFile
from bs4 import BeautifulSoup as bs

from init import logger_init, config_init
from aip import AipOcr

config = config_init()
log = logger_init('pdf utility')
temp_dir = 'temp_pdf2html'
rm_flag, img_flag, debug_mode = True, True, False  # whether analysis images in pdf
CHAR_SIZE_UPPER, CHAR_SIZE_LOWER = 30, 1
tocRE = re.compile('(.*?\.+[0-9]+)')  # re.compile('(.*?\.*[0-9]+)')
numRE = re.compile('.*?([0-9]+)')
tocTextRE = re.compile('(.*?)\.+[0-9]+$')  # re.compile('(.*?)\.+[0-9]+$')
tocLineRE = re.compile('^(.*?)\.+[0-9]+$')
tocRomanRE = re.compile('\.{2,}[lxvi]+', re.IGNORECASE)  # iv
numRomanRE = re.compile('(.*?)([lxvi]+)', re.IGNORECASE)
pageNumRE = re.compile('^\d+(?:-\d+)*$|^[-|－]\d+[-|－]$|^[-|－]?[lxvi]+[-|－]?$',
                       re.IGNORECASE)  # 1-1-1 or 11 or -1- or iv
cnRE = re.compile("[^\u4e00-\u9fa5]")
cnt = tdl = 0
im_keys = ['srcsize', 'height', 'width', 'bits']
Image.MAX_IMAGE_PIXELS = 10000000000  # int(5120 * 5120 * 5120 // 4 // 3)
ImageFile.LOAD_TRUNCATED_IMAGES = True
if not os.path.isdir(temp_dir):
    os.mkdir(temp_dir)


def plumber_pdf2html_page(pdf, pid, debug_flag=debug_mode, same=list(), logo=list(),
                          prefix='', double_tolerance=3, res=288):
    """
    :param pdf: pdfplumber pdf object
    :param pid: page number/id start from zero
    :param debug_flag: output debug images
    :param same: head and tail texts share between pages
    :param logo: logo images share between pages
    :param prefix: prefix when generating temp files better assign with id
    :param double_tolerance: lines less than the distance will be treated as double lines (table extraction)
    :param res: resolution when converting page to image 288 == 72 (default) * 4
    :return: dict of the parsed pdf
    """
    try:
        prefix = pdf.stream.name.split('.')[0].split('/')[-1] if not prefix else prefix
    except:
        prefix = str(dt.datetime.today()).replace(' ', '_')
    log.info('Processing {0} page {1}'.format(prefix, pid + 1))
    pdf_page = pdf.pages[pid]
    page_height, page_width = pdf_page.height, pdf_page.width

    chars = pdf_page.chars
    # 判断如果没有文字 就返回空 -- austinzy
    if len(chars) == 0:
        return {'page': pid + 1, 'content': ''}
    char_sizes = []
    # end -- austinzy

    # 删掉重复文本内容 -- austinzy
    delete_index_list = []
    deleted_count = 0
    for char_index, char in enumerate(chars):
        for i in reversed(range(char_index)):
            tmp_char = chars[i]
            if abs(tmp_char['x0'] - char['x0']) < 1 and \
                    abs(tmp_char['y0'] - char['y0']) < 1 and \
                    abs(tmp_char['x1'] - char['x1']) < 1 and \
                    abs(tmp_char['y1'] - char['y1']) < 1 and \
                    tmp_char['text'] == char['text']:
                delete_index_list.append(char_index)
                break
    for each_delete_index in delete_index_list:
        del chars[each_delete_index - deleted_count]
        deleted_count += 1
    # end -- austinzy

    for char in chars:
        if cnRE.sub('', char['text']):
            cs_tmp = char['size']
            if 0 < char['adv'] < 2 and char['adv'] != char['width'] and \
                    CHAR_SIZE_LOWER <= char['width'] / char['adv'] <= CHAR_SIZE_UPPER:
                cs_tmp = char['width'] / char['adv']
            char_sizes.append(cs_tmp)
    ave_cs = max(set(char_sizes), key=char_sizes.count) if char_sizes else Decimal('12.000')

    # 定义可容忍X Y -- austinzy
    if chars[0]['text'] in ['－', '-']:
        y_tolerance = Decimal('6.000')
    else:
        y_tolerance = ave_cs / 3
    x_tolerance = Decimal('12.000')
    # end -- austinzy

    all_text = pdf_page.extract_text(x_tolerance=x_tolerance, y_tolerance=y_tolerance)

    # 如果全是空白符就直接return -- austinzy
    if all_text.strip() == '':
        return {'page': pid + 1, 'content': ''}
    # end -- austinzy

    all_text_line = [kk.strip() for kk in all_text.split('\n') if kk.strip() != ''] if all_text else []
    lines = [i.replace(' ', '') for i in all_text_line]

    # ignore head lines on each page
    orient_mode = 'portrait' if page_height > page_width else 'landscape'
    bl_bottoms = [i['bottom'] + ave_cs for i in same if i['mode'] == orient_mode and i['level'] == 'head']
    bl_tops = [i['top'] - ave_cs for i in same if i['mode'] == orient_mode and i['level'] == 'tail']
    bl_bottom = max(bl_bottoms) if bl_bottoms else 0
    bl_top = min(bl_tops) if bl_tops else page_height
    words = [i for i in pdf_page.extract_words(x_tolerance=x_tolerance, y_tolerance=y_tolerance) if
             'top' in i and i['top'] >= bl_bottom and 'bottom' in i and i['bottom'] <= bl_top]

    line_heights = list(
        filter(lambda x: x > 0, [words[i + 1]['top'] - words[i]['bottom'] for i in range(len(words) - 1)]))
    ave_lh = mean(line_heights) if line_heights else ave_cs / 2  # 平均行高
    if debug_flag:
        im = pdf_page.to_image(resolution=res)
        im.draw_rects(words)
        im.save(os.path.join(temp_dir, prefix + '_text_border_{0}.png'.format(pid + 1)), format="PNG")

    # 去除页面底部的页码 -- austinzy
    page_words = list()
    if words and pageNumRE.findall(words[-1]['text']):
        if words[-1]['bottom'] >= page_height * 3 / 4:
            # if words[-1]['x0'] >= page_width * 4 / 9:
            #     if words[-1]['x1'] <= page_width * 5 / 9:
            page_words.append(words[-1])
    # end -- austinzy

    # 如果因为Y的容忍度不够，导致页码出现三个words，就把页码的全都加入page_words -- austinzy
    if len(words) >= 3:
        if words[-1]['text'] in ['－', '-', '—'] and words[-3]['text'] in ['－', '-', '—'] and \
                re.match('\d+', words[-2]['text']):
            page_words.append(words[-3])
            page_words.append(words[-2])
            page_words.append(words[-1])
    # end -- austinzy

    # 找出在border内的所有words -- austinzy
    in_border_words = []
    for each_word in words:
        if each_word not in page_words:
            in_border_words.append(each_word)
    if len(in_border_words) == 0:
        return {'page': pid + 1, 'content': ''}
    # end -- austinzy

    # calculate paragraph border
    words_outside_table = []
    tt = tb = ll = lr = None  # top-top, top-bottom, left-left, left-right
    same_tmp = []
    for i in same:
        j = copy.deepcopy(i)
        if 'mode' in j:
            j.pop('mode')
        same_tmp.append(j)

    for i in words:
        if (same and i in same_tmp) or i in page_words:
            continue

        tt = i['top'] if tt is None else tt
        tb = i['bottom'] if tb is None else tb
        ll = i['x0'] if ll is None else ll
        lr = i['x1'] if lr is None else lr
        tt = i['top'] if i['top'] < tt else tt
        tb = i['bottom'] if i['bottom'] > tb else tb
        ll = i['x0'] if i['x0'] < ll else ll
        lr = i['x1'] if i['x1'] > lr else lr
        words_outside_table.append(i)

    tt = 0 if tt is None else tt
    tb = page_height if tb is None else tb
    ll = 0 if ll is None else ll
    lr = page_width if lr is None else lr

    if debug_flag:
        im.reset()
        im.draw_rects([(ll, tt, lr, tb)])
        img_file = os.path.join(temp_dir, prefix + '_paragraph_border_table_excluded_{0}.png'.format(pid + 1))
        im.save(img_file, format="PNG")

    new_soup = bs('<div class="pdf-page"></div>', 'lxml')
    page_content = new_soup.div
    new_para = None
    previous_top = previous_bottom = tt
    previous_right = ll
    toc_flag = False
    for line in lines:
        toc_tmp = tocLineRE.findall(line)
        if toc_tmp and toc_tmp[0] and not toc_tmp[0][-1].isdigit():
            toc_flag = True
            break

    for idx, i in enumerate(words):
        if same and i in same_tmp:
            continue
        new_para_flag, new_line_flag = False, True
        div_flag = center_flag = False
        ave_ts = (i['x1'] - i['x0']) / len(i['text'])

        if new_para is None:
            new_para_flag = new_line_flag = True
        else:
            if abs(i['bottom'] - previous_bottom) >= ave_ts / 4:
                new_para_flag = new_line_flag = True
                # 调整了一些参数 -- austinzy
                if abs(i['top'] - previous_bottom) <= max(ave_cs * 6 / 5, ave_lh * 6 / 5):  # 小于 1.2 倍行距 / 平均行高
                    if abs(i['x0'] - ll) <= ave_ts and abs(previous_right - lr) <= max(ave_ts,
                                                                                       ave_cs) * 3 / 2:  # 页面左右边距
                        if abs((i['bottom'] - i['top']) - (previous_bottom - previous_top)) <= ave_lh / 8:
                            if idx >= 1 and abs(words[idx - 1]['x0'] - ll) >= ave_ts * 8:
                                # 解决标题右下角的下标问题
                                new_para_flag = True
                            else:
                                new_para_flag = False  # 被认定为同一个段落
                # end -- austinzy
                if new_para_flag:
                    if abs(page_width - i['x1'] - i['x0']) <= ave_ts / 2:
                        if abs(lr - i['x1']) >= 3 * ave_cs:  # 段前有四个 char_size 大小的空白
                            center_flag = True
                    if i['x0'] > ll + ave_cs * 3:
                        div_flag = True
            elif abs(i['x0'] - previous_right) >= ave_cs * 2:  # 同一行需要判定该段落是否为文本框组合
                if abs(i['top'] - previous_top) <= ave_cs / 2:
                    new_line_flag = new_para_flag = False
                if i['x0'] < previous_right:  # 有一些下标会出现换行问题
                    for char_id, char in enumerate(chars):
                        if i.items() < char.items():
                            break
                    if char_id == 0 or char_id == len(chars) - 1:
                        new_line_flag = True
                    else:
                        line_tmp = new_para.string
                        if line_tmp:
                            for lid, l in enumerate(line_tmp):
                                if lid < len(line_tmp) - 1:
                                    if chars[char_id - 1]['text'] == l:
                                        if chars[char_id + 1]['text'] == line_tmp[lid + 1]:
                                            break
                            if lid == len(line_tmp) - 1:
                                new_line_flag = True
                            else:
                                new_line_text = line_tmp[0:lid + 1] + i['text'] + line_tmp[lid + 1:]
                                new_para.string.replace_with(new_line_text)
                                continue

        if toc_flag:
            new_para_flag = False if previous_bottom > i['top'] else True

        if i in page_words:
            new_para_flag, new_line_flag = False, True

        text = i['text'].strip() if i['text'] else ''
        if new_para_flag:
            if new_para and new_para.text:
                page_content.append(new_para)
            new_para = new_soup.new_tag('p', **{'class': "pdf-paragraph"})
            new_para['style'] = 'margin-left: {0}px;'.format((i['x0'] - ll))
            for char in chars:
                if char['top'] == i['top'] and char['bottom'] == i['bottom'] and char['x0'] == i['x0']:
                    char_size = char['width'] / char['adv'] \
                        if 0 < char['adv'] < 2 and char['adv'] != char['width'] and \
                           CHAR_SIZE_LOWER <= char['width'] / char['adv'] <= CHAR_SIZE_UPPER \
                        else char['size']
                    char_size = char_size if CHAR_SIZE_LOWER <= char_size <= CHAR_SIZE_UPPER else ave_cs
                    if abs(char_size - ave_cs) >= char_size / 10:
                        new_para['style'] += ""
                    break
            if center_flag:
                new_para['align'] = ""
            elif div_flag:
                new_para['style'] += 'margin-left: {0}px;'.format((i['x0'] - ll))
            if not new_para['style']:
                del new_para['style']
        elif not new_line_flag and not toc_flag:
            new_span = new_soup.new_tag('span', **{'class': "text-span"})
            new_span['style'] = 'margin-left: {0}px;'.format(i['x0'] - previous_right)
            new_span.append(text)
            new_para.append(new_span)
            previous_right = i['x1']
            continue

        if i in page_words:
            continue
        prev_text = new_para.string
        if prev_text:
            new_text = prev_text + ' ' + text if text and text[0] in string.ascii_letters else prev_text + text
            new_para.string.replace_with(new_text)
        else:
            new_para.append(text)
        previous_bottom, previous_top, previous_right = i['bottom'], i['top'], i['x1']

    if new_para and new_para.text:
        page_content.append(new_para)

    # 加入page开始以及结束是否为换行的flag -- austinzy
    page_start_flag = True
    page_end_flag = True
    start_line_ave_ts = (in_border_words[0]['x1'] - in_border_words[0]['x0']) / len(in_border_words[0]['text'])
    end_line_ave_ts = (in_border_words[-1]['x1'] - in_border_words[-1]['x0']) / len(in_border_words[-1]['text'])
    if (in_border_words[0]['x0'] - ll) <= start_line_ave_ts:
        page_start_flag = False

    if abs(in_border_words[-1]['x1'] - lr) <= max(end_line_ave_ts, ave_cs) * 3 / 2:
        page_end_flag = False

    return {'page': pid + 1, 'content': str(page_content), 'page_end_flag': page_end_flag,
            'page_start_flag': page_start_flag}
    # end -- austinzy


def check_orientation(pdf, pid):
    orientation = 'landscape' if pdf.pages[pid].width >= pdf.pages[pid].height else 'portrait'
    return orientation


def plumber_pdf_head_tail(pdf, offset=0.1):
    """
    :param pdf: plumber pdf object
    :param offset: head/tail max-height percent form top & bottom of page
    :return: PDF 文件的页眉和页脚
    """
    same = []
    page_num = len(pdf.pages)
    # Portrait pages
    port_pages = [i for i in range(page_num) if pdf.pages[i].width < pdf.pages[i].height and i != 0]
    pt_size = len(port_pages)
    # Landscape pages
    land_pages = [i for i in range(page_num) if pdf.pages[i].width >= pdf.pages[i].height]
    ld_size = len(land_pages)

    def check_same(p1, p2, orientation=None, pure_text=False, same_text=list()):
        # 参数修改 -- austinzy
        fpage = pdf.pages[p1].extract_words(x_tolerance=Decimal('6.00'), y_tolerance=Decimal('6.00'))
        fpl = len(fpage)
        spage = pdf.pages[p2].extract_words(x_tolerance=Decimal('6.00'), y_tolerance=Decimal('6.00')) if p2 else None
        spl = len(spage) if p2 else None

        # end -- austinzy

        def head_tail(s='head', pt=False, st=same_text):
            sps = fpl + 1 if spl is None else spl
            start = 0 if s == 'head' else 1
            end = min(fpl, sps) if s == 'head' else min(fpl, sps) + 1
            for i in range(start, end):
                k = i if s == 'head' else -i
                if abs(fpage[k]['top'] / pdf.pages[p1].height - Decimal(0.5)) <= Decimal(0.5 - offset):
                    break
                fpc = fpage[k]['text'] if pt else fpage[k]
                if st:
                    if fpc in st:
                        fpage[k]['mode'] = check_orientation(pdf, p1)
                        fpage[k]['level'] = s
                        if fpage[k] not in same:
                            same.append(fpage[k])
                    else:
                        break
                else:
                    spc = spage[k]['text'] if pt else spage[k]
                    if fpc == spc:
                        fpage[k]['mode'] = orientation
                        fpage[k]['level'] = s
                        if fpage[k] not in same:
                            same.append(fpage[k])
                    else:
                        break
            if s != 'tail':
                head_tail(s='tail', pt=pt, st=st)

        if pure_text:
            if p2:
                head_tail(s='head', pt=True)
            else:
                head_tail(s='head', pt=True, st=same_text)
        else:
            head_tail(s='head')

    if pt_size + ld_size <= 1:
        return same
    elif pt_size == ld_size == 1:
        check_same(port_pages[0], land_pages[0], pure_text=True)
    elif pt_size == 1:
        check_same(land_pages[0], land_pages[1], orientation='landscape')
        st_tmp = [i['text'] for i in same]
        if st_tmp:
            check_same(port_pages[0], None, pure_text=True, same_text=st_tmp)
    elif ld_size == 1:
        check_same(port_pages[0], port_pages[1], orientation='portrait')
        st_tmp = [i['text'] for i in same]
        if st_tmp:
            check_same(land_pages[0], None, pure_text=True, same_text=st_tmp)
    elif pt_size == 0:
        check_same(land_pages[0], land_pages[1], orientation='landscape')
    elif ld_size == 0:
        check_same(port_pages[0], port_pages[1], orientation='portrait')
    else:
        check_same(port_pages[0], port_pages[1], orientation='portrait')
        check_same(land_pages[0], land_pages[1], orientation='landscape')

    return copy.deepcopy(same)


def plumber_pdf_logo(pdf):
    logo = []
    page_num = len(pdf.pages)
    port_pages = [i for i in range(page_num) if pdf.pages[i].width < pdf.pages[i].height and i != 0]
    pt_size = len(port_pages)
    land_pages = [i for i in range(page_num) if pdf.pages[i].width >= pdf.pages[i].height]
    ld_size = len(land_pages)

    def compare_image(p1, p2, s='head'):
        fpage = pdf.pages[p1].images
        spage = pdf.pages[p2].images
        fpl = len(fpage)
        spl = len(spage)
        start = 0 if s == 'head' else 1
        end = min(fpl, spl) if s == 'head' else min(fpl, spl) + 1
        for i in range(start, end):
            k = i if s == 'head' else -i
            fpc = fpage[k]
            spc = spage[k]
            for bk in copy.deepcopy(fpc).keys():
                if bk not in im_keys:
                    fpc.pop(bk)
            for bk in copy.deepcopy(spc).keys():
                if bk not in im_keys:
                    spc.pop(bk)
            if fpc == spc:
                if fpc not in logo:
                    logo.append(fpc)
            else:
                break
        if s != 'tail':
            compare_image(p1, p2, s='tail')

    if pt_size >= 2:
        compare_image(port_pages[0], port_pages[1])
    if ld_size >= 2:
        compare_image(land_pages[0], land_pages[1])

    for idx, i in enumerate(logo):
        img_tmp = copy.deepcopy(logo[idx])
        for bk in img_tmp.keys():
            if bk not in im_keys:
                logo[idx].pop(bk)

    return copy.deepcopy(logo)


def plumber_pdf2html(fpath, prefix=''):
    with pdfplumber.open(fpath) as pdf:
        page_num = len(pdf.pages)
        log.info('{0} total page num: {1}'.format(prefix, page_num))
        same = plumber_pdf_head_tail(pdf) if page_num > 1 else list()
        logo = plumber_pdf_logo(pdf) if page_num > 1 else list()

        # 判断返回的是否为空 -- austinzy
        html_content = []
        for i in range(page_num):
            result = plumber_pdf2html_page(pdf, i, same=same, logo=logo, prefix=prefix)
            if result['content'] != '':
                html_content.append(result)
        # end -- austinzy
        return html_content


# 加入pdf_to_text直接解析pdf为text -- austinzy
def pdf_to_text(fpath):
    html_content = plumber_pdf2html(fpath)
    pdf_text = ''
    for content_index, each_page_html in enumerate(html_content):
        soup = bs(each_page_html['content'], 'lxml')
        if content_index == 0:
            pdf_text += '\n'.join([each_p.text for each_p in soup.find_all('p')]).strip()
        else:
            if not each_page_html['page_start_flag'] and not html_content[content_index - 1]['page_end_flag']:
                pdf_text += '\n'.join([each_p.text for each_p in soup.find_all('p')]).strip()
            else:
                pdf_text += '\n' + '\n'.join([each_p.text for each_p in soup.find_all('p')]).strip()
    return pdf_text


# baidu ocr 解析png图片
def ocr_with_baidu(png_path):
    client = AipOcr(config['ocr']['baidu_app_id'], config['ocr']['baidu_api_key'], config['ocr']['baidu_secret_key'])

    with open(png_path, 'rb') as fp:
        content = fp.read()

    # 带参数调用通用文字识别, 图片参数为本地图片
    options = {
        'language_type': 'CHN_ENG',
        'detect_direction': 'true',
        'detect_language': 'true',
        'probability': 'false',
    }
    resp = client.basicGeneral(content, options)
    result = resp

    all_text = list()
    if result.get('words_result'):
        for line in result.get('words_result'):
            # print('Block Content: ' + line.get('words'))
            all_text.append(line.get('words'))
    return all_text


# 获取page页面图片上的文本
def pdf_page_to_png(pdf_path, page, page_bit):
    pdf_dir = os.path.dirname(pdf_path)
    pdf_file_name = os.path.basename(pdf_path).replace('.pdf', '')
    png_dir = os.path.join(pdf_dir, pdf_file_name)
    png_name_root = os.path.join(png_dir, 'page')
    png_name = (png_name_root + '-%0' + str(page_bit) + 'd.png') % page

    if not os.path.exists(png_dir):
        os.mkdir(png_dir)
    if not os.path.exists(png_name):
        try:
            subprocess.check_output(['pdftoppm',
                                     '-f', str(page),
                                     '-l', str(page),
                                     '-png',
                                     pdf_path,
                                     png_name_root])
            if os.path.getsize(png_name) > 4194304:
                im = Image.open(png_name)
                w, h = im.size
                im.thumbnail((w // (os.path.getsize(png_name) / 4194304), h // (os.path.getsize(png_name) / 4194304)))
                os.remove(png_name)
                im.save(png_name, 'png')
        except Exception as exc:
            # print(exc)
            raise Exception('页面截图失败 %s' % exc)
    ocr_result = ocr_with_baidu(png_name)
    return '\n'.join(ocr_result)


# 利用ocr解析pdf文本
def pdf_ocr_to_text(file_path):
    # pdf 总页数
    pdf = pdfplumber.open(file_path)
    page_num = len(pdf.pages)
    page_bit = len(str(page_num))

    all_text = list()
    # 遍历所有页面
    for page in range(1, page_num + 1):
        log.info('ocr pdf page %d ' % page)
        # 文本结果比较差的情况，就直接ocr
        page_text = pdf_page_to_png(file_path, page, page_bit)
        # 保存到总文件
        all_text.append(page_text)
    if os.path.exists('./test/tmp/'):
        for each_img in os.listdir('./test/tmp'):
            os.remove('./test/tmp/' + each_img)
        os.rmdir('./test/tmp')
    return '\n'.join(all_text), True


# baidu ocr 解析png图片
def ocr_table_with_baidu(png_path):
    table_result = defaultdict(lambda: defaultdict())
    client = AipOcr(config['ocr']['baidu_app_id'], config['ocr']['baidu_api_key'],
                    config['ocr']['baidu_secret_key'])
    with open(png_path, 'rb') as fp:
        png_content = fp.read()

    result = client.tableRecognitionAsync(png_content)

    if 'error_code' in result:
        return []

    request_id = result['result'][0]['request_id']
    log.info('ocr 等待ocr结果')
    time.sleep(5)
    for i in range(int(math.ceil(20))):
        return_result = client.getTableRecognitionResult(request_id, {'result_type': 'json'})
        # 完成

        if 'result' in return_result.keys():
            log.info('ocr状态：%s' % str(return_result['result']['ret_msg']))
            if int(return_result['result'].get('ret_code', '')) == 3:
                log.info('ocr完成，开始解析table')
                forms_str = return_result['result']['result_data']
                json_acceptable_string = forms_str.replace("'", "\"")
                forms_dict = json.loads(json_acceptable_string)
                # log.info('ocr结果：%s' % str(forms_dict['forms'][0]['body']))
                for each_word in forms_dict['forms'][0]['body']:
                    table_result[int(each_word['row'][0])][int(each_word['column'][0])] = each_word['word']
                table_list = []
                for each_row_list in list(iter_2d_dict(table_result)):
                    if len([i for i in each_row_list if i != '']) > 0:
                        table_list.append(each_row_list)
                # log.info('解析结果：%s' % str(table_list))
                return [''.join(kk) for kk in table_list]
        else:
            log.warning('ocr出错')
            return []
        time.sleep(2)
    return []


# 获取page页面图片上的table
def pdf_page_to_png_table(pdf_path, page, page_bit):
    pdf_dir = os.path.dirname(pdf_path)
    pdf_file_name = os.path.basename(pdf_path).replace('.pdf', '')
    png_dir = os.path.join(pdf_dir, pdf_file_name)
    png_name_root = os.path.join(png_dir, 'page')
    png_name = (png_name_root + '-%0' + str(page_bit) + 'd.png') % page

    if not os.path.exists(png_dir):
        os.mkdir(png_dir)
    if not os.path.exists(png_name):
        try:
            subprocess.check_output(['pdftoppm',
                                     '-f', str(page),
                                     '-l', str(page),
                                     '-png',
                                     pdf_path,
                                     png_name_root])
        except Exception as exc:
            # print(exc)
            raise Exception('页面截图失败 %s' % exc)
    ocr_result = ocr_table_with_baidu(png_name)
    return ocr_result


# 利用ocr解析pdf table 信息
def pdf_ocr_to_table(file_path):
    # pdf 总页数
    pdf = pdfplumber.open(file_path)
    page_num = len(pdf.pages)
    page_bit = len(str(page_num))

    all_table_text = list()
    # 遍历所有页面
    for page in range(1, page_num + 1):
        log.info('ocr pdf page %d ' % page)
        # 文本结果比较差的情况，就直接ocr
        table_text = pdf_page_to_png_table(file_path, page, page_bit)
        # 保存到总文件
        all_table_text.extend(table_text)
    if os.path.exists('./test/tmp/'):
        for each_img in os.listdir('./test/tmp'):
            os.remove('./test/tmp/' + each_img)
        os.rmdir('./test/tmp')
    return all_table_text, True

# end -- austinzy
