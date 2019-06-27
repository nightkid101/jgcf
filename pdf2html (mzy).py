from bson import ObjectId
from bson.decimal128 import Decimal128
import concurrent.futures as cf
import collections
import copy
import datetime as dt
from decimal import Decimal
import io
import os
import re
from statistics import mean
import string
from functools import partial
from pprint import pprint

import pdfplumber
# imagemagick -> https://github.com/dahlia/wand/issues/327
from PIL import Image, ImageFile
from bs4 import BeautifulSoup as bs
from bs4 import Comment
import requests

from init import logger_init, config_init

# from cmappings import apostrophe_map
# from config import disabled_keys
# from ocr_pdf import baidu_image_ocr
# from oss_utils import initAliOSS, ossAddFile, ossGetFile, saveLocalFile, alioss_base_url

log = logger_init('pdf2html')
temp_dir = 'temp_pdf2html'
rm_flag, img_flag, debug_mode = True, False, False  # whether analyze images in pdf
CHAR_SIZE_UPPER, CHAR_SIZE_LOWER = 30, 1
tocHeadRE = re.compile('^目录$')
tocRE = re.compile(r"(.*?[.…·]+[0-9]+)")  # re.compile('(.*?\.*[0-9]+)')
numRE = re.compile(r".*?([0-9]+)")
tocTextRE = re.compile(r"(.*?)[.…·]+[0-9]+$")  # re.compile('(.*?).*[0-9]+$')
tocLineRE = re.compile(r"^(.*?)[.…·]+[0-9]+$")
tocRomanRE = re.compile(r"[.…·]{2,}[lxvi]+", re.IGNORECASE)  # iv
numRomanRE = re.compile(r"(.*?)([lxvi]+)", re.IGNORECASE)
arabicTocRE = re.compile(r"^[0-9]+[.、·]+")
pageNumRE = re.compile(r"^\d+(?:[-－]+\d+)*$|^[-－]+\d+[-－]+$|^[-－]*[lxvi]+[-－]*$|^\d+(?:-\d+)*[-－]+[lxvi]+$", re.I)
# 1-1-1 or 11 or -1- or iv or 1-1-XVI
cnRE = re.compile(r"[^\u4e00-\u9fff]")
# bucket = initAliOSS()
cnt = tdl = 0
# collpdf2html = mon['touzhiwang']['pdf2html']
# collparsed = mon['touzhiwang']['parsed_pdf']
im_keys = ['srcsize', 'height', 'width', 'bits']
Image.MAX_IMAGE_PIXELS = 10000000000  # int(5120 * 5120 * 5120 // 4 // 3)
ImageFile.LOAD_TRUNCATED_IMAGES = True
cnNum = '一二三四五六七八九十百〇'
chapterZhRE = re.compile(r"^第[{}]+章".format(cnNum))
chapterJieRE = re.compile(r"^第[{}]+节".format(cnNum))
chapterZJRE = re.compile(r"^第[{}]+[章|节]".format(cnNum))
sectionNumRE = re.compile('^[{}]+[、.]|^[0-9]+[、.]'.format(cnNum))
sectionJNRE = re.compile('^[{}]+[、.]|^[0-9]+[、.]|第[{}]+节'.format(cnNum, cnNum))
subSectionRE = re.compile('^[（(][{}]+[)）]|^[（(][0-9]+[)）]'.format(cnNum))
navigationRE = re.compile('^[{}]+[、.]|^第[{}]+[章|节]|^（[{}]+）|^[0-9]+[、.]|^[（(][0-9]+[)）]'
                          .format(cnNum, cnNum, cnNum))
tocLackDotsRE = re.compile('[^.…·0-9]([0-9]+)([{}]+[、.]|第[{}]+[章|节]|（[{}]+）)'.format(cnNum, cnNum, cnNum))

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
    log.info('Processing {0} page [{1}/{2}]'.format(prefix, pid, len(pdf.pages)))
    pdf_page = pdf.pages[pid]
    page_height, page_width = pdf_page.height, pdf_page.width
    all_text = pdf_page.extract_text()
    all_text_line = all_text.split('\n') if all_text else []
    lines = [i.replace(' ', '') for i in all_text_line]
    chars = pdf_page.chars
    char_sizes = []

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

    for char in chars:
        if cnRE.sub('', char['text']):
            cs_tmp = char['size']
            if 0 < char['adv'] < 2 and char['adv'] != char['width'] and \
                    CHAR_SIZE_LOWER <= char['width'] / char['adv'] <= CHAR_SIZE_UPPER:
                cs_tmp = char['width'] / char['adv']
            char_sizes.append(cs_tmp)

    ave_cs = max(set(char_sizes), key=char_sizes.count) if char_sizes else Decimal('12.000')
    y_tolerance = 3 if ave_cs / 3 <= 4 else ave_cs / 2
    min_cs = min(char_sizes) if char_sizes else Decimal('12.000')
    same_text = [x['text'] for x in same]
    # ignore head lines on each page
    orient_mode = 'portrait' if page_height > page_width else 'landscape'
    mpft = 1  # ave_cs  #  main page frame tolerance
    bl_bottoms = [i['bottom'] + mpft for i in same if i['mode'] == orient_mode and i['level'] == 'head']
    bl_tops = [i['top'] - mpft for i in same if i['mode'] == orient_mode and i['level'] == 'tail']
    bl_bottom = max(bl_bottoms) if bl_bottoms else 0
    bl_top = min(bl_tops) if bl_tops else page_height
    words = [i for i in pdf_page.extract_words(x_tolerance=ave_cs * 3 / 2, y_tolerance=y_tolerance)
             if 'top' in i and i['top'] >= bl_bottom and 'bottom' in i and i['bottom'] <= bl_top]
    line_heights = list(filter(lambda x: x > 0, [words[i + 1]['top'] - words[i]['bottom']
                                                 for i in range(len(words) - 1)]))
    ave_lh = mean(line_heights) if line_heights else ave_cs / 2  # 平均行高
    if debug_flag:
        im = pdf_page.to_image(resolution=res)
        im.draw_rects(words)
        im.save(os.path.join(temp_dir, prefix + '_text_border_{0}.png'.format(pid + 1)), format="PNG")

    # 去除页面底部的页码
    page_words = list()
    if words and pageNumRE.findall(words[-1]['text']):
        if words[-1]['bottom'] >= page_height * 3 / 4:
            if words[-1]['x0'] >= page_width * 4 / 9:
                if words[-1]['x1'] <= page_width * 5 / 9:
                    page_words.append(words[-1])

    # extract v/h lines within the page
    table_test = pdf_page.find_tables(table_settings={'edge_min_length': ave_cs})
    table_test = pdf_page.find_tables(table_settings={'vertical_strategy': 'text'}) if not table_test else table_test
    table_special_flag = False
    try:
        main_page = pdf_page.within_bbox((0, bl_bottom, page_width, page_height))
        if table_test:
            rect_edges_raw = main_page.edges
        else:
            rect_edges_raw = []
            hl_raw = [i for i in pdf_page.edges if i['orientation'] == 'h' and i['top'] >= bl_bottom]
            if hl_raw:
                hl_set = list(sorted({i['top'] for i in hl_raw}))
                hl_rs = [list(map(lambda x: x['top'] == i, hl_raw)) for i in hl_set]
                hl_lr = [[[hl_raw[jid]['x0'], hl_raw[jid]['x1']] for jid, j in enumerate(i) if j] for i in hl_rs]
                hl_widths = []
                for i in hl_lr:
                    temp = i[0]
                    for j in i:
                        if j == temp:
                            continue
                        if j[0] <= temp[1] or abs(j[0] - temp[1]) <= 1:
                            temp[1] = j[1]
                        else:
                            break
                    hl_widths.append(abs(temp[1] - temp[0]))
                if hl_widths and len([i for i in hl_widths if i >= page_width * 2 / 3]) > 0:
                    # any case when unable to find table but still clear evidences for existing tables
                    # which is proven by lots of horizontal lines occupy 2/3 of the page_width
                    table_special_flag = True
                    rect_edges_raw = main_page.edges
    except:
        rect_edges_raw = pdf_page.edges
    rect_edges_bad = []
    if rect_edges_raw and img_flag:
        img_page = pdf_page.to_image(resolution=res).original
        ss_width = Decimal(1.5)
        for i in pdf_page.rect_edges:
            # expand rect line check if it has white only color surrounding
            d_width, d_height = i['width'] / 10, i['height'] / 10
            left = i['x0'] * res / 72 - ss_width
            top = i['top'] * res / 72 - ss_width
            right = i['x1'] * res / 72 + ss_width
            bottom = i['bottom'] * res / 72 + ss_width
            if i['orientation'] == 'h':
                left = (i['x0'] + d_width) * res / 72
                right = (i['x1'] - d_width) * res / 72
            else:
                top = (i['top'] + d_height) * res / 72
                bottom = (i['bottom'] - d_height) * res / 72
            bbox_nl = (left, top, right, bottom)
            sc_img = img_page.crop(bbox_nl)
            if set(sc_img.getdata()) == {(255, 255, 255)}:
                rect_edges_bad.append(i)

    rect_edges = [i for i in rect_edges_raw if i not in rect_edges_bad]
    v_lines, v_edges, h_lines, h_edges, hl, vl, extra_vl = [], [], [], [], [], [], []
    edge_in_words, eiwt = [], Decimal(0.5)  # eiwt 判定线段在文本内的容忍度

    for i in rect_edges:
        if i in edge_in_words:
            continue
        if i['orientation'] == 'h':
            if i not in hl:
                hl.append(i)
        else:
            if i not in vl:
                vl.append(i)
    # remove those single-line objects
    min_double = Decimal(0.05)  # extreme close double lines could be noise
    vhl_tolerance = Decimal(2)
    for i, hli in enumerate(hl):
        if hli not in h_lines:
            nearest_lines = list(filter(lambda x: min_double < abs(hli['y0'] - x['y0']) <= double_tolerance
                                                  and abs(hli['x0'] - x['x0']) <= vhl_tolerance and abs(
                hli['x1'] - x['x1']) <= vhl_tolerance and hli != x, hl[:]))
            if nearest_lines:
                h_lines.append(hli)
                h_lines.extend(nearest_lines)

    for i, vli in enumerate(vl):
        if vli not in v_lines:
            nearest_lines = list(filter(lambda x: min_double < abs(vli['x0'] - x['x0']) <= double_tolerance
                                                  and abs(x['y0'] - vli['y0']) <= vhl_tolerance and abs(
                vli['y1'] - x['y1']) <= vhl_tolerance and vli != x, vl[:]))
            if nearest_lines:
                v_lines.append(vli)
                v_lines.extend(nearest_lines)

    # 有些时候表格会隐藏在 pdf_page.lines 中，比如虚线
    h_lines.extend([i for i in pdf_page.lines if i['height'] == 0])
    v_lines.extend([i for i in pdf_page.lines if i['width'] == 0])

    # add v_line if table lack v_lines with cautions
    page_rects = copy.deepcopy(rect_edges_raw)
    hls = [i['x0'] for i in h_lines if i['width'] > 3]  # horizontal lefts
    vls = [i['x0'] for i in v_lines if i['height'] > 3]  # vertical lefts
    hll = min(hls) if page_rects and hls else 0
    vll = min(vls) if page_rects and vls else 0
    if page_rects and hll < vll:
        h_rects = [i for i in page_rects if i['height'] <= 5 and i['height'] < i['width']]
        v_rects = [i for i in page_rects if i['width'] <= 5 and i['height'] > i['width']]
        htr = [i['top'] for i in h_rects]
        htr.extend([i['bottom'] for i in h_rects])
        h_tops = sorted(set(htr))
        htl = len(h_tops)
        link_info = [False for i in range(len(h_tops))]  # is vertical line connects current horizontal line
        for idx, i in enumerate(h_tops):
            if idx == htl - 1:
                break
            if abs(i - h_tops[idx + 1]) <= 2 * ave_cs:
                link_info[idx] = True
                # remove navigation paragraph without table lines in between
                try:
                    sliced_cell = pdf_page.crop([0, i, page_width, h_tops[idx + 1]])
                    if sliced_cell.rect_edges + sliced_cell.lines:
                        continue
                    join_line_text = sliced_cell.extract_text()
                except:
                    continue
                if join_line_text and navigationRE.search(join_line_text):
                    link_info[idx] = False
            for j in v_rects:
                overlap_length = calc_overlap([j['top'], j['bottom']], [i, h_tops[idx + 1]])
                if j['height'] > ave_cs and overlap_length > abs(h_tops[idx + 1] - i) / 3:
                    link_info[idx] = True
                    break
        link_trigger = False
        for idx, i in enumerate(h_tops):
            if link_info[idx]:
                link_trigger = True
            if not link_trigger:
                continue
            if idx == 0:
                l_top = i
            elif link_info[idx]:
                if not link_info[idx - 1]:
                    l_top = i
            else:
                if link_info[idx - 1]:
                    l_bottom = i
                    h_left = min([j['x0'] for j in h_rects if j['top'] >= l_top and j['bottom'] <= l_bottom])
                    h_right = max([j['x1'] for j in h_rects if j['top'] >= l_top and j['bottom'] <= l_bottom])
                    v_tmp = {'orientation': 'v', 'x0': h_left, 'x1': h_left, 'top': l_top, 'bottom': l_bottom}
                    extra_vl.append(v_tmp)
                    v_tmp = {'orientation': 'v', 'x0': h_right, 'x1': h_right, 'top': l_top, 'bottom': l_bottom}
                    extra_vl.append(v_tmp)
    v_lines.extend(extra_vl)
    # add horizantal lines
    vlts_tolerance = 0.1
    vlts = [i['top'] for i in v_lines if 'height' in i and i['height'] > 3]
    vltls = [i for i in v_lines if abs(i['top'] - min(vlts)) < vlts_tolerance] if vlts else []
    vhls = [i for i in h_lines if i['width'] > 3 and abs(i['top'] - min(vlts)) < vlts_tolerance] if vlts else []
    if vltls and vhls:
        vhlsl, vhlsr = min([i['x0'] for i in vhls]), max([i['x1'] for i in vhls])
        vltl, vltr = min([i['x0'] for i in vltls]), max([i['x1'] for i in vltls])
        if abs(vhlsl - vltl) > vlts_tolerance or abs(vhlsr - vltr) > vlts_tolerance:
            h_lines.append({'orientation': 'h', 'x0': vltl, 'x1': vltr, 'top': min(vlts), 'bottom': min(vlts)})

    # 有些表格的边框是曲线...
    page_curves = pdf_page.curves
    if page_curves:
        for i in page_curves:
            if 'x0' not in i or 'x1' not in i or 'top' not in i or 'bottom' not in i:
                continue
            if abs(i['x1'] - i['x0']) > 2 * ave_cs and abs(i['top'] - i['bottom']) > 2 * ave_cs:
                continue
            h_lines.extend([
                {'orientation': 'h', 'x0': i['x0'], 'x1': i['x1'], 'top': i['top'], 'bottom': i['top']},
                {'orientation': 'h', 'x0': i['x0'], 'x1': i['x1'], 'top': i['bottom'], 'bottom': i['bottom']}
            ])
            v_lines.extend([
                {'orientation': 'v', 'x0': i['x0'], 'x1': i['x0'], 'top': i['top'], 'bottom': i['top']},
                {'orientation': 'v', 'x0': i['x1'], 'x1': i['x1'], 'top': i['bottom'], 'bottom': i['bottom']}
            ])

    h_edges = [{'top': i['top'], 'x0': i['x0'], 'x1': i['x1']} for i in h_lines]
    v_edges = [{'x': i['x0'], 'top': i['top'], 'bottom': i['bottom']} for i in v_lines]
    table_params = {
        'vertical_strategy': 'explicit',
        'horizontal_strategy': 'explicit',
        'explicit_vertical_lines': v_edges,
        'explicit_horizontal_lines': h_edges,
        'edge_min_length': ave_cs
    }
    if debug_flag:
        im.reset()
        im.draw_lines(h_lines)
        img_file = os.path.join(temp_dir, prefix + '_table_cell_border_h_clean_{0}.png'.format(pid + 1))
        im.save(img_file, format="PNG")
        im.reset()
        im.draw_lines(v_lines)
        img_file = os.path.join(temp_dir, prefix + '_table_cell_border_v_clean_{0}.png'.format(pid + 1))
        im.save(img_file, format="PNG")

    # extract ordered tables
    if (table_special_flag or table_test) and len(v_edges) >= 2 and len(h_edges) >= 2:
        tables_ori = pdf_page.find_tables(table_settings=table_params)
    else:
        tables_ori = []
    if (table_special_flag or table_test) and not tables_ori:
        log.warning('{0} Table ignored at page {1}'.format(prefix, pid))
        # tables_ori = table_test # fallback if all table are eliminated...
    table_clean = []
    tables_raw = sorted(tables_ori, key=lambda x: x.bbox[1])
    for idx, table in enumerate(tables_raw):
        table_single = []
        if table is None:
            continue
        for row in table.rows:
            table_row = []
            for cell in row.cells:
                if not cell:
                    table_row.append(cell)
                    continue
                c_w = cell[2] - cell[0]
                c_h = cell[3] - cell[1]
                if c_w < min_cs or c_h < min_cs:
                    continue
                cell_region = pdf_page.filter(
                    lambda x: 'top' in x and 'bottom' in x and 'x0' in x and 'x1' in x and
                              x['top'] >= cell[1] - (x['bottom'] - x['top']) / 2 and
                              x['bottom'] <= cell[3] + (x['bottom'] - x['top']) / 2 and
                              x['x0'] >= cell[0] - (x['x1'] - x['x0']) / 2 and
                              x['x1'] <= cell[2] + (x['x1'] - x['x0']) / 2
                )
                text = cell_region.extract_text(y_tolerance=ave_cs * 2 / 3)
                text = text.strip().replace('\n', '<br>') if text else ''
                text = '……' if text == '„„' else text
                table_row.append({'width': c_w, 'height': c_h, 'text': text, 'fs': ave_cs})
            if table_row and not all(v is None for v in table_row):
                table_single.append(table_row)
        if table_single:
            table_clean.append(table_single)
        else:
            tables_raw[idx] = None
    tables_raw = [i for i in tables_raw if i is not None]
    table_counts = len(table_clean)
    log.info('{0} / page-{1} tables count: {2}'.format(prefix, pid, table_counts))
    # generate table html string
    html_table = table_dict_to_html(table_clean, pid)

    if debug_flag and tables_ori:
        im.reset()
        for i in tables_ori:
            im.draw_rects(i.cells)
        im.save(os.path.join(temp_dir, prefix + '_table_cell_border_{0}.png'.format(pid + 1)), format="PNG")

    # extract figures | ignore figures within tables for now | ignore logo figures
    figures, logo_figures, figures_in_table = [], [], []
    fig_merge = pdf_page.figures
    figures_ori = pdf_page.images
    for idx, i in enumerate(copy.deepcopy(figures_ori)):
        if i is None:
            continue
        if 'srcsize' in i and (i['srcsize'][0] <= 3 or i['srcsize'][1] <= 3):
            figures_ori[idx] = None
            continue
        # merge properties of the same figure & image
        replace_trigger = False
        if 'x0' not in i:
            img_tmp = copy.deepcopy(i)
            for k in i.keys():
                if k not in ['height', 'width']:
                    img_tmp.pop(k)
            for j in fig_merge:
                if img_tmp.items() <= j.items():
                    figures_ori[idx].update(j)
                    replace_trigger = True
                    break
            if not replace_trigger:
                log.warning('figure ignored in page {0}'.format(pid))
                figures_ori[idx] = None

        # check if image within table region
        if tables_raw:
            for table in tables_raw:
                if figures_ori[idx]['top'] >= table.bbox[1] and \
                        figures_ori[idx]['bottom'] <= table.bbox[3]:
                    figures_in_table.append(figures_ori[idx])
        # check if the image is a logo
        if logo:
            img_tmp = copy.deepcopy(i)
            for bk in i.keys():
                if bk not in im_keys:
                    img_tmp.pop(bk)
            if img_tmp in logo:
                logo_figures.append(figures_ori[idx])

    figures_raw = sorted([i for i in figures_ori if i not in figures_in_table and i not in logo_figures and
                          i is not None], key=lambda x: x['top'])
    log.info('{0} / page-{1} figure count: {2}'.format(prefix, pid, len(figures_raw)))

    # if img_flag and len(figures_raw) > 4:  # merge figures to graph if too many images found on this page
    #     bbox = (
    #         min([i['x0'] for i in figures_raw]),
    #         min([i['top'] for i in figures_raw]),
    #         max([i['x1'] for i in figures_raw]),
    #         max([i['bottom'] for i in figures_raw]),
    #     )
    #     img_file = os.path.join(temp_dir, prefix + '_{0}_image_0.png'.format(pid + 1))
    #     image = pdf_page.within_bbox(bbox)
    #     pic = image.to_image(resolution=res)
    #     pic.save(img_file, format='PNG')
    #     ocr_text = baidu_image_ocr(img_file, raw=True)
    #     ocr_text = [] if not ocr_text else ocr_text
    #     figures = [{'bbox': bbox, 'text': ocr_text, 'src': alioss_base_url + img_file, 'scan': False}]
    #     if rm_flag and os.path.isfile(img_file):
    #         os.remove(img_file)
    # elif img_flag:
    #     for idx, i in enumerate(figures_raw):
    #         if i['height'] <= 30 or i['width'] <= 30:
    #             continue
    #         img_file = os.path.join(temp_dir, prefix + '_{0}_image_{1}.png'.format(pid + 1, idx + 1))
    #         bbox = (i['x0'], i['top'], i['x1'], i['bottom'])
    #         image = pdf_page.within_bbox(bbox)
    #         pic = image.to_image(resolution=res)
    #         pic.save(img_file, format='PNG')
    #         ocr_text = baidu_image_ocr(img_file, raw=True)
    #         ocr_text = [] if not ocr_text else ocr_text
    #         figure = {
    #             'bbox': bbox, 'text': ocr_text, 'src': alioss_base_url + img_file,
    #             'scan': i['width'] * i['height'] / page_width / page_height >= 0.7,
    #         }
    #         figures.append(figure)
    #         if rm_flag and os.path.isfile(img_file):
    #             os.remove(img_file)
    figures_counts = len(figures)

    if debug_flag and figures_raw:
        im.reset()
        im.draw_rects(pdf_page.figures)
        im.save(os.path.join(temp_dir, prefix + '_image_border_{0}.png'.format(pid + 1)), format="PNG")

    # calculate paragraph border
    words_outside_table = []
    table_words = [[] for i in table_clean]
    img_words = []
    for img in figures:
        if 'bbox' not in img:
            continue
        img_main = pdf_page.within_bbox(img['bbox'])
        try:
            img_words.extend(img_main.extract_words(x_tolerance=ave_cs * 3 / 2))
        except:
            pass

    tt = tb = ll = lr = None  # top-top, top-bottom, left-left, left-right
    same_tmp = []
    for i in same:
        j = copy.deepcopy(i)
        if 'mode' in j:
            j.pop('mode')
        same_tmp.append(j)
    for i in words:
        if (same and i in same_tmp) or i in img_words or i in page_words:
            continue
        inside = 0
        for idx, table in enumerate(tables_raw):
            if table.bbox[1] <= (i['top'] + i['bottom']) / 2 <= table.bbox[3]:
                inside = 1
                table_words[idx].append(i)
        for img in figures:
            if 'bbox' in img and i['top'] >= img['bbox'][1] - ave_cs and i['bottom'] <= img['bbox'][3] + ave_cs:
                img_words.append(i)
        if inside:
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

    new_soup = bs('<div id="page-{0}" class="pdf-page"></div>'.format(pid + 1), 'lxml')
    page_content = new_soup.div
    page_content['style'] = "font-size: {0}px;".format(ave_cs)
    new_para = None
    table_index = 0
    new_table_flag = True
    figure_index = 0
    previous_top = previous_bottom = tt
    previous_right = ll
    toc_flag = False
    for line in lines:
        toc_tmp = tocLineRE.findall(reformat_text(line))
        if toc_tmp and toc_tmp[0] and not toc_tmp[0][-1].isdigit():
            toc_flag = True
            break
    for idx, i in enumerate(words):
        if (same and i in same_tmp) or i in img_words:
            continue
        new_para_flag, new_line_flag = False, True
        div_flag = center_flag = table_flag = False
        ave_ts = max((i['x1'] - i['x0']) / len(i['text']), ave_cs)

        if table_words and i in table_words[table_index]:
            table_flag = True
        elif table_index < table_counts - 1:
            if i in table_words[table_index + 1]:
                table_index += 1
                new_table_flag = table_flag = True

        if table_flag and not new_table_flag:
            continue

        if new_para is None:
            new_para_flag = new_line_flag = True
        else:
            if abs(i['bottom'] - previous_bottom) >= ave_ts / 4:
                new_para_flag = new_line_flag = True
                if abs(i['top'] - previous_bottom) <= max(ave_ts * 6 / 5, ave_lh):  # 小于 1.2 倍行距 / 平均行高
                    if abs(i['x0'] - ll) <= ave_ts and abs(previous_right - lr) <= ave_ts * 3 / 2:  # 页面左右边距
                        if abs((i['bottom'] - i['top']) - (previous_bottom - previous_top)) <= 1:
                            new_para_flag = False  # 被认定为同一个段落
                if abs(i['x0'] - ll) <= 1 and abs(previous_right - lr) <= ave_ts * 3 / 2:
                    new_para_flag = False  # 如果该行的左边距特别小且上一行的右边距相对较小，则认为是同一个段落
                if new_para_flag:
                    if abs(page_width - i['x1'] - i['x0']) <= ave_ts / 2:
                        if abs(lr - i['x1']) >= 3 * ave_ts:  # 段前有四个 char_size 大小的空白
                            center_flag = True
                    if i['x0'] > ll + ave_ts * 3:
                        div_flag = True
            elif abs(i['x0'] - previous_right) >= ave_ts * 2:  # 同一行需要判定该段落是否为文本框组合
                if abs(i['top'] - previous_top) <= ave_ts / 2:
                    new_line_flag = new_para_flag = False
                if i['x0'] < previous_right and chars:  # 有一些下标会出现换行问题
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

        text = reformat_text(i['text'].strip()) if i['text'] else ''
        if navigationRE.findall(text):
            log.warning('navigation found in {} on page {}'.format(prefix, pid))

        if new_para_flag:
            if new_para and new_para.text:
                page_content.append(new_para)
            new_para = new_soup.new_tag('p', **{'class': "pdf-paragraph"})
            new_para['style'] = ''
            new_para['style'] += "font-size: {0}px;".format(ave_ts)
            if center_flag:
                new_para['align'] = "center"
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

        while figure_index < figures_counts and i['top'] >= figures[figure_index]['bbox'][3]:
            figure = figures[figure_index]
            img_id = "page-{0}-image-{1}".format(pid + 1, figure_index + 1)
            if not figure['scan']:
                img = new_soup.new_tag('img', id=img_id, **{'class': 'image-raw'})
                img['ocr'] = ' '.join(figure['text'])
                img['width'] = figure['bbox'][2] - figure['bbox'][0]
            else:
                img = new_soup.new_tag('div', id=img_id, **{'class': 'image-ocr'})
                for line in figure['text']:
                    if line in same_text:
                        continue
                    fig_para = new_soup.new_tag('p', **{'class': "pdf-paragraph"})
                    fig_para.append(line)
                    img.append(fig_para)
            img['src'] = figure['src']
            page_content.append(img)
            figure_index += 1

        if table_flag and new_table_flag:
            new_table = bs(html_table[table_index], 'lxml').table
            page_content.append(new_table)
            new_table_flag = False
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
        if not pageNumRE.findall(new_para.text):
            page_content.append(new_para)
        else:
            log.warning('last paragraph identified as page number {} - {}'.format(prefix, pid))

    if not page_content.find_all() and figures:
        while figure_index < figures_counts:
            figure = figures[figure_index]
            img_id = "page-{0}-image-{1}".format(pid + 1, figure_index + 1)
            if not figure['scan']:
                img = new_soup.new_tag('img', id=img_id, **{'class': 'image-raw'})
                img['ocr'] = ' '.join(figure['text'])
                img['width'] = figure['bbox'][2] - figure['bbox'][0]
            else:
                img = new_soup.new_tag('div', id=img_id, **{'class': 'image-ocr'})
                for line in figure['text']:
                    if line in same_text:
                        continue
                    fig_para = new_soup.new_tag('p', **{'class': "pdf-paragraph"})
                    fig_para.append(line)
                    img.append(fig_para)
            img['src'] = figure['src']
            page_content.append(img)
            figure_index += 1

    if not page_content.find_all('p', recursive=False):
        new_para_start_flag = new_para_end_flag = None
    else:
        new_para_start_flag = new_para_end_flag = True  # 页面的开始和结尾是否表示一个段落的完结
        ave_ts = (words[0]['x1'] - words[0]['x0']) / len(words[0]['text']) if words else 0
        if (words[0]['x0'] - ll) <= max(ave_ts, ave_cs):
            new_para_start_flag = False
        if abs(previous_right - lr) <= max(ave_ts, ave_cs) * 3 / 2:
            new_para_end_flag = False

    return {
        'page': pid + 1,
        'content': str(page_content),
        'table': table_clean,
        'figure': figures,
        'new_para_start': new_para_start_flag,
        'new_para_end': new_para_end_flag,
    }


def reformat_text(text):
    return text.replace('\xa0', '')


def calc_overlap(a, b):
    """检查两个线段的重叠部分长度
    :param a: [a_lower, a_upper]
    :param b: [b_lower, b_upper]
    :return: overlapping length
    """
    if a[0] >= b[0] and a[1] <= b[1]:
        overlap_length = abs(a[1] - a[0])
    elif a[0] <= b[0] and a[1] >= b[1]:
        overlap_length = abs(b[1] - b[0])
    elif b[0] <= a[0] <= b[1]:
        overlap_length = abs(b[1] - a[0])
    elif b[0] <= a[1] <= b[1]:
        overlap_length = abs(a[1] - b[0])
    else:
        overlap_length = 0
    return overlap_length


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

    def check_same(p1, p2, orientation=None, pure_text=False, same_text=None):
        fpage = pdf.pages[p1].extract_words(x_tolerance=6, y_tolerance=6)
        fpl = len(fpage)
        spage = pdf.pages[p2].extract_words(x_tolerance=6, y_tolerance=6) if p2 else None
        spl = len(spage) if p2 else None

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
        for key in img_tmp.keys():
            if key not in im_keys:
                logo[idx].pop(key)

    return copy.deepcopy(logo)


def plumber_pdf2html(fpath, prefix=''):
    with pdfplumber.open(fpath) as pdf:
        page_num = len(pdf.pages)
        print('{0} total page num: {1}'.format(prefix, page_num))
        same = plumber_pdf_head_tail(pdf) if page_num > 1 else list()
        logo = plumber_pdf_logo(pdf) if page_num > 1 else list()
        html_content = [plumber_pdf2html_page(pdf, i, same=same, logo=logo, prefix=prefix) for i in range(page_num)]
    return html_content, page_num, same, logo


def table_dict_to_html(table_clean, pid, merge_tolerance=2):
    html_table = []
    for tid, j in enumerate(copy.deepcopy(table_clean)):
        row_num = len(j)
        row_heights = [min([tc['height'] for tc in tr if tc]) for tr in j]
        try:
            column_num, column_widths = gen_column_cell_sizes(j)
        except:
            column_num = max([len(tr) for tr in j])
            column_widths = [min([tc['width'] for tc in tr if tc]) if not all(v is None for v in tr) else 0 for tr in
                             map(list, zip(*j))]
        html_table_string = '<table id="page-{0}-table-{1}" class="pdf-table">'.format(pid + 1, tid + 1)
        for rid, tr in enumerate(j):
            html_table_string += '<tr>'
            for cid, tc in enumerate(tr):
                if tc is None:
                    continue
                html_table_string += '<td'
                row_span = col_span = 1
                for i in range(rid + 1, row_num):
                    if abs(tc['height'] - sum(row_heights[rid:i])) > merge_tolerance:
                        row_span += 1
                    else:
                        break
                for i in range(cid + 1, column_num):
                    if abs(tc['width'] - sum(column_widths[cid:i])) > merge_tolerance:
                        col_span += 1
                    else:
                        break
                if row_span > 1:
                    html_table_string += ' rowspan="{0}"'.format(row_span)
                if col_span > 1:
                    html_table_string += ' colspan="{0}"'.format(col_span)
                html_table_string += ' style="font-size: {0}px;">{1}</td>'.format(tc['fs'], tc['text'])
            html_table_string += '</tr>'
        html_table_string += '</table>'
        html_table.append(html_table_string)
    return html_table


def gen_column_cell_sizes(t):
    raw_sizes = [[tc['width'] if tc else 0 for tc in tr] for tr in t]
    cell_num = max([len(tr) for tr in t])
    trans_sizes = list(map(list, zip(*raw_sizes)))
    cell_sizes = []
    for i in range(cell_num):
        ss = min([i for i in trans_sizes[i] if i])
        cell_sizes.append(ss)
        if i >= cell_num - 1:
            break
        tmp = [i - ss if i > ss else i for i in trans_sizes[i]]
        trans_sizes[i + 1] = [ts if ts else tmp[tsid] for tsid, ts in enumerate(trans_sizes[i + 1])]
    return cell_num, cell_sizes


if __name__ == '__main__':
    with open('/Users/austinzy/Desktop/1.html', 'w', encoding='utf-8') as html_file:
        html_content, page_num, same, logo = plumber_pdf2html('/Users/austinzy/Desktop/1.pdf')
        for kk in html_content:
            html_file.write(kk['content'] + '\n')
