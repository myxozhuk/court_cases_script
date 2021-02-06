# Сбербанк ОГРН 1027700132195
# alt - 73 courts - all 1 str
# amr - 28 - almost all 2 str (a few 1 and 3)
# arh - 28 -
# ast - 20

# blg - есть 3 str (eg ivniansky, rovensky)



# ОБРАБОТКА ОШИБОК:
# проверка на ошибку timeout
# проверка на остальные ошибки
# запись других ошибок - куда?
# ____________________________
# БЛОК НАВИГАЦИИ ПО СТРАНИЦАМ:
# как выглядит блок, если 1, 2, 3, 4, 5, 6, 7 страниц?
# ____________________________

import requests
from bs4 import BeautifulSoup
# import xlsxwriter
from datetime import datetime
import csv
import time
import os

from multiprocessing import Pool


start_time = str(datetime.now())
print('Start time =', start_time)


def not_empty(st):
    for ch in st:
        if 1040 <= ord(ch) <= 1103 or 48 <=ord(ch) <= 57:
            return True
    return False


def str_to_pgnum(last_page_tag_):
    last_href_ = last_page_tag_['href']
    str_ind_ = last_href_.find('page=')
    link_part_ = last_href_[(str_ind_ + 5):]
    str_ind_ = link_part_.find('&')
    if str_ind_ != -1:
        max_page_ = int(link_part_[:str_ind_])
    else:
        max_page_ = int(link_part_)
    return max_page_


def create_param_str(div_id_tag_, struc):
    input_tags_ = div_id_tag_.find_all('input')
    param_str_ = '/modules.php?'
    for input_tag_ in input_tags_:
        if struc == 1:
            if input_tag_['name'] == 'Submit' or \
                    input_tag_['name'] == 'dic' or \
                    input_tag_['name'] == 'Reset' or \
                    input_tag_['name'] == 'list' or \
                    not input_tag_.has_attr('value') or \
                    input_tag_.has_attr('value') and input_tag_['value'] == '':
                continue
            if input_tag_['name'] == 'G1_PARTS__NAMESS' or input_tag_['name'] == 'G2_PARTS__NAMESS':
                param_str_ += input_tag_['name'] + '=%D1%E1%E5%F0%E1%E0%ED%EA'
                param_str_ += '&'
                continue
        elif struc == 2:
            if input_tag_['name'] == 'parts__namess':
                param_str_ += input_tag_['name'] + '=%D1%E1%E5%F0%E1%E0%ED%EA'
                param_str_ += '&'
                continue
            if input_tag_['name'] == 'process-type' or \
                    not input_tag_.has_attr('value') or \
                    input_tag_.has_attr('value') and (input_tag_['value'] == '' or input_tag_['value'] is None):
                continue
        param_str_ += input_tag_['name'] + '=' + input_tag_['value'] + '&'
    if struc == 1:
        param_str_ += 'Submit=%CD%E0%E9%F2%E8'
    elif struc == 2:
        param_str_ += 'process-type=%CF%EE%E8%F1%EA+%EF%EE+%E2%F1%E5%EC+%E2%E8%E4%E0%EC+%E4%E5%EB'
    return court + param_str_


requests.packages.urllib3.disable_warnings()

session = requests.Session()
session.headers.update({
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'
    # 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
})

document_cnt = 1
all_documents_cnt = 0

courts_cnt = 0
ok_courts_cnt = 0
oth_str_courts_cnt = 0
third_str_courts_cnt = 0

timeout_courts = []



kas_and_ap_courts = {
    'http://1kas.sudrf.ru': 0,
    'http://2kas.sudrf.ru': 0,
    'http://3kas.sudrf.ru': 0,
    'http://4kas.sudrf.ru': 0,
    'http://5kas.sudrf.ru': 0,
    'http://6kas.sudrf.ru': 0,
    'http://7kas.sudrf.ru': 0,
    'http://8kas.sudrf.ru': 0,
    'http://9kas.sudrf.ru': 0,
    'http://1ap.sudrf.ru': 0,
    'http://2ap.sudrf.ru': 0,
    'http://3ap.sudrf.ru': 0,
    'http://4ap.sudrf.ru': 0,
    'http://5ap.sudrf.ru': 0
}

deb = 1                 #####################
if deb == 1:
    courts_cnt = 0
    # document_cnt = 0
    document_cnt = 32968
    head_fields = [
        'Номер дела',
        'Дата поступления',
        'Категория / Стороны',
        'Судья',
        'Дата решения',
        'Решение',
        'Дата вступления в законную силу',
        'Судебные акты'
    ]
    # ftimeout_courts = open('timeout_courts.txt', 'a')
    # ftimeout_pages = open('timeout_pages.txt', 'a')
    # ftimeout_cases = open('timeout_cases.txt', 'a')
    # fotherstructure = open('otherstructure.txt', 'a')

    f = open('links.txt', 'r')
    f_subjects_names = open('subjects_names.txt', 'r')
    f_subjects_codes = open('subjects_codes.txt', 'r')
    cases = []
    f_now = 0
    flag_end = 0
    norm_str = 0
    region_cnt = 0
    for region in f:
        region_cnt += 1
        if flag_end == 1:
            break
        for court in region.split():

            if court in kas_and_ap_courts:
                if kas_and_ap_courts[court] == 1:
                    continue
                else:
                    kas_and_ap_courts[court] = 1

            courts_cnt += 1
            # cases.clear()

            if court == 'http://agvs.arh.sudrf.ru':
            # if court == 'http://arharinskiy.amr.sudrf.ru':
                f_now = 1
            if f_now == 0:
                continue

            if court == 'http://astrahanskygvs.ast.sudrf.ru':
                flag_end = 1
                break

            # запись названия столбцов в таблицу
            # myFile = open('tables_w_links/' + str(courts_cnt) + '.csv', 'a')
            # myFile = open('tables2/' + str(courts_cnt) + '.csv', 'a')
            myFile = open('tables' + str(region_cnt) + '/' + str(courts_cnt) + '.csv', 'a')
            writer = csv.writer(myFile)
            writer.writerow(head_fields)
            myFile.close()

            print(court)  #################
            url_search_form = court + '/modules.php?name=sud_delo&name_op=sf&delo_id=1540005'
            time.sleep(3)
            try:
                resp = session.get(url_search_form, verify=False, timeout=30)
            except Exception as e:
                print('Error:', e, '\n URL:', url_search_form)
                # timeout_courts.append(court)
                ftimeout_courts = open('timeout' + str(region_cnt) + '/timeout_courts.txt', 'a')
                print(court, file=ftimeout_courts)
                ftimeout_courts.close()
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            if soup is None:
                print("Error, can't parse this site", court)
                continue
            div_class_tag = soup.find('div', {'class': 'box box_common m-all_m'})
            if div_class_tag is None:
                print("Error, can't parse this site", court)
                continue
            srv_num = 1
            div_id_tag = div_class_tag.find('div', id='box box_common m-all_m')
            if div_id_tag is not None:  # несколько серверов
                a_tags = div_id_tag.find_all('a')
                srv_num = len(a_tags)
            link = '/modules.php?name=sud_delo&name_op=sf&delo_id=1540005&srv_num='  # + srv_num
            for i in range(1, srv_num + 1):  # проход по всем серверам суда
                url_search = court + link + str(i)
                time.sleep(3)
                try:
                    resp = session.get(url_search, verify=False, timeout=30)
                except Exception as e:
                    print('Error:', e, '\n URL:', url_search)
                    # timeout_courts.append(url_search + str(i))
                    ftimeout_courts = open('timeout' + str(region_cnt) + '/timeout_courts.txt', 'a')
                    print(url_search, file=ftimeout_courts)
                    ftimeout_courts.close()
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                if soup is None:
                    print("Error, can't parse this site", court)
                    continue
                div_id_tag = soup.find('div', id='content')

                # _________________________1ST STRUCTURE:_________________________

                if div_id_tag is not None:
                    norm_str += 1
                    print('norm_str =', norm_str)
                    # continue    ########################################
                    page_link = create_param_str(div_id_tag, 1)
                    time.sleep(3)
                    try:
                        resp = session.get(page_link, verify=False, timeout=30)
                    except Exception as e:
                        print('Error:', e, '\n URL:', page_link)
                        ftimeout_courts = open('timeout' + str(region_cnt) + '/timeout_courts.txt', 'a')
                        print(page_link, file=ftimeout_courts)
                        ftimeout_courts.close()
                        continue

                    max_page = 1
                    soup = BeautifulSoup(resp.text, 'lxml')
                    if soup is None:
                        print("Error, can't parse this site:", court)
                        break
                    last_page_tag = soup.find('a', title="На последнюю страницу списка")
                    if last_page_tag is not None:
                        max_page = str_to_pgnum(last_page_tag)

                    for pg_num in range(1, max_page + 1):
                        if pg_num != 1:
                            page_link_ = page_link + '&page=' + str(pg_num)
                            time.sleep(3)
                            try:
                                resp = session.get(page_link_, verify=False, timeout=30)
                            except Exception as e:
                                print('Error:', e, '\n URL:', page_link_)
                                ftimeout_pages = open('timeout' + str(region_cnt) + '/timeout_pages.txt', 'a')
                                print(page_link_, file=ftimeout_pages)
                                ftimeout_pages.close()
                                continue

                            soup = BeautifulSoup(resp.text, 'lxml')
                            if soup is None:
                                print("Error, can't parse this site:", court)
                                break
                        table = soup.find('table', id='tablcont')
                        if table is not None:
                            records = table.find_all('tr')
                            if records is None:
                                print('records is None, break. pg_num =', pg_num)
                                break
                            records.pop(0)
                            for record in records:
                                fields = record.find_all('td')
                                if fields is None:
                                    print('fields is None, continue, pg_num =', pg_num)
                                    continue
                                a_t = fields[7].find('a')
                                if not_empty(fields[5].string) and a_t is not None:
                                    cases.append(list())
                                    document_cnt += 1
                                    print('document_cnt =', document_cnt)
                                    for ind in range(len(fields) - 1):
                                        cases[-1].append(fields[ind].text.strip())
                                    time.sleep(3)
                                    try:
                                        resp = session.get(court + a_t['href'], verify=False, timeout=30)
                                    except Exception as e:
                                        print('Error:', e, '\n URL:', court + a_t['href'])
                                        cases[-1].append(court + a_t['href'])
                                        ftimeout_cases = open('timeout' + str(region_cnt) + '/timeout_cases.txt', 'a')
                                        print(court + a_t['href'], file=ftimeout_cases)
                                        ftimeout_cases.close()
                                        continue
                                    soup_delo = BeautifulSoup(resp.text, "lxml")
                                    content_delo = soup_delo.find('div', id='content')
                                    scripts = content_delo.find_all('script')
                                    if len(scripts) != 0:
                                        for script in scripts:
                                            script.replaceWith('')
                                    f_delo = open('cases' + str(region_cnt) + '/' + str(document_cnt) + '.txt', 'w')
                                    print(content_delo.text, file=f_delo)
                                    f_delo.close()
                                    cases[-1].append('cases' + str(region_cnt) +'/' + str(document_cnt) + '.txt')
                        if len(cases) >= 500:
                            myFile = open('tables' + str(region_cnt) + '/' + str(courts_cnt) + '.csv', 'a')
                            writer = csv.writer(myFile)
                            writer.writerows(cases)
                            myFile.close()
                            cases.clear()

                # _______________2ND STRUCTURE_______________

                elif soup.find('div', id='search_results') is not None:

                    div_id_tag = soup.find('div', id='search_results')
                    page_link = create_param_str(div_id_tag, 2)

                    time.sleep(3)
                    try:
                        resp = session.get(page_link, verify=False, timeout=30)
                    except Exception as e:
                        print('Error:', e, '\n URL:', page_link)
                        ftimeout_courts = open('timeout' + str(region_cnt) + '/timeout_courts.txt', 'a')
                        print(page_link, file=ftimeout_courts)
                        ftimeout_courts.close()
                        break
                    soup = BeautifulSoup(resp.text, "lxml")
                    max_page = 1
                    pagination_tag = soup.find('ul', {'class': 'pagination result-pages'})
                    if pagination_tag is not None:
                        li_tags = pagination_tag.find_all('li')
                        # если 1, 2, 3, 4, 5, 6 страниц, то какая структура у этого блока?
                        if len(li_tags) == 6:
                            last_page_tag = li_tags[-2].find('a')
                            if last_page_tag is None:
                                print('Error, cannot parse this site')
                                continue
                            else:
                                max_page = str_to_pgnum(last_page_tag)
                        else:
                            a=0
                    for pg_num in range(1, max_page + 1):
                        if pg_num != 1:
                            page_link_ = page_link + '&_page=' + str(pg_num)
                            time.sleep(3)
                            try:
                                resp = session.get(page_link_, verify=False, timeout=30)
                            except Exception as e:
                                print('Error:', e, '\n URL:', page_link_)
                                ftimeout_pages = open('timeout' + str(region_cnt) + '/timeout_pages.txt', 'a')
                                print(page_link_, file=ftimeout_pages)
                                ftimeout_pages.close()
                                continue
                            soup = BeautifulSoup(resp.text, 'lxml')
                            if soup is None:
                                print("Error, can't parse this site:", court)
                                break
                        div_id_tag = soup.find('div', id='search_results')
                        if div_id_tag is None:
                            print('div_id_tag is None, continue, pg_num =', pg_num)
                            continue
                        table_ = div_id_tag.find('table')
                        if table_ is None:
                            break
                        table = table_.tbody
                        if table is not None:
                            records = table.find_all('tr')
                            if len(records) == 0:
                                print('records is None, break. pg_num =', pg_num)
                                break
                            for record in records:
                                fields = record.find_all('td')
                                if len(fields) == 0:
                                    print('fields is empty, continue, pg_num =', pg_num)
                                    continue
                                a_t = fields[7].find('a')
                                if not_empty(fields[5].string) and a_t is not None:
                                    cases.append(list())
                                    document_cnt += 1
                                    print('document_cnt =', document_cnt)
                                    for ind in range(len(fields) - 1):
                                        cases[-1].append(fields[ind].text.strip())
                                    time.sleep(3)
                                    try:
                                        resp = session.get(court + a_t['href'], verify=False, timeout=30)
                                    except Exception as e:
                                        print('Error:', e, '\n URL:', court + a_t['href'])
                                        cases[-1].append(court + a_t['href'])
                                        ftimeout_cases = open('timeout' + str(region_cnt) + '/timeout_cases.txt', 'a')
                                        print(court + a_t['href'], file=ftimeout_cases)
                                        ftimeout_cases.close()
                                        continue
                                    soup_delo = BeautifulSoup(resp.text, "lxml")
                                    content_delo = soup_delo.find('div', class_='container', id='case_window')
                                    # http://zavialovsky.alt.sudrf.ru
                                    scripts = content_delo.find_all('script')
                                    if len(scripts) != 0:
                                        for script in scripts:
                                            script.replaceWith('')
                                    f_delo = open('cases'+str(region_cnt) + '/' + str(document_cnt) + '.txt', 'w')
                                    print(content_delo.text, file=f_delo)
                                    f_delo.close()
                                    cases[-1].append('cases1/' + str(document_cnt) + '.txt')
                        else:
                            print('Error: cannot parse this site')
                            continue
                        if len(cases) >= 500:
                            myFile = open('tables' + str(region_cnt) + '/' + str(courts_cnt) + '.csv', 'a')
                            writer = csv.writer(myFile)
                            writer.writerows(cases)
                            myFile.close()
                            cases.clear()

                # _______________THIRD STRUCTURE________________

                else:
                    third_str_courts_cnt += 1
                    print('3rd structure:', court)
                    fotherstructure = open('otherstructure.txt', 'a')
                    print(court, file=fotherstructure)
                    fotherstructure.close()
                myFile = open('tables' + str(region_cnt) + '/' + str(courts_cnt) + '.csv', 'a')
                writer = csv.writer(myFile)
                if len(cases):
                    writer.writerows(cases)
                    cases.clear()
                myFile.close()


# mode = int(input('1 - sudrf.ru, 2 - mos-gorsud.ru: '))
mode = 0 ################
if mode == 1:
    # mode_upd = int(input('Обновить список судов общей юрисдикции? 1 - Да, 0 - Нет'))
    mode_upd = 0 ################
    if mode_upd:
        # сбор ссылок судов общей юрисдикции:
        url_gen_jur_courts = 'https://sudrf.ru/index.php?id=300'
        try:
            resp = session.get(url_gen_jur_courts, verify=False, timeout=10)
        except:
            print('Timeout occurred:', url_gen_jur_courts)
            exit(-1)
        soup = BeautifulSoup(resp.text, 'lxml')
        select_tag = soup.find('select', id='court_subj_cd')
        option_tags = select_tag.find_all('option')  # регионы в тегах option
        subjects_codes = []  # номера регионов
        subjects_names = []  # названия регионов
        all_courts = []
        for i in option_tags:
            if i['value'] != '0' and i['value'] != '77': # Не выбрано // Москва
                subjects_codes.append(i['value'])
                subjects_names.append(i.string)
        # запись в файл назавний регионов:
        f_subjects_names = open('subjects_names.txt', 'w')
        for i in subjects_names:
            print(i, file=f_subjects_names)
        f_subjects_names.close()
        # запись в файл кодов регионов:
        f_subjects_codes = open('subjects_codes.txt', 'w')
        for i in subjects_codes:
            print(i, file=f_subjects_codes)
        f_subjects_codes.close()
        # запись в файл ссылок судов по регионам
        for subject in subjects_codes:
            data = {
                'act': 'go_search',
                'searchtype': 'fs',
                'court_name': None,
                'court_subj': subject,
                'court_type': '0',
                'court_okrug': '0'
            }
            resp_subj = session.post(url_gen_jur_courts, data=data, verify=False)
            soup1 = BeautifulSoup(resp_subj.text, "lxml")
            fsSearchArea = soup1.find('div', id="fsSearchArea")
            subj_courts = []
            results = fsSearchArea.find('ul', {'class':'search-results'})
            li_tags = results.find_all('li')
            links = []
            for li_tag in li_tags:
                a_tag = None
                b_tag = li_tag.find('b', string='Официальный сайт:')
                for sibling in b_tag.next_siblings:
                    if sibling.name == 'a':
                        a_tag = sibling
                        break
                link = a_tag['href']
                links.append(link)
            all_courts.append(links)
        f = open('links.txt', 'w')
        for region in all_courts:
            for court in region:
                print(court, end=' ', file=f)
            print(file=f)
        f.close()

    # __________сбор информации на сайтах судов общей юрисдикции (сайт sudrf.ru)__________

    ftimeout_courts = open('timeout_courts.txt', 'a')
    ftimeout_pages = open('timeout_pages.txt', 'a')
    ftimeout_cases = open('timeout_cases.txt', 'a')

    head_fields = [
        'Номер дела',
        'Дата поступления',
        'Категория / Стороны',
        'Судья',
        'Дата решения',
        'Решение',
        'Дата вступления в законную силу',
        'Судебные акты'
    ]

    # myFile = open('mos-gorsud.csv', 'a')  ###############
    # writer = csv.writer(myFile)
    # writer.writerows(cases_list)
    # myFile.close()

    f = open('links.txt', 'r')
    f_subjects_names = open('subjects_names.txt', 'r')
    f_subjects_codes = open('subjects_codes.txt', 'r')
    flag = 0
    index_debug = 0
    for region in f:
        asdasd += 1
        # continue            ####################
        sheet_name = f_subjects_names.readline()
        subject_code = int(f_subjects_codes.readline())

        # worksheet = workbook.add_worksheet(sheet_name.strip()[:31])
        # заполнение таблицы названиями столбцов:
        row = 0
        col = 0
        # for head_field in head_fields:
        #     worksheet.write(0, col, head_field)
        row = 1
        col = 0

        for court in region.split():
            if court in kas_and_ap_courts:
                if kas_and_ap_courts[court] == 1:
                    continue
                else:
                    kas_and_ap_courts[court] = 1
            courts_cnt += 1
            url_search_form = court + '/modules.php?name=sud_delo&name_op=sf&delo_id=1540005'
            # time.sleep(2)
            try:
                resp = session.get(url_search_form, verify=False, timeout=10)
            except Exception as e:
                print('Error:', e, '\n URL:', url_search_form)
                timeout_courts.append(court)
                continue
            print(court)                      #################
            soup = BeautifulSoup(resp.text, "lxml")
            if soup is None:
                print("Error, can't parse this site", court)
                continue
            div_class_tag = soup.find('div', {'class': 'box box_common m-all_m'})
            if div_class_tag is None:
                print("Error, can't parse this site", court)
                continue
            srv_num = 1
            div_id_tag = div_class_tag.find('div', id='box box_common m-all_m')
            if div_id_tag is not None:  #несколько серверов
                a_tags = div_id_tag.find_all('a')
                srv_num = len(a_tags)
            link = '/modules.php?name=sud_delo&name_op=sf&delo_id=1540005&srv_num='  # + srv_num
            for i in range(1, srv_num + 1):  # проход по всем серверам суда
                url_search = court + link + str(i)
                # time.sleep(3)
                try:
                    resp = session.get(url_search, verify=False, timeout=0.001)
                except Exception as e:
                    print('Error:', e, '\n URL:', url_search)
                    timeout_courts.append(court)
                    continue
                # continue            ####################
                soup = BeautifulSoup(resp.text, "lxml")
                if soup is None:
                    print("Error, can't parse this site", court)
                    continue
                div_id_tag = soup.find('div', id='content')
                # 1ая структура страницы поиска:
                if div_id_tag is not None:
                    # continue    ########################################
                    input_tags = div_id_tag.find_all('input')
                    param_str = '/modules.php?'
                    for input_tag in input_tags:
                        if input_tag['name'] == 'Submit' or input_tag['name'] == 'dic' or input_tag['name'] == 'Reset' or input_tag['name'] == 'list':
                            continue
                        if input_tag['name'] == 'G1_PARTS__NAMESS' or input_tag['name'] == 'G2_PARTS__NAMESS':
                            param_str += input_tag['name'] + '=%D1%E1%E5%F0%E1%E0%ED%EA'
                            param_str += '&'
                            continue
                        if not input_tag.has_attr('value'):
                            continue
                        if input_tag.has_attr('value') and input_tag['value'] == '':
                            continue
                        param_str += input_tag['name'] + '=' + input_tag['value']
                        param_str += '&'
                    param_str += 'Submit=%CD%E0%E9%F2%E8'
                    page_link = court + param_str
                    next_page_tag = 1
                    pg_num = 0
                    while next_page_tag is not None:
                        pg_num += 1
                        try:
                            resp = session.get(page_link, verify=False, timeout=10)
                        # except:
                        #     print('Timeout occurred:', page_link)
                        #     break
                        except Exception as e:
                            print('Error:', e, '\n URL:', page_link)
                            timeout_courts.append(court)
                            continue
                        soup = BeautifulSoup(resp.text, 'lxml')
                        if soup is None:
                            print("Error, can't parse this site:", court)
                            break
                        table = soup.find('table', id='tablcont')
                        if table is not None:
                            records = table.find_all('tr')
                            if records is None:
                                break
                            records.pop(0)
                            for record in records:
                                fields = record.find_all('td')
                                if fields is None:
                                    continue
                                resh = fields[5].string
                                if not_empty(resh):
                                    col = 0
                                    for cell in fields:
                                        if col == 7:
                                            a_t = cell.find('a')
                                            if a_t is not None:  # если есть документ
                                                delo_link = court + a_t['href']
                                                # time.sleep(3)
                                                try:
                                                    resp = session.get(delo_link, verify=False, timeout=10)
                                                except:
                                                    print('Timeout occurred:', delo_link)
                                                    continue
                                                soup_delo = BeautifulSoup(resp.text, "lxml")
                                                content_delo = soup_delo.find('div', id='content')
                                                for script in content_delo.find_all('script'):
                                                    script.replaceWith('')
                                                f_delo = open('cases/' + str(document_cnt) + '.txt', 'w')
                                                print(content_delo.text, file=f_delo)
                                                f_delo.close()
                                                # запись названия файла с документом в таблицу:
                                                worksheet.write(row, col, str(document_cnt) + '.txt')
                                                document_cnt += 1
                                        else:
                                            worksheet.write(row, col, cell.text.strip())
                                        col += 1
                                    row += 1
                        next_page_tag = soup.find('a', title="Следующая страница")
                        if next_page_tag is not None:
                            page_link = court + next_page_tag['href']
                        else:
                            break
                    index_debug += 1  #################
                    ok_courts_cnt += 1
                    # if index_debug >= 2:
                    #     workbook.close()

                # __________2АЯ СТРУКТУРА СТРАНИЦЫ ПОИСКА__________

                else:
                    oth_str_courts_cnt += 1         ####################
                    # continue                    ####################

                    div_id_tag = soup.find('div', id='search_results')
                    if div_id_tag is not None:
                        continue                        ####################
                        input_tags = div_id_tag.find_all('input')
                        param_str = '/modules.php?'
                        for input_tag in input_tags:
                            if input_tag['name'] == 'parts__namess':
                                param_str += input_tag['name'] + '=%D1%E1%E5%F0%E1%E0%ED%EA'
                                param_str += '&'
                                continue
                            if input_tag['name'] == 'process-type':
                                continue
                            if not input_tag.has_attr('value'):
                                continue
                            if input_tag.has_attr('value') and input_tag['value'] == '':
                                continue
                            if input_tag.has_attr('value') and input_tag['value'] is None:
                                continue
                            param_str += input_tag['name'] + '=' + input_tag['value']
                            param_str += '&'
                        param_str += 'process-type=%CF%EE%E8%F1%EA+%EF%EE+%E2%F1%E5%EC+%E2%E8%E4%E0%EC+%E4%E5%EB'
                        page_link = court + param_str
                        next_page_tag = 1
                        pg_num = 0

                        try:
                            resp = session.get(page_link, verify=False, timeout=10)
                        except requests.exceptions.Timeout:
                            print('Timeout occurred:', page_link)
                            break
                        soup = BeautifulSoup(resp.text, "lxml")
                        pagination_tag = soup.find('ul', {'class': 'pagination result-pages'})
                        if pagination_tag is not None:
                            pass
                        else:
                            pass

                        while next_page_tag is not None:
                            pg_num += 1
                            # time.sleep(3)
                            try:
                                resp = session.get(page_link, verify=False, timeout=10)
                            except requests.exceptions.Timeout:
                                print('Timeout occurred:', page_link)
                                break
                            soup_next_pg = BeautifulSoup(resp.text, "lxml")
                            table = soup_next_pg.find('table', id='tablcont')
                            records = table.find_all('tr')
                            records.pop(0)
                            for record in records:
                                col = 0
                                fields = record.find_all('td')
                                resh = fields[5].string
                                if not_empty(resh):  # поверка на наличие решения суда
                                    ii = 0
                                    for cell in fields:
                                        if ii == 7:
                                            a_t = cell.find('a')
                                            if a_t is not None:  # если есть документ
                                                delo_link = court + a_t['href']

                                                time.sleep(3)
                                                try:
                                                    resp = session.get(delo_link, verify=False, timeout=10)
                                                except requests.exceptions.Timeout:
                                                    print('Timeout occurred:', delo_link)
                                                    continue
                                                soup_delo = BeautifulSoup(resp.text, "lxml")
                                                content_delo = soup_delo.find('div', id='content')
                                                for script in content_delo.find_all('script'):
                                                    script.replaceWith('')

                                                f_delo = open('cases/' + str(document_cnt) + '.txt', 'w')
                                                print(content_delo.text, file=f_delo)
                                                f_delo.close()

                                                # запись названия файла с документом в таблицу:
                                                worksheet.write(row, col, str(document_cnt) + '.txt')
                                                document_cnt += 1

                                        else:
                                            worksheet.write(row, col, cell.text.strip())
                                        col += 1
                                        ii += 1
                                    row += 1
                            next_page_tag = soup.find('a', title="Следующая страница")
                            if next_page_tag is not None:
                                page_link = court + next_page_tag['href']
                            else:
                                break

                        # time.sleep(3)
                        try:
                            resp = session.get(court + param_str, verify=False, timeout=10)
                        except requests.exceptions.Timeout:
                            print('Timeout occurred:', court + param_str)
                            continue
                        soup = BeautifulSoup(resp.text, 'lxml')
                        if soup is None:
                            print("Error, can't parse this site", court)
                            continue

                        table_div = soup.find('div', id='resultTable')
                        if table_div is None:
                            third_str_courts_cnt += 1
                            print('Can not parse this site, other structure:', court)
                            continue

                        table = table_div.find('table')
                        if table is None:
                            third_str_courts_cnt += 1
                            print('Can not parse this site, other structure:', court)
                        records = table.tbody.find_all('tr')
                        if records is None:
                            print('No cases in this court: ', court, ',', srv_num)
                            continue
                        next_page_tag = 1
                        for record in records:
                            col = 0
                            fields = record.find_all('td')
                            resh = fields[5].string
                            if not_empty(resh):
                                ii = 0
                                for cell in fields:
                                    if ii == 7:
                                        a_t = cell.find('a')
                                        if a_t is not None:  # если есть документ
                                            delo_link = court + a_t['href']

                                            time.sleep(3)
                                            try:
                                                resp = session.get(delo_link, verify=False, timeout=10)
                                            except requests.exceptions.Timeout:
                                                print('Timeout occurred:', delo_link)
                                                continue
                                            soup_delo = BeautifulSoup(resp.text, "lxml")
                                            content_delo = soup_delo.find('div', id='content')
                                            for script in content_delo.find_all('script'):
                                                script.replaceWith('')

                                            f_delo = open('cases/' + str(document_cnt) + '.txt', 'w')
                                            print(content_delo.text, file=f_delo)
                                            f_delo.close()

                                            # запись названия файла с документом в таблицу:
                                            worksheet.write(row, col, str(document_cnt) + '.txt')
                                            document_cnt += 1
                                    else:
                                        worksheet.write(row, col, cell.text.strip())
                                    col += 1
                                    ii += 1
                                row += 1

                        next_page_tag = soup.find('a', title="Следующая страница")
                        pg_num = 1
                        while next_page_tag is not None:
                            pg_num += 1
                            # link_next = court + '/modules.php?name=sud_delo&srv_num=1&name_op=r&page='+str(pg_num)+'&vnkod='+str(subject_code)+'RS0001&srv_num=1&name_op=r&vnkod='+str(subject_code)+'RS0001&delo_id=1540005&case_type=0&new=0&G1_PARTS__NAMESS=&g1_case__CASE_NUMBERSS=&g1_case__JUDICIAL_UIDSS=&delo_table=g1_case&g1_case__ENTRY_DATE1D=&g1_case__ENTRY_DATE2D=&G1_CASE__JUDGE=&g1_case__RESULT_DATE1D=&g1_case__RESULT_DATE2D=&G1_CASE__RESULT=&G1_CASE__BUILDING_ID=&G1_CASE__COURT_STRUCT=&G1_EVENT__EVENT_NAME=&G1_EVENT__EVENT_DATEDD=&G1_PARTS__PARTS_TYPE=&G1_PARTS__INN_STRSS=&G1_PARTS__KPP_STRSS=&G1_PARTS__OGRN_STRSS=1027700132195&G1_PARTS__OGRNIP_STRSS=&G1_RKN_ACCESS_RESTRICTION__RKN_REASON=&g1_rkn_access_restriction__RKN_RESTRICT_URLSS=&G1_DOCUMENT__PUBL_DATE1D=&G1_DOCUMENT__PUBL_DATE2D=&G1_CASE__VALIDITY_DATE1D=&G1_CASE__VALIDITY_DATE2D=&G1_ORDER_INFO__ORDER_DATE1D=&G1_ORDER_INFO__ORDER_DATE2D=&G1_ORDER_INFO__ORDER_NUMSS=&G1_ORDER_INFO__STATE_ID=&Submit=%CD%E0%E9%F2%E8'
                            link_next = court + next_page_tag['href']

                            time.sleep(3)
                            try:
                                resp = session.get(link_next, verify=False, timeout=10)
                            except requests.exceptions.Timeout:
                                print('Timeout occurred:', court + param_str)
                                break
                            soup_next_pg = BeautifulSoup(resp.text, "lxml")
                            table = soup_next_pg.find('table', id='tablcont')
                            records = table.find_all('tr')
                            records.pop(0)
                            for record in records:
                                col = 0
                                fields = record.find_all('td')
                                resh = fields[5].string
                                if not_empty(resh):  # поверка на наличие решения суда
                                    ii = 0
                                    for cell in fields:
                                        if ii == 7:
                                            a_t = cell.find('a')
                                            if a_t is not None:  # если есть документ
                                                delo_link = court + a_t['href']

                                                time.sleep(3)
                                                try:
                                                    resp = session.get(delo_link, verify=False, timeout=10)
                                                except requests.exceptions.Timeout:
                                                    print('Timeout occurred:', delo_link)
                                                    continue
                                                soup_delo = BeautifulSoup(resp.text, "lxml")
                                                content_delo = soup_delo.find('div', id='content')
                                                for script in content_delo.find_all('script'):
                                                    script.replaceWith('')

                                                f_delo = open('cases/' + str(document_cnt) + '.txt', 'w')
                                                print(content_delo.text, file=f_delo)
                                                f_delo.close()

                                                # запись названия файла с документом в таблицу:
                                                worksheet.write(row, col, str(document_cnt) + '.txt')
                                                document_cnt += 1

                                        else:
                                            worksheet.write(row, col, cell.text.strip())
                                        col += 1
                                        ii += 1
                                    row += 1
                            next_page_tag = soup_next_pg.find('a', title="Следующая страница")

                        # index_debug += 1  #################
                        # if index_debug >= 2:
                        #     workbook.close()
                    else:
                        third_str_courts_cnt += 1

                print('current time =', str(datetime.now()))
                print('courts_cnt =', courts_cnt)
                print('ok_courts_cnt =', ok_courts_cnt)
                print('oth_str_courts_cnt =', oth_str_courts_cnt)

                print('all_doc_cnt =', all_documents_cnt)
                print('doc_cnt =', document_cnt)
                if ok_courts_cnt >= 240 or courts_cnt >= 500:
                    finish_time = str(datetime.now())
                    print('FINISH TIME =', finish_time)
                    workbook.close()

elif mode == 2:

    head_fields_mos = [
        'Номер дела',
        'Стороны',
        'Текущее состояние',
        'Судья',
        'Статья',
        'Категория дела',
        'Список дел',
        'Судебные акты'
    ]

    site_url = 'https://mos-gorsud.ru'
    search_form_url = 'https://mos-gorsud.ru/search'
    results_url = 'https://mos-gorsud.ru/search?formType=fullForm&courtAlias=&uid=&instance=&processType=&letterNumber=&caseNumber=&participant=%D0%A1%D0%B1%D0%B5%D1%80%D0%B1%D0%B0%D0%BD%D0%BA&codex=&judge=&publishingState=&baseDecision=&documentType=&documentText=&year=&caseDateFrom=&caseDateTo=&caseFinalDateFrom=&caseFinalDateTo=&caseLegalForceDateFrom=&caseLegalForceDateTo=&docsDateFrom=&docsDateTo=&documentStatus=2'

    cases_list = []
    timeout_cases_list = []
    timeout_pages_list = []
    timeout_docs_list = []

    page = 0
    row = 0
    col = 0
    cases_list.append(head_fields_mos)
    # for field in head_fields_mos:
    #     worksheet.write(row, col, field, header_format)
    #     col += 1

    row = 1
    # workbook.close()
    case_cnt = 0

    # myFile = open('mos-gorsud.csv', 'a') ###############
    # writer = csv.writer(myFile)




    # writer.writerows(cases_list)
    # myFile.close()

    while True:
        page += 1
        print('page =', page)

        # if page >= 2:             ########################
        #     # workbook.close()
        #
        #     writer.writerows(cases_list)
        #     myFile.close()
        #     break

        if page % 100 == 0:
            myFile = open('mos-gorsud.csv', 'a') ###############
            writer = csv.writer(myFile)
            writer.writerows(cases_list)
            myFile.close()
            # myFile = open('mos-gorsud.csv', 'a')
            # writer = csv.writer(myFile)
            print('100 pages saved in file.')
            cases_list.clear()
        # request next page of table
        try:
            resp = session.get(results_url + '&page=' + str(page), verify=False, timeout=10)
        except:
            # print('Timeout occurred:', results_url + '&page=' + str(page))
            print('Timeout page ', page)
            timeout_pages_list.append(results_url + '&page=' + str(page))
            continue
        soup = BeautifulSoup(resp.text, 'lxml')
        if soup is not None:
            result_tag = soup.find('div', {'class': 'searchResultContainer'})
            table_tag = result_tag.find('table', {'class': 'custom_table'})
            if table_tag is None:
                break
            else:
                records = table_tag.tbody.find_all('tr')
                if len(records) == 0:
                    break
                a_link = ''

                for record in records:
                    all_documents_cnt += 1
                    cases_list.append(list())
                    col = 0
                    fields = record.find_all('td')
                    a_link = fields[0].find('a')['href']
                    for field in fields:
                        # worksheet.write(row, col, field.text.strip())
                        cases_list[-1].append(field.text.strip())
                        col += 1

                    case_link = site_url + a_link
                    # request page with case (to get document link)
                    try:
                        resp = session.get(case_link, verify=False, timeout=10)
                    except:
                        print('Timeout occurred:', case_link)
                        timeout_cases_list.append(case_link)
                        continue
                    soup = BeautifulSoup(resp.text, 'lxml')
                    div_tag = soup.find('div', id='content')
                    div_tabs = div_tag.find('div', id='tabs-3')
                    doc_link = div_tabs.find('a')['href']
                    if doc_link is None:
                        continue
                    # request to download file
                    try:
                        resp = session.get(site_url + doc_link, verify=False, timeout=10)
                    except:
                        print('Timeout occurred:', site_url + doc_link)
                        timeout_docs_list.append(site_url + doc_link)
                        continue
                    doc_file = open('cases/' + str(document_cnt) + '.doc', 'wb')
                    doc_file.write(resp.content)
                    doc_file.close()
                    # worksheet.write(row, col, str(document_cnt) + '.doc')
                    cases_list[-1].append(str(document_cnt) + '.doc')
                    document_cnt += 1

                    row += 1

    # myFile.close()

    ftimeout_cases = open('timeout_cases.txt', 'w')
    ftimeout_pages = open('timeout_pages.txt', 'w')
    ftimeout_docs = open('timeout_pages.txt', 'w')
    print(*timeout_cases_list, sep='\n', file=ftimeout_cases)
    print(*timeout_pages_list, sep='\n', file=ftimeout_pages)
    print(*timeout_docs_list, sep='\n', file=ftimeout_docs)
    ftimeout_cases.close()
    ftimeout_pages.close()
    ftimeout_docs.close()


# workbook.close()
# print('all_documents_cnt =', all_documents_cnt)
print('other structure:', oth_str_courts_cnt)
print('Start time =', start_time)
print('finish time =', str(datetime.now()))
