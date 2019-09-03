# -*- coding: utf-8 -*-

import csv
import json
import re
import os
import pandas as pd
import socket
import sys
from urllib.request import urlopen
from urllib.request import URLError
from bs4 import BeautifulSoup
from tqdm import tqdm
from random import randint
from time import sleep
from pyspark.sql import SparkSession
pd.set_option('display.max_columns', None)

def get_start_index(path, directory):
    os.chdir(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    os.chmod(directory, 0o0777)
    os.chdir(path + '/' + directory)
    return len(os.listdir())
def get_html(url_page, timeout):
    flag = True
    while flag:
        try:
            html = urlopen(url_page, timeout=timeout)
            flag = False
        except URLError as e:
            print(type(e))
            flag = True
        except socket.timeout as e:
            print(type(e))
            flag = True
    return html
def get_max_flats_pages(url_page):
    print('Requesting %s URL...' % url_page)
    html = get_html(url_page, 180)
    soup = BeautifulSoup(html, 'html.parser')
    find_count = soup.find_all('span', {'class': 'count'})
    max_flats = int(find_count[1].text.replace(' ', ''))
    max_pages = max_flats // 60 + 1
    return max_flats, max_pages
def get_links_from_page(url_page_all, page_num, min_time_sleep, max_time_sleep):
    url_page = url_page_all + '?page=' + str(page_num)
    html = get_html(url_page, 10)
    soup = BeautifulSoup(html, 'html.parser')
    all_links = soup.find_all('a', {'class': 'title bind-click'})
    sleep(randint(min_time_sleep, max_time_sleep))
    return all_links
def get_flats_from_page(url_page, all_links, min_time_sleep, max_time_sleep):
    flats_on_page = []
    for item in tqdm(all_links):
        url_flat = url_page + item['href']
        html_i = get_html(url_flat, 10)
        soup_i = BeautifulSoup(html_i, 'html.parser')
        # --- main features ----
        fields = soup_i.find_all('div', {'id': 'info-about'})
        keys = ['main_' + elm.text for elm in fields[0].find_all('div', {'class': 'name'})]
        values = [[string.strip() for string in elm.stripped_strings] if len(list(elm.stripped_strings)) > 1 
                  else elm.text.strip()
                  for elm in fields[0].find_all('div', {'class': 'value'})]
        flat_dict = dict(zip(keys, values))
        # --- description ---
        fields_desc = soup_i.find_all('div', {'id': 'bottom-text'})
        keys = ['desc_Описание квартиры']
        values = [string.strip() for string in fields_desc[0].stripped_strings]
        flat_dict_desc = dict(zip(keys, [values]))
        flat_dict.update(flat_dict_desc)
        # --- extended features ---
        try:
            fields_sub = soup_i.find_all('div', {'class': 'card-info gkh_house_info'})
            keys_sub = ['sub_' + elm.text for elm in fields_sub[0].find_all('div', {'class': 'field_header'})]
            values_sub = [elm.text.strip() for elm in fields_sub[0].find_all('div', {'class': 'field_value'})]
            flat_dict_sub = dict(zip(keys_sub, values_sub))
            flat_dict.update(flat_dict_sub)
        except:
            pass
        flats_on_page.append(flat_dict)
        sleep(randint(min_time_sleep, max_time_sleep))
    return flats_on_page
def get_dataframe(path, directory):
    os.chdir(path + '/' + directory)
    files = os.listdir()
    batches_load = []
    for file_load in tqdm(files):
        with open(file_load) as file:
            batches_load.extend(json.load(file))
    os.chdir(path)
    return pd.DataFrame(batches_load)
def get_proc(df):
    print('start processing...')
    print(df.shape)
    df['desc_Описание квартиры'] = df['desc_Описание квартиры'].apply(lambda x: re.sub('[\[\]\'\\\\n]', '', x))
    print('description done')
    df['main_Адрес'] = df['main_Адрес'].apply(lambda x: re.sub('[\[\]\'\\\\n]', '', x))
    print('address done')

    has_subway = 'main_Метро' in df

    if has_subway:
        df['main_Метро_станция'] = [re.split('[\']', x)[1] if len(x) > 0 else '' for x in df['main_Метро'].fillna('')]
        df['main_Метро_время'] = [re.split('[\']', x)[3] if len(x) > 0 else '' for x in df['main_Метро'].fillna('')]
        df['main_Метро_доп'] = [re.split('[\']', x)[-2] if len(x) > 0 else '' for x in df['main_Метро'].fillna('')]
        print('subway done')

    df['main_Цена_всего'] = [re.split('[\\t\\r\\n]', re.sub('[\(\)]', '', x))[0] if len(x) > 0 else '' 
                             for x in df['main_Цена'].fillna('')]
    df['main_Цена_метр'] = [re.split('[\\t\\r\\n]', re.sub('[\(\)]', '', x))[-1] if len(x) > 0 else '' 
                            for x in df['main_Цена'].fillna('')]
    print('price done')
    df['main_Срок сдачи_текущий'] = [re.split('[\']', x.replace('[', ''))[0] if len(re.split('[\']', x)) > 0 else '' 
                                     for x in df['main_Срок сдачи'].fillna('')]
    df['main_Срок сдачи_перенос'] = [re.split('[\']', x)[1] if len(re.split('[\']', x)) > 1 else '' 
                                     for x in df['main_Срок сдачи'].fillna('')]
    df['main_Срок сдачи_изменения'] = [re.split('[\']', x)[-2] if len(re.split('[\']', x)) > 1 else '' 
                                       for x in df['main_Срок сдачи'].fillna('')]
    print('deadline done')

    if has_subway:
        del(df['Unnamed: 0'], df['main_Метро'], df['main_Цена'], df['main_Срок сдачи'])
    else:
        del(df['Unnamed: 0'], df['main_Цена'], df['main_Срок сдачи'])

    print('...processing done')
    return df
def translit(text):
    symbols = ('абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ ',
               'abvgdeejzijklmnoprstufhccss_y_euaABVGDEEJZIJKLMNOPRSTUFHCCSS_Y_EUA_')
    tr = {ord(a):ord(b) for a, b in zip(*symbols)}
    return text.translate(tr)
def write_to_hdfs(df, table_name):
    print('creating spark dataframe: ', table_name)
    spark = SparkSession.builder.appName('spark').getOrCreate()
    df_spark = spark.createDataFrame(df)
    for col in df_spark.columns:
        df_spark = df_spark.withColumnRenamed(col, re.sub('[^_a-zA-Z0-9]+', '_', translit(col)))
    table_path = 'outer_data.' + table_name
    df_spark.write.saveAsTable(table_path, format='parquet', mode='overwrite')
    print(table_path, ' ...created')
def main():
    main_url = sys.argv[1]
    work_dir = sys.argv[2]
    limit = int(sys.argv[3])
    table_name = 'novostroy_data_flats_' + str(sys.argv[4]) #example su_20190113 / ru_20190113 / kaluga_20190113
    min_time_sleep = 1
    max_time_sleep = min_time_sleep + 4
    count_trial = 0
    flag = True
    
    while flag:
        try:
            print('starting trial %d...' % count_trial)
            start_index = get_start_index(work_dir, 'batches_load' + '_' + main_url[-2:])
            print('start index = %d' % start_index)
            max_pages = get_max_flats_pages(main_url + '/new-flats/')[1]
            if limit > 0:
                max_pages = min(max_pages, limit)
            print('trial num: ', count_trial, ' | total pages: ', max_pages, ' | start index: ', start_index)
            for page_num in tqdm(range(start_index, max_pages + 1)):
                all_links = get_links_from_page(main_url + '/new-flats/', 
                                                page_num, min_time_sleep, max_time_sleep)
                print('got total %d links ' % len(all_links))
                flats_on_page = get_flats_from_page(main_url, 
                                                    all_links, min_time_sleep, max_time_sleep)
                print('got total %d flats on the page ' % len(flats_on_page))
                filename = 'batches_load_' + str(page_num) + '.txt'
                with open(filename, 'w') as file:
                    json.dump(flats_on_page, file)
                os.chmod(filename, 0o0666)
            flag = False
        except BaseException as e:
            print(e)

            count_trial += 1
            flag = True

    df_flats = get_dataframe(work_dir, 'batches_load' + '_' + main_url[-2:])
    print('data frame created of shape: ', df_flats.shape)
    df_flats = get_proc(df_flats)
    print('data frame processed of shape: ', df_flats.shape)
    write_to_hdfs(df_flats, table_name)

if __name__ == '__main__':
    main()

