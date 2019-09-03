#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import bs4
import re
import json
import os
import sys
from pandas.io.json import json_normalize
from datetime import datetime
from random import randint
from time import sleep, time
from pyspark.sql import SparkSession

# In[2]:


def last_page(parser, page_num):
    '''
    Parameters
    ----------
    parser : BeautifulSoup, parser of current page.
    page_num : int, number of current page.
    
    Returns
    -------
    last_page : in, last page number.
    '''
    
    # find all number of pages on current page
    page_numbers = parser.find_all('div', {'class': 'page'})
    
    # get number of last page
    last_page = int(page_numbers[-1].text)
    
    return last_page


# In[3]:


def get_information_from_page(parser, information_fields):
    '''
    Parameters
    ----------
    parser : BeautifulSoup, parser of current page.
    information_fields : tuple or list, list with names of each building parameter.
    
    Returns
    -------
    information : list, list of dictionaries with information about each building on page.
    '''
    
    buildings = parser.find_all('div', {'class': 'nwb-list-item'})
    buildings_title_divs = tuple(map(lambda building: building.find('div', {'class': 'nwb-list-item__title'}), buildings))
    building_titles = (div.find('a').text.strip() for div in buildings_title_divs if div.find('a') is not None)
    links = (div.find('a')['href'].strip() for div in buildings_title_divs if div.find('a') is not None)
    white_list = map(lambda div: div.find('span', {'class': 'badge badge-white-list button spl'}), buildings)
    white_list = map(lambda div: '' if div is None else 'W', white_list)
    black_list = map(lambda div: div.find('span', {'class': 'badge badge-black-list button spl'}), buildings)
    black_list = map(lambda div: '' if div is None else 'B', black_list)
    information_gen = zip(building_titles, links, white_list, black_list)
    information = [dict(item for item in zip(information_fields, info)) for info in information_gen]
    
    return information


# In[4]:


def get_expert_ratings(expert_ratings_div):
    '''
    Parameters
    ----------
    expert_ratings_div : bs4.element.Tag, div containing a expert ratings.
    
    Returns
    -------
    expert_ratings_list : dict, dict of expert rating names and values.
    '''
    
    expert_ratings_map = map(lambda a: a['data-popup-text'].lower(), expert_ratings_div.find_all('a', {'data-popup-text': re.compile('.*')}))
    expert_ratings_map = map(lambda rating: rating.split('—'), expert_ratings_map)
    expert_ratings_dict = {re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', key).strip(): re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', value).strip() for key, value in expert_ratings_map}
    
    return expert_ratings_dict


# In[5]:


def get_people_ratings(people_ratings_div):
    '''
    Parameters
    ----------
    people_ratings_div : bs4.element.Tag, div containing a people ratings.
    
    Returns
    -------
    rating_dict : dict, tupe of aggregate rating, count of rated users, dict of detailed ratings.
    '''
    
    def __get_people_ratings(class_name):
        '''
        Parameters
        ----------
        class_name : string, parameter can be 'criterion location', 'criterion project', 'criterion risks'.

        Returns
        -------
        rating_dict : dict, detailed rating name, value and max value.
        '''
        
        item_people_ratings_div = people_ratings_div.find('div', {'class': class_name})
        stars_count = len(item_people_ratings_div.find_all('div', {'class': re.compile('star .*')}))
        stars_on_count = len(item_people_ratings_div.find_all('div', {'class': 'on'}))
        stars_off_count = len(item_people_ratings_div.find_all('div', {'class': 'off'}))
        rating = stars_on_count if stars_on_count + stars_off_count == stars_count else stars_on_count - 0.5
        rating_name = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', item_people_ratings_div.find('div', {'class': 'criterion_label'}).text).strip()
        rating_dict = {
            rating_name: {
                'Оценка': rating,
                'Максимально возможная оценка': stars_count
            }
        }
        
        return rating_dict
    
    class_names = ('criterion location', 'criterion project', 'criterion risks')
    
    detailed_raiting = {}
    for class_name in class_names:
        detailed_raiting.update(__get_people_ratings(class_name))
    
    agg_people_ratings_div = people_ratings_div.find('div', {'class': 'aggregate'})
    agg_stars_count = len(agg_people_ratings_div.find_all('div', {'class': re.compile('star .*')}))
    agg_stars_on_count = len(agg_people_ratings_div.find_all('div', {'class': 'on'}))
    agg_stars_off_count = len(agg_people_ratings_div.find_all('div', {'class': 'off'}))
    agg_rating = agg_stars_on_count if agg_stars_on_count + agg_stars_off_count == agg_stars_count else agg_stars_on_count - 0.5
    agg_users_count = agg_people_ratings_div.find('div', {'class': 'count'})
    agg_users_count = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', agg_users_count.text).strip()
    
    rating_dict = {
        'Количество оценивших': agg_users_count,
        'Агрегированый рейтинг': agg_rating,
        'Максимально возможный агрегированный рейтинг': agg_stars_count,
        'Детальный рейтинг': detailed_raiting
    }
    
    return rating_dict


# In[6]:


def get_builder_info(builder_div):
    '''
    Parameters
    ----------
    builder_div : bs4.element.Tag, div containing a builder info.
    
    Returns
    -------
    builder_info_dict : dict, dict with builder name and rating.
    '''
    
    builder = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', builder_div.find('a', {'class': 'link'}).text).strip()
    rating = builder_div.find('span', {'class': re.compile('builder-rating.*')})
    rating = '' if rating is None else re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', rating.text).strip()
    builder_info_dict = {'Название': builder, 'Рейтинг': rating}
    
    return builder_info_dict


# In[7]:


def get_address_info(address_div):
    '''
    Parameters
    ----------
    address_div : bs4.element.Tag, div containing a address.
    
    Returns
    -------
    address_info_dict : dict, dict of address and gps coordinates.
    '''
    
    address = address_div.find('p')
    address = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', address.text).strip()
    gps = address_div.find('a', {'class': re.compile('gps.*')})
    gps = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', gps.text).strip()
    address_info_dict = {'Адрес': address, 'GPS': gps}
    
    return address_info_dict


# In[8]:


def get_metro(metro_div):
    '''
    Parameters
    ----------
    metro_div : bs4.element.Tag, div containing a metro name.
    
    Returns
    -------
    metro_name : string, metro name.
    '''
    
    metro_name = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', metro_div.find('a', {'target': '_blank'}).text).strip()
    
    return metro_name


# In[9]:


def get_sales_status(sales_status_div):
    '''
    Parameters
    ----------
    sales_status_div : bs4.element.Tag, div containing a sales status.
    
    Returns
    -------
    sales_status : string, sales status.
    '''
    
    sales_status = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', sales_status_div.text).strip()
    
    return sales_status


# In[10]:


def get_apartments_count(apartments_count_div):
    '''
    Parameters
    ----------
    apartments_count_div : bs4.element.Tag, div containing a apartments count.
    
    Returns
    -------
    apartments_count : string, apartments count.
    '''
    
    apartments_count = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', apartments_count_div.find('span', {'class': 'spl'}).text).strip()
    
    return apartments_count


# In[11]:


def get_floors_count(floors_count_div):
    '''
    Parameters
    ----------
    floors_count_div : bs4.element.Tag, div containing a floors count.
    
    Returns
    -------
    floors_count : string, floors count.
    '''
    
    floors_count = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', floors_count_div.text).strip()
    
    return floors_count


# In[12]:


def get_ceiling_height(ceiling_height_div):
    '''
    Parameters
    ----------
    ceiling_height_div : bs4.element.Tag, div containing a ceiling height.
    
    Returns
    -------
    ceiling_height : string, ceiling height.
    '''
    
    ceiling_height = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', ceiling_height_div.text).strip()
    
    return ceiling_height


# In[13]:


def get_project_type(project_type_div):
    '''
    Parameters
    ----------
    project_type_div : bs4.element.Tag, div containing a project type.
    
    Returns
    -------
    project_type : string, project type.
    '''
    
    project_type = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', project_type_div.text).strip()
    
    return project_type


# In[14]:


def get_parking_flag(parking_flag_div):
    '''
    Parameters
    ----------
    parking_flag_div : bs4.element.Tag, div containing a parking flag.
    
    Returns
    -------
    parking_flag : strip, parking flag.
    '''
    
    parking_flag = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', parking_flag_div.text).strip()
    
    return parking_flag


# In[15]:


def get_deadline(building_parser):
    '''
    Parameters
    ----------
    building_parser : BeautifulSoup, parser of current building page.
    
    Returns
    -------
    deadline : dict, dict of deadline info - deadline, time/result, buildings quantity.
    '''
    
    building_parameters = building_parser.find('div', {'id': 'parameters-block'})
    deadline_divs = building_parameters.find('div', {'class': 'card-info deadline'})
    deadline_divs = deadline_divs.find('div', {'class': 'title'})
    deadline_divs = deadline_divs.find_all('span')
    deadline = list(map(lambda span: span.text.strip(), deadline_divs))
    deadline = {
        deadline[0]: {
            'Статус': deadline[1],
            'Количество корпусов': deadline[2] if len(deadline) > 2 else ''
        }
    }
    
    return deadline


# In[16]:


def get_card_info_description(building_parser):
    '''
    Parameters
    ----------
    building_parser : BeautifulSoup, parser of current building page.
    
    Returns
    -------
    description : dict, description of building.
    '''
    
    description_div = building_parser.find('div', {'class': 'card-info description'})
    description = list(map(lambda p: p.text, description_div.findAll('p')))
    description = ' '.join(description)
    description = re.sub('(\\n|\\t|\\r|\\xa0)+', ' ', description).strip()
    description = {'О жилищном комплексе': description}
    
    return description


# In[17]:


def glue_jsons_to_data_frame(path_to_json_files):
    '''
    Parameters
    ----------
    path_to_json_files : string, path to directory with .json files.
    
    Returns
    -------
    df : DataFrame, Pandas DataFrame with all data from .json files.
    '''
        
    json_names = os.listdir(path_to_json_files)
    glued_json = []
    for json_name in json_names:
        with open(os.path.join(path_to_json_files, json_name)) as file:
            json_file = json.load(file)
            glued_json.extend(json_file)
    df = json_normalize(glued_json)
    
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

# In[18]:


def main():
    
    # ------------------------- const -------------------------
    
    ROOT = {
        'Moscow': 'https://www.novostroy.ru',
        'St.Petersburg': 'https://www.novostroy.su',
        'Kaluga': 'https://kaluga.novostroy.su',
        'Kaliningrad': 'https://kaliningrad.novostroy.su',
        'Novosibirsk': 'https://novosibirsk.novostroy.su'
    }

    TYPE = 'buildings'

    URL_PARAMETERS = (
        'Building_page',
    )

    INFORMATION_FIELDS = (
        'Наименование комплекса',
        'Ссылка',
        'В белом списке',
        'В чёрном списке'
    )

    BUILDING_PARAMETERS = {
        'Экспертный рейтинг': get_expert_ratings,    # nullable
        'Народный рейтинг':   get_people_ratings,
        'Застройщик':         get_builder_info,      # nullable
        'Адрес':              get_address_info,
        'Метро':              get_metro,
        'Статус продаж':      get_sales_status,
        'Квартиры':           get_apartments_count,  # nullable
        'Этажей':             get_floors_count,
        'Высота потолков':    get_ceiling_height,    # nullable
        'Тип проекта':        get_project_type,
        'Парковка':           get_parking_flag
    }

    MIN_TIME_SLEEP = 1
    MAX_TIME_SLEEP = 3

    LOAD_PATH = sys.argv[1]
    
    TSV_NAME = 'data.tsv'
    
    city = sys.argv[2]
    if city not in ROOT:
        print('Wrong city, must be: ' + str(ROOT.keys()))

        exit(1)

    limit = int(sys.argv[3])
    
    table_name = 'novostroy_data_jk_' + str(sys.argv[4]) #example su_20190113 / ru_20190113

    # ------------------------- code --------------------------
    
    start_time = time()

    if not os.path.isdir(LOAD_PATH):
        os.mkdir(LOAD_PATH)
    os.chmod(LOAD_PATH, 0o0777)

    load_dt_city_json_path = os.path.join(LOAD_PATH, 'json')
    if not os.path.isdir(load_dt_city_json_path):
        os.mkdir(load_dt_city_json_path)
    os.chmod(load_dt_city_json_path, 0o0777)

    page_names = os.listdir(load_dt_city_json_path)
    if page_names:
        all_page_nums = list(map(lambda page_name: int(re.findall('\d+', page_name)[0]) + 1, page_names))
        page_num = max(all_page_nums)
    else:
        page_num = 1

    while True:
        page_url = '{root}/{type}/?{parameter}={value}'.format(
            root=ROOT[city],
            type=TYPE,
            parameter=URL_PARAMETERS[0],
            value=page_num
        )
        request = requests.get(page_url)
        parser = bs4.BeautifulSoup(request.text, 'lxml')
        last_page_num = last_page(parser, page_num)
        if page_num > last_page_num or (limit > 0 and page_num > limit):
            break
        else:
            page_info = get_information_from_page(parser, INFORMATION_FIELDS)
            page_info_updated = []
            for i in range(len(page_info)):
                print(
                    "\rLOAD_PATH: {}, city: {}, page № {} from {}, building № {} from {}".format(
                    LOAD_PATH,
                    city,
                    page_num,
                    last_page_num,
                    i + 1,
                    len(page_info)
                    ),
                    end=""
                )
                new_item = {}
                new_item.update(page_info[i])
                link = page_info[i]['Ссылка']
                building_url = '{root}{link}'.format(root=ROOT[city], link=link)
                building_request = requests.get(building_url)
                building_parser = bs4.BeautifulSoup(building_request.text, 'lxml')
                building_parameters = building_parser.find('div', {'id': 'parameters-block'})
                parameter_names = building_parameters.find_all('div', {'class': 'name'})
                parameter_names = tuple(map(lambda div: div.text, parameter_names))
                parameter_value_divs = building_parameters.find_all('div', {'class': 'value'})
                parameter_dict = {k: v for k, v in zip(parameter_names, parameter_value_divs)}
                for param, func in BUILDING_PARAMETERS.items():
                    try:
                        div = parameter_dict[param]
                        new_item.update({param: func(div)})
                    except:
                        pass

                try:
                    new_item.update(get_deadline(building_parser))
                except:
                    pass

                try:
                    new_item.update(get_card_info_description(building_parser))
                except:
                    pass

                page_info_updated.append(new_item)
                sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))

            file_name = '{load_dt_city_json_path}/page_{page_num}.json'.format(load_dt_city_json_path=load_dt_city_json_path, page_num=page_num)
            with open(file_name, 'w') as f:
                json.dump(page_info_updated, f, ensure_ascii=False)
            os.chmod(file_name, 0o0666) 
            page_num += 1
            
        load_dt_city_csv_path = os.path.join(LOAD_PATH, TSV_NAME)
        #if not os.path.isfile(load_dt_city_csv_path):
        #    df = glue_jsons_to_data_frame(load_dt_city_json_path)
        #    df.to_csv(load_dt_city_csv_path, index=False, sep='\t')
        
        df = glue_jsons_to_data_frame(load_dt_city_json_path)
        print('data frame created of shape: ', df.shape)
        write_to_hdfs(df, table_name)
        
        
    total_time = time() - start_time
    print()
    print('Total time: {:.2f} s'.format(total_time))


# In[19]:


if __name__ == '__main__':
    main()

