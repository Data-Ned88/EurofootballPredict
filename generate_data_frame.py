import pandas as pd
import pymysql as pm
import datetime as dt
from bs4 import BeautifulSoup
import requests
import re

def check_frame(grid_conn, comp, country):
    with grid_conn.cursor() as cursor:
        
        
        sql2 = 'SELECT idmatches, game_date, game_time, \
            result FROM football_matches.matches \
            WHERE season = "2018/19" AND competition = \"{0}\" AND country = "\{1}\"'.format(comp, country)
        
        
        cursor.execute(sql2)
        result = cursor.fetchall()


    try:
        
        key_list = list(result[0].keys())
        top_res = result[0]
        start_dict = {k: [top_res[k]] for k in key_list}
        for r in result[1:]:
            for k in key_list:
                start_dict[k].append(r[k])

        sqlframe = pd.DataFrame(start_dict, index = start_dict['idmatches'])

        return(sqlframe)
    except:
        return(None)