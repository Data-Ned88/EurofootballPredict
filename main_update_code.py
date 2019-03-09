import generate_data_frame as gdf
import scrape_update as sup
import pandas as pd
import pymysql as pm
import datetime as dt
from bs4 import BeautifulSoup
import requests
import re

connection = pm.connect(host='localhost',
                             user='root',
                             password='Terry1943',
                             db='football_matches',
                             charset='utf8mb4',
                             cursorclass=pm.cursors.DictCursor)

comp_excel = 'C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\Excel_Resources\\Football Project Xscores URLs.xlsx'
competitions = pd.read_excel(comp_excel)

for x in competitions.index:
    
    surl = str(competitions.loc[x, 'URL'])
    sleague = str(competitions.loc[x, 'League'])
    scountry = str(competitions.loc[x, 'Country'])
    smodel = str(competitions.loc[x, 'Model'])
    checkframe = gdf.check_frame(connection, sleague, scountry)
    
    sup.scrape_update(surl, scountry, sleague, '2018/19', smodel, connection, checkframe)
    
docs = '--------\n All competitions completed.\n--------'
print(docs)
connection.close()
