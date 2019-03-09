import pandas as pd
import pymysql as pm
import datetime as dt
from bs4 import BeautifulSoup
import requests
import re

def scrape_update(url, country, comp, season, model, grid_conn, compareframe):
    
    st_time = dt.datetime.now()
    season_req = requests.get(url)
    soup = BeautifulSoup(season_req.text, 'lxml')
    inserts, results, time_updates = 0,0,0
    inserts_l, results_l, time_updates_l = [], [], []
    #makes dictionary with round and date for each match ID
    match_date_round = {}

    rounds = []
    dates = []
    match_ids = []
    for art in soup.find_all('div'):
        j = art.attrs
        if 'class' in j:
            b = j.values()
            for bb in b:
                check = ' '.join(bb)
                if check == 'score_row round_header score_header':
                    x = art.get_text()
                    xform = re.sub('\\n+','',x)

                elif check == 'score_row padded_date country_header':
                    y = art.get_text()
                    yform = re.sub('\\n+','',y)

                elif check == 'score_row match_line e_true':
                    dates.append(yform)
                    rounds.append(xform)
                    match_ids.append(art['id'])
                elif check == 'score_row match_line o_true':
                    dates.append(yform)
                    rounds.append(xform)
                    match_ids.append(art['id'])
    for i, v in enumerate(match_ids):
        datex = dates[i].split('-')
        datel = '-'.join([datex[2], datex[1], datex[0]])
        match_date_round[v] = [datel, rounds[i]]
    #list for season with lists for each match. each match's list has 14 list elements. 
    #The 14 have 2 sub elements, key and value
    main_match_list = []

    for rw in soup.find_all('div'):

        main_attribs = rw.attrs
        if 'class' in main_attribs:
            data = ' '.join(main_attribs['class'])
            if data in ('score_row match_line e_true', 'score_row match_line o_true'):

                match_list = []
                if 'id' in main_attribs:
                    key = 'match_id'
                    value = main_attribs['id']
                    tup = [key, value]
                    match_list.append(tup)               

                for art in rw.find_all('div'):
                    attribs = art.attrs
                    if 'class' in attribs:
                        if attribs['class'][0] in ('score_ko', 'score_time', 'score_league_txt', 'score_home_txt',
                                      'y_cards', 'r_cards', 'score_away_txt', 'score_ht', 'score_score',
                                      'score_et', 'score_pen'):
                            key = attribs['class'][0]
                            value = art.get_text()
                            valform = re.sub('\\n+','',value)

                            tup = [key, valform]
                            match_list.append(tup)
                main_match_list.append(match_list)
    # removes blank elements from the list
    clean_list = []
    for item in main_match_list:
        if len(item) != 0:
            clean_list.append(item)
    #DONT PROCEDE IF CLEAN LIST OF MATCHES IS EMPTY
    if clean_list == [] or len(clean_list) == 0:
        return('No fixtures for {0}: {1}'.format(comp, url))
    else:
        

        #combines the chanageble variables (league name, model, season,country), the match date/round disctionary,
        # and the match info list to make an ordered values row for each match.
        # then works with grid_conn variable to check row against database on id.
        #    if id not there, insert as new
        #    elif there, check change of result and update
        #    elif datediff or timediff, change date/time
        #   else do nothinh
        for row in clean_list:
            ident = row[0][1]
            inputrow = [ident, country, comp, season, model, match_date_round[ident][1], match_date_round[ident][0],
                        row[1][1]]
            if row[2][1] == 'Fin':
                inputrow.append('1')
            else:
                inputrow.append('0')
            inputrow.append(row[4][1])
            if row[5][1] == '':
                inputrow.append('0')
            else:
                inputrow.append(row[5][1])
            if row[6][1] == '':
                inputrow.append('0')
            else:
                inputrow.append(row[6][1])
            inputrow.append(row[7][1])
            if row[8][1] == '':
                inputrow.append('0')
            else:
                inputrow.append(row[8][1])
            if row[9][1] == '':
                inputrow.append('0')
            else:
                inputrow.append(row[9][1])
            h_ht, a_ht = row[10][1].split('-')
            h_ft, a_ft = row[11][1].split('-')
            h_aet, a_aet = row[12][1].split('-')
            h_pen, a_pen = row[12][1].split('-')
            scores_list = [h_ht, a_ht, h_ft, a_ft, h_aet, a_aet, h_pen, a_pen]
            prep_scores_list = [item.replace(' ','') for item in scores_list]
            refi_scores_list = ['NULL' if x == '' else x for x in prep_scores_list]


            inputrow.extend(refi_scores_list)
            sql_list = []
            for el in inputrow:
                if el.isdigit() or el == 'NULL':
                    sql_list.append(el)
                else:
                    sql_list.append('\"' + el + '\"')

            #make time and date checking variables
            subj_y = dt.datetime.strptime(inputrow[6], '%Y-%m-%d').year
            subj_m = dt.datetime.strptime(inputrow[6], '%Y-%m-%d').month
            subj_d = dt.datetime.strptime(inputrow[6], '%Y-%m-%d').day
            subj_h = dt.datetime.strptime(inputrow[7], '%H:%M').hour
            subj_min = dt.datetime.strptime(inputrow[7], '%H:%M').minute
            #is the compareframe a None?
            if compareframe is None:
                with grid_conn.cursor() as cursor:
                    sqlq = 'INSERT INTO football_matches.matches VALUES ({0})'.format(', '.join(sql_list))

                    cursor.execute(sqlq)
                    grid_conn.commit()
                    msg = '1 Row Inserted: {0}: {1}'.format(comp, ', '.join(sql_list))
                    inserts +=1
                    inserts_l.append(msg)
            else:
                
                #check if match already in db
                if int(ident) in compareframe['idmatches']:

                    for c in compareframe.index:



                        tar_d = compareframe.loc[c, 'game_date'].day
                        tar_y = compareframe.loc[c, 'game_date'].year
                        tar_m = compareframe.loc[c, 'game_date'].month
                        tar_h = compareframe.loc[c, 'game_time'].components.hours
                        tar_min = compareframe.loc[c, 'game_time'].components.minutes

                        if int(ident) == c:
                            #has it been resulted?
                            if compareframe.loc[int(ident), 'result'] == 0 and inputrow[8] == '1':
                                #if so, update match full record
                                with grid_conn.cursor() as cursor:
                                    sqlq = 'UPDATE football_matches.matches SET \
                                    game_date = {0}, game_time = {1}, result = 1, h_yellow = {2}, h_red = {3}, \
                                    a_yellow = {4}, a_red = {5}, h_ht = {6}, a_ht = {7}, h_ft = {8}, a_ft = {9}, \
                                    h_aet = {10}, a_aet = {11}, h_pens = {12}, a_pens = {13} WHERE idmatches = \
                                    {14}'.format(sql_list[6],sql_list[7],sql_list[10],sql_list[11],sql_list[13],sql_list[14],
                                                sql_list[15],sql_list[16],sql_list[17],sql_list[18],sql_list[19],sql_list[20],
                                                sql_list[21],sql_list[22],sql_list[0])

                                    cursor.execute(sqlq)
                                    grid_conn.commit()
                                    msg = '1 Row resulted: {0}: {1} vs {2}: FT {3}-{4}'.format(comp, sql_list[9], sql_list[12],
                                                                                              sql_list[17],sql_list[18])
                                    results += 1
                                    results_l.append(msg)
                            elif not (subj_y == tar_y and subj_m == tar_m and subj_d == tar_d and subj_h == tar_h and \
                                     subj_min == tar_min):
                                with grid_conn.cursor() as cursor:
                                    sqlq = 'UPDATE football_matches.matches SET \
                                    game_date = {0}, game_time = {1} WHERE idmatches = \
                                    {2}'.format(sql_list[6],sql_list[7], sql_list[0])

                                    cursor.execute(sqlq)
                                    grid_conn.commit()
                                    msg = '1 Row Updated - new time date: {0}: {1} vs {2} now at {3} {4}'.format(comp, 
                                                                                                                 sql_list[9], 
                                                                                                                 sql_list[12],
                                                                                                                 sql_list[6],
                                                                                                                 sql_list[7])
                                    time_updates +=1
                                    time_updates_l.append(msg)
                            else:
                                pass
                else:
                    with grid_conn.cursor() as cursor:
                        sqlq = 'INSERT INTO football_matches.matches VALUES ({0})'.format(', '.join(sql_list))

                        cursor.execute(sqlq)
                        grid_conn.commit()
                        msg = '1 Row Inserted: {0}: {1}'.format(comp, ', '.join(sql_list))
                        inserts +=1
                        inserts_l.append(msg)
                        
    end_time = dt.datetime.now()
    delta = end_time - st_time
    mins = round(delta.seconds/60,1)
    secs = delta.seconds % 60
    report_msg = '{0} {1} completed in {2} minutes and {3} seconds: \
Following changes made \n {4} new fixtures: \n {5} \n {6} results: \n {7} \n {8} \
updates: \n {9}'.format(country, comp, mins, secs, inserts,
                           '\n'.join(inserts_l), results, '\n'.join(results_l),
                           time_updates, '\n'.join(time_updates_l))
    print(report_msg)
