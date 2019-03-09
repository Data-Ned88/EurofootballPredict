import re
import pandas as pd
import pymysql as pm
import datetime as dt
from collections import OrderedDict
from warnings import filterwarnings

def get_fixtures(date, connection, season):
    
    sd_pandas = pd.to_datetime(date)
    day_inc = pd.Timedelta(1, unit='D')
    sd_pandas_i = sd_pandas + day_inc
    sd_pandas_ii = sd_pandas + day_inc*2
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    sql_day_i = '"%s-%s-%s"' % (sd_pandas_i.year, sd_pandas_i.month, sd_pandas_i.day)
    sql_day_ii = '"%s-%s-%s"' % (sd_pandas_ii.year, sd_pandas_ii.month, sd_pandas_ii.day)
    
    format_season =  '"' + season + '"'
    with connection.cursor() as cursor:


        fixtures_sql = 'select idmatches, comp_model, country, competition, game_date, game_time, home_team, away_team, \
h_ft, a_ft from football_matches.matches \
WHERE game_date IN ({0}, {1},  {2}) \
AND comp_model IN (1,2) AND season = {3} \
order by game_date, home_team'.format(sql_day, sql_day_i, sql_day_ii, format_season)

        cursor.execute(fixtures_sql)
        result = cursor.fetchall()
    
    try:
        key_list = list(result[0].keys())
        top_res = result[0]
        start_dict = {k: [top_res[k]] for k in key_list}
        for r in result[1:]:
            for k in key_list:
                start_dict[k].append(r[k])

        return(pd.DataFrame(start_dict))
    except:
        return(None)


def get_league_pos(date, competition, country, connection, season, club):
    
    sd_pandas = pd.to_datetime(date)
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    
    format_comp = '"' + competition + '"'
    format_country = '"' + country + '"'
    format_season =  '"' + season + '"'
    format_club = '"' + club + '"'
    

    league_pos_sql = 'select position from \
(select {0} as lg, {1} as ctry, (@row_number:=@row_number + 1) as position, club, played, points, scored, conceded from \
(select club, sum(gsc) as scored, sum(gcon) as conceded, sum(pts) as points, sum(gm) as played, (SELECT @row_number:=0) from \
(select home_team as club, h_ft as gsc, a_ft as gcon, home_pts as pts, game_unit as gm from \
(select country, competition, season, match_round, home_team, away_team, h_ft, a_ft, 1 as game_unit \
, case when h_ft = a_ft then 1 \
when h_ft > a_ft then 3 else 0 end as home_pts, case when h_ft = a_ft then 1 when h_ft > a_ft then 0 \
else 3 end as away_pts \
from football_matches.matches where competition = {0} \
and country = {1} and season = {2} and game_date < {3} \
AND result = 1) as base \
union all \
select away_team as club, a_ft as gsc, h_ft as gcon, away_pts as pts, game_unit as gm from \
(select country, competition, season, match_round, home_team, away_team, h_ft, a_ft, 1 as game_unit \
, case when h_ft = a_ft then 1 \
when h_ft > a_ft then 3 else 0 end as home_pts \
, case when h_ft = a_ft then 1 when h_ft > a_ft then 0 else 3 end as away_pts \
from football_matches.matches where competition = {0} \
and country = {1} and season = {2} and game_date < {3} \
AND result = 1) as base) as base_two \
group by club \
order by sum(pts) desc, (sum(gsc) - sum(gcon)) desc, sum(gsc) desc, sum(gm) asc ) as base) as outers \
where club = {4}'.format(format_comp, format_country, format_season, sql_day, format_club)
    try:
        
        with connection.cursor() as cursor:
            cursor.execute(league_pos_sql)
            result = cursor.fetchall()

        return(int(result[0]['position']))
    except:
        return('LEAGUE POS NA')


def form_data(connection, club, game_date, game_time):
    filterwarnings('ignore', category = pm.Warning)
    
    pdc = pd.to_datetime(game_date) + game_time
    pdcp = '"' + '%s-%s-%s  %s:%s:00' % (pdc.year, pdc.month, pdc.day, pdc.hour, pdc.minute) + '"' 
    sd_pandas = pd.to_datetime(game_date)
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    format_club = '"' + club + '"'
    form_sql = 'select a.idx, b.*, case when hours_ago < 169 then 1 else 0 end as within_week from \
(select game_date, (@row_number:=@row_number + 1) as idx from \
(select game_date, (SELECT @row_number:=0) from football_matches.matches \
where result = 1 and (home_team = {0} or away_team = {0})  and game_date < str_to_date({1},"%Y-%m-%d") \
order by game_date desc limit 6) as last_6) as a \
INNER JOIN ( \
select game_date, game_time, opponent, home_or_away, hours_ago, game_type, went_to, abs(score_diff) as win_loss_by \
, case when score_diff = 0 then "D" when home_or_away = "H" and score_diff > 0 then "W" \
when home_or_away = "A" and score_diff > 0 then "L" when home_or_away = "H" and score_diff < 0 then "L" \
when home_or_away = "A" and score_diff < 0 then "W" else "VOID" end as result from \
(select game_date, game_time, opponent, home_or_away, hours_ago, game_type \
, case when pens_diff IS NOT NULL then "pens" when aet_diff IS NOT NULL then "et" \
else "ft" end as went_to, case when pens_diff IS NOT NULL then pens_diff when aet_diff IS NOT NULL then aet_diff \
else ft_diff end as score_diff from \
(select game_date, game_time, h_ft, a_ft, h_ft-a_ft as ft_diff, h_aet-a_aet as aet_diff, h_pens-a_pens as pens_diff \
, hour(timediff(str_to_date({2},"%Y-%m-%d %H:%i:%s"), str_to_date(concat(game_date, " " ,  game_time),"%Y-%m-%d %H:%i:%s"))) as hours_ago \
, case when home_team = {0} then away_team else home_team end as opponent \
, case when home_team = {0} then "H" else "A" end as home_or_away \
, case when comp_model = 3 then "CUP" else "LEAGUE" end as game_type \
 from football_matches.matches \
where result = 1 and (home_team = {0} or away_team = {0}) and game_date < str_to_date({1},"%Y-%m-%d") \
) as last_game_base) as last_game_2 ) as b \
on a.game_date = b.game_date'.format(format_club, sql_day, pdcp)
    
    with connection.cursor() as cursor:
     
        cursor.execute(form_sql)
        result = cursor.fetchall()
        
    match_dict = OrderedDict()

    for item in result:
        index = item['idx']
        match_dict[index] = {'date': item['game_date'],
                             'game_type':item['game_type'],
                             'HA':item['home_or_away'], 'hours_ago': item['hours_ago'],
                            'opponent':item['opponent'], 'result': item['result'],
                            'went_to': item['went_to'], 'margin': item['win_loss_by'],
                            'within_week': item['within_week']}        
    return(match_dict)


def form_and_tiredness(game_dict, club, competition, country, season, connection):
    
    form_score, games_within_week, lastg_et = 0, 0, 0
    
    for mtch in game_dict:

        compare_date = game_dict[mtch]['date']
        opponent = game_dict[mtch]['opponent']
        target_club_pos = get_league_pos(compare_date, competition, country, connection, season, club)
        opponent_pos = get_league_pos(compare_date, competition, country, connection, season, opponent)
        game_dict[mtch]['target_club_pos'] = target_club_pos
        game_dict[mtch]['opponent_pos'] = opponent_pos
        
        if game_dict[mtch]['result'] == 'W':
            form_score += 4

        elif game_dict[mtch]['result'] == 'D':
            form_score += 1
        else:
            form_score -= 2
        
        if game_dict[mtch]['HA'] == 'A':
            form_score +=1
        
        if game_dict[mtch]['within_week'] == 1:
            games_within_week += 1
        
    if game_dict[1.0]['went_to'] in ['pens', 'et']:
        lastg_et = 1
    last_ghrs = game_dict[1.0]['hours_ago']
    return(form_score, games_within_week, lastg_et, last_ghrs)


def get_old_sackings(url, date):
    #sackings from within 4 weeks of supplied date
    #parse the input date
    date_pandas = pd.to_datetime(date)
    
    #read_url
    sackings_df = pd.read_excel(url)
    sackings_df.sort_values(by='Appdate', ascending=False, inplace=True)
    sackings_df['Division'] = sackings_df['League']
    sackings_df['Appointed'] = sackings_df['Appdate']
    sackings_df.drop(['Appdate', 'League'], axis=1, inplace=True)
    
    sackings_df['Last_mgr_appt'] = sackings_df['Appointed'].apply(lambda x: (date_pandas-x).days + 0.2)
    appts_three_weeks_df = sackings_df[(sackings_df['Last_mgr_appt'] <= 28 ) & (sackings_df['Last_mgr_appt'] > 0 )]
    return(appts_three_weeks_df)

def get_h_or_a_record(connection, club, game_date, season, ha):
    sd_pandas = pd.to_datetime(game_date)
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    format_club = '"' + club + '"'
    format_season = '"' + season + '"'
    format_ha = '"' + ha + '"'
    
    sqlq = 'select game_date, opponent, home_or_away, game_type, went_to \
, case when score_diff = 0 then "D" \
when home_or_away = "H" and score_diff > 0 then "W" \
when home_or_away = "A" and score_diff > 0 then "L" \
when home_or_away = "H" and score_diff < 0 then "L" \
when home_or_away = "A" and score_diff < 0 then "W" \
else "VOID" end as result from \
(select game_date, opponent, home_or_away, game_type \
, case when pens_diff IS NOT NULL then "pens" \
when aet_diff IS NOT NULL then "et" \
else "ft" end as went_to \
, case when pens_diff IS NOT NULL then pens_diff \
when aet_diff IS NOT NULL then aet_diff \
else ft_diff end as score_diff from \
(select game_date, h_ft, a_ft, h_ft-a_ft as ft_diff, h_aet-a_aet as aet_diff, h_pens-a_pens as pens_diff \
, case when home_team = {0} then away_team else home_team end as opponent \
, case when home_team = {0} then "H" else "A" end as home_or_away \
, case when comp_model = 3 then "CUP" else "LEAGUE" end as game_type \
from football_matches.matches \
where result = 1 and (home_team = {0} or away_team = {0})  \
and season = {1} and game_date < str_to_date({3},"%Y-%m-%d") \
) as last_game_base) as last_game_2 \
where home_or_away = {2}'.format(format_club, format_season, format_ha, sql_day)
    
    with connection.cursor() as cursor:
     
        cursor.execute(sqlq)
        result = cursor.fetchall()
        
    match_list = []

    for item in result:
        
        match_res = item['result']
        if match_res == 'W':
            num = 4
        elif match_res == 'D':
            num = 2
        else:
            num = 0
        match_list.append(num)
        form_avg = sum(match_list)/len(match_list)
    return(form_avg)

def get_head_to_head(connection, club, opponent, game_date):
    sd_pandas = pd.to_datetime(game_date)
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    format_club = '"' + club + '"'
    format_opp = '"' + opponent + '"'
    
    sqlq = 'select game_date, game_time, home_team, away_team, game_type, went_to, ft_diff \
, case when score_diff = 0 then "D" \
when score_diff > 0 and home_team = {0} then "HTW" \
when score_diff > 0 and away_team = {0} then "ATW" \
when score_diff < 0 and away_team = {0} then "HTW" \
when score_diff < 0 and home_team = {0} then "ATW" \
else "VOID" end as result from \
(select game_date, game_time,  home_team, away_team, game_type, ft_diff \
, case when pens_diff IS NOT NULL then "pens" \
when aet_diff IS NOT NULL then "et" else "ft" end as went_to \
, case when pens_diff IS NOT NULL then pens_diff \
when aet_diff IS NOT NULL then aet_diff else ft_diff end as score_diff from \
(select game_date, game_time, home_team, away_team, h_ft, a_ft, h_ft-a_ft as ft_diff \
, h_aet-a_aet as aet_diff, h_pens-a_pens as pens_diff \
, case when comp_model = 3 then "CUP" else "LEAGUE" end as game_type \
 from football_matches.matches \
where result = 1 and (home_team = {0} or away_team = {0}) and (home_team = {1} or away_team = {1}) \
and game_date < str_to_date({2},"%Y-%m-%d") \
) as last_game_base) as last_game_2 \
order by game_date DESC'.format(format_club, format_opp, sql_day)
    
    with connection.cursor() as cursor:
     
        cursor.execute(sqlq)
        result = cursor.fetchall()
        
    if len(result) > 3:
        hth = 0
        for item in result:

            match_res = item['result']
            if match_res == 'HTW':
                hth += 2
            elif match_res == 'ATW':
                hth -= 2

        return(hth)
    else:
        return(0)

