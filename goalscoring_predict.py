import pandas as pd
import pymysql as pm
import datetime

#connect to db
grid_conn = pm.connect(host='localhost',
                             user='root',
                             password='Terry1943',
                             db='football_matches',
                             charset='utf8mb4',
                             cursorclass=pm.cursors.DictCursor)

#function for goals scored in last 6 games

def six_game_goals(conn, team, season, date):
    
    sd_pandas = pd.to_datetime(date)
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    format_club = '"' + team + '"'
    format_season = '"' + season + '"'
    
    sqlq = 'select game_date \
, case when home_team = {0} then h_ft else a_ft end as goals_scored \
, case when home_team = {0} then a_ft else h_ft end as goals_conceded \
 from football_matches.matches \
where result = 1 and (home_team = {0} or away_team = {0})  \
and season = {1} and result = 1 and comp_model <> 3 and game_date < str_to_date({2},"%Y-%m-%d") \
order by game_date desc \
limit 6'.format(format_club,format_season,sql_day)
    
    
    with conn.cursor() as cursor:

        cursor.execute(sqlq)
        result = cursor.fetchall()
        
    scored_list = [x['goals_scored'] for x in result]
    concede_list = [x['goals_conceded'] for x in result]
    
        
    return(round(sum(scored_list)/6,3), round(sum(concede_list)/6,3))

#function for goals scored in season at home or away

def ha_goals_season(conn, team, season, date, home=True):
    
    sd_pandas = pd.to_datetime(date)
    sql_day = '"%s-%s-%s"' % (sd_pandas.year, sd_pandas.month, sd_pandas.day)
    format_club = '"' + team + '"'
    format_season = '"' + season + '"'
    
    sqlq = 'select home_or_away, sum(goals_scored)/count(*) as scored_avg, sum(goals_conceded)/count(*) as concd_avg from \
(select game_date \
, case when home_team = {0} then h_ft else a_ft end as goals_scored \
, case when home_team = {0} then a_ft else h_ft end as goals_conceded \
, case when home_team = {0} then "H" else "A" end as home_or_away \
 from football_matches.matches \
where result = 1 and (home_team = {0} or away_team = {0}) \
and season = {1} and result = 1 and comp_model <> 3 and game_date < str_to_date({2},"%Y-%m-%d")) as q \
group by home_or_away \
order by home_or_away desc'.format(format_club,format_season,sql_day)
    
    
    with conn.cursor() as cursor:

        cursor.execute(sqlq)
        result = cursor.fetchall()
 
    if home:
        return(float(result[0]['scored_avg']),float(result[0]['concd_avg']))
    else:
        return(float(result[1]['scored_avg']),float(result[1]['concd_avg']))

# user inputs name of spreadsheet which is read as dataframe

ssheet = input("Name of csv file (DONT include extension): " )

surl = "C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\v2\\{0}.csv".format(ssheet)
sample_df = pd.read_csv(surl)

#set up the variable goalscoring feature columns

sample_df['home_scored_avg_last6g'] = [0]*sample_df.shape[0]
sample_df['home_conceded_avg_last6g']  = [0]*sample_df.shape[0]
sample_df['away_scored_avg_last6g']  = [0]*sample_df.shape[0]
sample_df['away_conceded_avg_last6g']  = [0]*sample_df.shape[0]
sample_df['home_h_scored_avg_seas']  = [0]*sample_df.shape[0]
sample_df['home_h_conceded_avg_seas']  = [0]*sample_df.shape[0]
sample_df['away_a_scored_avg_seas']  = [0]*sample_df.shape[0]
sample_df['away_a_conceded_avg_seas']  = [0]*sample_df.shape[0]

print('Generating variables from database...')

for c in sample_df.index:
    
    dt = sample_df.loc[c,'game_date']
    hm = sample_df.loc[c,'home_team']
    awa = sample_df.loc[c,'away_team']
    seas = '2018/19'
    
    sample_df.loc[c,'home_scored_avg_last6g'] = six_game_goals(grid_conn, hm, seas, dt)[0]
    sample_df.loc[c,'home_conceded_avg_last6g'] = six_game_goals(grid_conn, hm, seas, dt)[1]
    sample_df.loc[c,'away_scored_avg_last6g'] = six_game_goals(grid_conn, awa, seas, dt)[0]
    sample_df.loc[c,'away_conceded_avg_last6g'] = six_game_goals(grid_conn, awa, seas, dt)[1]
    sample_df.loc[c,'home_h_scored_avg_seas'] = ha_goals_season(grid_conn, hm, seas, dt)[0]
    sample_df.loc[c,'home_h_conceded_avg_seas'] = ha_goals_season(grid_conn, hm, seas, dt)[1]
    sample_df.loc[c,'away_a_scored_avg_seas'] = ha_goals_season(grid_conn, awa, seas, dt,False)[0]
    sample_df.loc[c,'away_a_conceded_avg_seas'] = ha_goals_season(grid_conn, awa, seas, dt,False)[1]

print('Variables done.')

sample_df['teams_scoring_last6'] = sample_df.home_scored_avg_last6g + sample_df.away_scored_avg_last6g
sample_df['teams_conceding_last6'] = sample_df.home_conceded_avg_last6g + sample_df.away_conceded_avg_last6g
sample_df['home_away_scoring'] = sample_df.home_h_scored_avg_seas + sample_df.away_a_scored_avg_seas
sample_df['home_away_conceding'] = sample_df.home_h_conceded_avg_seas + sample_df.away_a_conceded_avg_seas
sample_df['lg_pos_diff'] = abs(sample_df.home_lg_pos - sample_df.away_lg_pos)

#read in goal thresholds Excel

goal_thresholds = pd.read_excel('C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\GoalScoringThresholds.xlsx')
sample_df['gsc_pred'] = ['-']*sample_df.shape[0]
sample_df['gsc_level'] = [0]*sample_df.shape[0]

# function to predict games

def draw_predict_thresh (fix_df, thresh_df, uprate=1):
    counter = 0
    varibles_list = ['home_away_scoring', 'home_away_conceding', 'lg_pos_diff',
           'teams_scoring_last6', 'teams_conceding_last6',
           'home_h_scored_avg_seas', 'away_a_conceded_avg_seas',
           'home_scored_avg_last6g', 'away_conceded_avg_last6g',
           'away_scored_avg_last6g', 'home_conceded_avg_last6g',
           'away_a_scored_avg_seas', 'home_h_conceded_avg_seas']


    for c in fix_df.index:

        row = fix_df.loc[c,varibles_list].tolist()

        for x in thresh_df.index:
            overund = thresh_df.loc[x,'Over_Under']
            cert = thresh_df.loc[x,'Certainty']
            checks = 0
            checkrow = thresh_df.loc[x,varibles_list].tolist()

            for i, v in enumerate(checkrow):
                if pd.isnull(v):
                    checks +=1
                    counter +=1
                elif row[i] >= v*uprate and overund == 'Over':
                    checks+=1
                elif row[i] <= v/uprate and overund == 'Under':
                    checks +=1
            if checks == 13:
                fix_df.loc[c, 'gsc_pred'] = overund
                fix_df.loc[c,'gsc_level'] = cert*uprate
                break
    return(fix_df)

# call prediction function 3 times, each time tighter

print('Predicting...')

sdf2 = draw_predict_thresh(sample_df,goal_thresholds)
sdf3 = draw_predict_thresh(sdf2,goal_thresholds,1.3)
sdf4 = draw_predict_thresh(sdf3,goal_thresholds,1.7)

out_csvname = ssheet + '_WITH_GSC_PREDICT'

outurl = 'C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\predicts\\{0}.csv'.format(out_csvname)
print('Done. Written to csv in predicts folder.')
sdf4.to_csv(outurl)