import re
import pandas as pd
import pymysql as pm
import datetime as dt
from collections import OrderedDict
from warnings import filterwarnings
import fixtures_frame_support as ffs

#for single week, comment out 2 lines below and switch dd loop to in [dt_list]
#and un comment the 2 input variables
#fixdates = 'C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\fix_dates.xlsx'
#dates_frame = pd.read_excel(fixdates, index_col='Dated')

#define connection
grid_conn = pm.connect(host='localhost',
                             user='root',
                             password='Terry1943',
                             db='football_matches',
                             charset='utf8mb4',
                             cursorclass=pm.cursors.DictCursor)

dt_list = str(input("Type in Saturday as YYYY-MM-DD"))
season = str(input("Type in Season as YYYY/YY"))
for dd in [dt_list]:
    #season = dates_frame.loc[dd,'Season']
    #main_upcoming_fixtures_code
    sttime = dt.datetime.now()
    #get-fixtures for a date, and the 2 days after, into a dataframe. Make the match ID the index col
    start_fix = ffs.get_fixtures(dd,  grid_conn, season)
    if start_fix is not None:
        
        start_fix.set_index('idmatches', drop=True, inplace=True)

        #actual result
        start_fix['res'] = start_fix['h_ft'] - start_fix['a_ft']
        start_fix['resu'] = start_fix['res'].apply(lambda x: 'hw' if x > 0 else ('d' if x == 0 else 'aw'))
        start_fix.drop('res', axis=1, inplace = True)
        # Zero-populated columns for each variable column you want to add in
        start_fix['home_form'] = [0]*start_fix.shape[0]
        start_fix['away_form'] = [0]*start_fix.shape[0]
        start_fix['home_lg_pos'] = [0]*start_fix.shape[0]
        start_fix['away_lg_pos'] = [0]*start_fix.shape[0]
        start_fix['home_gwin_wk'] = [0]*start_fix.shape[0]
        start_fix['away_gwin_wk'] = [0]*start_fix.shape[0]
        start_fix['home_new_mgr'] = [0]*start_fix.shape[0]
        start_fix['away_new_mgr'] = [0]*start_fix.shape[0]
        start_fix['home_lg_et'] = [0]*start_fix.shape[0]
        start_fix['away_lg_et'] = [0]*start_fix.shape[0]
        start_fix['home_lg_hrs'] = [0]*start_fix.shape[0]
        start_fix['away_lg_hrs'] = [0]*start_fix.shape[0]
        start_fix['homet_hform'] = [0]*start_fix.shape[0]
        start_fix['awayt_aform'] = [0]*start_fix.shape[0]
        start_fix['head_to_head'] = [0]*start_fix.shape[0]
        #read in the club mapping spreadsheet
        club_mapper = pd.read_excel('C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\Club_Mapper.xlsx',
                                    index_col = 'team')

        #get the sackings for 4 weeks before the main game date (argument in get_fixutres)
        #twoseas is the sackings dataframe read in via the Excel database
        twoseas_url = 'C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\Excel_Resources\\Sackings database.xlsx'
        twoseas_df = ffs.get_old_sackings(twoseas_url, dd)
        twoseas_df.drop_duplicates(subset='Club', inplace=True)
        twoseas_df.set_index('Club', inplace=True, drop=True)


        for fx in start_fix.index:
            homet = start_fix.loc[fx, 'home_team']
            awayt = start_fix.loc[fx, 'away_team']
            cntry = start_fix.loc[fx, 'country']
            cmpt = start_fix.loc[fx, 'competition']
            gdate = start_fix.loc[fx, 'game_date']
            gtime = start_fix.loc[fx, 'game_time']

            home_form_data = ffs.form_data(grid_conn, homet, gdate, gtime)
            home_f, home_games_winwk, home_lg_et, home_lghrs =  ffs.form_and_tiredness(home_form_data, homet, cmpt, cntry, season, grid_conn)

            away_form_data = ffs.form_data(grid_conn, awayt, gdate, gtime)
            away_f, away_games_winwk, away_lg_et, away_lghrs =  ffs.form_and_tiredness(away_form_data, awayt, cmpt, cntry, season, grid_conn)

            home_home_fm = ffs.get_h_or_a_record(grid_conn, homet, gdate, season, 'H')
            away_away_form = ffs.get_h_or_a_record(grid_conn, awayt, gdate, season, 'A')

            start_fix.loc[fx, 'home_form'] = home_f
            start_fix.loc[fx, 'away_form'] = away_f
            start_fix.loc[fx, 'home_lg_pos'] = ffs.get_league_pos(dd, cmpt, cntry, grid_conn, season, homet)
            start_fix.loc[fx, 'away_lg_pos'] = ffs.get_league_pos(dd, cmpt, cntry, grid_conn, season, awayt)
            start_fix.loc[fx, 'home_gwin_wk'] = home_games_winwk
            start_fix.loc[fx, 'away_gwin_wk'] = away_games_winwk
            start_fix.loc[fx, 'home_lg_et'] = home_lg_et
            start_fix.loc[fx, 'away_lg_et'] = away_lg_et
            start_fix.loc[fx, 'home_lg_hrs'] = home_lghrs
            start_fix.loc[fx, 'away_lg_hrs'] = away_lghrs
            start_fix.loc[fx, 'homet_hform'] = home_home_fm
            start_fix.loc[fx, 'awayt_aform'] = away_away_form  
            start_fix.loc[fx, 'head_to_head'] = ffs.get_head_to_head(grid_conn,homet,awayt,dd)

            home_sack_names = [club_mapper.loc[homet, 'sacking_match'], club_mapper.loc[homet, 'sacking_match_ii']]
            away_sack_names = [club_mapper.loc[awayt, 'sacking_match'], club_mapper.loc[awayt, 'sacking_match_ii']]    

            for val in home_sack_names:
                idx = str(val)
                for cell in twoseas_df.index:
                    if idx == cell:
                        start_fix.loc[fx,'home_new_mgr'] = 1

            for val in away_sack_names:
                idx = str(val)
                for cell in twoseas_df.index:
                    if idx == cell:
                        start_fix.loc[fx,'away_new_mgr'] = 1

        sackings_nm = []
        sacking_names = club_mapper.sacking_match.values.tolist()
        sacking_names.extend(club_mapper.sacking_match_ii.values.tolist())
        for cell in twoseas_df.index:
            if cell not in sacking_names:
                sackings_nm.append(cell)
        endtime = dt.datetime.now()
        delta = (endtime-sttime).seconds
        print('SACKINGS NOT MATCHED:', '\n'.join(sackings_nm), '{0} Done in {1} seconds'.format(dd, delta))
        csv_str = 'C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\v2\\start_fix{0}.csv'.format(dd)
        start_fix.to_csv(csv_str)
    else:
        print('No fixtures for {0}'.format(dd))
    #pre-testing. This will do a dataframe for the league games on a specific date, and 2 days after.