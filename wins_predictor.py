import pandas as pd

docstring = 'You need to have run the goalscorer predict on your fixtures frame \n and \
    generated the csv to go into the predicts subfolder. If so, type in the csv name \n \
        without the .csv extension. Otherwise type CANCEL to quit this code.'
incsv = input(':')

if incsv == 'CANCEL':
    pass
else:
    full_url = "C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\predicts\\{0}.csv".format(incsv)
    
    home_model_df = pd.read_csv(full_url,index_col='idmatches')
    away_model_df = pd.read_csv(full_url,index_col='idmatches')

    home_model_df['target_team_form'] = home_model_df.home_form
    home_model_df['opponent_form'] = home_model_df.away_form
    home_model_df['target_team_league_pos'] = home_model_df.home_lg_pos
    home_model_df['opponent_league_pos'] = home_model_df.away_lg_pos
    home_model_df['target_team_games_inwk'] = home_model_df.home_gwin_wk
    home_model_df['opponent_games_inwk'] = home_model_df.away_gwin_wk
    home_model_df['target_team_new_mgr'] = home_model_df.home_new_mgr
    home_model_df['opponent_new_mgr'] = home_model_df.away_new_mgr
    home_model_df['target_team_lastg_extratime_pen'] = home_model_df.home_lg_et
    home_model_df['opponent_lastg_extratime_pen'] = home_model_df.away_lg_et
    home_model_df['target_team_hrs_since_lastg'] = home_model_df.home_lg_hrs
    home_model_df['opponent_hrs_since_lastg'] = home_model_df.away_lg_hrs
    home_model_df['target_team_ha_record'] = home_model_df.homet_hform
    home_model_df['opponent_ha_record'] = home_model_df.awayt_aform
    home_model_df['h2h'] = home_model_df.head_to_head



    #assign the variables and new index in the away copy - exactly as per the home df, but in reverse

    away_model_df['target_team_form'] = away_model_df.away_form
    away_model_df['opponent_form'] = away_model_df.home_form
    away_model_df['target_team_league_pos'] = away_model_df.away_lg_pos
    away_model_df['opponent_league_pos'] = away_model_df.home_lg_pos
    away_model_df['target_team_games_inwk'] = away_model_df.away_gwin_wk
    away_model_df['opponent_games_inwk'] = away_model_df.home_gwin_wk
    away_model_df['target_team_new_mgr'] = away_model_df.away_new_mgr
    away_model_df['opponent_new_mgr'] = away_model_df.home_new_mgr
    away_model_df['target_team_lastg_extratime_pen'] = away_model_df.away_lg_et
    away_model_df['opponent_lastg_extratime_pen'] = away_model_df.home_lg_et
    away_model_df['target_team_hrs_since_lastg'] = away_model_df.away_lg_hrs
    away_model_df['opponent_hrs_since_lastg'] = away_model_df.home_lg_hrs
    away_model_df['target_team_ha_record'] = away_model_df.awayt_aform
    away_model_df['opponent_ha_record'] = away_model_df.homet_hform
    away_model_df['h2h'] = away_model_df.head_to_head.apply(lambda x: 0 - x)



    #drop previous variables
    home_model_df.drop(['h_ft', 'a_ft'],  axis=1, inplace=True)
    home_model_df.drop(['home_form', 'away_form', 'home_lg_pos', 'away_lg_pos', 'home_gwin_wk'],  axis=1, inplace=True)
    home_model_df.drop(['away_gwin_wk', 'home_new_mgr', 'away_new_mgr', 'home_lg_et', 'head_to_head'],  axis=1, inplace=True)
    home_model_df.drop(['away_lg_et', 'home_lg_hrs', 'away_lg_hrs', 'homet_hform','awayt_aform' ],  axis=1, inplace=True)

    away_model_df.drop(['h_ft', 'a_ft'],  axis=1, inplace=True)
    away_model_df.drop(['home_form', 'away_form', 'home_lg_pos', 'away_lg_pos', 'home_gwin_wk'],  axis=1, inplace=True)
    away_model_df.drop(['away_gwin_wk', 'home_new_mgr', 'away_new_mgr', 'home_lg_et', 'head_to_head'],  axis=1, inplace=True)
    away_model_df.drop(['away_lg_et', 'home_lg_hrs', 'away_lg_hrs', 'homet_hform','awayt_aform'],  axis=1, inplace=True)


    home_model_df['pos_diff'] = home_model_df.opponent_league_pos - home_model_df.target_team_league_pos
    home_model_df['compare_form'] = home_model_df.target_team_form - home_model_df.opponent_form
    home_model_df['rest_adv'] = home_model_df.target_team_hrs_since_lastg - home_model_df.opponent_hrs_since_lastg
    home_model_df['ha_record_compare'] = home_model_df.target_team_ha_record - home_model_df.opponent_ha_record

    away_model_df['pos_diff'] = away_model_df.opponent_league_pos - away_model_df.target_team_league_pos
    away_model_df['compare_form'] = away_model_df.target_team_form - away_model_df.opponent_form
    away_model_df['rest_adv'] = away_model_df.target_team_hrs_since_lastg - away_model_df.opponent_hrs_since_lastg
    away_model_df['ha_record_compare'] = away_model_df.target_team_ha_record - away_model_df.opponent_ha_record


    home_thresholds = pd.read_excel('C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\Home_Pockets.xlsx')
    home_model_df['home_win_pred'] = [0]*home_model_df.shape[0]

    # define prediction function
    def predict_home_wins(fix_df, thresh_df, uprate=1):
        for c in fix_df.index:

            fix_h2h = fix_df.loc[c, 'h2h']
            fix_posdiff = fix_df.loc[c, 'pos_diff']
            fix_formcompare = fix_df.loc[c, 'compare_form']
            fix_ha_reccompare = fix_df.loc[c, 'ha_record_compare']

            for x in range(thresh_df.shape[0]):
                checks = 0
                thresh_h2h = thresh_df.iloc[x, 0]
                thresh_posdiff = thresh_df.iloc[x, 1]
                thresh_formcompare = thresh_df.iloc[x, 2]
                thresh_ha_reccompare = thresh_df.iloc[x, 3]

                fix_l = [fix_h2h, fix_posdiff, fix_formcompare, fix_ha_reccompare]
                thresh_l = [thresh_h2h, thresh_posdiff, thresh_formcompare, thresh_ha_reccompare]
                thresh_l = [x if pd.isnull(x) else x*uprate for x in thresh_l]

                for i, v in enumerate(thresh_l):
                    if pd.isnull(v):
                        checks +=1
                    elif fix_l[i] >= v:
                        checks+=1
                if checks == 4:
                    fix_df.loc[c, 'pred'] = uprate

                    break
        return(fix_df)

    home_model_df2 = predict_home_wins(home_model_df, home_thresholds)
    home_model_df3 = predict_home_wins(home_model_df2, home_thresholds, 1.25)
    home_model_df3 = predict_home_wins(home_model_df3, home_thresholds, 1.3)
    home_model_df4 = predict_home_wins(home_model_df3, home_thresholds, 1.7)

    out_df = incsv + '_hw_predicted'
    outurl = "C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\predicts\\{0}.csv".format(out_df)
    home_model_df4.to_csv(outurl)
    print('Predicted home. Predicting away...')

    away_thresholds = pd.read_excel('C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\Away_Pockets.xlsx')

    away_model_df['pred'] = [0]*away_model_df.shape[0]

    for c in away_model_df.index:

        afix_h2h = away_model_df.loc[c, 'h2h']
        afix_posdiff = away_model_df.loc[c, 'pos_diff']
        afix_formcompare = away_model_df.loc[c, 'compare_form']
        afix_ha_reccompare = away_model_df.loc[c, 'ha_record_compare']
        
        for x in range(away_thresholds.shape[0]):
            checks = 0
            athresh_h2h = away_thresholds.iloc[x, 0]
            athresh_posdiff = away_thresholds.iloc[x, 1]
            athresh_formcompare = away_thresholds.iloc[x, 2]
            athresh_ha_reccompare = away_thresholds.iloc[x, 3]
            
            afix_l = [afix_h2h, afix_posdiff, afix_formcompare, afix_ha_reccompare]
            athresh_l = [athresh_h2h, athresh_posdiff, athresh_formcompare, athresh_ha_reccompare]
            athresh_li = [x if pd.isnull(x) else x*1.7 for x in athresh_l]

            for i, v in enumerate(athresh_li):
                if pd.isnull(v):
                    checks +=1
                elif afix_l[i] >= v:
                    checks+=1
            if checks == 4:
                away_model_df.loc[c, 'pred'] = 1
                break
    away_out_df = incsv + '_AWAY_predicted'
    awayouturl = "C:\\Users\\nedst\\OneDrive\\Documents\\VBA prac\\predicts\\{0}.csv".format(away_out_df)
    away_model_df.to_csv(awayouturl)
    print('Done.')
