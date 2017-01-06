# -*- coding: utf-8 -*-
import pandas as pd
from df2gspread import df2gspread as d2g
from datetime import date
from datetime import datetime


def upload_to_drive(df):
    # prod: 
    # spreadsheet = '1gAUh4rXrD5vlVodmmC5wARmIKWssJOxMWbTGQzs-FlE'
    # staging: 
    spreadsheet = '1ZDXIEn6CQMEZmbPsQ0GsDpu4oS65RUjZtp2dFwExdDA'
    extract_date = datetime.now().strftime('%d%b%y_%H'+'h'+'%M')
    wks_name = 'GMV_affiliate_@' + str(extract_date)
    print('uploading')
    result = None
    n=0
    while (result is None) and (n!=3):
        try:
            # upload
            result = d2g.upload(df, gfile=spreadsheet, wks_name=wks_name, df_size=True)
            n=3
        except:
            n+=1
            print(n)
            pass
    d2g.del_inBetween_wks(spreadsheet)
    
if __name__ == '__main__':
    df = pd.read_csv('./data/gmv_affiliates.csv')
    df = df.fillna('').reset_index(drop=True)
    upload_to_drive(df)
    