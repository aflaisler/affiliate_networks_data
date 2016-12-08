# -*- coding: utf-8 -*-
import pandas as pd
# import json
from df2gspread import df2gspread as d2g


def upload_to_drive(df):
    # spreadsheet = '1gAUh4rXrD5vlVodmmC5wARmIKWssJOxMWbTGQzs-FlE'
    # staging: #
    spreadsheet = '1ZDXIEn6CQMEZmbPsQ0GsDpu4oS65RUjZtp2dFwExdDA'
    wks_name = 'GMV_affiliate_3m'
    print('uploading')
    d2g.upload(df, gfile=spreadsheet, wks_name=wks_name, clean=True, df_size=True)

    
if __name__ == '__main__':
    df = pd.read_csv('./data/gmv_affiliates.csv')
    df = df.fillna('').reset_index(drop=True)
    upload_to_drive(df)