
# coding: utf-8

import pycurl
from datetime import date
from datetime import datetime
from datetime import timedelta
import hashlib
import StringIO
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
import sys
from suds.xsd.doctor import ImportDoctor, Import
from suds.client import Client
import dateutil.parser



# merge and deduplicate old report and new !!! run userID first
def dedup(df1):
    df = df1.reset_index(drop=True)
    # order by date - ascending is true to avoid refund
    df["date"] = pd.to_datetime(df["date"], dayfirst=1)
    df["orderValue"] = df[["orderValue"]].astype(np.float)
    df_refund = df.loc[df['orderValue'] < 0]
    df_order = df.loc[df['orderValue'] >= 0]
    df_order = df_order.sort_values(by="date", axis=0, ascending=0)
    df_order = df_order.sort_values(by="orderValue", axis=0, ascending=1).reset_index(drop=True)
    # remove dupplicates (keep first)
    df_order = df_order.groupby(["customID", "orderValue"], as_index=False).first()
    # select first refund
    df_refund = df_refund.sort_values(by="orderValue", axis=0, ascending=0)
    df_refund = df_refund.groupby(["customID", "orderValue"], as_index=False).first()
    df = df_order.append(df_refund).reset_index(drop=True)
    return df

    
# give an orderId to the ones that don't
def create_userId(df):
    # identify the clicks with users data
    custom_data = df['customID'].str.startswith("1_", na=False)
    userId = np.where(custom_data == True)[0]
    # select the users with an id and split it into separated columns for clarity
    df_id = df.loc[userId, 'customID']
    x = [i.split('_') for i in df_id]
    y = pd.DataFrame(x, columns=['A','userId','timestamp','click_source'], \
                   index=df_id.index)
    y = y.drop(['A'], 1)
    # add the data to the initial data set of users with id
    df = df.join(y)
    # select user without an id and add an artificial id (<dateTime>_<merchantID>_<orderValue>)
    no_userId = np.where(custom_data == False)[0]
    df_art_id = pd.to_datetime(df['date']).dt.strftime('%y%m%d') \
    + "_" + df['merchantID'].astype(str) + "_" + df['orderValue'].astype(int).apply(str)
    df.loc[no_userId, 'customID'] = df_art_id[no_userId]
    df = df.reset_index(drop=True)
    return df

# remove order with value == 0
def remove_z_order(df):
    order_positive = np.where(df['orderValue'] != 0)
    df = df.loc[order_positive[0],:].reset_index(drop=True)
    return df
    
# remove entries which are not transaction (ie: bonus, offline compensation, etc.)
def remove_non_trans(df):
    x = []
    for i in range(len(df['customID'])):
        if(df['customID'].loc[i].isalpha()):
            x.append(i)
    df = df.drop(df.index[x]).reset_index(drop=True)
    return df

    
##########################
## Retrieve skimlinks data
##########################

def rev_aff_skimlinks(start_date, end_date, sk_api_key, sk_private):
    m = hashlib.md5()
    i = datetime.now()
    current_timestamp = i.isoformat()
    api_key = sk_api_key
    private_key = sk_private
    m.update(current_timestamp + private_key)
    token = m.hexdigest()
    del m
    # last 30 days
    starttime = start_date.strftime('%Y-%m-%d')
    endtime = end_date.strftime('%Y-%m-%d')
    # XML
    xml = "\
    <skimlinksAccount  version='0.5'>\
      <timestamp>"+current_timestamp+"</timestamp>\
      <apikey>"+api_key+"</apikey>\
      <authtoken>"+token+"</authtoken>\
      <startDate>"+starttime+"</startDate>\
      <endDate>"+endtime+"</endDate>\
    </skimlinksAccount>"
    # api call
    c = pycurl.Curl()
    c.setopt(pycurl.URL, "https://api-reports.skimlinks.com/publisher/reportcommissions")
    c.setopt(c.HTTPHEADER, ['Accept: text/xml'])
    c.setopt(pycurl.SSL_VERIFYPEER, False)
    c.setopt(pycurl.POSTFIELDS, xml)
    c.setopt(pycurl.POST, 1)
    b = StringIO.StringIO()
    c.setopt(pycurl.WRITEFUNCTION, b.write)
    c.perform()
    c.close()
    response = b.getvalue()
    # parse the response:
    root_ = ET.fromstring(response)
    # loop whithin commissionS and loop in its child
    x = []
    y = pd.DataFrame()
    z = pd.DataFrame()
    for commission in root_[3]:
        x = []
        for child in commission:
            x.append(child.text)
        y = pd.DataFrame(data=[x])
        z = z.append(y)
    # give the dataframe's columns a name
    col = []
    for child in root_[3][1]:
        col.append(child.tag)
    z.columns = col
    # convert to correct type
    z['commissionValue'] = z[['commissionValue']].astype(np.float)/100
    z['orderValue'] = z[['orderValue']].astype(np.float)/100
    z = z.reset_index(drop=True)
    # remove cpc entries
    z = z.loc[z['commissionType'] == 'sale']
    # convert string date
    # pass if a none type error
    z = z.reset_index(drop=True)
    for i in range(len(z)):
        try:
            # for skimlinks
            z.loc[i,'clickTime'] = dateutil.parser.parse(z['date'][i]).strftime('%d/%m/%Y')
        except:
            pass
        try:
            z.loc[i,'date'] = dateutil.parser.parse(z['date'][i]).strftime('%d/%m/%Y')
        except:
            pass
    # wrap all the data
    skimlinks = z[['date', 'clickTime', 'merchantID','commissionValue', 'status','customID', 'orderValue']]
    skimlinks.insert(0, 'affiliateNetwork', 'skimlinks', allow_duplicates=True)
    # add merchant name
    sk_merchant = pd.read_csv('./data/merchant_name/sk_merchantName.csv', dtype=object)
    skimlinks = skimlinks.merge(sk_merchant, how='left', left_on='merchantID', right_on='id')
    skimlinks.drop('id', axis=1, inplace=True)
    return skimlinks


###################
## Retrieve aw data 
###################

# (the api is limited to 30days so requests need to be batched)
def batch_request_aw(start_date, end_date, aw_pw):
    if(end_date - start_date > timedelta(days=30)):
        #if > 30days
        df_date = pd.DataFrame(columns=['no','start','end'])
        for i in range(0,25):
            aw_st_date = start_date + i*timedelta(days=30)
            aw_end_date = start_date + (i+1)*timedelta(days=30)
            df_date.loc[i] = [i,aw_st_date,aw_end_date]
            # break if 
            if aw_end_date >= end_date: break
        # replace last end date with the real end date
        if (df_date.loc[len(df_date)-1, 'start'] ==  end_date):
            df_date.drop(df_date.index[len(df_date)-1], inplace=True)
        elif (df_date.loc[len(df_date)-1, 'end'] > end_date):
            df_date.loc[len(df_date)-1, 'end'] = end_date
        # loop thourgh the dat range
        df_aw_long = pd.DataFrame()
        for i in range(0, len(df_date)):
            df_aw_long = df_aw_long.append(rev_aff_aw(df_date['start'][i], df_date['end'][i], aw_pw))
            df_aw_long = df_aw_long.fillna('').reset_index(drop=True)
        return df_aw_long 
    else:
        df = rev_aff_aw(start_date, end_date, aw_pw)
        df = df.fillna('').reset_index(drop=True)
        return df

def rev_aff_aw(start_date, end_date, aw_pw):
    # api_key = '18809dd6e20dffe282ce7616ac94c8ce'
    imp = Import('http://schemas.xmlsoap.org/soap/encoding/')
    d = ImportDoctor(imp)
    url = 'http://api.affiliatewindow.com/v6/AffiliateService?wsdl'
    client = Client(url, doctor=d)
    auth = client.factory.create('UserAuthentication')
    auth.iId = '215843'
    auth.sPassword = aw_pw
    auth.sType = 'affiliate'
    client.set_options(soapheaders=auth)
    # 
    json = client.service.getTransactionList(dStartDate = start_date.isoformat(), \
                                             dEndDate = end_date.isoformat(), sDateType = 'transaction')
    # json mapping
    w = pd.DataFrame()
    for Transaction in json.getTransactionListReturn[0]:
        x = []
        x = [
        Transaction.sStatus[0],\
        Transaction.sClickref[0],\
        Transaction.iMerchantId[0],\
        Transaction.dClickDate[0],\
        Transaction.dTransactionDate[0],\
        Transaction.mCommissionAmount[0].dAmount[0],\
        Transaction.mSaleAmount[0].dAmount[0]]
        v = pd.DataFrame(data=[x])
        w = w.append(v)
    col = [
        'status',\
        'customID',\
        'merchantID',\
        'clickTime',\
        'date',\
        'commissionValue',\
        'orderValue']
    w.columns = col
    w.insert(0, 'affiliateNetwork', 'affiliateWindow', allow_duplicates=True)
    # convert string date
    w = w.reset_index(drop=True)
    for i in range(len(w['clickTime'])):
        try:
            w.loc[i,'clickTime'] = dateutil.parser.parse(w['clickTime'][i]).strftime('%d/%m/%Y')
        except:
            pass
        try:
            w.loc[i,'date'] = dateutil.parser.parse(w['date'][i]).strftime('%d/%m/%Y')
        except:
            pass     
    aw = w
    # add merchant name
    aw_merchant = pd.read_csv('./data/merchant_name/aw_merchantName.csv', dtype=object)
    aw = aw.merge(aw_merchant, how='left', left_on='merchantID', right_on='id')
    aw.drop('id', axis=1, inplace=True)
    return aw
    
    
##########################
## Retrieve linkshare data
##########################

def rev_aff_linkshare(start_date, end_date, lk_api_token):
    # uk network
    url = 'https://ran-reporting.rakutenmarketing.com/en/reports/api_report_aymeric/filters?' \
    'start_date='+start_date.strftime('%Y-%m-%d')+'&end_date='+end_date.strftime('%Y-%m-%d')+ \
    '&include_summary=N&network=3&tz=GMT&date_type=transaction&token='+lk_api_token
    lkshare_uk = pd.read_csv(url, thousands=',')
    lkshare_uk.insert(0, 'affiliateNetwork', 'linkshare_uk', allow_duplicates=True)
    
    # us network
    url = 'https://ran-reporting.rakutenmarketing.com/en/reports/api_report_aymeric_us/filters?' \
    'start_date='+start_date.strftime('%Y-%m-%d')+'&end_date='+end_date.strftime('%Y-%m-%d')+ \
    '&include_summary=N&network=3&tz=GMT&date_type=transaction&token='+lk_api_token
    lkshare_us = pd.read_csv(url, thousands=',')
    # conversion in GBP
    lkshare_us['Total Commission'] = lkshare_us[['Total Commission']].astype(np.float)*0.79
    lkshare_us['Sales'] = lkshare_us[['Sales']].astype(np.float)*0.79
    lkshare_us.insert(0, 'affiliateNetwork', 'linkshare_us', allow_duplicates=True)
    lkshare = lkshare_uk.append(lkshare_us).reset_index(drop=True)
    
    # 
    col = ['affiliateNetwork',\
        'merchantID',\
        'customID',\
        'clickTime',\
        'date',\
        'commissionValue',\
        'orderValue']
    # 
    lkshare.columns = col 
    lkshare['date'] = pd.to_datetime(lkshare['date']).dt.strftime('%d/%m/%Y')
    lkshare['clickTime'] = pd.to_datetime(lkshare['clickTime']).dt.strftime('%d/%m/%Y')
    # add merchant name
    lkshare_merchant = pd.read_csv('./data/merchant_name/lk_merchantName.csv')
    lkshare = lkshare.merge(lkshare_merchant, how='left',
                            left_on='merchantID', right_on='id')
    lkshare.drop('id', axis=1, inplace=True)
    return lkshare
    
    
def df_append_rev(start_date, end_date, filename):
    key = get_keys(filename)
    try:
        print('starting skimlinks')
        skimlinks = rev_aff_skimlinks(start_date, end_date, key['sk_api_key'], key['sk_private'])
    except Exception as e: 
        print(e)
        sys.exit(1)
    try:
        print('starting aw')
        aw = batch_request_aw(start_date, end_date, key['aw_pw'])
    except Exception as e: 
        print(e)
        sys.exit(1)
    try:
        print('starting linkshare')
        lkshare = rev_aff_linkshare(start_date, end_date, key['lk_api_token'])
    except Exception as e: 
        print(e)
        sys.exit(1)
    print('merging and cleaning data')
    aw_not_declined = np.where(aw['status'].str.lower()!='declined')
    aw = aw.loc[aw_not_declined[0],:].reset_index(drop=True)
    aw = remove_z_order(aw)
    aw = remove_non_trans(aw)
    df = pd.DataFrame(columns = [
        'merchantID',\
        'customID',\
        'clickTime',\
        'date',\
        'commissionValue',\
        'orderValue'])
    if('skimlinks' in locals()): df = df.append(skimlinks)
    if('aw' in locals()): df = df.append(aw)
    if('lkshare' in locals()): df = df.append(lkshare)
    df = dedup(df)
    df = remove_z_order(df)
    df = create_userId(df)
    return df

# store the api keys in a dictionary
def get_keys(filename):
    _keys = {}
    with open(filename, 'r') as myfile:
        for line in myfile:
            line = line.rstrip('\n')
            line = line.rstrip('\r')
            name, var = line.partition("=")[::2]
            _keys[name.strip()] = str(var)
    return _keys

# download the data and apply the cleaning scripts
def download_to_drive(start_date, end_date, filename):
    df = df_append_rev(start_date, end_date, filename)
    df = dedup(df)
    df.sort_values('date', axis=0, ascending=False, inplace=True)
    df = df.fillna('').reset_index(drop=True)
    df.to_csv('./data/gmv_affiliates.csv', encoding='utf-8', index=False)
    print('done')

if __name__ == '__main__':
    ####
    filename = './private/api_keys'
    start_date = datetime.combine(date.today(),
                                  datetime.min.time()) - timedelta(days=100)
    end_date = datetime.combine(date.today(), datetime.min.time())
    download_to_drive(start_date, end_date, filename)
    

