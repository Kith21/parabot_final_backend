import pandas as pd
import datetime
import re
import numpy as np
def new_user_count():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(subset=['question'])
    data['date']=pd.to_datetime(data['date'])
    time=data.groupby(['date'])['user_id'].apply(lambda x: list(np.unique(x))).reset_index(name='user_id')
    date=[]
    users=[]
    prev_users=[]
    for row in range(len(time)):
            #for row1 in range(len(time.iloc[row][1])):
                #print(time.iloc[row][0])
                date.append(time.iloc[row][0])
                temp=time.iloc[row][1]
                main_list = [item for item in temp if item not in prev_users]
                for row1 in range(len(time.iloc[row][1])):
                    prev_users.append(time.iloc[row][1][row1])
                users.append(len(main_list))
    b=data.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    add_zero=dates[dates['date'].apply(lambda x: x not in date)]
    add_zero['count']=0
    final=pd.DataFrame(columns=['date','count'])
    final.date=date
    final['count']=users
    final=final.append(add_zero)
    final.date=pd.to_datetime(final.date)
    blankIndex=[''] * len(final)
    final.index=blankIndex
    final=final.sort_values(by='date')
    dff=final.to_json(orient='records')
    return dff
def ret_user_count():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(subset=['question'])
    data['date']=pd.to_datetime(data['date'])
    time=data.groupby(['date'])['user_id'].apply(lambda x: list(np.unique(x))).reset_index(name='user_id')
    date=[]
    users=[]
    prev_users=[]
    for row in range(len(time)):
            #for row1 in range(len(time.iloc[row][1])):
                #print(time.iloc[row][0])
                date.append(time.iloc[row][0])
                temp=time.iloc[row][1]
                main_list = [item for item in temp if item in prev_users]
                for row1 in range(len(time.iloc[row][1])):
                    prev_users.append(time.iloc[row][1][row1])
                users.append(len(main_list))
    b=data.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    add_zero=dates[dates['date'].apply(lambda x: x not in date)]
    add_zero['count']=0
    final=pd.DataFrame(columns=['date','count'])
    final.date=date
    final['count']=users
    final=final.append(add_zero)
    final.date=pd.to_datetime(final.date)
    blankIndex=[''] * len(final)
    final.index=blankIndex
    final=final.sort_values(by='date')
    dff=final.to_json(orient='records')
    return dff


def total_user_count():
    data = pd.read_csv('Erc_updatedfinal.csv')
    data['date'] = pd.to_datetime(data['date'])
    time = data.groupby(['date'])['user_id'].apply(lambda x: list(np.unique(x))).reset_index(name='user_id')
    count = []
    for row in range(len(time)):
        count.append(len(time.user_id[row]))
    time['count'] = count
    time = time.drop(['user_id'], axis=1)
    blankIndex = [''] * len(time)
    time.index = blankIndex
    b = data.groupby('date')['date'].count().reset_index(name='ccc')
    dates = pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex = [''] * len(dates)
    dates.index = blankIndex
    time.date = pd.to_datetime(time.date)
    joinn1 = pd.merge(time, dates, on='date', how='right')
    joinn1 = pd.DataFrame(joinn1)
    joinn1["count"].fillna(0, inplace=True)

    temp1 = []
    for row in range(len(joinn1)):
        temp1.append(int(joinn1["count"][row]))
    joinn1 = joinn1.drop(['count'], axis=1)
    joinn1["count"] = temp1
    joinn1 = joinn1.sort_values(by='date')
    blankIndex = [''] * len(joinn1)
    joinn1.index = blankIndex
    dff = joinn1.to_json(orient='records')
    return dff
def msgcount_total():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(how='any')
    d1=data.groupby(['date'])['question'].count().reset_index(name="in_count")
    blankIndex=[''] * len(d1)
    d1.index=blankIndex
    d2=data.groupby(['date'])['answer'].count().reset_index(name="out_count")
    blankIndex=[''] * len(d2)
    d2.index=blankIndex
    d=pd.merge(d1,d2)
    blankIndex=[''] * len(d)
    d.index=blankIndex
    d['count'] = d.sum(axis=1)
    d['date'] = pd.to_datetime(d['date'])
    d=d.sort_values(by="date")
    b=data.groupby('date')['date'].count().reset_index(name='ccc')
    b['date'] = pd.to_datetime(b['date'])
    dates=pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    dates['date']=dates['date'].dt.date
    d['date'] = d.date.dt.date
    d=d[['date','count']]
    add_zero=dates[dates['date'].apply(lambda x: x not in d['date'].values)]
    add_zero['count']=0
    real=d.append(add_zero)
    real=real.sort_values(by='date')
    dff=real.to_json(orient='records')
    return dff