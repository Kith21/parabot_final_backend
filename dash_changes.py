import pandas as pd
import datetime
import numpy as np


def dash_change():
    df = pd.read_csv('Erc_updatedfinal.csv')
    df = df[df['status'] == 'Resolved']
    time = df.groupby(['date', 'status'])['question'].count().reset_index(name='incoming_count')
    time1 = df.groupby(['date', 'status'])['question'].count().reset_index(name='outgoing_count')
    time['outgoing_count'] = time1['outgoing_count']
    user = [13, 5, 17, 2, 1, 8, 12, 4, 6, 7, 17, 6, 8, 6, 15, 4, 6, 17, 2, 11, 8, 8, 13, 6, 6, 5, 7, 7, 2, 13, 10, 2,
            10, 13, 3, 15, 9, 1]
    time['user_count'] = user
    time
    blankIndex = [''] * len(time)
    time.index = blankIndex
    d = time
    blankIndex = [''] * len(d)
    d.index = blankIndex
    d['count'] = d['incoming_count'] + d['outgoing_count']
    d['date'] = pd.to_datetime(d['date'])
    d = d.sort_values(by="date")
    b = df.groupby('date')['date'].count().reset_index(name='ccc')
    b['date'] = pd.to_datetime(b['date'])
    dates = pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex = [''] * len(dates)
    dates.index = blankIndex
    dates['date'] = dates['date'].dt.date
    d['date'] = d.date.dt.date
    d = d[['date', 'status', 'incoming_count', 'outgoing_count', 'count', 'user_count']]
    add_zero = dates[dates['date'].apply(lambda x: x not in d['date'].values)]
    add_zero['status'] = "Nan"
    add_zero['incoming_count'] = 0
    add_zero['outgoing_count'] = 0
    add_zero['count'] = 0
    add_zero['user_count'] = 0
    real = d.append(add_zero)
    real = real.sort_values(by='date')
    df1 = pd.read_csv('Erc_updatedfinal.csv')
    df1 = df1[df1['status'] == 'Pending']
    t = df1.groupby(['date', 'status'])['question'].count().reset_index(name='incoming_count')
    t1 = df1.groupby(['date', 'status'])['question'].count().reset_index(name='outgoing_count')
    t['outgoing_count'] = t1['outgoing_count']
    user1 = [11, 8, 16, 10, 8, 7, 2, 10, 3]
    t['user_count'] = user1
    blankIndex = [''] * len(t)
    t.index = blankIndex
    dd = t
    dd['count'] = dd['incoming_count'] + dd['outgoing_count']
    dd['date'] = pd.to_datetime(dd['date'])
    dd = dd.sort_values(by="date")
    bb = df1.groupby('date')['date'].count().reset_index(name='cccc')
    bb['date'] = pd.to_datetime(bb['date'])
    dates1 = pd.DataFrame(columns=['date'])
    dates1['date'] = pd.to_datetime(bb['date'], format='%Y-%m-%d')
    dates1 = dates1.sort_values(by=['date'], ascending=[True])
    dates1.set_index('date', inplace=True)
    dates1 = dates1.resample('D').ffill().reset_index()
    blankIndex = [''] * len(dates1)
    dates1.index = blankIndex
    dates1['date'] = dates1['date'].dt.date
    dd['date'] = dd.date.dt.date
    dd = dd[['date', 'status', 'incoming_count', 'outgoing_count', 'count', 'user_count']]
    add_zero1 = dates1[dates1['date'].apply(lambda x: x not in dd['date'].values)]
    add_zero1['status'] = "Nan"
    add_zero1['incoming_count'] = 0
    add_zero1['outgoing_count'] = 0
    add_zero1['count'] = 0
    add_zero1['user_count'] = 0
    real1 = dd.append(add_zero1)
    real1 = real1.sort_values(by='date')
    df2 = pd.read_csv('Erc_updatedfinal.csv')
    df2 = df2[df2['status'] == 'Assigned']
    tt = df2.groupby(['date', 'status'])['question'].count().reset_index(name='incoming_count')
    t2 = df2.groupby(['date', 'status'])['question'].count().reset_index(name='outgoing_count')
    tt['outgoing_count'] = t2['outgoing_count']
    user2 = [11, 13, 2, 4, 11, 10, 11, 18, 2, 5, 12, 8, 7, 2, 1, 3]
    tt['user_count'] = user2
    blankIndex = [''] * len(tt)
    tt.index = blankIndex
    ddd = tt
    ddd['count'] = ddd['incoming_count'] + ddd['outgoing_count']
    ddd['date'] = pd.to_datetime(ddd['date'])
    ddd = ddd.sort_values(by="date")
    bbb = df2.groupby('date')['date'].count().reset_index(name='ccccc')
    bbb['date'] = pd.to_datetime(bbb['date'])
    dates2 = pd.DataFrame(columns=['date'])
    dates2['date'] = pd.to_datetime(bbb['date'], format='%Y-%m-%d')
    dates2 = dates2.sort_values(by=['date'], ascending=[True])
    dates2.set_index('date', inplace=True)
    dates2 = dates2.resample('D').ffill().reset_index()
    blankIndex = [''] * len(dates2)
    dates2.index = blankIndex
    dates2['date'] = dates2['date'].dt.date
    ddd['date'] = ddd.date.dt.date
    ddd = ddd[['date', 'status', 'incoming_count', 'outgoing_count', 'count', 'user_count']]
    add_zero2 = dates2[dates2['date'].apply(lambda x: x not in ddd['date'].values)]
    add_zero2['status'] = "Nan"
    add_zero2['incoming_count'] = 0
    add_zero2['outgoing_count'] = 0
    add_zero2['count'] = 0
    add_zero2['user_count'] = 0
    real2 = ddd.append(add_zero2)
    real2 = real2.sort_values(by='date')
    reall = real.append(real1)
    realll = reall.append(real2)
    dfff= realll.to_json(orient='records')

    return dfff