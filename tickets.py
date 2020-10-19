import pandas as pd
import numpy as np
def tickets():
    df=pd.read_csv('Erc_updatedfinal.csv')  ##reading csv file
    df1=df.groupby('status')['chat_id'].unique().apply(lambda group_series: group_series.tolist()).reset_index(name="chat_id")
    ##grouing status based on chat id
    ticket_count=pd.DataFrame(columns=['status','count']) ## creating new dataframe
    ticket_count['status']=df1.status   ##assigning status columns with values from df1 table columns status
    ticket_count['count']=[len(df1.chat_id[0]),len(df1.chat_id[1]),len(df1.chat_id[2])]  ## assigning count columns with len of count column from df1
    blankIndex=[''] * len(ticket_count)  ##clearing index
    ticket_count.index=blankIndex
    dff=ticket_count.to_json(orient='records')
    return dff
def resolved_count():
    df=pd.read_csv('ERC_Datasetfinal.csv')
    df1=pd.read_csv('ERC_Datasetfinal.csv')
    df=df[df['status']=='Resolved']
    df['date']=pd.to_datetime(df['date'])
    df1['date']=pd.to_datetime(df1['date'])
    time=df.groupby(['date'])['status'].apply(lambda x: list(np.unique(x))).reset_index(name='status')
    count=[]
    for row in range(len(time)):
        count.append(len(time.status[row]))
    time['count']=count
    time=time.drop(['status'],axis=1)
    blankIndex=[''] * len(time)
    time.index=blankIndex
    b=df1.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    time.date=pd.to_datetime(time.date)
    joinn1=pd.merge(time,dates,on='date',how='right')
    joinn1=pd.DataFrame(joinn1)
    joinn1["count"].fillna(0, inplace = True)
    temp1=[]
    for row in range(len(joinn1)):
            temp1.append(int(joinn1["count"][row]))
    joinn1=joinn1.drop(['count'],axis=1)
    joinn1["count"]=temp1
    joinn1=joinn1.sort_values(by='date')
    blankIndex=[''] * len(joinn1)
    joinn1.index=blankIndex
    dff=joinn1.to_json(orient='records')
    return dff
def pending_count():
    df=pd.read_csv('ERC_Datasetfinal.csv')
    df1=pd.read_csv('ERC_Datasetfinal.csv')
    df=df[df['status']=='Pending']
    df['date']=pd.to_datetime(df['date'])
    df1['date']=pd.to_datetime(df1['date'])
    time=df.groupby(['date'])['status'].apply(lambda x: list(np.unique(x))).reset_index(name='status')
    count=[]
    for row in range(len(time)):
        count.append(len(time.status[row]))
    time['count']=count
    time=time.drop(['status'],axis=1)
    blankIndex=[''] * len(time)
    time.index=blankIndex
    b=df1.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    time.date=pd.to_datetime(time.date)
    joinn1=pd.merge(time,dates,on='date',how='right')
    joinn1=pd.DataFrame(joinn1)
    joinn1["count"].fillna(0, inplace = True)
    temp1=[]
    for row in range(len(joinn1)):
            temp1.append(int(joinn1["count"][row]))
    joinn1=joinn1.drop(['count'],axis=1)
    joinn1["count"]=temp1
    joinn1=joinn1.sort_values(by='date')
    blankIndex=[''] * len(joinn1)
    joinn1.index=blankIndex
    dff=joinn1.to_json(orient='records')
    return dff
def assigned_count():
    df=pd.read_csv('ERC_Datasetfinal.csv')
    df1=pd.read_csv('ERC_Datasetfinal.csv')
    df=df[df['status']=='Assigned']
    df['date']=pd.to_datetime(df['date'])
    df1['date']=pd.to_datetime(df1['date'])
    time=df.groupby(['date'])['status'].apply(lambda x: list(np.unique(x))).reset_index(name='status')
    count=[]
    for row in range(len(time)):
        count.append(len(time.status[row]))
    time['count']=count
    time=time.drop(['status'],axis=1)
    blankIndex=[''] * len(time)
    time.index=blankIndex
    b=df1.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    time.date=pd.to_datetime(time.date)
    joinn1=pd.merge(time,dates,on='date',how='right')
    joinn1=pd.DataFrame(joinn1)
    joinn1["count"].fillna(0, inplace = True)
    temp1=[]
    for row in range(len(joinn1)):
            temp1.append(int(joinn1["count"][row]))
    joinn1=joinn1.drop(['count'],axis=1)
    joinn1["count"]=temp1
    joinn1=joinn1.sort_values(by='date')
    blankIndex=[''] * len(joinn1)
    joinn1.index=blankIndex
    dff=joinn1.to_json(orient='records')
    return dff