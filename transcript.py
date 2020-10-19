import pandas as pd
import re
import datetime
def transcript_7():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(how='any')
    d1=data.groupby(['date','chat_id','user_id'])['chat_id'].count().reset_index(name="count")
    d1['date']=pd.to_datetime(d1['date'])
    d1['date']=d1.date.dt.date
    d1=d1[['chat_id','user_id','date']]
    blankIndex=[''] * len(d1)
    d1.index=blankIndex
    d1=d1.sort_values(by="date")
    b=data.groupby('date')['date'].count().reset_index(name='ccc')
    b['date'] = pd.to_datetime(b['date'])
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    dates['date']=dates['date'].dt.date
    add_zero=dates[dates['date'].apply(lambda x: x not in d1['date'].values)]
    add_zero['chat_id']='No chats'
    add_zero['user_id']='No users'
    new=d1.append(add_zero)
    new=new.sort_values(by='date')
    uniq_dates=new.date.unique()
    uniq_dates=pd.DataFrame(uniq_dates)
    uniq_dates.columns=['date']
    uniq_dates=uniq_dates.tail(7)
    uniq_dates_list=[]
    for row in range(len(uniq_dates)):
        uniq_dates_list.append(uniq_dates.date.iloc[row])
    final_last_7=pd.DataFrame(columns=new.columns)
    for row in range(len(uniq_dates_list)):
        temp=new[new['date']==uniq_dates_list[row]]
        final_last_7=final_last_7.append(temp)
    final_last_7=final_last_7[['date','chat_id','user_id']]
    dff = final_last_7.to_json(orient='records')
    return dff


def transcript():
    data = pd.read_csv('Erc_updatedfinal.csv')
    data = data.dropna(how='any')
    d1 = data.groupby(['date', 'chat_id', 'user_id'])['chat_id'].count().reset_index(name="count")
    d1['date'] = pd.to_datetime(d1['date'])
    d1['date'] = d1.date.dt.date
    d1 = d1[['chat_id', 'user_id', 'date']]
    blankIndex = [''] * len(d1)
    d1.index = blankIndex
    d1 = d1.sort_values(by="date")
    b = data.groupby('date')['date'].count().reset_index(name='ccc')
    b['date'] = pd.to_datetime(b['date'])
    dates = pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex = [''] * len(dates)
    dates.index = blankIndex
    dates['date'] = dates['date'].dt.date
    add_zero = dates[dates['date'].apply(lambda x: x not in d1['date'].values)]
    add_zero['chat_id'] = 'No chats'
    add_zero['user_id'] = 'No users'
    new = d1.append(add_zero)
    new = new.sort_values(by='date')
    new = new[['date', 'chat_id', 'user_id']]
    dff = new.to_json(orient='records')
    return dff

def trans_fullchat():
    data = pd.read_csv('Erc_updatedfinal.csv')
    data = data.dropna(how='any')
    d1 = data.groupby(['date', 'chat_id', 'user_id'])['chat_id'].count().reset_index(name="count")
    d1['date'] = pd.to_datetime(d1['date'])
    d1['date'] = d1.date.dt.date
    d1 = d1[['chat_id', 'user_id', 'date']]
    blankIndex = [''] * len(d1)
    d1.index = blankIndex
    d1 = d1.sort_values(by="date")
    b = data.groupby('date')['date'].count().reset_index(name='ccc')
    b['date'] = pd.to_datetime(b['date'])
    dates = pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex = [''] * len(dates)
    dates.index = blankIndex
    dates['date'] = dates['date'].dt.date
    add_zero = dates[dates['date'].apply(lambda x: x not in d1['date'].values)]
    add_zero['chat_id'] = 'No chats'
    add_zero['user_id'] = 'No users'
    new = d1.append(add_zero)
    new = new.sort_values(by='date')
    new = new[['date', 'chat_id', 'user_id']]
    dataset = pd.read_csv('Query_data_modified.csv')
    dataset = dataset.dropna(how='any')
    dataset['question'] = dataset['question'].apply(lambda x: x.lower())
    dataset['question'] = dataset['question'].map(lambda x: re.sub(r'[?|$|.|!]', r'', x))
    dataset['question'] = dataset['question'].map(lambda x: re.sub(r'[^a-zA-Z0-9 ]', r'', x))
    chat_list = new['chat_id'].unique()
    chat_list = chat_list.tolist()
    final_with_chat = pd.DataFrame(columns=dataset.columns)
    for row in range(len(chat_list)):
        new1 = dataset[dataset['chat_id'] == chat_list[row]]
        final_with_chat = final_with_chat.append(new1)
    final_chat_filter = pd.DataFrame(columns=['chat_id', 'converser', 'text1'])
    chat_id_list = []
    converser_list = []
    text1 = []
    for row in range(len(final_with_chat)):
        chat_id_list.append(final_with_chat.chat_id.iloc[row])
        converser_list.append('user')
        text1.append(final_with_chat.question.iloc[row])
        chat_id_list.append(final_with_chat.chat_id.iloc[row])
        converser_list.append('wizard')
        text1.append(final_with_chat.answer.iloc[row])
    final_chat_filter.chat_id = chat_id_list
    final_chat_filter.converser = converser_list
    final_chat_filter.text1 = text1
    blankIndex = [''] * len(final_chat_filter)
    final_chat_filter.index = blankIndex
    dff = final_chat_filter.to_json(orient='records')
    return dff
