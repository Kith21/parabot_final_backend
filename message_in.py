import pandas as pd
import datetime
import re
def message_in():
    data=pd.read_csv('Query_data_modified.csv')
    data=data.dropna(subset=['question'])
    data['question']=data['question'].apply(lambda x: x.lower())
    data.question=data.question.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    data.question=data.question.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    texts=data.groupby('question')['question'].count().reset_index(name="ccc")
    texts=texts[texts['ccc']>4]
    text=[]
    f=texts.question.unique()
    for row in range(len(f)):
                if(f[row].isdigit() or len(f[row])<=2):
                    continue
                else:
                    text.append(f[row])
    print(data['date'])

    texts_date=data.groupby(['question','date'])['question'].count().reset_index(name="ccc")
    texts_date.date=pd.to_datetime(texts_date.date)
    final=pd.DataFrame(columns=texts_date.columns)
    for row in range(len(text)):
        temp=texts_date[texts_date['question']==text[row]]
        final=final.append(temp)
    time=data['date'].groupby([data.user_id,data.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    dates=pd.DataFrame(columns=['date'])
    dates['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    dates['date']=dates['date'].dt.date
    blankIndex=[''] * len(final)
    final.index=blankIndex
    final_date=pd.DataFrame(columns=["question","date","ccc"])
    final['date']=final.date.dt.date
    for row in range(len(text)):
            new=final[final['question']==text[row]]
            add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
            add_zero['ccc']=0
            add_zero['question']=text[row]
            new=new.append(add_zero)
            new=new.sort_values(by='date')
            final_date=final_date.append(new)
    final_date.columns=["text",'date','count']  # 3 2 1
    final_date.index=[x for  x in range(0,len(final_date)) ]
    list_len=[]
    for row in range(len(final_date)):
        list_len.append(len(final_date.text[row]))
    final_date['len']=list_len
    final_date=final_date[final_date['len']<100]
    final_date=final_date.drop(['len'],axis=1)
    blankIndex=[''] * len(final_date)
    final_date.index=blankIndex
    dff=final_date.to_json(orient='records')
    return dff
def message_in_chat():
    data=pd.read_csv('Query_data_modified.csv')
    data=data.dropna(subset=['question'])
    data['question']=data['question'].apply(lambda x: x.lower())
    data.question=data.question.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    data.question=data.question.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    texts=data.groupby('question')['question'].count().reset_index(name="ccc")
    texts=texts[texts['ccc']>4]
    text=[]
    f=texts.question.unique()
    for row in range(len(f)):
                if(f[row].isdigit() or len(f[row])<=2):
                    continue
                else:
                    text.append(f[row])
    data_chat=data.groupby(['question','chat_id','user_id','date'])['question'].count().reset_index(name="count")
    data_chat.date=pd.to_datetime(data_chat.date)
    final=pd.DataFrame(columns=data_chat.columns)
    for row in range(len(text)):
        temp=data_chat[data_chat['question']==text[row]]
        final=final.append(temp)
    time=data['date'].groupby([data.user_id,data.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    dates=pd.DataFrame(columns=['date'])
    dates['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    dates['date']=dates['date'].dt.date
    blankIndex=[''] * len(final)
    final.index=blankIndex
    final_chat=pd.DataFrame(columns=final.columns)
    final['date']=final.date.dt.date
    for row in range(len(text)):
                new=final[final['question']==text[row]]
                add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
                add_zero['count']=0
                add_zero['question']=text[row]
                add_zero['chat_id']='No chats'
                add_zero['user_id']='No users'
                new=new.append(add_zero)
                new=new.sort_values(by='date')
                final_chat=final_chat.append(new)
    final_chat.columns=['text1','chat_id','user_id','date','count']  #  2 5 4 1 3
    list_len=[]
    final_chat.index=[x for  x in range(0,len(final_chat)) ]
    for row in range(len(final_chat)):
            list_len.append(len(final_chat.text1[row]))
    final_chat['len']=list_len
    final_chat=final_chat[final_chat['len']<100]
    final_chat=final_chat.drop(['len'],axis=1)
    blankIndex=[''] * len(final_chat)
    final_chat.index=blankIndex
    dff=final_chat.to_json(orient='records')
    return dff
def chat_display_in():
    data = pd.read_csv('Query_data_modified.csv')
    data = data.dropna(subset=['question'])
    data['question1'] = data['question'].apply(lambda x: x.lower())
    data.question1 = data.question1.map(lambda x: re.sub(r'[?|$|.|!]', r'', x))
    data.question1 = data.question1.map(lambda x: re.sub(r'[^a-zA-Z0-9 ]', r'', x))
    texts = data.groupby('question1')['question1'].count().reset_index(name="ccc")
    texts = texts[texts['ccc'] > 4]
    text = []
    f = texts.question1.unique()
    for row in range(len(f)):
        if (f[row].isdigit() or len(f[row]) <= 2):
            continue
        else:
            text.append(f[row])
    data_chat = data.groupby(['question1', 'chat_id', 'user_id', 'date'])['question1'].count().reset_index(name="count")
    data_chat.date = pd.to_datetime(data_chat.date)
    final = pd.DataFrame(columns=data_chat.columns)
    for row in range(len(text)):
        temp = data_chat[data_chat['question1'] == text[row]]
        final = final.append(temp)
    blankIndex = [''] * len(final)
    final.index = blankIndex
    dataset = pd.read_csv('Query_data_modified.csv')
    dataset = dataset.dropna(subset=['question'])
    dataset['question1'] = dataset['question'].apply(lambda x: x.lower())
    dataset.question1 = dataset.question1.map(lambda x: re.sub(r'[?|$|.|!]', r'', x))
    dataset.question1 = dataset.question1.map(lambda x: re.sub(r'[^a-zA-Z0-9 ]', r'', x))
    chat_list = final['chat_id'].unique()
    chat_list = chat_list.tolist()
    final_with_chat = pd.DataFrame(columns=dataset.columns)

    for row in range(len(chat_list)):
        new = dataset[dataset['chat_id'] == chat_list[row]]
        final_with_chat = final_with_chat.append(new)
    final_chat_filter = pd.DataFrame(columns=['chat_id', 'converser', 'text', 'text1'])
    chat_id_list = []
    converser_list = []
    text = []
    text1 = []
    for row in range(len(final_with_chat)):
        chat_id_list.append(final_with_chat.chat_id.iloc[row])
        converser_list.append('user')
        text.append(final_with_chat.question.iloc[row])
        text1.append(final_with_chat.question1.iloc[row])
        chat_id_list.append(final_with_chat.chat_id.iloc[row])
        converser_list.append('wizard')
        text.append(final_with_chat.answer.iloc[row])
        text1.append(final_with_chat.answer.iloc[row])
    final_chat_filter.chat_id = chat_id_list
    final_chat_filter.converser = converser_list
    final_chat_filter.text = text
    final_chat_filter.text1 = text1
    blankIndex = [''] * len(final_chat_filter)
    final_chat_filter.index = blankIndex
    dff = final_chat_filter.to_json(orient='records')
    return dff













