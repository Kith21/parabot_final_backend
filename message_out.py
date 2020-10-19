import pandas as pd
import datetime
import re
def message_out():
    data=pd.read_csv('Query_data_modified.csv')
    data=data.dropna(subset=['answer'])
    data['answer']=data['answer'].apply(lambda x: x.lower())
    data.answer=data.answer.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    data.answer=data.answer.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    texts=data.groupby('answer')['answer'].count().reset_index(name="ccc")
    texts=texts[texts['ccc']>4]
    text=[]
    f=texts.answer.unique()
    for row in range(len(f)):
                if(f[row].isdigit() or len(f[row])<=2):
                    continue
                else:
                    text.append(f[row])
    texts_date=data.groupby(['answer','date'])['answer'].count().reset_index(name="ccc")
    texts_date.date=pd.to_datetime(texts_date.date)
    final=pd.DataFrame(columns=texts_date.columns)
    for row in range(len(text)):
        temp=texts_date[texts_date['answer']==text[row]]
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
    final_date=pd.DataFrame(columns=["answer","date","ccc"])
    final['date']=final.date.dt.date
    for row in range(len(text)):
            new=final[final['answer']==text[row]]
            add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
            add_zero['ccc']=0
            add_zero['answer']=text[row]
            new=new.append(add_zero)
            new=new.sort_values(by='date')
            final_date=final_date.append(new)
    final_date.columns=["text",'date','count']  # 1 3 2
    final_date.index=[x for  x in range(0,len(final_date)) ]
    list_len=[]
    for row in range(len(final_date)):
        list_len.append(len(final_date.text[row]))
    final_date['len']=list_len
    final_date=final_date[final_date['len']<100]
    final_date=final_date.drop(['len'],axis=1)
    blankIndex=[''] * len(final_date)
    final_date.index=blankIndex
    dff = final_date.to_json(orient='records')
    return dff
def message_out_chat():
    data=pd.read_csv('Query_data_modified.csv')
    data=data.dropna(subset=['answer'])
    data['answer']=data['answer'].apply(lambda x: x.lower())
    data.answer=data.answer.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    data.answer=data.answer.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    texts=data.groupby('answer')['answer'].count().reset_index(name="ccc")
    texts=texts[texts['ccc']>4]
    text=[]
    f=texts.answer.unique()
    for row in range(len(f)):
                if(f[row].isdigit() or len(f[row])<=2):
                    continue
                else:
                    text.append(f[row])
    data_chat=data.groupby(['answer','chat_id','user_id','date'])['answer'].count().reset_index(name="count")
    data_chat.date=pd.to_datetime(data_chat.date)
    final=pd.DataFrame(columns=data_chat.columns)
    for row in range(len(text)):
        temp=data_chat[data_chat['answer']==text[row]]
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
                new=final[final['answer']==text[row]]
                add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
                add_zero['count']=0
                add_zero['answer']=text[row]
                add_zero['chat_id']='No chats'
                add_zero['user_id']='No users'
                new=new.append(add_zero)
                new=new.sort_values(by='date')
                final_chat=final_chat.append(new)
    final_chat.columns=["text1",'chat_id','user_id','date','count']  # 1 2 5 4 3
    final_chat.index=[x for  x in range(0,len(final_chat)) ]
    list_len=[]
    for row in range(len(final_chat)):
            list_len.append(len(final_chat.text1[row]))
    final_chat['len']=list_len
    final_chat=final_chat[final_chat['len']<100]
    final_chat=final_chat.drop(['len'],axis=1)
    blankIndex=[''] * len(final_chat)
    final_chat.index=blankIndex
    dff = final_chat.to_json(orient='records')
    return dff
def chat_display_out():
    data=pd.read_csv('Query_data_modified.csv')
    data=data.dropna(subset=['answer'])
    data['answer1']=data['answer'].apply(lambda x: x.lower())
    data.answer1=data.answer1.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    data.answer1=data.answer1.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    texts=data.groupby('answer1')['answer1'].count().reset_index(name="ccc")
    texts=texts[texts['ccc']>4]
    text=[]
    f=texts.answer1.unique()
    for row in range(len(f)):
                    if(f[row].isdigit() or len(f[row])<=2):
                        continue
                    else:
                        text.append(f[row])
    data_chat=data.groupby(['answer1','chat_id','user_id','date'])['answer1'].count().reset_index(name="count")
    data_chat.date=pd.to_datetime(data_chat.date)
    final=pd.DataFrame(columns=data_chat.columns)
    for row in range(len(text)):
            temp=data_chat[data_chat['answer1']==text[row]]
            final=final.append(temp)
    blankIndex=[''] * len(final)
    final.index=blankIndex
    dataset=pd.read_csv('Query_data_modified.csv')
    dataset=dataset.dropna(subset=['answer'])
    dataset['answer1']=dataset['answer'].apply(lambda x: x.lower())
    dataset.answer1=dataset.answer1.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    dataset.answer1=dataset.answer1.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    chat_list=final['chat_id'].unique()
    chat_list=chat_list.tolist()
    final_with_chat=pd.DataFrame(columns=dataset.columns)
    dataset=pd.read_csv('Query_data_modified.csv')
    dataset=dataset.dropna(subset=['answer'])
    dataset['answer1']=dataset['answer'].apply(lambda x: x.lower())
    dataset.answer1=dataset.answer1.map(lambda x:re.sub(r'[?|$|.|!]',r'',x))
    dataset.answer1=dataset.answer1.map(lambda x:re.sub(r'[^a-zA-Z0-9 ]',r'',x))
    chat_list=final['chat_id'].unique()
    chat_list=chat_list.tolist()
    final_with_chat=pd.DataFrame(columns=dataset.columns)
    for row in range(len(chat_list)):
            new=dataset[dataset['chat_id']==chat_list[row]]
            final_with_chat=final_with_chat.append(new)
    final_chat_filter=pd.DataFrame(columns=['chat_id','converser','text','text1'])
    chat_id_list=[]
    converser_list=[]
    text=[]
    text1=[]
    for row in range(len(final_with_chat)):
        chat_id_list.append(final_with_chat.chat_id.iloc[row])
        converser_list.append('user')
        text.append(final_with_chat.question.iloc[row])
        text1.append(final_with_chat.question.iloc[row])
        chat_id_list.append(final_with_chat.chat_id.iloc[row])
        converser_list.append('wizard')
        text.append(final_with_chat.answer.iloc[row])
        text1.append(final_with_chat.answer1.iloc[row])
    final_chat_filter.chat_id=chat_id_list
    final_chat_filter.converser=converser_list
    final_chat_filter.text=text
    final_chat_filter.text1=text1
    blankIndex=[''] * len(final_chat_filter)
    final_chat_filter.index=blankIndex
    dff = final_chat_filter.to_json(orient='records')
    return dff