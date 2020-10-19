import pandas as pd
import re
import nltk

from wordcloud import STOPWORDS
def word_list():
    dataset = pd.read_csv('Query_data_modified.csv')
    def gen_freq(text):
        word_list = [] #Will store the list of words
        for tw_words in text.split(): #Loop over all the tweets and extract words into word_list
            word_list.extend(tw_words)
        word_freq = pd.Series(word_list).value_counts() #Create word frequencies using word_list
        return word_freq

    def clean_text(text):
        text=str(text)
        text = re.sub(r'[?!.;:,#@-]', '', text)
        text = text.lower()
        return text
    quest_text = dataset.question.apply(lambda x: clean_text(x))
    quest_text=quest_text.str.replace("[^a-zA-Z0-9]"," ")
    ans_text = dataset.answer.apply(lambda x: clean_text(x))
    ans_text=ans_text.str.replace("[^a-zA-Z0-9]"," ")
    quest_text1=quest_text[quest_text!='nan']
    ans_text1=ans_text[ans_text!='nan']
    text=quest_text1.append(ans_text1)
    blankIndex=[''] * len(text)
    text.index=blankIndex
    word_freq= gen_freq(text.str)
    new_table=pd.DataFrame(columns=['word','count'])
    new_table['word']=word_freq.index
    new_table['count']=word_freq.values
    filtered_words=[]
    filtered_freq=[]
    row=0
    for w in new_table.word:
        if w not in STOPWORDS:
            filtered_words.append(w)
            filtered_freq.append(new_table.iloc[row][1])
        row=row+1
    word_count=pd.DataFrame(columns=['word','count'])
    word_count['word']=filtered_words
    word_count['count']=filtered_freq
    blankIndex=[''] * len(word_count)
    word_count.index=blankIndex
    json_ob = word_count.to_json(orient='records')
    return json_ob
def word_freq_date():
    dataset = pd.read_csv('Query_data_modified.csv')
    dataset=dataset.dropna(how='any')
    dataset.date=pd.to_datetime(dataset.date)
    dataset=dataset.dropna(subset = ['question'])
    def gen_freq(text):
            word_list = [] #Will store the list of words
            for tw_words in text.split(): #Loop over all the tweets and extract words into word_list
                word_list.extend(tw_words)
            word_freq = pd.Series(word_list).value_counts() #Create word frequencies using word_list
            return word_freq
    def clean_text(text):
            text=str(text)
            text = re.sub(r'[?!.;:,#@-]', '', text)
            text = text.lower()
            return text
    question_date=pd.DataFrame(columns=['date','text'])
    question_date.date=dataset.date
    question_date.text=dataset.question
    answer_date=pd.DataFrame(columns=['date','text'])
    answer_date.date=dataset.date
    answer_date.text=dataset.answer
    text_date=pd.DataFrame(columns=['date','text'])
    text_date=question_date.append(answer_date)
    text_date.text = text_date.text.apply(lambda x: clean_text(x))
    text_date.text=text_date.text.str.replace("[^a-zA-Z]"," ")
    blankIndex=[''] * len(text_date)
    text_date.index=blankIndex
    users_datewise=dataset.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise.date, format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    blankIndex=[''] * len(users_datewise)
    users_datewise.index=blankIndex
    unique_dates=pd.DataFrame(columns=['date'])
    unique_dates['date']=users_datewise.date
    unique_dates.index=[i for i in range(0,len(unique_dates))]
    import numpy as np
    final=pd.DataFrame(columns=['date','word','count'])
    for row in range(len(unique_dates)):
        new=text_date[text_date['date']==unique_dates['date'][row]]
        word_freq= gen_freq(new.text.str)
        new_table=pd.DataFrame(columns=['date','word','count'])
        new_table['word']=word_freq.index
        new_table['count']=word_freq.values
        new_table['date']=new_table['date'].replace(np.nan,unique_dates['date'][row])
        final=final.append(new_table)
    filtered_date=[]
    filtered_words=[]
    filtered_freq=[]
    row=0
    for w in final.word:
        if w not in STOPWORDS:
                filtered_words.append(w)
                filtered_freq.append(final.iloc[row][2])
                filtered_date.append(final.iloc[row][0])
        row=row+1
    word_count=pd.DataFrame(columns=['date','word','count'])
    word_count['date']=filtered_date
    word_count['word']=filtered_words
    word_count['count']=filtered_freq
    word_count=word_count.sort_values(by='word')
    unique_words=word_count.word.unique()
    blankIndex=[''] * len(word_count)
    word_count.index=blankIndex
    b=dataset.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date']=pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates=dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date',inplace=True)
    dates=dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    dates['date']=dates['date'].dt.date
    word_count.date=word_count.date.dt.date
    final_date=pd.DataFrame(columns=["date","word","count"])
    for row in range(len(unique_words)):
                new=word_count[word_count['word']==unique_words[row]]
                add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
                add_zero['count']=0
                add_zero['word']=unique_words[row]
                new=new.append(add_zero)
                new=new.sort_values(by='date')
                final_date=final_date.append(new)
    json_ob = final.to_json(orient='records')
    return json_ob
