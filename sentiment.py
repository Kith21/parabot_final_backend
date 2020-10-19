
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

from sklearn.feature_extraction.text import CountVectorizer
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.models import Sequential
from keras.layers import Dense, Embedding, LSTM, SpatialDropout1D
from sklearn.model_selection import train_test_split
from keras.utils.np_utils import to_categorical
from keras.optimizers import Adam
import re
pd.set_option('max_colwidth', -1)
from nltk.corpus import stopwords
#stopwords_list = stopwords.words('english')
stopwords_list=['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't",
 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]
def senti():
    data1 = pd.read_csv('data_history1.csv')
    data2=data1.groupby(['date','chat_id','sentiment'])['text'].apply(lambda x: ' '.join(x)).reset_index()
    data2.dtypes
    data2['sentiment'].value_counts()
    data2=data2.loc[data2['sentiment']!=2.0]
    #sentiment1 = {1.0: 'positive',0.0: 'negative'}
    #data2.sentiment = [sentiment1[item] for item in data2.sentiment]
    data2 = data2.replace('\n',' ', regex=True)
    data2['text'] = data2['text'].apply(lambda x: x.lower())
    data2['text'] = data2['text'].apply((lambda x: re.sub('[^a-zA-z0-9\s]','',x)))
    data2['text']=data2['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stopwords_list)]))
    pd.set_option('display.width', 1000)
    data2['sentiment'].value_counts()
    max_words = 50
    batch_size = 64
    epochs = 7
    num_classes=2
    max_features = 20000
    from keras.utils import to_categorical
    target=data2.sentiment.values
    y=to_categorical(target)
    X_train , X_test , Y_train , Y_test = train_test_split(data2,y,test_size = 0.15)
    tokenizer = Tokenizer(num_words=max_features,split=' ')
    tokenizer.fit_on_texts(X_train['text'])
    X_train1 = tokenizer.texts_to_sequences(X_train['text'])
    X_train1 =pad_sequences(X_train1,maxlen=max_words)
    X_test1 = tokenizer.texts_to_sequences(X_test['text'])
    X_test1 =pad_sequences(X_test1,maxlen=max_words)
    model3_LSTM=Sequential()
    model3_LSTM.add(Embedding(max_features,100,mask_zero=True))
    model3_LSTM.add(LSTM(64,dropout=0.4,return_sequences=True))
    model3_LSTM.add(LSTM(32,dropout=0.5,return_sequences=False))
    model3_LSTM.add(Dense(num_classes,activation='sigmoid'))
    model3_LSTM.compile(loss='binary_crossentropy',optimizer=Adam(lr = 0.001),metrics=['accuracy'])
    model3_LSTM.summary()
    history3=model3_LSTM.fit(X_train1, Y_train,epochs=epochs, batch_size=batch_size, verbose=1)
    y_pred3=model3_LSTM.predict_classes(X_test1, verbose=1)
    y_pred4=model3_LSTM.predict_classes(X_train1, verbose=1)
    #sub.Sentiment=y_pred3
    X_test['Sentiment']=y_pred3
    X_train['Sentiment']=y_pred4
    score,acc = model3_LSTM.evaluate(X_test1, Y_test, verbose = 2, batch_size = batch_size)
    print("score: %.2f" % (score))
    print("acc: %.2f" % (acc))
    X_testfin=X_test[['date','Sentiment','chat_id']]
    X_trainfin=X_train[['date','Sentiment','chat_id']]
    merge_fin=X_testfin.append(X_trainfin).sort_values(by=['date'], ascending=[True])
    X_testfin=X_test[['date','Sentiment','chat_id']]
    X_trainfin=X_train[['date','Sentiment','chat_id']]
    merge=X_testfin.append(X_trainfin)
    d2=merge_fin[merge_fin['Sentiment']==1].groupby(['date','Sentiment'])['Sentiment'].count().reset_index(name="positive")
    d3=merge_fin[merge_fin['Sentiment']==0].groupby(['date','Sentiment'])['Sentiment'].count().reset_index(name="negative")
    d2['negative']=d3['negative']
    d2=d2.sort_values(by="date")
    b=data2.groupby(['date'])['date'].count().reset_index(name="cc")
    dates=pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%d-%m-%Y')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    dates['date']=dates['date'].dt.date
    d2.date=pd.to_datetime(d2.date)
    dates.date=pd.to_datetime(dates.date)
    d2.date=d2.date.dt.date
    dates.date=dates.date.dt.date
    add_zero=dates[dates['date'].apply(lambda x: x not in d2['date'].values)]
    add_zero['positive']=0
    add_zero['negative']=0
    add_zero['Sentiment']='no sentiment'
    new=d2.append(add_zero)
    new=new.sort_values(by='date')
    blankIndex=[''] * len(new)
    new.index=blankIndex
    dfff= new.to_json(orient='records')

    return dfff


def senti1():
    data1 = pd.read_csv('data_history1.csv')
    data2=data1.groupby(['date','chat_id','sentiment'])['text'].apply(lambda x: ' '.join(x)).reset_index()
    data2.dtypes
    data2['sentiment'].value_counts()
    data2=data2.loc[data2['sentiment']!=2.0]
    #sentiment1 = {1.0: 'positive',0.0: 'negative'}
    #data2.sentiment = [sentiment1[item] for item in data2.sentiment]
    data2 = data2.replace('\n',' ', regex=True)
    data2['text'] = data2['text'].apply(lambda x: x.lower())
    data2['text'] = data2['text'].apply((lambda x: re.sub('[^a-zA-z0-9\s]','',x)))
    data2['text']=data2['text'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stopwords_list)]))
    pd.set_option('display.width', 1000)
    data2['sentiment'].value_counts()
    max_words = 50
    batch_size = 64
    epochs = 7
    num_classes=2
    max_features = 20000
    from keras.utils import to_categorical
    target=data2.sentiment.values
    y=to_categorical(target)
    X_train , X_test , Y_train , Y_test = train_test_split(data2,y,test_size = 0.15)
    tokenizer = Tokenizer(num_words=max_features,split=' ')
    tokenizer.fit_on_texts(X_train['text'])
    X_train1 = tokenizer.texts_to_sequences(X_train['text'])
    X_train1 =pad_sequences(X_train1,maxlen=max_words)
    X_test1 = tokenizer.texts_to_sequences(X_test['text'])
    X_test1 =pad_sequences(X_test1,maxlen=max_words)
    model3_LSTM=Sequential()
    model3_LSTM.add(Embedding(max_features,100,mask_zero=True))
    model3_LSTM.add(LSTM(64,dropout=0.4,return_sequences=True))
    model3_LSTM.add(LSTM(32,dropout=0.5,return_sequences=False))
    model3_LSTM.add(Dense(num_classes,activation='sigmoid'))
    model3_LSTM.compile(loss='binary_crossentropy',optimizer=Adam(lr = 0.001),metrics=['accuracy'])
    model3_LSTM.summary()
    history3=model3_LSTM.fit(X_train1, Y_train,epochs=epochs, batch_size=batch_size, verbose=1)
    y_pred3=model3_LSTM.predict_classes(X_test1, verbose=1)
    y_pred4=model3_LSTM.predict_classes(X_train1, verbose=1)
    #sub.Sentiment=y_pred3
    X_test['Sentiment']=y_pred3
    X_train['Sentiment']=y_pred4
    score,acc = model3_LSTM.evaluate(X_test1, Y_test, verbose = 2, batch_size = batch_size)
    print("score: %.2f" % (score))
    print("acc: %.2f" % (acc))
    X_testfin=X_test[['date','Sentiment','chat_id']]
    X_trainfin=X_train[['date','Sentiment','chat_id']]
    merge_fin=X_testfin.append(X_trainfin).sort_values(by=['date'], ascending=[True])
    X_testfin=X_test[['date','Sentiment','chat_id']]
    X_trainfin=X_train[['date','Sentiment','chat_id']]
    merge=X_testfin.append(X_trainfin)


    dff1 = merge_fin.to_json(orient='records')


    return dff1


def senti2():
    data1 = pd.read_csv('data_history1.csv')
    data5 = data1[['chat_id', 'text', 'converser']]
    dff2 = data5.to_json(orient='records')

    return dff2

senti();