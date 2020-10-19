import pandas as pd
import datetime
def geoo():
    df=pd.read_csv('Erc_updatedfinal.csv')
    df.date=pd.to_datetime(df.date)
    df1=df.groupby(['date','client_StateOrProvince','client_City','user_id'])['user_id'].unique().reset_index(name="count")
    df1=df1.sort_values(by='client_StateOrProvince')
    unique_state=[]
    unique_state=df1.client_StateOrProvince.unique()
    unique_city=df1.client_City.unique()
    b=df.groupby('date')['date'].count().reset_index(name='ccc')
    dates=pd.DataFrame(columns=['date'])
    dates['date'] = pd.to_datetime(b['date'], format='%Y-%m-%d')
    dates = dates.sort_values(by=['date'], ascending=[True])
    dates.set_index('date', inplace=True)
    dates = dates.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates)
    dates.index=blankIndex
    blankIndex=[''] * len(df1)
    df1.index=blankIndex
    dates['date']=dates['date'].dt.date
    df1['date']=df1.date.dt.date
    final_data=pd.DataFrame(columns=['date','client_StateOrProvince','client_City','user_id','count'])
    for row in range(len(unique_state)):
            new=df1[df1['client_StateOrProvince']==unique_state[row]]
            add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
            add_zero['count']=0
            add_zero['user_id']='No user'
            add_zero['client_StateOrProvince']=unique_state[row]
            add_zero['client_City']=unique_city[row]
            new=new.append(add_zero)
            new=new.sort_values(by='date')
            final_data=final_data.append(new)
    final_data['longitude']=119.4179
    final_data['latitude']=36.7783
    final_data.loc[final_data['client_StateOrProvince'] == "Uttar Pradesh", 'longitude'] = 80.9462
    final_data.loc[final_data['client_StateOrProvince'] == "Maharashtra", 'longitude'] = 75.7139
    final_data.loc[final_data['client_StateOrProvince'] == "Uttar Pradesh", 'latitude'] = 26.8467
    final_data.loc[final_data['client_StateOrProvince'] == "Maharashtra", 'latitude'] = 19.7515
    final_data = final_data.rename(columns = {"client_StateOrProvince":"location"})
    final_data = final_data.rename(columns = {"client_City":"city"})
    dff = final_data.to_json(orient='records')
    return dff
