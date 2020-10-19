import pandas as pd
import datetime
import re
def user_activity():
    df=pd.read_csv('Erc_updatedfinal.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'])
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    last_7_days_new=users_datewise.tail(7)
    last_7_days_new_start=  last_7_days_new.date.head(1)
    last_7_days_new_start=pd.to_datetime(last_7_days_new_start)
    last_7_days_new_end=  last_7_days_new.date.tail(1)
    last_7_days_new_end=pd.to_datetime(last_7_days_new_end)
    blankIndex=[''] * len(last_7_days_new_start)
    last_7_days_new_start.index=blankIndex
    blankIndex=[''] * len(last_7_days_new_end)
    last_7_days_new_end.index=blankIndex
    range6to12 = (time['date'] >=last_7_days_new_start[0]) & (time['date'] <= last_7_days_new_end[0])
    last_7_days=time.loc[range6to12]
    before_7_days_new=users_datewise.head(len(users_datewise)-7)
    before_7_days_new_end= before_7_days_new.date.tail(1)
    before_7_days_new_end=pd.to_datetime(before_7_days_new_end)
    blankIndex=[''] * len(before_7_days_new_end)
    before_7_days_new_end.index=blankIndex
    rangenot6to12 = (time['date'] <=before_7_days_new_end[0])
    before_7_days=time.loc[rangenot6to12]
    last_7_days_users=pd.DataFrame(last_7_days.user_id.unique())
    last_7_days_users.columns=['user_id']
    before_7_days_users=pd.DataFrame(before_7_days.user_id.unique())
    before_7_days_users.columns=['user_id']
    new_user=last_7_days_users[last_7_days_users['user_id'].apply(lambda x: x not in before_7_days_users['user_id'].values)]
    return_user=pd.merge(before_7_days_users,last_7_days_users,on='user_id')
    total_user=df.groupby('user_id').count()
    User_Activity=pd.DataFrame()
    User_Activity["Desc"]=["New Users","Returning Users","Lifetime"]
    User_Activity["Count"]=[len(new_user),len(return_user),len(total_user)]
    user_activity=pd.DataFrame(index=["New Users","Returning Users","Lifetime"])
    user_activity["Count"]=[len(new_user),len(return_user),len(total_user)]
    blankIndex=[''] * len(User_Activity)
    User_Activity.index=blankIndex
    dff=User_Activity.to_json(orient='records')
    return dff
def new_ret():
    df=pd.read_csv('Query_data_modified.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    users_datewise['date'] = pd.to_datetime(users_datewise['date'])
    user_activity_date=pd.DataFrame(columns=['Date','count_users'])
    #users_datewise['date']=users_datewise['date'].dt.date
    before_days_combine=pd.DataFrame(columns=['user_id'])
    list_newusers=[]
    list_date=[]
    for row in range(len(users_datewise)):
            date_loc=(time['date']==users_datewise['date'][row])
            date_users=time.loc[date_loc]
            new_user=date_users[date_users['user_id'].apply(lambda x: x not in before_days_combine['user_id'].values)]
            if(date_users.empty):
                users_datewise['user_id'][row]=[]
            before_days_combine=before_days_combine.append(date_users)
            list_date.append(users_datewise['date'][row])
            list_newusers.append(len(new_user))
    user_activity_date['Date']=list_date
    user_activity_date['count_users']=list_newusers
    blankIndex=[''] * len(user_activity_date)
    user_activity_date.index=blankIndex
    new=users_datewise.set_index('date').resample('M')["user_id"].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    new['date']=pd.to_datetime(new['date']).dt.month_name()
    new.columns=['Month_name','user_id']
    ret_final=[]
    temp=[]
    for row in range(len(new)):
        ret_final.append(temp)
        for row1 in range(len(new.user_id[row])):
                for row2 in range(len(new.user_id[row][row1])):
                    #print(sum_userss[row][row1][row2])
                    temp.append(new.user_id[row][row1][row2])
        temp=[]
    final_list = []
    temp1=[]
    for row in range(len(ret_final)):
            final_list.append(temp1)
            for num in ret_final[row]:
                if num not in temp1:
                    temp1.append(num)
            temp1=[]
    week_new_users=pd.DataFrame(columns=['user_id'])
    week_new_users.user_id=final_list
    blankIndex=[''] * len(week_new_users)
    week_new_users.index=blankIndex
    list_count=[]
    count=0
    before_days_combine=[]
    for row in range(len(week_new_users)):
            if(row==0):
                list_count.append(len(week_new_users.user_id.iloc[row]))
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                        before_days_combine.append(week_new_users.user_id.iloc[row][row1])

            else:
                list_temp=week_new_users.user_id.iloc[row]
                new_user=[x for x in list_temp if x not  in before_days_combine]
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                    before_days_combine.append(week_new_users.user_id.iloc[row][row1])
                list_count.append(len(new_user))
                list_temp=None
    month_new_users_final=pd.DataFrame(columns=["Month","count"])
    month_new_users_final.Month=new.Month_name
    month_new_users_final['count']=list_count
    dff = month_new_users_final.to_json(orient='records')
    return dff
def time_zone():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(how='any')
    fresh=data.groupby(['timezone','date'])['chat_id'].unique().reset_index(name="count")
    for row in range(len(fresh)):
        fresh['count'][row]=len(fresh['count'][row])
    blankIndex=[''] * len(fresh)
    fresh.index=blankIndex
    fresh['date'] = pd.to_datetime(fresh['date'])
    n1=data.groupby(['timezone','date'])['question'].count().reset_index(name="incoming_msg_count")
    n1['date'] = pd.to_datetime(n1['date'])
    n1=n1.sort_values(by="timezone")
    fresh=fresh.sort_values(by="timezone")
    f2=pd.merge(fresh,n1)
    blankIndex=[''] * len(f2)
    f2.index=blankIndex
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
    fresh['date'] = fresh.date.dt.date
    n1['date'] = n1.date.dt.date
    f2['date'] = f2.date.dt.date
    f=n1.timezone.unique()
    final=pd.DataFrame(columns=f2.columns)
    for row in range(len(f)):
        new=f2[f2['timezone']==f[row]]
        add_zero=dates[dates['date'].apply(lambda x: x not in new['date'].values)]
        add_zero['incoming_msg_count']=0
        add_zero['timezone']=f[row]
        add_zero['count']=0
        new=new.append(add_zero)
        new=new.sort_values(by='date')
        final=final.append(new)
    dff = final.to_json(orient='records')
    return dff
def user_real():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(how='any')
    d1=data.groupby(['date'])['question'].count().reset_index(name="user_count")
    blankIndex=[''] * len(d1)
    d1.index=blankIndex
    d1['date'] = pd.to_datetime(d1['date'])
    d1=d1.sort_values(by="date")
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
    d1['date'] = d1.date.dt.date
    add_zero=dates[dates['date'].apply(lambda x: x not in d1['date'].values)]
    add_zero['user_count']=0
    real_usermsg=d1.append(add_zero)
    real_usermsg = real_usermsg.sort_values(by=['date'], ascending=[True])
    dff = real_usermsg.to_json(orient='records')
    return dff
def userwiz_count():
    data=pd.read_csv('Erc_updatedfinal.csv')
    data=data.dropna(how='any')
    d1=data.groupby(['date'])['question'].count().reset_index(name="User")
    d2=data.groupby(['date'])['answer'].count().reset_index(name="wizard")
    d1['date'] = pd.to_datetime(d1['date'])
    d2['date'] = pd.to_datetime(d2['date'])
    d1=pd.DataFrame(d1)
    blankIndex=[''] * len(d1)
    d1.index=blankIndex
    d1=d1.sort_values(by="date")
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
    d1['date']=d1.date.dt.date
    add_zero=dates[dates['date'].apply(lambda x: x not in d1['date'].values)]
    add_zero['User']=0
    real_usermsg=d1.append(add_zero)
    real_usermsg = real_usermsg.sort_values(by=['date'], ascending=[True])
    df = real_usermsg.rename(columns={'User': 'user_count'})
    df['datereal'] = pd.to_datetime(df['date'])
    t=df.tail(7)
    df_1=t[['datereal','user_count']]
    user_count=df_1['user_count'].sum()
    user_count_data=pd.DataFrame(columns=["User_Count"])
    user_count_data["User_Count"]=[user_count]
    blankIndex=[''] * len(user_count_data)
    user_count_data.index=blankIndex
    d2=pd.DataFrame(d2)
    blankIndex=[''] * len(d2)
    d2.index=blankIndex
    dates1=pd.DataFrame(columns=['date'])
    dates1['date'] = pd.to_datetime(d2['date'], format='%Y/%m/%d')
    dates1 = dates1.sort_values(by=['date'], ascending=[True])
    dates1.set_index('date', inplace=True)
    dates1 = dates1.resample('D').ffill().reset_index()
    blankIndex=[''] * len(dates1)
    dates1.index=blankIndex
    dates1['date']=dates1['date'].dt.date
    d2['date']=d2.date.dt.date
    add_zero_wiz=dates1[dates1['date'].apply(lambda x: x not in d2['date'].values)]
    add_zero_wiz['wizard']=0
    real_wizmsg=d2.append(add_zero_wiz)
    real_wizmsg = real_wizmsg.sort_values(by=['date'], ascending=[True])
    df1 = real_wizmsg.rename(columns={'wizard': 'wizard_count'})
    df1['datereal'] = pd.to_datetime(df1['date'])
    t1=df1.tail(7)
    df_2 = t1[['datereal','wizard_count']]
    wiz_count=df_2['wizard_count'].sum()
    wiz_count_data=pd.DataFrame(columns=["Wizard_Count"])
    wiz_count_data["Wizard_Count"]=[wiz_count]
    blankIndex=[''] * len(wiz_count_data)
    wiz_count_data.index=blankIndex
    df_3 = pd.DataFrame({'User_count':user_count_data['User_Count'],'Wizard_count':wiz_count_data['Wizard_Count']})
    df_3["total"]=df_3['User_count']+df_3['Wizard_count']
    t2=df.tail(1)
    d_1 = t2[['datereal','user_count']]
    t3=df1.tail(1)
    d_2 = t3[['datereal','wizard_count']]
    d_r=pd.merge(d_1,d_2)
    blankIndex=[''] * len(wiz_count_data)
    d_r.index=blankIndex
    df_final = pd.DataFrame({'date':d_r['datereal'],'User_last':d_r['user_count'],'Wizard_last':d_r['wizard_count'],'User_count':user_count_data['User_Count'],'Wizard_count':wiz_count_data['Wizard_Count']})
    df_final["total"]=df_final['User_count']+df_final['Wizard_count']
    dff = df_final.to_json(orient='records')
    return dff
def ret_user_month():
    df=pd.read_csv('Query_data_modified.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    users_datewise['date'] = pd.to_datetime(users_datewise['date'])
    user_activity_date=pd.DataFrame(columns=['Date','count_users'])
    #users_datewise['date']=users_datewise['date'].dt.date
    before_days_combine=pd.DataFrame(columns=['user_id'])
    list_newusers=[]
    list_date=[]
    for row in range(len(users_datewise)):
            date_loc=(time['date']==users_datewise['date'][row])
            date_users=time.loc[date_loc]
            new_user=date_users[date_users['user_id'].apply(lambda x: x not in before_days_combine['user_id'].values)]
            if(date_users.empty):
                users_datewise['user_id'][row]=[]
            before_days_combine=before_days_combine.append(date_users)
            list_date.append(users_datewise['date'][row])
            list_newusers.append(len(new_user))
    user_activity_date['Date']=list_date
    user_activity_date['count_users']=list_newusers
    blankIndex=[''] * len(user_activity_date)
    user_activity_date.index=blankIndex
    new=users_datewise.set_index('date').resample('M')["user_id"].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    new['date']=pd.to_datetime(new['date']).dt.month_name()
    new.columns=['Month_name','user_id']
    ret_final=[]
    temp=[]
    for row in range(len(new)):
        ret_final.append(temp)
        for row1 in range(len(new.user_id[row])):
                for row2 in range(len(new.user_id[row][row1])):
                    #print(sum_userss[row][row1][row2])
                    temp.append(new.user_id[row][row1][row2])
        temp=[]
    final_list = []
    temp1=[]
    for row in range(len(ret_final)):
            final_list.append(temp1)
            for num in ret_final[row]:
                if num not in temp1:
                    temp1.append(num)
            temp1=[]
    week_new_users=pd.DataFrame(columns=['user_id'])
    week_new_users.user_id=final_list
    blankIndex=[''] * len(week_new_users)
    week_new_users.index=blankIndex
    list_count=[]
    count=0
    before_days_combine=[]
    for row in range(len(week_new_users)):
            if(row==0):
                list_count.append(0)
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                        before_days_combine.append(week_new_users.user_id.iloc[row][row1])

            else:
                list_temp=week_new_users.user_id.iloc[row]
                new_user=[x for x in list_temp if x in before_days_combine]
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                    before_days_combine.append(week_new_users.user_id.iloc[row][row1])
                list_count.append(len(new_user))
                list_temp=None
    month_new_users_final=pd.DataFrame(columns=["Month","count"])
    month_new_users_final.Month=new.Month_name
    month_new_users_final['count']=list_count
    dff = month_new_users_final.to_json(orient='records')
    return dff
def ret_users_week():
    df=pd.read_csv('Query_data_modified.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    users_datewise['date'] = pd.to_datetime(users_datewise['date'])
    user_activity_date=pd.DataFrame(columns=['Date','count_users'])
    #users_datewise['date']=users_datewise['date'].dt.date
    before_days_combine=pd.DataFrame(columns=['user_id'])
    list_date=[]
    list_newusers=[]
    for row in range(len(users_datewise)):
            date_loc=(time['date']==users_datewise['date'][row])
            date_users=time.loc[date_loc]
            new_user=date_users[date_users['user_id'].apply(lambda x: x not in before_days_combine['user_id'].values)]
            if(date_users.empty):
                users_datewise['user_id'][row]=[]
            before_days_combine=before_days_combine.append(date_users)
            list_date.append(users_datewise['date'][row])
            list_newusers.append(len(new_user))
    user_activity_date['Date']=list_date
    user_activity_date['count_users']=list_newusers
    blankIndex=[''] * len(user_activity_date)
    user_activity_date.index=blankIndex
    new=users_datewise.set_index('date').resample('W-Wed')["user_id"].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    ret_final=[]
    temp=[]
    for row in range(len(new)):
        ret_final.append(temp)
        for row1 in range(len(new.user_id[row])):
                for row2 in range(len(new.user_id[row][row1])):
                    #print(sum_userss[row][row1][row2])
                    temp.append(new.user_id[row][row1][row2])
        temp=[]
    final_list = []
    temp1=[]
    for row in range(len(ret_final)):
            final_list.append(temp1)
            for num in ret_final[row]:
                if num not in temp1:
                    temp1.append(num)
            temp1=[]
    for row in range(len(new)):
        new['user_id'][row]=final_list[row]
    week_new_users=pd.DataFrame(columns=["start_date","end_date","user_id"])
    week_new_users_start_date=[]
    week_new_users_end_date=[]
    sum_userss=[]
    count=0
    sum=0
    for row in range(len(user_activity_date)):
            if(user_activity_date.Date.iloc[row]==user_activity_date.Date.iloc[len(user_activity_date)-1]):

                #print("hh")
                sum=sum+user_activity_date.count_users.iloc[row]
                sum_userss.append(sum)
                for cou in range(count,7):
                    #print(cou)
                    if(cou==0):
                        week_new_users_start_date.append(user_activity_date.Date.iloc[row])
                    elif(cou==6):
                        #print("aa")
                        week_new_users_end_date.append(user_activity_date.Date.iloc[row]+datetime.timedelta(days=6))
                        break
            elif(count==0):
                week_new_users_start_date.append(user_activity_date.Date.iloc[row])
                count=count+1
                sum=sum+user_activity_date.count_users.iloc[row]
            elif(count==6):
                sum=sum+user_activity_date.count_users.iloc[row]
                week_new_users_end_date.append(user_activity_date.Date.iloc[row])
                sum_userss.append(sum)
                count=0
                sum=0

            else:
                count=count+1
                sum=sum+user_activity_date.count_users.iloc[row]
    week_new_users.user_id=final_list
    list_count=[]
    count=0
    before_days_combine=[]
    for row in range(len(week_new_users)):
            if(row==0):
                list_count.append(0)
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                        before_days_combine.append(week_new_users.user_id.iloc[row][row1])

            else:
                list_temp=week_new_users.user_id.iloc[row]
                new_user=[x for x in list_temp if x in before_days_combine]
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                    before_days_combine.append(week_new_users.user_id.iloc[row][row1])
                list_count.append(len(new_user))
                list_temp=None
    week_new_users_final=pd.DataFrame(columns=["start_date","end_date","count"])
    week_new_users_final.start_date=week_new_users_start_date
    week_new_users_final.end_date=week_new_users_end_date
    week_new_users_final['count']=list_count
    blankIndex=[''] * len(week_new_users_final)
    week_new_users_final.index=blankIndex
    week_new_users_final['start_date'] =week_new_users_final['start_date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    week_new_users_final['end_date']=week_new_users_final['end_date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    dff = week_new_users_final.to_json(orient='records')
    return dff
def newuser_week():
    df=pd.read_csv('Query_data_modified.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    users_datewise['date'] = pd.to_datetime(users_datewise['date'])
    user_activity_date=pd.DataFrame(columns=['Date','count_users'])
    #users_datewise['date']=users_datewise['date'].dt.date
    before_days_combine=pd.DataFrame(columns=['user_id'])
    list_date=[]
    list_newusers=[]
    for row in range(len(users_datewise)):
            date_loc=(time['date']==users_datewise['date'][row])
            date_users=time.loc[date_loc]
            new_user=date_users[date_users['user_id'].apply(lambda x: x not in before_days_combine['user_id'].values)]
            if(date_users.empty):
                users_datewise['user_id'][row]=[]
            before_days_combine=before_days_combine.append(date_users)
            list_date.append(users_datewise['date'][row])
            list_newusers.append(len(new_user))
    user_activity_date['Date']=list_date
    user_activity_date['count_users']=list_newusers
    blankIndex=[''] * len(user_activity_date)
    user_activity_date.index=blankIndex
    new=users_datewise.set_index('date').resample('W-Wed')["user_id"].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    ret_final=[]
    temp=[]
    for row in range(len(new)):
        ret_final.append(temp)
        for row1 in range(len(new.user_id[row])):
                for row2 in range(len(new.user_id[row][row1])):
                    #print(sum_userss[row][row1][row2])
                    temp.append(new.user_id[row][row1][row2])
        temp=[]
    final_list = []
    temp1=[]
    for row in range(len(ret_final)):
            final_list.append(temp1)
            for num in ret_final[row]:
                if num not in temp1:
                    temp1.append(num)
            temp1=[]
    for row in range(len(new)):
        new['user_id'][row]=final_list[row]
    week_new_users=pd.DataFrame(columns=["start_date","end_date","user_id"])
    week_new_users_start_date=[]
    week_new_users_end_date=[]
    sum_userss=[]
    count=0
    sum=0
    for row in range(len(user_activity_date)):
            if(user_activity_date.Date.iloc[row]==user_activity_date.Date.iloc[len(user_activity_date)-1]):

                #print("hh")
                sum=sum+user_activity_date.count_users.iloc[row]
                sum_userss.append(sum)
                for cou in range(count,7):
                    #print(cou)
                    if(cou==0):
                        week_new_users_start_date.append(user_activity_date.Date.iloc[row])
                    elif(cou==6):
                        #print("aa")
                        week_new_users_end_date.append(user_activity_date.Date.iloc[row]+datetime.timedelta(days=6))
                        break
            elif(count==0):
                week_new_users_start_date.append(user_activity_date.Date.iloc[row])
                count=count+1
                sum=sum+user_activity_date.count_users.iloc[row]
            elif(count==6):
                sum=sum+user_activity_date.count_users.iloc[row]
                week_new_users_end_date.append(user_activity_date.Date.iloc[row])
                sum_userss.append(sum)
                count=0
                sum=0

            else:
                count=count+1
                sum=sum+user_activity_date.count_users.iloc[row]
    week_new_users.user_id=final_list
    list_count=[]
    count=0
    before_days_combine=[]
    for row in range(len(week_new_users)):
            if(row==0):
                list_count.append(len(week_new_users.user_id.iloc[row]))
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                        before_days_combine.append(week_new_users.user_id.iloc[row][row1])

            else:
                list_temp=week_new_users.user_id.iloc[row]
                new_user=[x for x in list_temp if x not in before_days_combine]
                for row1 in range(len(week_new_users.user_id.iloc[row])):
                    before_days_combine.append(week_new_users.user_id.iloc[row][row1])
                list_count.append(len(new_user))
                list_temp=None
    week_new_users_final=pd.DataFrame(columns=["start_date","end_date","count"])
    week_new_users_final.start_date=week_new_users_start_date
    week_new_users_final.end_date=week_new_users_end_date
    week_new_users_final['count']=list_count
    blankIndex=[''] * len(week_new_users_final)
    week_new_users_final.index=blankIndex
    week_new_users_final['start_date'] =week_new_users_final['start_date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    week_new_users_final['end_date']=week_new_users_final['end_date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    dff = week_new_users_final.to_json(orient='records')
    return dff
def newuser_date():
    df=pd.read_csv('Erc_updatedfinal.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    users_datewise['date'] = pd.to_datetime(users_datewise['date'])
    user_activity_date=pd.DataFrame(columns=['Date','count_users'])
    users_datewise['date']=users_datewise['date'].dt.date
    before_days_combine=pd.DataFrame(columns=['user_id','date','count'])
    list_date=[]
    list_newusers=[]
    for row in range(len(users_datewise)):
        date_loc=(time.date.dt.date==users_datewise['date'][row])
        date_users=time.loc[date_loc]
        new_user=date_users[date_users['user_id'].apply(lambda x: x not in before_days_combine['user_id'].values)]
        before_days_combine=before_days_combine.append(date_users)
        list_date.append(users_datewise['date'][row])
        list_newusers.append(len(new_user))
    user_activity_date['Date']=list_date
    user_activity_date['count_users']=list_newusers
    blankIndex=[''] * len(user_activity_date)
    user_activity_date.index=blankIndex
    user_activity_date['Date'] =user_activity_date['Date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    dff = user_activity_date.to_json(orient='records')
    return dff
def ret_users():
    df=pd.read_csv('Query_data_modified.csv')
    df=df.dropna(subset=['question'])
    print(len(df))
    time=df['date'].groupby([df.user_id,df.date]).agg('count').reset_index(name="count")
    time=pd.DataFrame(time)
    time=time.sort_values(by=['date'],ascending=False)
    time['date'] = pd.to_datetime(time['date'])
    users_datewise=time.groupby('date')['user_id'].apply(lambda group_series: group_series.tolist()).reset_index(name="user_id")
    users_datewise=pd.DataFrame(users_datewise)
    users_datewise['date'] =  pd.to_datetime(users_datewise['date'], format='%Y/%m/%d')
    users_datewise = users_datewise.sort_values(by=['date'], ascending=[True])
    users_datewise.set_index('date', inplace=True)
    users_datewise = users_datewise.resample('D').ffill().reset_index()
    users_datewise['date'] = pd.to_datetime(users_datewise['date'])
    user_activity_date=pd.DataFrame(columns=['Date','count_users'])
    users_datewise['date']=users_datewise['date'].dt.date
    before_days_combine=pd.DataFrame(columns=['user_id'])
    list_date=[]
    list_retusers=[]
    for row in range(len(users_datewise)):
        date_loc=(time.date.dt.date==users_datewise['date'][row])
        date_users=time.loc[date_loc]
        new_user=date_users[date_users['user_id'].apply(lambda x: x  in before_days_combine['user_id'].values)]
        before_days_combine=before_days_combine.append(date_users)
        list_date.append(users_datewise['date'][row])
        list_retusers.append(len(new_user))
    user_activity_date['Date']=list_date
    user_activity_date['count_users']=list_retusers
    blankIndex=[''] * len(user_activity_date)
    user_activity_date.index=blankIndex
    user_activity_date['Date'] =user_activity_date['Date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
    dff = user_activity_date.to_json(orient='records')
    return dff
def user_wiz():
    data = pd.read_csv('Erc_updatedfinal.csv')
    data = data.dropna(how='any')
    d1 = data.groupby(['date'])['question'].count().reset_index(name="incoming_msg")
    d2 = data.groupby(['date'])['answer'].count().reset_index(name="outgoing_msg")
    d1['date'] = pd.to_datetime(d1['date'])
    d2['date'] = pd.to_datetime(d2['date'])
    d1 = pd.DataFrame(d1)
    d2 = pd.DataFrame(d2)
    d3 = pd.merge(d1, d2)
    blankIndex = [''] * len(d3)
    d3.index = blankIndex
    d3 = d3.sort_values(by="date")
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
    d3['date'] = d3.date.dt.date
    add_zero = dates[dates['date'].apply(lambda x: x not in d3['date'].values)]
    add_zero['incoming_msg'] = 0
    add_zero['outgoing_msg'] = 0
    real_msg = d3.append(add_zero)
    real_msg = real_msg.sort_values(by=['date'], ascending=[True])
    real_msg = real_msg[['date', 'outgoing_msg']]
    dff = real_msg.to_json(orient='records')
    return dff

