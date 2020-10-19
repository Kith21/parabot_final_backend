from flask import Flask
from useractivity import *
from message_in import *
from compare import *
from transcript import *
from tickets import *
from message_out import *
from WordCloudd import *
from geomap import *
from dash_changes import *
from sentiment import *
import logging
app = Flask(__name__)
@app.route('/useractivity')
def hello_world1():
       return user_activity()
@app.route('/retmonth')
def hello_world2():
       return new_ret()
@app.route('/timezone')
def hello_world3():
       return time_zone()
@app.route('/user')
def hello_world4():
       return user_real()
@app.route('/userwizcount')
def hello_world5():
       return userwiz_count()
@app.route('/retusermonth')
def hello_world6():
       return ret_user_month()
@app.route('/retusersweek')
def hello_world7():
       return ret_users_week()
@app.route('/newuserweek')
def hello_world8():
       return newuser_week()
@app.route('/newuserdate')
def hello_world9():
       return newuser_date()
@app.route('/retusers')
def hello_world10():
       return ret_users()
@app.route('/daywise')
def hello_world11():
       return user_wiz()
@app.route('/messageinchat')
def messageinchat():
      return message_in_chat()
@app.route('/chatin')
def chatin():
      return chat_display_in()
@app.route('/messageout')
def messageout():
      return message_out()
@app.route('/messageoutchat')
def messageoutchat():
      return message_out_chat()
@app.route('/chatout')
def chatout():
      return chat_display_out()
@app.route('/newusercompare')
def new():
    return new_user_count()
@app.route('/retusercompare')
def ret():
    return ret_user_count()
@app.route('/totalusercompare')
def tot():
    return total_user_count()
@app.route('/messagecompare')
def msg():
    return msgcount_total()
@app.route('/transcript')
def trans():
    return transcript_7()
@app.route('/transcript_next')
def transnext():
    return transcript()
@app.route('/transcript_second')
def transsec():
    return trans_fullchat()
@app.route('/tickets')
def ticket():
    return tickets()
@app.route('/resolved')
def res():
    return resolved_count()
@app.route('/pending')
def pen():
    return pending_count()
@app.route('/assigned')
def assign():
    return assigned_count()
@app.route('/wordcount')
def wordlist():
    return word_list()
@app.route('/wordcountdate')
def worddate():
    return word_freq_date()
@app.route('/newgeomap')
def geomap():
    return geoo()
@app.route('/newdashchanges')
def dash_changes():
    return dash_change()
@app.route('/newsentiment')
def sentiment():
    return senti()
@app.route('/newsentiment1')
def sentiment1():
    return senti1()
@app.route('/newsentiment2')
def sentiment2():
    return senti2()
if __name__ == '__main__':
       app.run(host='127.0.0.1', port=8080)


