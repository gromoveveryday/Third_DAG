from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import requests
import pandas as pd
import numpy as np
import pandahouse
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.pyplot import figure
import telegram
import io

# Импортируем все необходимые библиотеки

connection1 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Задаем словарь для подключения к БД Clickhouse

default_args = {
    'owner': 'i-gromov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 16),
}

# Задаем словарь с основными параметрами для настройки DAG'а

schedule_interval = '0 11 * * *'

# Каждые 11 часов утра по московскому времени DAG будет срабатывать

bot_token = '5805207394:AAEl7-_Oqdh90P7jniulur3E9Y7DDSjz9Og'
bot = telegram.Bot(token = bot_token)
chat_id = -850804180

# Для создания DAG'а был создан бот, имеющий следующий токен, также задан id чата, в которой будет приходить сообщение от бота

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def i_gromov_dag_task_7_2():
    @task()
    def send_message_and_photo_feed_actions_2_1(chat_id, connection1):
        query1 = """SELECT toDate(time) as date,
        COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views,
        ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 6) AS CTR
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 6 AND yesterday()
        GROUP BY date"""
        
        query2 = """SELECT  COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views,
        ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 6) AS CTR
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 6 AND yesterday()"""
        
        query3 = """SELECT COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views,
        ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 6) AS CTR
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()"""
        
        df1 = pandahouse.read_clickhouse(query1, connection = connection1)
        df2 = pandahouse.read_clickhouse(query2, connection = connection1)
        df3 = pandahouse.read_clickhouse(query3, connection = connection1)

# Делаются запросы к БД Clickhouse по созданному словарю и создаются по запросам датафреймы        
        
        yestarday_date = datetime.now() - timedelta(days=1)
        _7_days = datetime.now()- timedelta(days=6)
        _30_days = datetime.now()- timedelta(days=29)
        message1 = f"Ключевые показатели по ленте новостей за вчерашний день ({yestarday_date}) следующие:\nDAU: {df1.iloc[-1].DAU};\nКоличество лайков: {df1.iloc[-1].likes};\nКоличество просмотров: {df1.iloc[-1].views};\nЗначение CTR: {df1.iloc[-1].CTR}."
        message2 = f"Ключевые показатели по ленте новостей за прошедшие 7 дней ({_7_days} - {yestarday_date}) следующие:\nDAU: {df2.iloc[0].DAU};\nКоличество лайков: {df2.iloc[0].likes};\nКоличество просмотров: {df2.iloc[0].views};\nЗначение CTR: {df2.iloc[0].CTR}."
        message3 = f"Ключевые показатели по ленте новостей за прошедшие 30 дней ({_30_days} - {yestarday_date}) следующие:\nDAU: {df3.iloc[0].DAU};\nКоличество лайков: {df3.iloc[0].likes};\nКоличество просмотров: {df3.iloc[0].views};\nЗначение CTR: {df3.iloc[0].CTR}."
        
        figure = plt.figure()
        figure.set_size_inches(18.5, 10.5, forward=True)
        
        ax1 = figure.add_subplot(2, 2, 1)
        ax2 = figure.add_subplot(2, 2, 2)
        ax3 = figure.add_subplot(2, 2, 3)
        ax4 = figure.add_subplot(2, 2, 4)
        
        ax1.plot(df1['date'], df1['DAU'], color = 'blue')
        ax2.plot(df1['date'], df1['likes'], color = 'blue')
        ax3.plot(df1['date'], df1['views'], color = 'blue')
        ax4.plot(df1['date'], df1['CTR'], color = 'blue')
        
        ax1.set_title('DAU')
        ax2.set_title('Количество лайков')
        ax3.set_title('Количество просмотров')
        ax4.set_title('CTR')
        
        figure.suptitle('Значения метрик по ленте новостей за предыдущие 7 дней', fontsize=28)
        
        four_charts = io.BytesIO()
        plt.savefig(four_charts)
        four_charts.seek(0)
        four_charts.name = 'four_charts.png'
        plt.close()
        
        bot.sendMessage(chat_id = chat_id, text = message1)
        bot.sendMessage(chat_id = chat_id, text = message2)
        bot.sendMessage(chat_id = chat_id, text = message3)
        bot.sendPhoto(chat_id = chat_id, photo = four_charts)

# В данном таске происходит расширенное выполнение задач из предыдущего DAG'а (https://github.com/gromoveveryday/Second_DAG), а именно происходит вызов ключевых продуктовых показателей за прошедшие 7 и 30 дней

    @task()
    def send_message_and_photo_feed_actions_2_2(chat_id, connection1):
        query4 = """SELECT country,
        COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY country
        ORDER BY DAU DESC, likes DESC, views DESC"""
        
        query5 = """SELECT source,
        COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY source
        ORDER BY DAU DESC, likes DESC, views DESC"""
        
        query6 = """SELECT os,
        COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY os
        ORDER BY DAU DESC, likes DESC, views DESC"""
        
        df4 = pandahouse.read_clickhouse(query4, connection = connection1)
        df5 = pandahouse.read_clickhouse(query5, connection = connection1)
        df6 = pandahouse.read_clickhouse(query6, connection = connection1)

# Делаются запросы к БД Clickhouse по созданному словарю и создаются по запросам датафреймы            
        
        fig1, ax1 = plt.subplots()
        fig1.set_size_inches(18, 10, forward=True)
        x1 = df4.iloc[1:]['DAU']
        y1 = df4.iloc[1:]['country']
        ax1.bar(y1, x1, color = 'blue')

        ax1.set_title('Количество уникальных пользователей за предыдущие 30 дней по иностранным странам ', fontsize = 20)
        ax1.set_xlabel('Страны')
        ax1.set_ylabel('Количество пользователей')
        
        month_dau_countries = io.BytesIO()
        plt.savefig(month_dau_countries)
        month_dau_countries.seek(0)
        month_dau_countries.name = 'month_dau_countries.png'
        plt.close()
        
        fig3, ax3 = plt.subplots()
        fig3.set_size_inches(18, 10, forward=True)
        x3 = df5['DAU']
        y3 = df5['source']
        ax3.bar(y3, x3, color = 'blue')

        ax3.set_title('Количество уникальных пользователей за предыдущие 30 дней по источнику', fontsize = 20)
        ax3.set_xlabel('Источник')
        ax3.set_ylabel('Количество пользователей')
        
        month_dau_source = io.BytesIO()
        plt.savefig(month_dau_source)
        month_dau_source.seek(0)
        month_dau_source.name = 'month_dau_source.png'
        plt.close()
        
        fig4, ax4 = plt.subplots()
        fig4.set_size_inches(18, 10, forward=True)
        x4 = df6['DAU']
        y4 = df6['os']
        ax4.bar(y4, x4, color = 'blue')

        ax4.set_title('Количество уникальных пользователей за предыдущие 30 дней по операционной системе', fontsize = 20)
        ax4.set_xlabel('Операционная система')
        ax4.set_ylabel('Количество пользователей')
        
        month_dau_os = io.BytesIO()
        plt.savefig(month_dau_os)
        month_dau_os.seek(0)
        month_dau_os.name = 'month_dau_os.png'
        plt.close()
        
        bot.sendPhoto(chat_id = chat_id, photo = month_dau_countries)
        bot.sendPhoto(chat_id = chat_id, photo = month_dau_source)
        bot.sendPhoto(chat_id = chat_id, photo = month_dau_os)

# В этом таске формируются и отправляются в чат графики динамики изменениня уникальных пользователей в зависимости от разных признаков за предыдущие 30 дней       
        
    @task()
    def send_message_and_photo_feed_actions_2_3(chat_id, connection1):
        today_time = datetime.now()
        query7 = """SELECT post_id,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views,
        COUNT(DISTINCT user_id) AS DAU,
        ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 6) AS CTR
        FROM simulator_20221220.feed_actions
        GROUP BY post_id
        ORDER BY likes DESC, views DESC, DAU DESC
        LIMIT 100"""

# Делатся запрос к ПД Clickhouse        
        
        df7 = pandahouse.read_clickhouse(query7, connection = connection1)
        df7.index = np.arange(1, len(df7) + 1)
        
        message4 = f"Топ 100 постов в приложении за все время ({today_time}):"
        
        df7_csv = io.BytesIO()
        df7.to_csv(df7_csv)
        df7_csv.name = 'df7.csv'
        df7_csv.seek(0)
        
        bot.sendMessage(chat_id = chat_id, text = message4)
        bot.sendDocument(chat_id = chat_id, document = df7_csv)

# В этом таске формируется документ в формате .csv по наиболее популярным постам за все время работы приложения
        
    @task()
    def send_message_and_photo_message_actions_1(chat_id, connection1):
        query8 = """SELECT toStartOfDay(toDateTime(time)) AS date,
        count(DISTINCT user_id) AS "Количество пользователей"
        FROM
        (SELECT user_id,
        time
        FROM simulator_20221220.message_actions
        LEFT JOIN simulator_20221220.feed_actions ON simulator_20221220.feed_actions.user_id = simulator_20221220.message_actions.user_id) AS virtual_table
        WHERE toStartOfDay(toDateTime(time)) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY toStartOfDay(toDateTime(time))
        ORDER BY "Количество пользователей" DESC"""
        
        query9 = """SELECT toStartOfDay(toDateTime(time)) AS date,
        count(DISTINCT user_id) AS "Количество пользователей"
        FROM
        (SELECT user_id,
        time
        FROM simulator_20221220.feed_actions
        INNER JOIN simulator_20221220.message_actions ON simulator_20221220.feed_actions.user_id = simulator_20221220.message_actions.user_id) AS virtual_table
        WHERE toStartOfDay(toDateTime(time)) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY toStartOfDay(toDateTime(time))
        ORDER BY "Количество пользователей" DESC"""
        
        query10 = """SELECT toStartOfDay(toDateTime(time)) AS date,
        count(DISTINCT user_id) AS "Количество пользователей"
        FROM
        (SELECT user_id,
        time
        FROM simulator_20221220.feed_actions
        LEFT JOIN simulator_20221220.message_actions ON simulator_20221220.feed_actions.user_id = simulator_20221220.message_actions.user_id) AS virtual_table
        WHERE toStartOfDay(toDateTime(time)) BETWEEN yesterday() - 29 AND yesterday()
        GROUP BY toStartOfDay(toDateTime(time))
        ORDER BY "Количество пользователей" DESC"""

# Делаются запросы к БД Clickhouse по созданному словарю и создаются по запросам датафреймы            
        
        df8 = pandahouse.read_clickhouse(query8, connection = connection1)
        df9 = pandahouse.read_clickhouse(query9, connection = connection1)
        df10 = pandahouse.read_clickhouse(query10, connection = connection1)
        
        fig1, ax1 = plt.subplots()
        fig1.set_size_inches(18, 10, forward=True)
        x1 = df8['Количество пользователей']
        y1 = df8['date']
        ax1.bar(y1, x1, color = 'blue')
        
        ax1.set_title('Количество пользователей мессенджера за предыдущие 30 дней', fontsize = 20)
        ax1.set_xlabel('Даты')
        ax1.set_ylabel('Количество пользователей')
        
        month_message_users = io.BytesIO()
        plt.savefig(month_message_users)
        month_message_users.seek(0)
        month_message_users.name = 'month_message_users.png'
        plt.close()
        
        fig2, ax2 = plt.subplots()
        fig2.set_size_inches(18, 10, forward=True)
        x2 = df10['Количество пользователей']
        y2 = df10['date']
        ax2.bar(y2, x2, color = 'blue')
        
        ax2.set_title('Количество пользователей ленты новостей за предыдущие 30 дней', fontsize = 20)
        ax2.set_xlabel('Даты')
        ax2.set_ylabel('Количество пользователей')
        
        month_feed_users = io.BytesIO()
        plt.savefig(month_feed_users)
        month_feed_users.seek(0)
        month_feed_users.name = 'month_feed_users.png'
        plt.close()
        
        fig3, ax3 = plt.subplots()
        fig3.set_size_inches(18, 10, forward=True)
        x3 = df9['Количество пользователей']
        y3 = df9['date']
        ax3.bar(y3, x3, color = 'blue')
        
        ax3.set_title('Количество пользователей мессенджера и ленты сообщений за предыдущие 30 дней', fontsize = 20)
        ax3.set_xlabel('Даты')
        ax3.set_ylabel('Количество пользователей')
        
        month_message_and_feed_users = io.BytesIO()
        plt.savefig(month_message_and_feed_users)
        month_message_and_feed_users.seek(0)
        month_message_and_feed_users.name = 'month_message_and_feed_users.png'
        plt.close()
        
        bot.sendPhoto(chat_id = chat_id, photo = month_message_users)
        bot.sendPhoto(chat_id = chat_id, photo = month_feed_users)
        bot.sendPhoto(chat_id = chat_id, photo = month_message_and_feed_users)

# В данном таске создаются и отправляются графики изменения количества уникальных пользователей которые как пользуются одним из двух функций приложения по отдельности, так и совместно пользуются обоими функциями      
        
    send_message_and_photo_feed_actions_2_1(chat_id, connection1)
    send_message_and_photo_feed_actions_2_2(chat_id, connection1)
    send_message_and_photo_feed_actions_2_3(chat_id, connection1)
    send_message_and_photo_message_actions_1(chat_id, connection1)

i_gromov_dag_task_7_2 = i_gromov_dag_task_7_2()
    