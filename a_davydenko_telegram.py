# импортируем библиотеки
import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# подключение к CH
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'some_database',
                      'user':'student', 
                      'password':'some_password'
                     }

# задаем параметры для DAG

default_args = {
    'owner': 'a.davydenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 13)
}


# Интервал запуска DAG каждый день в 11:00
schedule_interval = '0 11 * * *'

# задаем переменные бота и чата (для проверки bot_token и some_id_chat заменить на свои)
bot = telegram.Bot(token='bot_token')
chat_id = some_id_chat

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)  
def dag_feed_week_a_davydenko():

    # Таск для выгрузки данных из feed_actions за неделю
    @task()
    def extract_feed_week():
        query_feed_actions = '''
        SELECT toDate(time) AS day,
            count(DISTINCT user_id) as DAU,
            countIf(action = 'view') as Views,
            countIf(action = 'like') as Likes,
            countIf(action = 'like')/countIf(action = 'view') as CTR
        FROM {df}.feed_actions
        WHERE day <= today() - 1 and day >= today() - 8
        GROUP BY day
        ORDER BY day DESC
        '''
        
        feed_week = ph.read_clickhouse(query_feed_actions, connection=connection)
        return feed_week

    @task()
    def send_message_last_day(feed_week, chat_id):
        feed_d = feed_week.iloc[0]
        day = feed_d['day']
        dau = feed_d['DAU']
        views = feed_d['Views']
        likes = feed_d['Likes']
        ctr = round(feed_d['CTR'], 2)
            
        msge = f'''
                Доброе утро. 
                Ключевые метрики ленты новостей:
                День: {day}.
                DAU: {dau}.
                Просмотры: {views}.
                Лайки: {likes}.
                CTR: {ctr}.
        '''
        bot.sendMessage(chat_id=chat_id, text=msge)
            
    @task()
    def send_photo_last_day(feed_week, chat_id):
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = feed_week, x = 'day', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = feed_week, x = 'day', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[1, 0], data = feed_week, x = 'day', y = 'Views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = feed_week, x = 'day', y = 'Likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Stats.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    # выполняем таски
    feed_week = extract_feed_week()
    send_message_last_day(feed_week, chat_id)
    send_photo_last_day(feed_week, chat_id)
        
dag_feed_week_a_davydenko = dag_feed_week_a_davydenko()