# Telegram_Bot

## Входные данные

Дана база данных в ClickHouse некоторого приложения "Лента новостей и мессенджер". База данных состоит из 2 таблиц feed_action и message_action:

feed_action (лента новостей):
- user_id - id пользователя,
- time - время,
- action - действие (like or view),
- age (возраст пользователя),
- city - город,
- country - страна,
- exp_group - номер экспериментальной группы для AB и AA тестов,
- gender - гендер пользователя (0- мужчина, 1- женщина),
- os - операционная система (ios, android),
- post_id - номер поста,
- source - трафик (ads - реклама, organic).

message_action (мессенджер):

- user_id - id пользователя, который отправляет сообщение,
- reciever_id - id пользователя, который получает сообщение,
- time - время,
- source - трафик (ads - реклама, organic),
- gender - гендер пользователя (0- мужчина, 1- женщина),
- age (возраст пользователя),
- country - страна,
- city - город,
- os - операционная система (ios, android).

## Задание
Настроить автоматическую отправку аналитической сводки работы приложения в чат телеграма каждое утро.
Отчет должен состоять из двух частей:
- текст с информацией о значениях ключевых метрик за предыдущий день,
- график со значениями метрик за предыдущие 7 дней. Метрики: DAU, Просмотры, Лайки, CTR.
Автоматизировать отправку отчета с помощью Airflow.

## Решение
Создан DAG, который работает через Apache Airflow, и состоит из тасок:
- выгрузка данных из базы ClickHouse,
- подсчет показателей за предыдущий день, формирование и отправка текстового сообщения в телеграм-чат,
- построение графиков за последние 7 дней и отправка картинки в телеграм-чат.

## Результат
![Screenshot_bot](https://user-images.githubusercontent.com/122218714/211319699-cc037612-5c53-44e5-a387-b0c1f7b2c263.png)
