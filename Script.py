import time
from sqlalchemy import create_engine  # для работы с функцией преобразования данных в SQL
import psycopg2  # подключаем Postgres
import pandas as pd  # для работы с данными
import gspread  # работа google sheets
from oauth2client.service_account import ServiceAccountCredentials as sac  # авторизация в google
import requests  # обращаемся к сайту цб рф и телеграм
import xml.etree.ElementTree as ET  # работаем с полученной информацией
from datetime import datetime  # для проверки дат

"""
Исрафилов Руслан,
выполнение тестового задания,
время, потраченное на работу ~8-10 чаcов.

Реализация постоянной работы скрипта сделана через While 1: и таймер задержки.
"""

# константы
TABLE_NAME = 'TEST'  # имя  Google таблицы
HOLD_SCRIPT = 15  # время в сек  для обновления БД


# стандартный бот для отправки сообщения в телеграм
def send_telegram(text: str):
    token = "5494186151:AAGUSqhue9VMw0TPRjlJ1xrxmYdQhnoY40k"
    url = "https://api.telegram.org/bot"
    channel_id = "110642781"
    url += token
    method = url + "/sendMessage"

    r = requests.post(method, data={
        "chat_id": channel_id,
        "text": text
    })

    if r.status_code != 200:  # проверка на статус код, при != 200, поднимаем ошибку
        raise Exception("post_text error")


# 4.a
def check_date(i: int):
    """
        выполнение пункта задания 4.a - b. Разработка функционала проверки соблюдения «срока поставки» из таблицы. В случае, если срок прошел, скрипт отправляет уведомление в Telegram.
        функция вызывается для проверки срока, если срок прошел отправляет сообщение через бота в телеграмм
        для работы необходимо использовать ID  -  channel_id конкретного человека. Сейчас используется мой ID. ( фото прилагается )

    :param i: номер строки в БД
    :return:

    """
    today = datetime.now()
    today = datetime.strftime(today, '%d.%m.%Y')
    today = datetime.strptime(today, '%d.%m.%Y')  # получаем сегодняш. дату
    cur.execute('SELECT срок_поставки FROM orders WHERE №=' + str(i))
    date_check = cur.fetchall()
    bd_dt = datetime.strptime(date_check[0][0], '%d.%m.%Y')  # получаем срок поставки тек. заказа
    if today > bd_dt:  # проверяем,  истек ли срок поставки, если да, отправляем сообщение в тг
        cur.execute('SELECT * FROM orders WHERE №=' + str(i))
        order_number = cur.fetchall()
        order_number = str(order_number[0][1])
        send_telegram(f'Срок поставки  заказа № {order_number} истек!')


def gsheet2BD(spreadsheet_name, sheet_num=0):
    """
    функция для преобразования данных с google sheets в BD PostgresSQL
    :param spreadsheet_name: имя таблица
    :param sheet_num: номер листа
    :return: возвращает количество строк (int)
    """
    global engine, conn, cur  # объявляем переменные для скрипта

    engine = create_engine(
        'postgresql://postgres:123@localhost:5432/mydb')  # подключаемся к BD через sqlalchemy так как она содержит необходимую ф-ю to_sql
    conn = psycopg2.connect(host='localhost', database='mydb', user='postgres',
                            password='123')  # подключаемся к BD через psycopg2
    cur = conn.cursor()  # устанавливаем  указатель для обращения к БД

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']  # подключаемся к google API
    credentials_path = 'titanium-flash-357012-37c4b47937d1.json'  # google key
    credentials = sac.from_json_keyfile_name(credentials_path, scope)  # используем ключ полученный в google
    client = gspread.authorize(credentials)  # авторизация  пользователя для работы с таблицами
    sheet = client.open(spreadsheet_name).get_worksheet(sheet_num).get_all_records()  # получаем все записи с таблицы
    df = pd.DataFrame.from_dict(sheet)
    df['срок_поставки'] = pd.to_datetime(df['срок_поставки'], format='%d.%m.%Y')
    df = pd.DataFrame.from_dict(sheet).to_sql('orders', engine, if_exists='replace', index=False)

    """
    # используя полученные данные вызываем метод to_sql, указывая навзание таблицы. 
    Так как данные могут обновляться в google, перезаписываем таблицу в бд.
    """
    cur.execute('ALTER TABLE %s ADD COLUMN %s INT' % ('orders', 'rubles'))  # добавляем в БД  столбец с рублями
    # cur.execute('ALTER TABLE %s ALTER COLUMN %s  TYPE DATE USING' % ('orders', 'срок_поставки'))  # добавляем в БД  столбец с рублями

    conn.commit()  # приминяем изменения в БД

    return df  # возвращаем число строк


def add_rubles(x: int):
    """

    :param x: количество строк в БД
    :return: None
    Функция добавляет данные  в рублях по курсу ЦБ в столбец rubles
    """
    for i in range(1, x + 1):
        try:
            cur.execute('SELECT * FROM orders WHERE №=' + str(i))
            rub_price = cur.fetchall()
            rub_price = str(int(rub_price[0][2]) * usd_rate)
            cur.execute('UPDATE orders SET rubles =' + rub_price + 'WHERE №=' + str(i))
            check_date(i)
        except Exception:
            continue
    conn.commit()
    conn.close()
    cur.close()


def get_usd():
    #  получаем курс с ЦБ РФ

    global usd_rate
    usd_rate = float(
        ET.fromstring(requests.get('http://www.cbr.ru/scripts/XML_daily.asp').text).find(
            "./Valute[CharCode='USD']/Value")
        .text.replace(",", ".")
        # гет запросом получаем XML файл, далее в нем находим по CharCode доллары, извлекаем текущий курс, преобразововаем в Float
    )


while 1:
    get_usd()  # получаем курс
    x = gsheet2BD(TABLE_NAME)  # заносим данные  в БД
    add_rubles(x)  # добавляем рубли и проверка на дату
    print(f'ОК, ожидаю {HOLD_SCRIPT} секунд')
    time.sleep(HOLD_SCRIPT)  # скрипт срабатыывает раз в HOLD_SCRIPT секунд
