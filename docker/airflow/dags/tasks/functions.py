# Тут лежат функции, которые выполняются в дагах
import html
import json
import psycopg2
import requests
import re
from tasks.queries import *
from airflow.models import Variable


# Функция для проверки наличия нового реплея
def check_replay(**kwargs):
    # Парсим страничку со списком реплеев, извелкаем айди и количество игроково
    all_replays_url = 'https://stats.wogames.info/projects/wog-a3/games/'
    # Удаляем знаки табуляции и переводы строки для облегчения парсинга
    response = requests.get(all_replays_url).text.replace('\t', '').replace('\n', '')
    replays = re.findall(r'/games/(\d+)/.*?(\d+) / \d+', response)
    # Получение переменной с айди последнего записанного в БД реплея
    query = (f'''
            SELECT replay_number
            FROM messages
            WHERE posted IS TRUE
            ORDER BY replay_number DESC
            LIMIT 1
            ''')
    result = sql_to_db(query)
    # Оставляем только те реплеи, где количество игроков больше 99
    filtered_replays = [(id_replay, players) for id_replay, players in replays if int(players) > 99]
    # Если в БД нет записей, то последним реплеем будем считать предпоследний реплей из оставшегося списка
    last_replay = result[0][0] if result else filtered_replays[1][0]
    # Выбираем те айди реплеев, которые больше последнего айди и у которых счетчик игроков больше 100
    for replay in filtered_replays:
        id_replay, players = replay
        # Если такой айди найден, то меняем глобальную переменную Airflow и возвращаем команду на запуск следующего дага
        if int(id_replay) > int(last_replay):
            # Обновляем переменную в Airflow
            Variable.set('current_replay', id_replay, description='id of current replay')
            return 'trigger_next_dag'
        # Иначе просто завершаем даг
    return 'end'


def sql_to_db(query, multiple_data=None):
    # Коннектимся к БД PostgreSQL
    connection = None
    # Получаем данные для подключения из AirFlow
    conn_param_json = Variable.get("db_conn_param", default_var=0)
    conn_param = json.loads(conn_param_json)  # Преобразуем строку JSON обратно в словарь
    # Используем значения из словаря
    host = conn_param['host']
    user = conn_param['user']
    password = conn_param['password']
    db_name = conn_param['db_name']
    port = conn_param['port']
    print(f"Connecting to database at {host}:{port} as {user}")
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            if multiple_data:
                cursor.executemany(query, multiple_data)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    except Exception as _ex:
        if str(_ex) != 'no results to fetch':
            print('[INFO] Error while working with PostgreSQL', _ex)
    finally:
        if connection:
            connection.close()


def is_exists(replay_number):
    # Здесь мы проверяем, есть ли такой реплей в базе
    rows = sql_to_db('SELECT replay_number FROM replay_main')
    for row in rows:
        if row[0] == int(replay_number):
            print('Такой реплей уже есть в базе')
            return 'end'
    return 'load_data_to_db'


def parsing_replay_html(replay_number):
    # Здесь мы по номеру миссии получаем HTML-код странички с реплеем и парсим его для сохранения данных.
    # Функция возвращает следующую инфу:
    # дата, название миссии, остров, фракции, командиры сторон (с указанием фракции), победитель,
    # численность игроков/слотов, длительность миссии, ссылка на реплей
    mission = {}
    commanders = {}
    replay_url = f'https://stats.wogames.info/games/{replay_number}/'
    response = requests.get(replay_url)
    # Удаляем знаки табуляции и переводы строки для облегчения парсинга
    html_data = response.text.replace('\t', '').replace('\n', '')
    mission['number'] = replay_number
    mission['date'] = re.search(r'от (.*?) / WOG Stats</title>', html_data).group(1)
    mission['name'] = re.search(r'href="/missions/\d+/">(.*?)</a>', html_data).group(1)
    # Здесь мы используем библиотеку html для преобразования символов на случай, если есть символы не латинского алфавита (такое встречается в названиях островов)
    mission['island'] = html.unescape(re.search(r'<th>Остров</th><td>(.*?)</td>', html_data).group(1))
    mission['factions'] = re.findall(r'Командир стороны <.*?>(.*?)</span></th>', html_data)
    for faction in mission['factions']:
        commanders[faction] = re.search(rf'{faction}</span></th><td><div class=\"position-relative\" data-toggle=\"current\"><a href=\"/projects/wog-a3/players/\d+/\">(.*?)</a>', html_data).group(1)
    mission['commanders'] = commanders
    # Проверяем, все ли стороны в реплее, если какой-то нет, то рисуем ей пустого командира
    commander_key = ['EAST','WEST','GUER','CIV']
    for key in commander_key:
        if key not in mission['commanders']:
            mission['commanders'][key] = 'None'
    mission['winner'] = re.search(r'<th>Сторона-победитель</th><td><span style=\"color: #.*?\">(.*?)</span></td>', html_data).group(1)
    mission['slots'] = re.search(r'<th>Количество игроков / слотов</th><td>.*? / (.*?)</td>', html_data).group(1)
    mission['players'] = re.search(r'<th>Количество игроков / слотов</th><td>(.*?) / .*?</td>', html_data).group(1)
    mission['start'] = re.search(r'<th>Дата и время старта миссии</th><td>.*?, (.*?)</td>', html_data).group(1)
    mission['end'] = re.search(r'<th>Дата и время окончания миссии</th><td>.*?, (.*?)</td>', html_data).group(1)
    mission['duration'] = re.search(r'<th>Длительность миссии</th><td>(.*?)</td>', html_data).group(1)
    mission['url'] = replay_url
    return mission


def parsing_replay_json(replay_number):
    # Здесь мы по номеру миссии получаем JSON-код странички с реплеем и парсим его для сохранения данных.
    # Функция возвращает следующую инфу:
    # число игроков каждой стороны, список техники, фраги, список игроков с никами и айди
    mission = {
        'count_players': {}
    }
    ###ПОТОМ УДАЛИТЬ VVV
    ###ОТЛАДОЧНЫЙ ФАЙЛ, ЧТОБЫ НЕ ГРУЗИТЬ ЖСОН КАЖДЫЙ РАЗ ПО СЕТИ
    # with open('data.json', 'r', encoding='utf-8') as file:
    #     response = json.load(file)
    ###ПОТОМ УДАЛИТЬ ^^^ И РАССКОМЕНТИРОВАТЬ VVV
    replay_url = f'https://stats.wogames.info/json/replay-data.json?game={replay_number}'
    response = requests.get(replay_url).json()
    mission['count_players']['EAST'] = response.get('factions', {}).get('1', [0, 0, 0])[2]
    mission['count_players']['WEST'] = response.get('factions', {}).get('2', [0, 0, 0])[2]
    mission['count_players']['GUER'] = response.get('factions', {}).get('3', [0, 0, 0])[2]
    mission['count_players']['CIV'] = response.get('factions', {}).get('4', [0, 0, 0])[2]
    mission['vehicles'] = response.get('vehiclesUnits')
    mission['frags'] = response.get('playersDead')
    mission['players'] = response.get('players')
    return mission


def load_data_to_db(html_data, json_data):
    # Здесь мы получаем распарсенные данные и загружаем их в БД PostgreSQL.
    # Заполняем главную таблицу
    query = (f'''
    INSERT INTO replay_main (replay_number, start_time, end_time, date, name_mission, island, commander_east,
    commander_west, commander_guer, commander_civ, winner, count_players_east, count_players_west, count_players_guer,
    count_players_civ, count_players_slots, count_players_active, duration, replay_url)
    VALUES ({html_data['number']}, '{html_data['start']}', '{html_data['end']}', to_date('{html_data['date']}', 'DD.MM.YYYY'),
    '{html_data['name']}', '{html_data['island']}', '{html_data['commanders']['EAST']}', '{html_data['commanders']['WEST']}',
    '{html_data['commanders']['GUER']}', '{html_data['commanders']['CIV']}', '{html_data['winner']}',
    '{json_data['count_players']['EAST']}', '{json_data['count_players']['WEST']}', '{json_data['count_players']['GUER']}',
    '{json_data['count_players']['CIV']}', '{html_data['slots']}', '{html_data['players']}', '{html_data['duration']}', 
    '{html_data['url']}');
    ''')
    sql_to_db(query)
    # Заполняем таблицу с техникой
    data_to_insert = []
    for id_from_json, (type_, name) in json_data['vehicles'].items():
        record = (id_from_json, html_data['number'], type_, name.replace("'", "").replace('"', ''))
        data_to_insert.append(record)
    query = (f'''
    INSERT INTO vehicles (id, replay_number, type, name)
    VALUES (%s, %s, %s, %s);
    ''')
    sql_to_db(query, data_to_insert)
    # Заполняем словарь с айди/никами игроков
    data_to_insert = []
    for id_from_json, (side, nickname, slot, squad) in json_data['players'].items():
        record = (id_from_json, nickname.replace("'", "").replace('"', ''))
        data_to_insert.append(record)
    query = (f'''
        INSERT INTO d_players (id_from_json, nickname)
        VALUES (%s, %s)
        ON CONFLICT (id_from_json) DO UPDATE SET nickname = EXCLUDED.nickname;
        ''')
    sql_to_db(query, data_to_insert)
    # Заполняем таблицу с игроками на текущей миссии
    data_to_insert = []
    for id_from_json, (side, nickname, slot, squad) in json_data['players'].items():
        record = (id_from_json, html_data['number'], side, slot)
        data_to_insert.append(record)
    query = (f'''
    INSERT INTO players (id_from_json, replay_number, side, slot)
    VALUES (%s, %s, %s, %s);
    ''')
    sql_to_db(query, data_to_insert)
    # Заполняем таблицу с фрагами
    data_to_insert = []
    for main_key, sub_dict in json_data['frags'].items():
        # Итерация по вложенным словарям
        for sub_key, values in sub_dict.items():
            record = [main_key, sub_key] + values
            data_to_insert.append(tuple(record))
    query = (f'''
    INSERT INTO frags (replay_number, time, victim, victim_vehicle, killer, killer_vehicle, gun, distance, is_tk)
    VALUES ({html_data['number']}, to_char(to_timestamp(%s), 'HH24:MI:SS')::time, %s, %s, %s, %s, %s, %s, (%s = 1));
    ''')
    sql_to_db(query, data_to_insert)


def group_vehicles(vehicles):
    # Группируем технику по типам
    # Словарь
    type_mapping = {
        'static-mortar': 'Миномет',
        'static-weapon': 'Стационарное',
        'apc': 'БМП/БТР',
        'car': 'Автомобиль',
        'tank': 'Танк',
        'truck': 'Грузовик',
        'parachute': 'Парашют',
        'plane': 'Авиация',
        'heli': 'Вертолет',
        'sea': 'Флот'
    }
    grouped_vehicles = {}
    for name, vehicle_type, quantity in vehicles:
        # Заменяем тип на новое значение, если оно есть в словаре
        new_type = type_mapping.get(vehicle_type, vehicle_type)
        # Если нет, оставляем оригинал
        if new_type not in grouped_vehicles:
            grouped_vehicles[new_type] = []
        grouped_vehicles[new_type].append((name, quantity))
    return grouped_vehicles


def data_message(replay_number):
    try:
        # Собираем данные для сообщения.
        # Берем то, что не надо высчитывать через SQL-запросы
        data = sql_to_db(f'''
        SELECT ROW_TO_JSON(t) FROM (SELECT * FROM replay_main WHERE replay_number = {replay_number}) t
        ''')
        if not data:
            raise ValueError(f"No data found for replay number: {replay_number}")
        data_list = [row[0] for row in data][0]
        if not data_list:
            raise ValueError("Data list is empty after processing.")
        # Теперь остальное, что вычисляется через SQL-запросы
        # Список техники
        data_list['vehicles'] = sql_to_db(fs_vehicles.format(replay_number=replay_number))
        # Техника по типам
        data_list['grouped_vehicles'] = group_vehicles(data_list['vehicles'])
        # Котлеты
        data_list['cutlets'] = sql_to_db(fs_cutlets.format(replay_number=replay_number))
        # Тимкиллеры
        data_list['tks'] = sql_to_db(fs_tks.format(replay_number=replay_number))
        # Первый килл
        data_list['fb'] = sql_to_db(fs_fb.format(replay_number=replay_number))
        # Последний килл
        data_list['lh'] = sql_to_db(fs_lh.format(replay_number=replay_number))
        # Самый дальний килл
        data_list['ls'] = sql_to_db(fs_ls.format(replay_number=replay_number))
        # Выжившие
        data_list['survivors'] = sql_to_db(fs_survivors.format(replay_number=replay_number))
        # Выжившие группировка
        data_list['survivors_group'] = sql_to_db(fs_survivors_group.format(replay_number=replay_number))
        # Номер реплея
        data_list['replay_number'] = replay_number
        json_data = json.dumps(data_list)
        query = (f'''
                INSERT INTO messages (replay_number, text_data, posted)
                VALUES ({data_list['replay_number']}, '{json_data}', False);
                ''')
        sql_to_db(query)
    except ValueError as e:
        print(f"Error: {e}")