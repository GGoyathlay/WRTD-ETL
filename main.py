import requests
import re
import html
import psycopg2
from dags.tasks.queries import *
from config import host, user, password, db_name, port


def sql_to_db(query, multiple_data=None):
    # Коннектимся к БД PostgreSQL
    connection = None
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

def is_404(replay_number):
    replay_url = f'https://stats.wogames.info/games/{replay_number}/'
    response = requests.get(replay_url)
    if response.status_code != 200:
        print('Такого реплея нет')
        return True
    elif '<title>Ошибка / WOG Stats</title>' in response.text:
        print('Такого реплея нет')
        return True
    else:
        return False


def is_exists(replay_number):
    # Здесь мы проверяем, есть ли такой реплей в базе
    rows = sql_to_db('SELECT replay_number FROM replay_main')
    for row in rows:
        if row[0] == replay_number:
            print('Такой реплей уже есть в базе')
            return True
    return False


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
        record = (id_from_json, html_data['number'], type_, name)
        data_to_insert.append(record)
    query = (f'''
    INSERT INTO vehicles (id, replay_number, type, name)
    VALUES (%s, %s, %s, %s);
    ''')
    sql_to_db(query, data_to_insert)
    # Заполняем словарь с айди/никами игроков
    data_to_insert = []
    for id_from_json, (side, nickname, slot, squad) in json_data['players'].items():
        record = (id_from_json, nickname)
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


def data_message(replay_number):
    # Собираем данные для сообщения.
    # Берем то, что не надо высчитывать через SQL-запросы
    data = sql_to_db(f'''
    SELECT ROW_TO_JSON(t) FROM (SELECT * FROM replay_main WHERE replay_number = {replay_number}) t
    ''')
    data_list = [row[0] for row in data][0]
    # Теперь остальное, что вычисляется через SQL-запросы
    # Список техники
    data_list['vehicles'] = sql_to_db(fs_vehicles.format(replay_number=replay_number))
    # Котлеты
    data_list['cutlets'] = sql_to_db(fs_cutlets.format(replay_number=replay_number))
    # Тимкиллеры
    data_list['tks'] = sql_to_db(fs_tks.format(replay_number=replay_number))
    # Первый килл
    data_list['fb'] = sql_to_db(fs_fb.format(replay_number=replay_number))
    # Последний килл
    data_list['lh'] = sql_to_db(fs_lh.format(replay_number=replay_number))
    # Выжившие
    data_list['survivors'] = sql_to_db(fs_survivors.format(replay_number=replay_number))
    # Выжившие группировка
    data_list['survivors_group'] = sql_to_db(fs_survivors_group.format(replay_number=replay_number))
    # Номер реплея
    data_list['replay_number'] = replay_number
    return data_list


def tks_or_not(tks):
    if not tks:
        return 'Удивительно, но тимкиллов не зафиксировано.'
    else:
        return f'''Лучшие тимкиллеры:\n{'\n'.join(f'{entry[1]} {entry[2]}' for entry in tks)}'''


def sk(k_data):
    # Проверка на то, известен ли убийца и тимкилл ли это
    tk = ''
    if k_data[0][8]:
        tk = ' Кажется, это был тимкилл.'
    if k_data[0][3]:
        return f'Жертвой стал {k_data[0][4]}, погибший от выстрела {k_data[0][3]} с расстояния {k_data[0][7]} м.{tk}'
    else:
        return f'Жертвой стал {k_data[0][4]}. Убийца неизвестен.'


def create_text(data_m):
    # Формируем список сторон (их может быть от 2 до 4, если есть пустые, то не выводим).
    sides_dict = {'east': f'EAST: {data_m['count_players_east']}, командир - {data_m['commander_east']}',
            'west' : f'WEST: {data_m['count_players_west']}, командир - {data_m['commander_west']}',
            'guer' : f'GUER: {data_m['count_players_guer']}, командир - {data_m['commander_guer']}',
            'civ' : f'CIV: {data_m['count_players_civ']}, командир - {data_m['commander_civ']}'
             }
    sides = []
    for key, value in sides_dict.items():
        if data_m[f'count_players_{key}'] > 0:
            sides.append(value)
    sides_output = '\n'.join(sides)
    text = f'''
Доступен новый реплей!
Миссия: {data_m['name_mission']}, {data_m['island']}, {data_m['date']}
Начало {data_m['start_time']}, конец {data_m['end_time']}, длительность {data_m['duration']}, {data_m['count_players_active']}/{data_m['count_players_slots']}
Стороны: 
{sides_output}
Победитель: {data_m['winner']}
Доступная техника:
{'\n'.join(f'{entry[0]} {entry[2]}' for entry in data_m['vehicles'])}
До конца миссии дожили:
{', '.join(f'{side}: {count}' for side, count in data_m['survivors_group'])}
Лучшие кибератлеты:
{'\n'.join(f'{entry[1]} {entry[2]}' for entry in data_m['cutlets'])}
{tks_or_not(data_m['tks'])}
Первый фраг произошел в {data_m['fb'][0][0]}. {sk(data_m['fb'])} 
Последний фраг случился в {data_m['lh'][0][0]}. {sk(data_m['lh'])}
С полным реплеем и статистикой миссии вы можете ознакомиться по ссылке: {data_m['replay_url']}
    '''
    query = (f'''
        INSERT INTO messages (replay_number, message, posted)
        VALUES ({replay_number}, {text}, False);
        ''')
    sql_to_db(query)
    print(text)


replay_num = 3405
# Проверяем, есть ли такой реплей на вогомесе
is_404(replay_num)
# Проверяем подключение к БД
print(sql_to_db('SELECT version();'))
# Если такого реплея нет в БД, то грузим и парсим данные
if not is_404(replay_num) and not is_exists(replay_num):
    load_data_to_db(parsing_replay_html(replay_num), parsing_replay_json(replay_num))
    # Забираем данные для сообщения из БД и формируем текст сообщения в Дискорд
create_text(data_message(replay_num))