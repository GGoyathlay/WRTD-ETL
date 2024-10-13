import json
import os
import discord
from twitchAPI.twitch import Twitch
import requests
from discord.ext import commands, tasks
import datetime
import sqlite3
import psycopg2
from config import host, user, password, db_name, port

bot = commands.Bot(command_prefix='!', intents=discord.Intents.all())


@bot.command()  # Команды для бота, команда = 'название_функции+префикс'
async def test(ctx):
    await ctx.reply('Какой тебе ещё тест? IQ? На беременность? Что ты пристал ко мне?')


# Авторизация в Twitch API.
client_id = "<your_id>"
client_secret = "<your_cs>"
response = requests.post(
    f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials")
response.raise_for_status()
access_token = (response.json())["access_token"]
twitch = Twitch(client_id, client_secret)
TWITCH_STREAM_API_ENDPOINT_V5 = "https://api.twitch.tv/kraken/streams/{}"
API_HEADERS = {
    'Client-ID': client_id,
    'Accept': 'application/vnd.twitchtv.v5+json',
}


def moscow_time():
    # Определяем смещение +3 часа
    utc_plus_3 = datetime.timezone(datetime.timedelta(hours=3))
    # Получаем текущее время с учетом смещения
    current_time = datetime.datetime.now(utc_plus_3)
    # Преобразуем в строку формата ISO 8601 и обрезаем до секунд
    formatted_time = current_time.isoformat()[:19]
    return formatted_time


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


# Проверка, онлайн ли стример
def checkuser(channelname):
    headers = {"Client-Id": client_id, "Authorization": f"Bearer {access_token}"}  # Указываем переменные для запроса
    url = f"https://api.twitch.tv/helix/streams?user_login={channelname}"  # Формируем URL
    response = requests.get(url, headers=headers).json()  # Получаем ответ от Твича
    try:
        if response["data"][0]["type"] == 'live':
            return response["data"]
    except (IndexError, KeyError):  # Если ошибка, то False
        return False


# Проверка, есть ли новые клипы
def checkclips(channelname):
    now_time2 = str(datetime.datetime.now(datetime.UTC).isoformat())[:19]  # Меняем формат даты
    now_time2 = datetime.datetime.strptime(now_time2, '%Y-%m-%dT%H:%M:%S')  # Меняем формат даты
    now_time = str(datetime.datetime.now(datetime.UTC).isoformat())[:22] + 'Z'  # Меняем формат даты
    # print('now_time', now_time)
    interval = datetime.timedelta(minutes=2)  # Интервал, который вычитаем из текущего времени
    time_start = str(now_time2 - interval).replace(" ", "T") + 'Z'
    # print('time_start', time_start)
    url_for_id = f'https://api.twitch.tv/helix/users?login={channelname}'
    headers = {"Client-Id": client_id, "Authorization": f"Bearer {access_token}"}  # Указываем переменные для запроса
    response = requests.get(url_for_id, headers=headers).json()
    channel_id = response['data'][0]['id']
    url = f'https://api.twitch.tv/helix/clips?broadcaster_id={channel_id}&first=10&started_at={time_start}'  # Формируем URL
    response = requests.get(url, headers=headers).json()  # Получаем ответ от Твича
    # print(channel_id)
    try:
        return response["data"]
    except (IndexError, KeyError):  # Если ошибка, то False
        return False


def tks_or_not(tks):
    if not tks:
        return 'Удивительно, но тимкиллов не зафиксировано.'
    else:
        return f'''{'\n'.join(f'{entry[1]} **{entry[2]}**' for entry in tks)}'''


def sk(k_data):
    # Проверка на то, известен ли убийца и тимкилл ли это
    # Прописываем оружие или технику убийцы
    weapon = ' Орудие убийства неизвестно.'
    gun = k_data[0][9]
    vehicle = k_data[0][5]
    if vehicle:
        weapon = f'Орудие убийства - {vehicle}'
    if gun:
        weapon = f'Орудие убийства - {gun}'
    tk = ''
    if k_data[0][8]:
        tk = ' Кажется, это был тимкилл.'
    if k_data[0][3]:
        return f'Жертвой стал **{k_data[0][4]}**, погибший от выстрела **{k_data[0][3]}** с расстояния {k_data[0][7]} м. {weapon}.{tk}'
    else:
        return f'Жертвой стал **{k_data[0][4]}**. Убийца неизвестен.'


def create_text(data_m):
    # Формируем окончательное сообщение
    # Создаем список сторон (их может быть от 2 до 4, если есть пустые, то не выводим).
    sides_dict = {'east': f':red_square: EAST: {data_m['count_players_east']}, командир - {data_m['commander_east']}',
            'west' : f':blue_square: WEST: {data_m['count_players_west']}, командир - {data_m['commander_west']}',
            'guer' : f':green_square: GUER: {data_m['count_players_guer']}, командир - {data_m['commander_guer']}',
            'civ' : f':purple_square: CIV: {data_m['count_players_civ']}, командир - {data_m['commander_civ']}'
             }
    sides = []
    for key, value in sides_dict.items():
        if data_m[f'count_players_{key}'] > 0:
            sides.append(value)
    sides_output = '\n'.join(sides)
    text = f'''Доступен новый реплей!
Миссия: {data_m['name_mission']}, {data_m['island']}, {data_m['date']}
Начало {data_m['start_time'][:-3]}, конец {data_m['end_time'][:-3]}, длительность {data_m['duration']}, {data_m['count_players_active']}/{data_m['count_players_slots']}
Стороны: 
{sides_output}
Победитель: {data_m['winner']}
Доступная техника:
{'\n'.join(f'{entry[0]} {entry[2]}' for entry in data_m['vehicles'])}
{data_m['grouped_vehicles']}
До конца миссии дожили:
{', '.join(f'{side}: {count}' for side, count in data_m['survivors_group'])}
Лучшие кибератлеты:
{'\n'.join(f'{entry[1]} {entry[2]}' for entry in data_m['cutlets'])}
{tks_or_not(data_m['tks'])}
Первый фраг произошел в {data_m['fb'][0][0]}. {sk(data_m['fb'])} 
Последний фраг произошел в {data_m['lh'][0][0]}. {sk(data_m['lh'])}
Самый дальний фраг произошел в {data_m['ls'][0][0]}. {sk(data_m['ls'])}
С полным реплеем и статистикой миссии вы можете ознакомиться по ссылке: {data_m['replay_url']}'''
    text = text.replace("'", "").replace('"', '')
    query = (f'''
        UPDATE messages SET message = E'{text}' 
        WHERE replay_number = {data_m['replay_number']};
        ''')
    sql_to_db(query)
    print(text)
    # Создаем сообщение с оформлением и отправляем
    return text


def square(winner):
    # Создаем эмодзи с цветом стороны и прикрепляем его
    emo =''
    if winner == "EAST":
        emo = ':red_square: '
    elif winner == "WEST":
        emo = ':blue_square: '
    elif winner == "GUER":
        emo = ':green_square: '
    elif winner == "CIV":
        emo = ':purple_square: '
    return emo


def create_embed(data_m):
    # Формируем embed сообщение
    # Создаем список сторон (их может быть от 2 до 4, если есть пустые, то не выводим).
    sides_dict = {'east': f':red_square: **EAST:** {data_m['count_players_east']}, {data_m['commander_east']}',
            'west' : f':blue_square: **WEST:** {data_m['count_players_west']}, {data_m['commander_west']}',
            'guer' : f':green_square: **GUER:** {data_m['count_players_guer']}, {data_m['commander_guer']}',
            'civ' : f':purple_square: **CIV:** {data_m['count_players_civ']}, {data_m['commander_civ']}'
             }
    sides = []
    for key, value in sides_dict.items():
        if data_m[f'count_players_{key}'] > 0:
            sides.append(value)
    sides_output = '\n'.join(sides)
    embeds = []
    # Общая статистика
    embed_stats = discord.Embed(title=":chart_with_downwards_trend: Общая статистика:", color=2326507)
    embed_stats.add_field(name="Информация", value=f"**Дата:** {data_m['date']}\n**Миссия:** {data_m['name_mission']}\n**Остров:** {data_m['island']}", inline=True)
    embed_stats.add_field(name="Стороны и командиры", value=f"{sides_output}", inline=True)
    embed_stats.add_field(name="Всего игроков", value=f"{data_m['count_players_active']}/{data_m['count_players_slots']}", inline=True)
    embed_stats.add_field(name="Время", value=f"Начало {data_m['start_time'][:-3]},\nконец {data_m['end_time'][:-3]},\nдлительность {data_m['duration']}", inline=True)
    embed_stats.add_field(name="Итоги", value=f"Победитель:\n{square(data_m['winner'])} **{data_m['winner']}**", inline=True)
    embed_stats.add_field(name="До конца миссии дожили", value=f"{'\n'.join(f'**{side}:** {count}' for side, count in data_m['survivors_group'])}", inline=True)
    embeds.append(embed_stats)
    # Личная статистика
    embed_personal = discord.Embed(title=":pencil: Личная статистика:", color=2326507)
    embed_personal.add_field(name="Лучшие кибератлеты", value=f"{'\n'.join(f'{entry[1]} **{entry[2]}**' for entry in data_m['cutlets'])}", inline=True)
    embed_personal.add_field(name="Лучшие тимкиллеры", value=f"{tks_or_not(data_m['tks'])}", inline=True)
    embeds.append(embed_personal)
    # Выдающиеся фраги
    embed_frags = discord.Embed(title=":gun: Выдающиеся фраги:", color=2326507)
    embed_frags.add_field(name="Первый фраг", value=f"Произошел в {data_m['fb'][0][0]}. {sk(data_m['fb'])}", inline=True)
    embed_frags.add_field(name="Последний фраг", value=f"Произошел в {data_m['lh'][0][0]}. {sk(data_m['lh'])}", inline=True)
    embed_frags.add_field(name="Самый дальний фраг", value=f"Произошел в {data_m['ls'][0][0]}. {sk(data_m['ls'])}", inline=True)
    embeds.append(embed_frags)
    # Доступная техника
    embed_vehicles = discord.Embed(title=":truck: Доступная техника:", color=2326507)
    for vehicle_type, items in data_m['grouped_vehicles'].items():
        # Формируем строку для каждого типа техники
        vehicles_list = "\n".join([f"{quantity}x{name}" for name, quantity in items])
        embed_vehicles.add_field(name=vehicle_type, value=vehicles_list, inline=True)
    embeds.append(embed_vehicles)
    # Ссылка на полный реплей
    embed_instruction = discord.Embed(description=f"С полным реплеем и статистикой миссии можно ознакомиться по ссылке: {data_m['replay_url']}/",color=0x0099ff)
    embeds.append(embed_instruction)
    return embeds


# Отправка сообщения с клипом в дискорд
async def send_clip_alert(data_clip):
    channel = bot.get_channel(<your_channel_name>)
    await channel.send(
        f'[{data_clip['creator_name']}](<http://www.twitch.tv/{data_clip['creator_name']}>) сделал новый [клип]({data_clip['url']}) \"{data_clip['title']}\"')


# Задачи, которые надо выполнять в лупе
@tasks.loop(seconds=60)
# С периодичностью в N секунд проверяем, онлайн ли стример, если да, то отправляем сообщение в дискорд
async def send_stream_online():
    channel = bot.get_channel(<your_channel_name>)  # Получаем айди канала для сообщения
    data_stream = checkuser('<streamer_name>')
    if isinstance(data_stream, bool):
        pass
    else:
        now_time = str(datetime.datetime.now(datetime.UTC).isoformat())[:19]
        # print('now_time', now_time)
        old_time_str = cur.execute('SELECT start_stream_message_datetime FROM alerts WHERE id = ?', (1,)).fetchone()[0]
        old_time = datetime.datetime.strptime(old_time_str, '%Y-%m-%dT%H:%M:%S')
        # print('old_time', old_time)
        now_time2 = datetime.datetime.strptime(now_time, '%Y-%m-%dT%H:%M:%S')
        diff_time = now_time2 - old_time
        max_interval = datetime.timedelta(hours=8)
        # print('diff_time', diff_time)
        # print('max_interval', max_interval)
        thumbnail_url = data_stream[0]['thumbnail_url'].replace("{width}", "854").replace("{height}", "480")
        # print(thumbnail_url)
        if diff_time > max_interval:
            print('Стрим онлайн')
            cur.execute('UPDATE alerts SET start_stream_message_datetime = ? WHERE id = ?', (now_time, 1))
            base.commit()
            emb = discord.Embed(title='<streamer_name>', url="https://www.twitch.tv/<streamer_name>",
                                description=f':red_circle: Запущен поток **{data_stream[0]['game_name']}** \n {data_stream[0]['title']}')
            emb.set_image(url=thumbnail_url)
            await channel.send('@here Стрим онлайн! <https://www.twitch.tv/<streamer_name>>', embed=emb)


# С периодичностью в N секунд проверяем, есть ли новые клипы, если да, то отправляем сообщение на сервер
@tasks.loop(seconds=120)
async def send_clips():
    data_clips = checkclips('<streamer_name>')
    if isinstance(data_clips, bool):
        pass
    else:
        cur.execute('SELECT * FROM clips ORDER BY id DESC LIMIT 10')
        rows = cur.fetchall()
        for i in data_clips:
            f = 0
            for row in rows:
                if row[1] == i['url']:
                    f = 1
                    break
            if f == 0:
                cur.execute('INSERT into clips (clip_url, clip_id) VALUES (?, ?)', (i['url'], i['id']))
                base.commit()
                print(moscow_time(), 'Есть новый клип:', i['url'])
                await send_clip_alert(i)


# С периодичностью в N секунд проверяем, есть ли новые реплеи, если да, то отправляем сообщение на сервер
@tasks.loop(seconds=60)
async def check_replay():
    query = 'SELECT text_data, replay_number FROM messages WHERE posted IS NOT TRUE LIMIT 1'
    result = sql_to_db(query)
    if result:
        data_m = result[0][0]
        replay_number = result[0][1]
        data_dict = json.loads(data_m)
        channel = bot.get_channel(<channel_id>)
        create_text(data_dict)
        # Отправка всех Embed-сообщений в канал
        await channel.send(content=":incoming_envelope: Доступен новый реплей!",embeds=create_embed(data_dict))
        print(moscow_time(), f'Сообщение о реплее https://stats.wogames.info/games/{replay_number} отправлено')
        query = f'UPDATE messages SET posted = TRUE WHERE replay_number = {replay_number}'
        sql_to_db(query)


@bot.event
async def on_ready():  # Действия бота при активации
    # Отладочное сообщение
    print(moscow_time(), 'Бот запущен')
    # Коннектимся к БД
    global base, cur
    base = sqlite3.connect('bots_database.db')
    cur = base.cursor()
    if base:
        print(moscow_time(), 'Подключение к БД успешно')
    # Запуск проверки стрима на онлайн
    send_clips.start()
    send_stream_online.start()
    check_replay.start()


bot.run(os.getenv('TOKEN'))
