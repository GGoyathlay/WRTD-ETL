# Большие запросы выносим в отдельный файл, откуда будем импортировать их в основной код через строковые переменные

# Список техники
fs_vehicles = '''SELECT v.name, v.type, count(*)
FROM vehicles v
WHERE replay_number = {replay_number} 
GROUP BY v.name, v.type
ORDER BY v.type, v.name'''

# Котлеты
fs_cutlets = '''SELECT f.killer, dp.nickname, count(killer), DENSE_RANK() OVER (ORDER BY COUNT(f.killer) DESC) AS rank
FROM frags f
JOIN d_players dp ON f.killer = dp.id_from_json
WHERE is_tk = FALSE AND f.replay_number = {replay_number} 
GROUP BY f.killer, dp.nickname 
ORDER BY rank
LIMIT 5'''

# Тимкиллеры
fs_tks = '''SELECT f.killer, dp.nickname, count(killer), DENSE_RANK() OVER (ORDER BY COUNT(f.killer) DESC) AS rank
FROM frags f
JOIN d_players dp ON f.killer = dp.id_from_json
WHERE is_tk = TRUE AND f.replay_number = {replay_number} 
GROUP BY f.killer, dp.nickname 
ORDER BY rank
LIMIT 5'''

# Первый килл
fs_fb = '''SELECT f.time::varchar, f.killer, f.victim, dp.nickname AS killer_nickname, dp2.nickname AS victim_nickname, killer_vehicle, victim_vehicle, distance, f.is_tk, gun 
FROM frags f
LEFT JOIN d_players dp ON f.killer = dp.id_from_json
LEFT JOIN d_players dp2 ON f.victim = dp2.id_from_json
WHERE f.replay_number = {replay_number} 
ORDER BY f.time 
LIMIT 1'''

# Последний килл
fs_lh = '''SELECT f.time::varchar, f.killer, f.victim, dp.nickname AS killer_nickname, dp2.nickname AS victim_nickname, killer_vehicle, victim_vehicle, distance, f.is_tk, gun 
FROM frags f
LEFT JOIN d_players dp ON f.killer = dp.id_from_json
LEFT JOIN d_players dp2 ON f.victim = dp2.id_from_json
WHERE f.replay_number = {replay_number} 
ORDER BY f.time DESC
LIMIT 1'''

# Самый дальний фраг
fs_ls = '''SELECT f.time::varchar, f.killer, f.victim, dp.nickname AS killer_nickname, dp2.nickname AS victim_nickname, killer_vehicle, victim_vehicle, distance, f.is_tk, gun 
FROM frags f
LEFT JOIN d_players dp ON f.killer = dp.id_from_json
LEFT JOIN d_players dp2 ON f.victim = dp2.id_from_json
WHERE f.replay_number = {replay_number} 
ORDER BY distance IS NULL, distance DESC
LIMIT 1'''

# Выжившие
fs_survivors = '''SELECT p.id_from_json, dp.nickname, side
FROM players p 
JOIN d_players dp ON p.id_from_json = dp.id_from_json
WHERE p.id_from_json NOT IN (SELECT victim FROM frags f) AND p.replay_number = {replay_number} '''

# Выжившие группировка
fs_survivors_group = '''SELECT CASE 
        WHEN side = 1 THEN ':red_square: EAST'
        WHEN side = 2 THEN ':blue_square: WEST'
        WHEN side = 3 THEN ':green_square: GUER'
        WHEN side = 4 THEN ':purple_square: CIV' 
        END 
        AS side,
        count(p.id_from_json)
FROM players p 
JOIN d_players dp ON p.id_from_json = dp.id_from_json
WHERE p.id_from_json NOT IN (SELECT victim FROM frags f) AND p.replay_number = {replay_number} 
GROUP BY side
ORDER BY count DESC '''