--Большой запрос в БД для создания всех таблиц и связей между ними
CREATE TABLE IF NOT EXISTS "replay_main" (
	"replay_number" int NOT NULL UNIQUE,
	"start_time" time without time zone,
	"end_time" time without time zone,
	"date" date,
	"name_mission" varchar(255),
	"island" varchar(255),
	"commander_east" varchar(255),
	"commander_west" varchar(255),
	"commander_guer" varchar(255),
	"commander_civ" varchar(255),
	"winner" varchar(255),
	"count_players_east" int,
	"count_players_west" int,
	"count_players_guer" int,
	"count_players_civ" int,
	"count_players_slots" int,
	"count_players_active" int,
	"duration" time without time zone,
	"replay_url" varchar(255),
	PRIMARY KEY ("replay_number")
);

CREATE TABLE IF NOT EXISTS "vehicles" (
	"id" serial NOT NULL,
	"replay_number" int,
	"name" varchar(255),
	"type" varchar(255),
	PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "players" (
    "id" serial NOT NULL,
	"id_from_json" int,
	"replay_number" int,
	"side" int,
	"slot" varchar(255),
	PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "d_players" (
	"id_from_json" int UNIQUE,
	"nickname" varchar(255),
	PRIMARY KEY ("id_from_json")
);

CREATE TABLE IF NOT EXISTS "frags" (
	"id" serial NOT NULL,
	"replay_number" int,
	"time" time without time zone,
	"victim_vehicle" varchar(255),
	"victim" int,
	"killer" int,
	"killer_vehicle" varchar(255),
	"gun" varchar(255),
	"distance" int,
	"is_tk" boolean,
	PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "messages" (
	"replay_number" int NOT NULL,
	"message" varchar NULL,
	"text_data" text NULL,
	"posted" boolean NULL,
	PRIMARY KEY ("replay_number")
);


ALTER TABLE "vehicles" ADD CONSTRAINT "vehicles_fk1" FOREIGN KEY ("replay_number") REFERENCES "replay_main"("replay_number");
ALTER TABLE "players" ADD CONSTRAINT "players_fk1" FOREIGN KEY ("replay_number") REFERENCES "replay_main"("replay_number");
ALTER TABLE "frags" ADD CONSTRAINT "frags_fk1" FOREIGN KEY ("replay_number") REFERENCES "replay_main"("replay_number");

ALTER TABLE "frags" ADD CONSTRAINT "frags_fk4" FOREIGN KEY ("victim") REFERENCES "d_players"("id_from_json");

ALTER TABLE "frags" ADD CONSTRAINT "frags_fk5" FOREIGN KEY ("killer") REFERENCES "d_players"("id_from_json");

ALTER TABLE "players" ADD CONSTRAINT "players_fk2" FOREIGN KEY ("id_from_json") REFERENCES "d_players"("id_from_json");

ALTER TABLE "messages" ADD CONSTRAINT "messages_fk1" FOREIGN KEY ("replay_number") REFERENCES "replay_main"("replay_number");
