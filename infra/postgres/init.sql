-- Postgres do projeto: um banco por serviço dono do dado (migrations via Flyway nas specs 002 e 005).
-- Executado uma única vez, na criação do volume.
CREATE DATABASE rules_db;   -- gestão de regras (spec 002)
CREATE DATABASE alerts_db;  -- notificações (spec 005)
