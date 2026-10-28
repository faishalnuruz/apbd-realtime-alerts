-- The Debezium connector reads the binlog with a dedicated account.
-- REPLICATION SLAVE/CLIENT: read the log; SELECT: read the rows.
-- LOCK TABLES + RELOAD: Debezium's consistent snapshot takes a table lock and
-- flushes, so without these the snapshot phase cannot run.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium-2026';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, LOCK TABLES ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
