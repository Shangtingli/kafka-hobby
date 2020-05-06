CREATE KEYSPACE "stock" WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} AND durable_writes = 'true';
USE stock;
CREATE TABLE stock (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol,trade_time));