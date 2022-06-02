from db_query_helper import PSQL_Connection

host = 'localhost'
user = 'postgres'
password = 'stocks'
database = 'stocks'

try:
	con = PSQL_Connection(host=host, user=user, password=password, dbname=database)
except Exception as e:
	print(e)

## Test insert

columns = 'ticker, name, sector, industry, exchange, exchange_short, country, isETF'
values = "'TSLA', 'Tesla', 'Tech', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'True'"
#print(columns.split())

# RUN ONCE INSERT
#try:
#	con.table_insert('stock', columns, values)
#except Exception as e:
#	print(e)


# INSERT IGNORE USING ON CONFLICT
#try:
#	con.table_insert_ignore('stock', columns, values)
#except Exception as e:
#	print(e)

# INSERT MULTI VALUES IGNORE ON CONFLICT
#values = [('AMD', 'Advanced Micro', 'Tech', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'False'),
#		 	('MSFT', 'Microsoft', 'Tech', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'False')]
#print(values)
#try:
#	con.table_insert_multi_unique('stock', columns, values)
#except Exception as e:
#	print(e)

# SELECT QUERY TO READ TABLE
#query = 'SELECT * FROM stock WHERE isetf = True'
#result = con.read_query(query)
#print(result)

# UPDATE RECORD
#con.update_record(tablename='stock', column1='industry', val='EV', wherecolumn='ticker', whereval='TSLA')

# UPSERT RECORDS
values = "'TSLA', 'Tesla', 'Auto', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'False'"
con.upsert_records('stock', 'ticker', columns, values)

con.close_connection()