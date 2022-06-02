from db_query_helper import MYSQL_Connection

host = '127.0.0.1'
user = 'stocks'
password = 'stocks'
database = 'stocks'

try:
	con = MYSQL_Connection(host=host, user=user, password=password, dbname=database)
except Exception as e:
	print(e)

## Test insert

columns = "ticker, name, sector, industry, exchange, exchange_short, country, isETF, outdated"
values = "'TSLA', 'Tesla', 'Tech', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'false', 'false'"
#print(columns.split())

RUN ONCE INSERT
try:
	con.table_insert('stock', values)
	print('done')
except Exception as e:
	print(e)


# INSERT IGNORE
# try:
# 	con.table_insert_unique('stock', values)
# 	print('done')
# except Exception as e:
# 	print(e)

# INSERT MULTI VALUES IGNORE
# values = [('AMD', 'Advanced Micro', 'Tech', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'false', 'false'),
# 		 	('MSFT', 'Microsoft', 'Tech', 'Tech', 'NASDAQ', 'NASDAQ', 'US', 'false', 'false')]
# print(values)
# try:
# 	con.table_insert_multi_unique('stock', columns, values)
# 	print('done')
# except Exception as e:
# 	print(e)

# SELECT QUERY TO READ TABLE
# query = 'SELECT * FROM stock'
# result = con.read_query(query)
# print(result)

# UPDATE RECORD
#con.update_record(tablename='stock', column='industry', newval='EV', wherecol='ticker', whereval='TSLA')

# DELETE RECORD
#con.delete_record(tablename='stock', column='ticker', value='MSFT')

con.close_connection()