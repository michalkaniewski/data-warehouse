import time
import datetime
import random

from sqlalchemy import create_engine

db_name = 'warehouse'
db_user = 'postgres'
db_pass = '1q2w3e4r'
db_host = 'warehouse'
db_port = '5432'

# Connecto to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

def add_new_row(n):
    # Insert a new number into the 'numbers' table.
    db.execute("INSERT INTO numbers (number,timestamp) "+
        "VALUES ("+
        str(n) + "," + 
        str(int(round(time.time() * 1000))) + ");")

def add_new_transaction_row(market, timestamp, quantity, price, total, order_type):
    day_of_week = datetime.datetime.strptime("2021-05-29T17:06:20.08", '%Y-%m-%dT%H:%M:%S.%f').strftime('%A')
    date = datetime.datetime.strptime("2021-05-29T17:06:20.08", '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')
    day = datetime.datetime.strptime("2021-05-29T17:06:20.08", '%Y-%m-%dT%H:%M:%S.%f').strftime('%d')
    month = datetime.datetime.strptime("2021-05-29T17:06:20.08", '%Y-%m-%dT%H:%M:%S.%f').strftime('%B')
    year = datetime.datetime.strptime("2021-05-29T17:06:20.08", '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y')
    month_number = datetime.datetime.strptime("2021-05-29T17:06:20.08", '%Y-%m-%dT%H:%M:%S.%f').strftime('%m')
    quarter = (int(month_number)-1)//3 + 1
    
    last_id = db.execute("INSERT INTO TransactionTime (t_date, t_day, t_month, t_year, quarter, day_of_week) "+
        "VALUES ("+ "'"+
        date + "'," +
        str(day) + "," + "'" +
        str(month) + "'," +
        str(year) + "," +
        str(quarter) + "," + "'" +
        str(day_of_week) + "') RETURNING id_time;")

    id_market = db.execute("SELECT id_market FROM Market WHERE market_name = '" + market + "';")
    id_market = next(iter(id_market))[0]
    id_type = db.execute("SELECT id_type FROM TransactionType WHERE t_type_name = '" + order_type + "';")
    id_type = next(iter(id_type))[0]
    id_time = next(iter(last_id))[0]
    '''
    for l, m, t in zip(last_id, id_market, id_type):
        id_time = l[0]
        id_market = m[0]
        id_type = t[0]
    '''
    db.execute("INSERT INTO CryptoTransaction (id_time, id_market, id_type, quantity, price, total) "+
        "VALUES ("+
        str(id_time) + "," +
        str(id_market) + "," +
        str(id_type) + "," + 
        str(quantity) + "," +
        str(price) + "," +
        str(total) + ");")

def get_last_row():
    # Retrieve the last number inserted inside the 'numbers'
    query = "" + \
            "SELECT number " + \
            "FROM numbers " + \
            "WHERE timestamp >= (SELECT max(timestamp) FROM numbers)" +\
            "LIMIT 1"

    result_set = db.execute(query)  
    for (r) in result_set:  
        return r[0]

if __name__ == '__main__':
    print('Application started')
    '''
    while True:
        add_new_row(random.randint(1,100000))
        print('The last value insterted is: {}'.format(get_last_row()))
        time.sleep(5)
    '''
    
    add_new_transaction_row('BTC-DOGE', "2014-07-09T03:21:20.08", 0.30802438, 0.012634, 0.00389158, 'BUY')
    add_new_transaction_row('BTC-DOGE', "2014-07-10T03:21:20.08", 0.50802438, 0.112634, 0.20389158, 'BUY')
    add_new_transaction_row('BTC-DOGE', "2014-07-11T03:21:20.08", 0.50802438, 0.112634, 0.20389158, 'SELL')
    add_new_transaction_row('USD-BTC', "2014-07-10T03:21:20.08", 0.50222438, 0.14534, 0.203589158, 'BUY')
    
    query = "SELECT c.id_time, c.id_type, c.id_market " + \
            "FROM CryptoTransaction c " + \
            "JOIN TransactionTime t ON t.id_time=c.id_time " + \
            "JOIN Market m ON m.id_market=c.id_market " + \
            "JOIN TransactionType tt ON tt.id_type=c.id_type " + \
            "WHERE m.market_name='BTC-DOGE'"
    
    results = db.execute(query)
    
    for r in results:
        print(r)