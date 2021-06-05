import time
import random

from sqlalchemy import create_engine
import pandas as pd
import requests
from datetime import datetime
import datetime

# warehouse db
w_name = 'warehouse'
w_user = 'postgres'
w_pass = '1q2w3e4r'
w_host = 'warehouse'
w_port = '5432'

# bittrex-buffer db
b_name = 'buffer'
b_user = 'postgres'
b_pass = '1q2w3e4r'
b_host = 'bittrex-buffer'

# Connecto to the database
b_string = 'postgresql://{}:{}@{}/{}'.format(b_user, b_pass, b_host, b_name)
w_string = 'postgresql://{}:{}@{}:{}/{}'.format(w_user, w_pass, w_host, w_port, w_name)

# bittrex-buffer engine
db_b = create_engine(b_string)
# warehouse engine
db_w = create_engine(w_string)


def insert_market(market):
    '''
    Wstawianie wartości do tabeli Market.

    Parametry: nazwa marketu

    Zwraca: id_marketu
    '''
    query = "INSERT INTO Market(market_name) " + \
            "SELECT '" + market + "' " + \
            "WHERE NOT EXISTS (" + \
            "SELECT 1 FROM Market WHERE market_name='" + market + "'" + \
            ") RETURNING id_market;"
    id_market = db_w.execute(query)
    id_market = next(iter(id_market))[0]
    return id_market


def insert_transaction_type(transaction_type):
    '''
    Wstawianie wartości do tabeli TransactionType.

    Parametry: nazwa typu transakcji

    Zwraca: id_type
    '''
    query = "INSERT INTO TransactionType(t_type_name) " + \
            "SELECT '" + transaction_type + "' " + \
            "WHERE NOT EXISTS (" + \
            "SELECT 1 FROM TransactionType WHERE t_type_name='" + transaction_type + "'" + \
            ") RETURNING id_type;"
    id_type = db_w.execute(query)
    id_type = next(iter(id_type))[0]
    return id_type


def get_time_id(date):
    '''
    Wyszukiwanie id_time z tabeli TransactionTime dla danej daty.

    Parametry: data w formacie %Y-%m-%d np. '2021-06-01'

    Zwraca: id_time
    '''
    query = "SELECT id_time " + \
            "FROM TransactionTime " + \
            "WHERE t_date = '" + date + "';"
    id_time = db_w.execute(query)
    id_time = list(iter(id_time))
    if len(id_time) != 0:
        return id_time[0][0]
    else:
        return None


def get_market_id(market):
    '''
    Wyszukiwanie id_market z tabeli Market dla danej nazwy marketu, np. 'BTC-DOGE'

    Parametry: nazwa marketu

    Zwraca: id_market
    '''
    query = "SELECT id_market " + \
            "FROM Market " + \
            "WHERE market_name = '" + market + "';"
    id_market = db_w.execute(query)
    id_market = list(iter(id_market))
    if len(id_market) != 0:
        return id_market[0][0]
    else:
        return None


def get_transaction_type_id(transaction_type):
    '''
    Wyszukiwanie id_type z tabeli TransactionType dla danej nazwy typu transakcji, np. 'BUY'

    Parametry: nazwa typu transakcji

    Zwraca: id_type
    '''
    query = "SELECT id_type " + \
            "FROM TransactionType " + \
            "WHERE t_type_name = '" + transaction_type + "';"
    id_type = db_w.execute(query)
    id_type = list(iter(id_type))
    if len(id_type) != 0:
        return id_type[0][0]
    else:
        return None


def insert_transaction_time(date, day, month, year, quarter, day_of_week):
    '''
    Wstawianie wartości do tabeli TransactionTime.

    Parametry: data, dzień, miesiąc, rok, kwartał, dzień tygodnia
               np. '2021-06-01', 1, 'June', 2021, 2, 'Tuesday'

    Zwraca: id_time
    '''
    query = "INSERT INTO TransactionTime(t_date, " + \
            "t_day, t_month, t_year, quarter, day_of_week) " + \
            "SELECT '" + date + "', " + \
            "" + str(day) + ", " + \
            "'" + str(month) + "', " + \
            "" + str(year) + ", " + \
            "" + str(quarter) + ", " + \
            "'" + str(day_of_week) + "' " + \
            "WHERE NOT EXISTS (" + \
            "SELECT 1 FROM TransactionTime " + \
            "WHERE t_date='" + date + "') " + \
            "RETURNING id_time;"

    id_time = db_w.execute(query)
    id_time = next(iter(id_time))[0]
    return id_time


def insert_crypto_transaction(id_time, id_market, id_type, quantity, price, total):
    '''
    Wstawianie wartości to tabeli CryptoTransaction.
    '''
    query = "INSERT INTO CryptoTransaction(id_time, id_market, " + \
            "id_type, quantity, price, total) " + \
            "VALUES (" + str(id_time) + ", " + \
            str(id_market) + ", " + \
            str(id_type) + ", " + \
            str(quantity) + ", " + \
            str(price) + ", " + \
            str(total) + ");"
    db_w.execute(query)


def drop_duplicates():
    '''
    Usuwanie duplikatów z tabeli BittrexBuffer.
    Za duplikaty uważane są takie wiersze, które mają to samo id_transaction.
    id_transaction pochodzi z BittrexAPI
    '''
    query = "DELETE FROM BittrexBuffer " + \
            "WHERE id_buffer NOT IN " + \
            "(SELECT id_buffer FROM " + \
            "(SELECT DISTINCT ON (id_transaction) * FROM BittrexBuffer) AS u);"
    db_b.execute(query)


def insert_buffer_row(row):
    '''
    Wstawianie wartości do tabeli BittrexBuffer.

    Parametry: jeden wiersz z dataframe'u o kolumnach:
               ['Id', 'Date', 'Day', 'Month', 'Year',
                'Quarter', 'DayOfWeek', 'Market',
                'Quantity', 'Price', 'Total', 'OrderType']
    '''
    query = "INSERT INTO BittrexBuffer(" + \
            "id_transaction, b_date, b_day, b_month, " + \
            "b_year, quarter, day_of_week, " + \
            "market, quantity, price, " + \
            "total, order_type) " + \
            "VALUES (" + str(row["Id"]) + ", '" + row['Date'] + "', " + \
            str(row['Day']) + ", '" + \
            row['Month'] + "', " + \
            str(row['Year']) + ", " + \
            str(row['Quarter']) + ", '" + \
            row['DayOfWeek'] + "', '" + \
            row['Market'] + "', " + \
            str(row['Quantity']) + ", " + \
            str(row['Price']) + ", " + \
            str(row['Total']) + ", '" + \
            row['OrderType'] + "');"
    db_b.execute(query)


def get_transaction_max_date():
    '''
    Zwraca najnowszą datę z TransactionTime.

    Zwraca: data w formacie %Y-%m-%d np. '2021-06-01'
            lub None, jeśli tabela TransactionTime jest pusta
    '''
    query = "SELECT MAX(t_date) FROM TransactionTime;"
    result = db_w.execute(query)
    result = list(iter(result))[0][0]
    if result is not None:
        return result.strftime('%Y-%m-%d')
    return result


def get_buffer_new_dates(max_date):
    '''
    Zwraca daty z BittrexBuffer, których nie ma w TransactionTime,
    czyli takie, które są nowsze od max_date, czyli od najnowszej daty
    z TransactionTime.

    Parametry: data w formacie %Y-%m-%d np. '2021-06-01'

    Zwraca: lista dat w formacie %Y-%m-%d np. '2021-06-01',
            jeśli nie ma nowszych dat od max_date, to zwraca None
    '''
    query = "SELECT DISTINCT b_date FROM BittrexBuffer " + \
            "WHERE b_date > '" + max_date + "'::DATE;"
    result = db_b.execute(query)
    result = list(iter(result))
    if len(result) != 0:
        result = [r[0].strftime('%Y-%m-%d') for r in result]
        return result
    else:
        return None


def get_buffer_data(dates):
    '''
    Zwraca dane z BittrexBuffer pogrupowane po dacie, markecie i order_type.

    Parametry: lista dat, dla których mają być zwrócone wyniki,
               jeśli lista jest pusta, to zwraca wszystkie dane

    Zwraca: Dane z BittrexBuffer pogrupowane po dacie, markecie i order_type.
            quantity, price i total są uśredniane dla grupy.
            Dane zwracane w postaci df.
    '''
    results = []
    end = " GROUP BY b_date, b_day, b_month, b_year, " + \
          "quarter, day_of_week, market, order_type;"
    query = "SELECT b_date, b_day, b_month, " + \
            "b_year, quarter, day_of_week, market, " + \
            "AVG(quantity), AVG(price), AVG(total), order_type " + \
            "FROM BittrexBuffer "
    if len(dates) > 0:
        for date in dates:
            date_query = query + "WHERE b_date = " + date + end
            date_result = db_b.execute(date_query)
            results.extend(list(iter(date_result)))
    else:
        query += end
        results = db_b.execute(query)
    columns = ['Date', 'Day', 'Month', 'Year', 'Quarter',
               'DayOfWeek', 'Market', 'MeanQuantity', 'MeanPrice', 'MeanTotal',
               'OrderType']
    df = pd.DataFrame(columns=columns, data=results)
    return df


def add_buffer_data(markets):
    '''
    Pobiera dane z API dla podanych marketów i wstawia do BittrexBuffer.

    Parametry: lista nazw marketów
    '''
    full_df = get_bittrex_data(markets)
    for _, row in full_df.iterrows():
        insert_buffer_row(row)


def get_transaction_data():
    '''
    Zwraca wszystkie dane z tabeli CryptoTransaction.

    Zwraca: dataframe
    '''
    query = "SELECT * FROM CryptoTransaction;"
    result = db_w.execute(query)
    result = list(iter(result))
    columns = ['IdTime', 'IdMarket', 'IdType', 'Quantity', 'Price', 'Total']
    df = pd.DataFrame(columns=columns, data=result)
    return df


def add_transaction_data():
    '''
    Wstawianie danych z BittrexBuffer do CryptoTransaction.
    1. Usunięcie duplikatów z BittrexBuffer.
    2. Znalezienie najnowszej daty z CryptoTransaction.
    3. Znalezienie nowszych dat od max_date w BittrexBuffer.
        - Jeśli w CryptoTransaction nie ma żadnych danych,
          wstawione będzie wszystko z BittrexBuffer.
        - Jeśli nie ma nowych dat w BittrexBuffer, nic nie zostanie wstawione
          do CryptoTransaction.
    4. Wyciągnięcie danych z BittrexBuffer dla podanych dat.
    5. Wstawianie kolejnych wierszy do CryptoTransaction.
    '''
    drop_duplicates()
    max_date = get_transaction_max_date()
    buffer_dates = get_buffer_new_dates(max_date) if max_date is not None else []
    if buffer_dates is not None:
        buffer_df = get_buffer_data(buffer_dates)
        for _, row in buffer_df.iterrows():
            id_time = get_time_id(row['Date'].strftime('%Y-%m-%d'))
            if id_time is None:
                id_time = insert_transaction_time(row['Date'].strftime('%Y-%m-%d'), row['Day'],
                                                  row['Month'], row['Year'],
                                                  row['Quarter'], row['DayOfWeek'])
            id_type = get_transaction_type_id(row['OrderType'])
            id_type = id_type if id_type is not None else insert_transaction_type(row['OrderType'])
            id_market = get_market_id(row['Market'])
            id_market = id_market if id_market is not None else insert_market(row['Market'])
            insert_crypto_transaction(id_time, id_market, id_type, row['MeanQuantity'],
                                      row['MeanPrice'], row['MeanTotal'])


def get_bittrex_data(markets):
    '''
    Pobiera dane z Bittrex API dla podanych nazw marketów.

    Parametry: lista nazw marketów

    Zwraca: dataframe z danymi
    '''
    dfs = []
    for market_name in markets:
        market_history = requests.get(f"https://api.bittrex.com/api/v1.1/public/getmarkethistory?market={market_name}")
        market_history_json = market_history.json()["result"]
        market_history_df = pd.json_normalize(market_history_json)
        market_history_df = market_history_df[["Id", "TimeStamp", "Quantity", "Price", "Total", "OrderType"]]
        market_history_df['Market'] = market_name
        dfs.append(market_history_df)
    full_df = pd.concat(dfs)
    full_df['TimeStamp'] = pd.to_datetime(full_df["TimeStamp"],
                                          format="%Y-%m-%dT%H:%M:%S.%f")
    full_df['Day'] = full_df['TimeStamp'].dt.day
    full_df['Month'] = full_df['TimeStamp'].dt.month_name()
    full_df['Year'] = full_df['TimeStamp'].dt.year
    full_df['Quarter'] = full_df['TimeStamp'].dt.quarter
    full_df['DayOfWeek'] = full_df['TimeStamp'].dt.day_name()
    full_df['Date'] = full_df['TimeStamp'].dt.strftime('%Y-%m-%d')
    full_df.drop(columns='TimeStamp', inplace=True)
    full_df.reset_index(drop=True, inplace=True)
    # print(full_df[['Date', 'Market', 'OrderType', 'Quantity', 'Price', 'Total']].groupby(by=['Date', 'Market', 'OrderType']).mean())
    return full_df


def add_gpw_data():
    date, df = get_gpw_data()
    # df = df.head()
    insert_equity_time(date)

    for _, row in df.iterrows():
        insert_company(row)
        insert_equity(date, row)


def get_gpw_data():
    # today = datetime.date.today()
    today = datetime.date(2021, 5, 28)

    if len(pd.bdate_range(today, today)) > 0:
        today_string = f"{today.day}-{today.month}-{today.year}"
        gpw_url = f"https://www.gpw.pl/archiwum-notowan?fetch=0&type=10&instrument=&date={today_string}&show_x=Poka%C5%BC+wyniki"
        data = pd.read_html(gpw_url, decimal=',', thousands='.')
        return today, data[1]


def insert_equity_time(data):
    query = f"INSERT INTO EquityTime " \
            f"(t_date, t_day, t_month, t_year, quarter, day_of_week) " \
            f"VALUES " \
            f"('{str(data)}', '{str(data.day)}', '{str(data.month)}', {data.year}, {((data.month - 1) // 3) + 1}, {data.weekday()});"
    db_w.execute(query)


def insert_company(row):
    nazwa = row["Nazwa"]
    query = f"INSERT INTO Company " \
            f"(company_name) " \
            f"VALUES " \
            f"('{str(nazwa)}');"
    db_w.execute(query)


def insert_equity(date, row):
    date = date
    query = f"SELECT id_time FROM EquityTime WHERE t_date='{str(date)}';"
    id_time = db_w.execute(query)
    id_time = list(iter(id_time))

    if len(id_time) != 0:
        id_time = id_time[0][0]
    else:
        id_time = None

    company_name = row["Nazwa"]
    query = f"SELECT id_company FROM Company WHERE company_name='{str(company_name)}';"
    id_company = db_w.execute(query)
    id_company = list(iter(id_company))

    if len(id_company) != 0:
        id_company = id_company[0][0]
    else:
        id_company = None

    opening_price = str(row['Kurs otwarcia'])
    opening_price = opening_price.replace("\xa0", "")
    opening_price = opening_price.replace(",", ".")

    maximum_price = str(row['Kurs maksymalny'])
    maximum_price = maximum_price.replace("\xa0", "")
    maximum_price = maximum_price.replace(",", ".")

    minimum_price = str(row['Kurs minimalny'])
    minimum_price = minimum_price.replace("\xa0", "")
    minimum_price = minimum_price.replace(",", ".")

    closing_price = str(row['Kurs zamknięcia'])
    closing_price = closing_price.replace("\xa0", "")
    closing_price = closing_price.replace(",", ".")

    percent_price_change = str(row['Zmiana kursu %'])
    percent_price_change = percent_price_change.replace("\xa0", "")
    percent_price_change = percent_price_change.replace(",", ".")

    trade_volume = str(row['Wartość obrotu (w tys.)'])
    trade_volume = trade_volume.replace("\xa0", "")
    trade_volume = trade_volume.replace(",", ".")

    query = f"INSERT INTO Equity " \
            f"(id_time," \
            f" id_company," \
            f" opening_price," \
            f" maximum_price," \
            f" minimum_price," \
            f" closing_price," \
            f" percent_price_change," \
            f" trade_volume) " \
            f"VALUES " \
            f"({id_time}," \
            f" {id_company}," \
            f" {opening_price}," \
            f" {maximum_price}," \
            f" {minimum_price}," \
            f" {closing_price}," \
            f" {percent_price_change}," \
            f" {trade_volume});"
    db_w.execute(query)


def get_equity_time():
    query = "SELECT * FROM EquityTime;"
    equity_time = db_w.execute(query)
    equity_time = list(iter(equity_time))
    columns = ['IdTime', 'Date', 'Day', 'Month', 'Year', 'Quarter', 'DayOfWeek']
    df = pd.DataFrame(columns=columns, data=equity_time)
    return df


def get_company():
    query = "SELECT * FROM Company;"
    company = db_w.execute(query)
    company = list(iter(company))
    columns = ["IdCompany", "CompanyName"]
    df = pd.DataFrame(columns=columns, data=company)
    return df


def get_equity():
    query = "SELECT * FROM Equity"
    equity = db_w.execute(query)
    equity = list(iter(equity))
    columns = ["IdTime", "IdCompany", "OpeningPrice",
               "MaximumPrice", "MinimumPrice", "ClosingPrice",
               "PercentPriceChange", "TradeVolume"]
    df = pd.DataFrame(columns=columns, data=equity)
    return df


if __name__ == '__main__':
    print('Application started')

    # Wstawienie danych z GPW do bazy
    print("add gpw data")
    add_gpw_data()
    print(get_equity_time())
    print(get_company())
    print(get_equity())

    go = True
    while True:
        if go:
            add_buffer_data(['BTC-DOGE', 'BTC-LTC'])
            result = get_buffer_data([])
            print(result[['Date', 'Market', 'OrderType']])
            add_transaction_data()
            result = get_transaction_data()
            print(result)
            go = False
