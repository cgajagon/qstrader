#!/usr/bin/env python
# coding: utf-8

# In[158]:


import sqlite3
from sqlite3 import Error
import datetime
import pandas as pd
import dateutil.parser


# In[50]:


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    
    return conn


# In[51]:


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


# In[55]:


def create_symbol(conn, symbol):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' INSERT INTO symbols(id,symbol,description,securityType,listingExchange,isQuotable,isTradable,currency)
              VALUES(?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, symbol)
    
    return cur.lastrowid


# In[137]:


def create_candle(conn, candle):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """
 
    sql = ''' INSERT INTO candles(start,end,open,high,low,close,volume,symbol_id)
              VALUES(?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, candle)
    return cur.lastrowid


# In[141]:


def create_historical(conn, historical):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """
    sql = ''' INSERT INTO candles(start,end,open,high,low,close,volume,symbol_id)
              VALUES(?,?,?,?,?,?,?,?) '''
    
    cur = conn.cursor()
    conn.executemany(sql, historical)

    return cur.lastrowid


# In[53]:


def main():
    database = r"C:\Users\pw54700\Documents\Scripts\sqlite\db\marquet.db"
 
    sql_create_symbols_table = """CREATE TABLE IF NOT EXISTS symbols (
                                    id integer PRIMARY KEY,
                                    symbol text NOT NULL,
                                    description text NOT NULL,
                                    securityType text NOT NULL,
                                    listingExchange text NOT NULL,
                                    isQuotable text NOT NULL,
                                    isTradable text NOT NULL,
                                    currency text NOT NULL
                                    );"""

    sql_create_candles_table = """ CREATE TABLE IF NOT EXISTS candles (
                                    id integer PRIMARY KEY,
                                    start text NOT NULL,
                                    end text NOT NULL,
                                    open real NOT NULL,
                                    high real NOT NULL,
                                    low real NOT NULL,
                                    close real NOT NULL,
                                    volume integer NOT NULL,
                                    symbol_id integer NOT NULL,
                                    FOREIGN KEY (symbol_id) REFERENCES symbols (id)
                                    ); """

    # create a database connection
    conn = create_connection(database)
 
    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_symbols_table)
 
        # create tasks table
        create_table(conn, sql_create_candles_table)
    else:
        print("Error! cannot create the database connection.")
 
 
if __name__ == '__main__':
    main()


# In[64]:


def main():
    database = r"C:\Users\pw54700\Documents\Scripts\sqlite\db\marquet.db"
 
    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new symbol
        #symbol = ('9292', 'BMO', 'BANK OF MONTREAL', 'Stock', 'NYSE', 'true','true','USD');
        #symbol_id = create_symbol(conn, symbol)
 
        # tasks
        candle_1 = ('2014-01-02T00:00:00.000000-05:00','2014-01-03T00:00:00.000000-05:00',70.3, 70.78, 70.68,70.73,983609,9292)
 
        # create tasks
        create_candle(conn, candle_1)
 
 
if __name__ == '__main__':
    main()


# In[142]:


def main():
    database = r"C:\Users\pw54700\Documents\Scripts\sqlite\db\marquet.db"
 
    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new symbol
        #symbol = ('9292', 'BMO', 'BANK OF MONTREAL', 'Stock', 'NYSE', 'true','true','USD');
        #symbol_id = create_symbol(conn, symbol)
 
        # tasks
        historical = [
            ('2014-01-03T00:00:00.000000-05:00','2014-01-04T00:00:00.000000-05:00',70.3, 70.78, 70.68,70.73,983609,9292),
            ('2014-01-04T00:00:00.000000-05:00','2014-01-05T00:00:00.000000-05:00',70.3, 70.78, 70.68,70.73,983609,9292)
        ]
 
        # create historical
        create_historical(conn, historical)
 
 
if __name__ == '__main__':
    main()


# In[143]:


def select_all_candles(conn, symbol):
    """
    TODO
    """
    cur = conn.cursor()
    cur.execute("SELECT start,low,high,open,close,volume,symbol_id FROM candles WHERE symbol_id=?",(symbol,))
 
    rows = cur.fetchall()
    
    return rows


# In[150]:


def select_last_date(conn, symbol):
    """
    TODO
    """
    cur = conn.cursor()
    cur.execute("SELECT MAX (start) AS 'last_date' FROM candles WHERE symbol_id=?",(symbol,))
 
    last_date = cur.fetchone()
    
    return last_date


# In[161]:


database = r"C:\Users\pw54700\Documents\Scripts\sqlite\db\marquet.db"
columns_name = ['date','low','high','open','close','volume','symbol_id']

# create a database connection
conn = create_connection(database)
with conn:
    data = select_all_candles(conn, 9292)
    last_date = select_last_date(conn, 9292)
    print(last_date[0])
    date_parsed = dateutil.parser.parse(last_date[0])
    print(date_parsed.date())
    df = pd.DataFrame(data, columns=columns_name)


# In[145]:


df['date'] = pd.to_datetime(df['date']).dt.date
df.set_index('date', inplace=True)
df.head()


# In[ ]:




