import psycopg2
import MySQLdb
import time
import datetime

# PostgreSQL database configurations
postgres_host = 'localhost'
postgres_port = '5432'
postgres_dbname = 'mydb'
postgres_user = 'myuser'
postgres_password = 'mypassword'

# MySQL database configurations
mysql_host = 'localhost'
mysql_port = 3306
mysql_dbname = 'mydb'
mysql_user = 'myuser'
mysql_password = 'mypassword'

# Connect to the PostgreSQL database
postgres_conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    dbname=postgres_dbname,
    user=postgres_user,
    password=postgres_password
)

# Create a cursor object for the PostgreSQL connection
postgres_cur = postgres_conn.cursor()

# Get all the tables from the PostgreSQL database
postgres_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")

# Connect to the MySQL database
mysql_conn = MySQLdb.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    passwd=mysql_password,
    db=mysql_dbname
)

# Create a cursor object for the MySQL connection
mysql_cur = mysql_conn.cursor()

# Loop through all the tables from the PostgreSQL database
for table in postgres_cur.fetchall():
    table_name = table[0]
    print("Migrating table:", table_name)

    # Get all the data from the table in the PostgreSQL database
    postgres_cur.execute('SELECT * FROM "{}"'.format(table_name))
    rows = postgres_cur.fetchall()

    if not rows:
        print("No data in table, skipping...")
        continue

    # Get the column names from the table in the PostgreSQL database
    postgres_cur.execute('SELECT column_name FROM information_schema.columns WHERE table_name=%s', (table_name,))
    columns = [column[0] for column in postgres_cur.fetchall()]

    # Convert the rows into a list of tuples
    values = []
    for row in rows:
        row_values = []
        for i, column in enumerate(row):
            if column is None:
                row_values.append('NULL')
                continue
            if isinstance(column, str):
                column = column.replace("'", "''")
            elif isinstance(column, unicode):
                column = column.replace("'", "''").encode('utf-8')
            elif isinstance(column, datetime.datetime):
                column = column.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(column, int):
                row_values.append(str(column))
                continue
            row_values.append("\"" + str(column) + "\"")
        values.append("(" + ",".join(row_values) + ")")

    # Insert the data into the table in the MySQL database
    #mysql_cur.execute('DROP TABLE IF EXISTS `{}`'.format(table_name))
    #mysql_cur.execute('CREATE TABLE `{}` ({})'.format(table_name, ",".join(['`{}` varchar(255)'.format(column) for column in columns])))
    print('INSERT INTO {} ({}) VALUES {}'.format(table_name, ",".join(['`{}`'.format(column) for column in columns]), ",".join(values)))
    mysql_cur.execute('INSERT INTO {} ({}) VALUES {}'.format(table_name, ",".join(['`{}`'.format(column) for column in columns]), ",".join(values)))

    mysql_conn.commit()

# Close the database connections
postgres_conn.close()
mysql_conn.close()
