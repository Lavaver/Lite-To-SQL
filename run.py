import sqlite3
import pymysql

def sqlite_to_mysql_type(sqlite_type):
    # 根据 SQLite 数据类型转换为 MySQL 数据类型
    if sqlite_type.startswith('INTEGER'):
        return 'INT'
    elif sqlite_type.startswith('TEXT'):
        return 'TEXT'
    elif sqlite_type.startswith('REAL'):
        return 'FLOAT'
    elif sqlite_type.startswith('BLOB'):
        return 'BLOB'
    elif sqlite_type.startswith('NUMERIC'):
        return 'DECIMAL'
    else:
        return 'VARCHAR(255)'  # 默认处理为 VARCHAR

def copy_all_tables_to_mysql(sqlite_db_path: str, sqlhost: str , sqlport: int , sqluser: str, sqlpassword: str, sqldatabase: str):
    # 连接到SQLite数据库
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    # 获取所有表名
    print("Reading database table(s)...")
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = sqlite_cursor.fetchall()

    # 连接到MySQL数据库
    mysql_conn = pymysql.connect(host=sqlhost,
                                 port=sqlport,
                                 user=sqluser,
                                 password=sqlpassword,
                                 database=sqldatabase)
    mysql_cursor = mysql_conn.cursor()

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"Copying table '{table_name}' to {sqlhost}:{sqlport}/{sqldatabase} , please wait...")

        # 从SQLite数据库读取数据
        sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = sqlite_cursor.fetchall()

        columns = []
        for column in columns_info:
            print("Discrepancies are being analyzed.")
            print(f"Column '{column[1]}' is of type '{column[2]}' in SQLite.")
            column_name = column[1]
            column_type = sqlite_to_mysql_type(column[2])  # 转换数据类型
            columns.append(f"`{column_name}` {column_type}")

        columns_str = ', '.join(columns)

        sqlite_cursor.execute(f"SELECT * FROM {table_name}")
        sqlite_data = sqlite_cursor.fetchall()

        # 创建MySQL表格（如果不存在）
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_str});"
        mysql_cursor.execute(create_table_query)
        print(f"Table '{table_name}' already created on {sqlhost}:{sqlport}/{sqldatabase}.")

        # 删除MySQL表中的所有现有记录
        mysql_cursor.execute(f"TRUNCATE TABLE `{table_name}`")

        # 将数据插入到MySQL数据库
        insert_columns_str = ', '.join([f"`{column[1]}`" for column in columns_info])
        insert_query = f"INSERT INTO `{table_name}` ({insert_columns_str}) VALUES ({', '.join(['%s'] * len(columns_info))})"
        print(f"Inserting {len(sqlite_data)} records into table '{table_name}' on {sqlhost}:{sqlport}/{sqldatabase}, please wait...")
        
        # 确保数据库不会因为无数据而报错
        if sqlite_data:
            mysql_cursor.executemany(insert_query, sqlite_data)

    # 提交更改并关闭连接
    mysql_conn.commit()
    mysql_conn.close()
    sqlite_conn.close()
    print("All tables copied successfully.")

if __name__ == "__main__":
    database_path = input("Please enter the path of the SQLite database: ")
    sqlhost = input("Please enter the host of the MySQL database: ")
    sqlport = int(input("Please enter the port of the MySQL database: "))
    sqluser = input("Please enter the user of the MySQL database: ")
    sqlpassword = input("Please enter the password of the MySQL database: ")
    sqldatabase = input("Please enter the name of the MySQL database: ")
    copy_all_tables_to_mysql(database_path, sqlhost, sqlport, sqluser, sqlpassword, sqldatabase)
