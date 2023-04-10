import mysql.connector
import configparser

def read_db_config(filename, section):
    config = configparser.ConfigParser()
    config.read(filename)

    db_config = {
        'host': config.get(section, 'host'),
        'user': config.get(section, 'user'),
        'password': config.get(section, 'password'),
        'database': config.get(section, 'database'),
    }

    return db_config

def get_columns(db_config, table_name):
    query = f"SHOW COLUMNS FROM {table_name};"
    connection = mysql.connector.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()

def calculate_checksum(db_config, table_name, id_column, range_start, range_end, columns_concat):
    query = f"""
    SELECT SUM(CRC32(CONCAT_WS('', {columns_concat})))
    FROM {table_name}
    WHERE {id_column} >= {range_start} AND {id_column} <= {range_end};
    """
    connection = mysql.connector.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]
    finally:
        connection.close()

def main():
    # 데이터베이스 접속 정보 읽기
    db1_config = read_db_config('config.ini', 'database1')
    db2_config = read_db_config('config.ini', 'database2')

    # 테이블 정보
    table_name = 'mytable'
    id_column = 'id'
    range_start = 1
    range_end = 100

    # 열 이름 가져오기
    columns = get_columns(db1_config, table_name)
    columns_concat = ', '.join(columns)

    # 체크섬 계산 및 출력
    checksum1 = calculate_checksum(db1_config, table_name, id_column, range_start, range_end, columns_concat)
    checksum2 = calculate_checksum(db2_config, table_name, id_column, range_start, range_end, columns_concat)

    print(f"Checksum for database 1: {checksum1}")
    print(f"Checksum for database 2: {checksum2}")

    if checksum1 == checksum2:
        print("Checksums match!")
    else:
        print("Checksums do not match!")

if __name__ == "__main__":
    main()
