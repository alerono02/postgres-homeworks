"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os


def main():
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='02042002')

    try:
        fill_table(conn, 'employees', os.path.join("north_data", "employees_data.csv"))
        fill_table(conn, 'customers', os.path.join("north_data", "customers_data.csv"))
        fill_table(conn, 'orders', os.path.join("north_data", "orders_data.csv"))
    finally:
        conn.close()


def fill_table(conn: psycopg2.connect, table_name: str, filename: str) -> None:
    """Функция для добавления данных в таблицу из файла 'csv'"""
    with conn:
        with open(filename, 'r', encoding="utf-8") as file:
            new_list = file.readlines()  # возвращает список строчек из файла, как элемент списка
            #  Получаем список с именами колонок (без кавычек) и убираем все лишнее
            table_headers = new_list[0].replace('"', '').rstrip()
            with conn.cursor() as cur:
                for value in range(1, len(new_list)):  # перебираем список без названий колонок
                    table_values = new_list[value].replace("'", "''").replace('"', "'").rstrip()
                    sql = f"INSERT INTO {table_name} ({table_headers}) VALUES({table_values})"
                    cur.execute(sql)


if __name__ == '__main__':
    main()
