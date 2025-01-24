import os
import mysql.connector
import pandas as pd
from dotenv import load_dotenv


load_dotenv()


def connect_sql():
    cnx = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    return cnx


def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")


def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for db in cursor:
        print(db)


def create_product_table(cursor, db_name, tb_name):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
            id VARCHAR(100),
            produto VARCHAR(100),
            categoria VARCHAR(100),
            preco FLOAT(10,2),
            frete FLOAT(10,2),
            data_compra DATE,
            vendedor VARCHAR(200),
            local VARCHAR(100),
            avaliacao INT,
            tipo_pagamento VARCHAR(100),
            qtd_parcelas INT,
            latitude FLOAT(10,2),
            longitude FLOAT(10,2),

            PRIMARY KEY (id)
        );
        """
    )


def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")

    for tb in cursor:
        print(tb)


def create_dataframe(file_path):
    return pd.read_csv(file_path)


def insert_data_to_database(cnx, cursor, db_name, tb_name, df):
    df_list = [tuple(row) for i, row in df.iterrows()]
    query = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cursor.executemany(query, df_list)
    cnx.commit()
    print(cursor.rowcount, f"dados foram inseridos na tabela {tb_name}.")


def show_data(cursor, db_name, tb_name, limit_value=3):
    cursor.execute(f"SELECT * FROM {db_name}.{tb_name} LIMIT {str(limit_value)}")

    print(f"Dados de {tb_name}:")
    for row in cursor:
        print(row)


def main():
    cnx = connect_sql()
    cursor = cnx.cursor()
    db_name = "dbprodutos"
    create_database(cursor, db_name)
    show_databases(cursor)

    tb_books = "tb_livros"
    create_product_table(cursor, db_name, tb_books)
    show_tables(cursor, db_name)

    df_books = create_dataframe("../data/books_data.csv")
    insert_data_to_database(cnx, cursor, db_name, tb_books, df_books)

    tb_products_2021_onwards = "tb_produtos_2021_em_diante"
    create_product_table(cursor, db_name, tb_products_2021_onwards)
    show_tables(cursor, db_name)

    df_products_2021_onwards = create_dataframe('../data/products_from_2021_onwards.csv')
    insert_data_to_database(cnx, cursor, db_name, tb_products_2021_onwards, df_products_2021_onwards)

    show_data(cursor, db_name, tb_books)
    show_data(cursor, db_name, tb_products_2021_onwards)


if __name__ == '__main__':
    main()
