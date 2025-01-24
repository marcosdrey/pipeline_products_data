import os
import pandas as pd
from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
from dotenv import load_dotenv


load_dotenv()


def update_field_names(collection, mapping_fields):
    collection.update_many({}, {'$rename': mapping_fields})


def get_filtered_documents(collection, query):
    final_list = []
    for doc in collection.find(query):
        final_list.append(doc)
    return final_list


def transform_date_field(date_series):
    return pd.to_datetime(date_series, format="%d/%m/%Y").dt.strftime("%Y-%m-%d")


def save_csv(item_list, path_save):
    df = pd.DataFrame(item_list)
    df['Data da Compra'] = transform_date_field(df['Data da Compra'])
    df.to_csv(path_save, index=False)


def main():
    uri = os.getenv("MONGO_ATLAS_URL")
    client = connect_mongo(uri)
    print("Conectado no MongoDB Atlas. Conectando no banco de dados..")
    db = create_connect_db(client, 'db_produtos')
    print(f"Conectado em '{db.name}'! Conectando na coleção...")
    collection = create_connect_collection(db, 'produtos')
    print(f"Conectado em '{collection.name}'! Atualizando nomes dos campos abreviados...")
    mapping_fields = {"lat": "Latitude", "lon": "Longitude"}
    update_field_names(collection, mapping_fields)
    print(
        f"""
        {str(collection.find_one({}, {'Latitude': 1, 'Longitude': 1}))}\n
        Campos atualizados com sucesso. Filtrando registros da categoria livros...
        """
    )
    query = {"Categoria do Produto": "livros"}
    book_list = get_filtered_documents(collection, query)
    print("Documentos filtrados com sucesso! Salvando em csv...")
    save_csv(book_list, "../data/books_data.csv")

    print("Filtrando registros de produtos vendidos a partir de 2021...")
    query = {"Data da Compra": {'$regex': '/202[1-9]'}}
    list_from_2021_onwards = get_filtered_documents(collection, query)
    print("Documentos filtrados com sucesso! Salvando em csv...")
    save_csv(list_from_2021_onwards, '../data/products_from_2021_onwards.csv')


if __name__ == '__main__':
    main()
