import os
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv


load_dotenv()


def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    return client


def create_connect_db(client, db_name):
    return client[db_name]


def create_connect_collection(db, collection_name):
    return db[collection_name]


def get_api_data(url):
    response = requests.get(url)
    return response.json()


def add_data_to_collection(data, collection):
    return collection.insert_many(data)


def main():
    uri = os.getenv("MONGO_ATLAS_URL")
    client = connect_mongo(uri)
    print("Conectado no MongoDB Atlas. Criando banco de dados..")
    db = create_connect_db(client, 'db_produtos')
    print(f"Banco de dados '{db.name}' criado! Criando coleção...")
    collection = create_connect_collection(db, 'produtos')
    print(f"Coleção '{collection.name}' criada! Pegando dados da API...")

    url_data = "https://labdados.com/produtos"
    data = get_api_data(url_data)

    print(f"Quantidade de dados: {len(data)}")

    print("Adicionando documentos...")
    docs = add_data_to_collection(data, collection)
    print(f"Qtd. de Documentos Adicionados: {len(docs.inserted_ids)}")
    print(f"Qtd. de documentos na coleção '{collection.name}': {collection.count_documents({})}")
    client.close()


if __name__ == '__main__':
    main()
