import requests
import os
from dotenv import load_dotenv
import json
import datetime
import csv
from time import sleep
import pandas as pd

load_dotenv()

auth_url = os.getenv("auth_url")
credencial = {"credential": os.getenv("credential"), "password": os.getenv("password")}


def get_auth_token():
    """
    Autentica na API usando POST e retorna o token
    """
    response = requests.post(auth_url, json=credencial)
    status = response.status_code

    if status == 200:
        data = response.json()
        token_api = data.get("token")

        if token_api:
            return token_api
    else:
        print("Autenticação falhou")


def count_pages():
    """
    Realiza a contagem das paginas da API
    """

    page_count = 0
    page_param = 1

    url_completed = f"{os.getenv('client_url')}?page={page_param}"

    bearer_token = get_auth_token()

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    while True:
        response = requests.get(url=url_completed, headers=headers)
        status = response.status_code

        if status == 200:
            data_json = response.json()
            get_key = data_json.get("nextPage")

            if get_key:
                url_completed = f"{os.getenv('client_url')}?page={page_param}"
            else:
                break

            page_param += 1
        page_count += 1

    return page_count


def extract_clients():
    """
    Busca dados de clientes do endpoint da API usando um token de autenticação
    """
    bearer_token = get_auth_token()
    page_number = count_pages()
    items_clients = []

    for page in range(1, page_number + 1):
        url_completed = f"{os.getenv('client_url')}?page={page}"

        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(url=url_completed, headers=headers)

        response = response.json()

        items = response.get("items", None)

        items_clients.extend(items)

        sleep(1)

    return items_clients


def transform_clients():
    """
    Transforma dados de clientes extraídos em uma lista de dicionários com informações padronizadas
    """
    dados = extract_clients()

    lista_cliente = []

    for cliente in dados:
        id_parceiro = cliente.get("_id")
        nome_parceiro = cliente.get("name")
        cpf_cnpj = cliente.get("taxId", "N/A")
        tipo_entidade = cliente.get("kind", "N/A")
        data_hora_extracao = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        parceiro = {
            "id_parceiro": id_parceiro,
            "nome_parceiro": nome_parceiro,
            "cpf_cnpj": cpf_cnpj,
            "tipo_entidade": tipo_entidade,
            "data_hora_extracao": data_hora_extracao,
        }
        lista_cliente.append(parceiro)

    return lista_cliente


def load_clients():
    """
    Carrega dados transformados de clientes em um DataFrame e os salva em um arquivo .csv
    """
    lista_cliente = transform_clients()
    df_parceiro = pd.DataFrame(lista_cliente)

    df_parceiro.to_csv("parceiros.csv", index=False)

    return df_parceiro


if __name__ == "__main__":
    print(load_clients())
