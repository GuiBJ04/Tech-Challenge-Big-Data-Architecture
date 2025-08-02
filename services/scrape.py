import requests
import pandas as pd
import base64
import json

def get_table():
    try:
        payload = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 120,
            "index": "IBOV"
        }

        json_payload_str = json.dumps(payload, separators=(',', ':'))
        payload_b64 = base64.b64encode(json_payload_str.encode('utf-8')).decode('utf-8')
        
        url_api = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{payload_b64}"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br",
            "Host": "sistemaswebb3-listados.b3.com.br"
        }

        resposta = requests.get(url_api, headers=headers, timeout=15)
        resposta.raise_for_status()

        dados_json = resposta.json()
        lista_acoes = dados_json.get('results')

        if not lista_acoes:
            print("Não foi possível encontrar a lista de ações nos dados recebidos.")
            return None

        df = pd.DataFrame(lista_acoes)

        # Corrige os tipos numéricos
        if 'theoricalQty' in df.columns:
            df['theoricalQty'] = (
                df['theoricalQty']
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .astype(float)
            )

        if 'part' in df.columns:
            df['part'] = (
                df['part']
                .str.replace(',', '.', regex=False)
                .astype(float)
            )

        # Salva como parquet
        nome_arquivo = "carteira_ibov.parquet"
        df.to_parquet(nome_arquivo, index=False, engine='pyarrow')

        print("Dados Parquet obtidos com sucesso!")

    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro ao fazer a requisição para a B3: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao processar os dados: {e}")
