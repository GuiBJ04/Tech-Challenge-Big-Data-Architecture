from fastapi import APIRouter, HTTPException
import boto3
import os
import pandas as pd
import traceback
import datetime

from services.scrape import get_table

router = APIRouter()

@router.get(
    "/pregao",
    summary="Consulta dados de pregão",
    description="Retorna dados de pregão de ações do índice IBOV diretamente da B3."
)
def get_data_production():
    try:
        now = datetime.datetime.now()
        get_table()
        file_name = "carteira_ibov.parquet"
        file_key = f'raw/date={now.strftime('%Y%m%d')}/{file_name}'

        # Upload para S3
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'fiap2025-tc2-guilherme'

        with open(file_name, 'rb') as f:
            s3.upload_fileobj(f, bucket_name, file_key)

        # Remove arquivo local
        if os.path.exists(file_name):
            os.remove(file_name)

        return {"status": "Upload realizado com sucesso para o S3"}

    except Exception as e:
        print("Erro ao processar:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
