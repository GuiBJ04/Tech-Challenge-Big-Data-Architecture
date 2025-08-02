from fastapi import FastAPI
from api.scraped_data import router as scraped_data_router

app = FastAPI(
    title="B3 Scraper", 
    description="Salvando dados do Pregão no Amazon S3",
    version="1.0"
)

app.include_router(scraped_data_router)