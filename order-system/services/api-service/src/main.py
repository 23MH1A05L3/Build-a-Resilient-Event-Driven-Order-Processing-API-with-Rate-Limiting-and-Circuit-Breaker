import logging
from fastapi import FastAPI

from routes.order_routes import router as order_router
from routes.product_routes import router as product_router

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Order Processing API")

app.include_router(order_router)
app.include_router(product_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
