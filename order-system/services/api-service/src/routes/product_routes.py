from fastapi import APIRouter
from fastapi.responses import JSONResponse

from db import get_connection

router = APIRouter()


def get_product_details_handler(product_id: str):
    """Retrieves product details. Returns (status_code, body)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id AS product_id, name, price, stock_quantity FROM products WHERE id = %s",
                (product_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return 404, {"error": "Product not found"}
    row["price"] = float(row["price"])
    return 200, row


@router.get("/api/v1/products/{product_id}")
async def get_product(product_id: str):
    status_code, payload = get_product_details_handler(product_id)
    return JSONResponse(status_code=status_code, content=payload)
