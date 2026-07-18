from pydantic import BaseModel, Field


class OrderCreateRequest(BaseModel):
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    customer_id: str = Field(..., min_length=1)


class OrderCreateResponse(BaseModel):
    message: str
    order_id: str
