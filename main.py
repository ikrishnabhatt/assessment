from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import pandas as pd
from io import StringIO
import mysql_handler

app = FastAPI()

class TransactionInput(BaseModel):
    customer_id: int
    amount: float

@app.get("/customers")
def get_customers():
    customers = mysql_handler.get_all_customers()
    return customers

@app.get("/customers/{customer_id}/transactions")
def get_transactions(customer_id: int):
    transactions = mysql_handler.get_customer_transactions(customer_id)
    if not transactions:
        raise HTTPException(status_code=404, detail="Customer or transactions not found")
    return transactions

@app.post("/transactions")
def create_transaction(tx: TransactionInput):
    if not mysql_handler.customer_exists(tx.customer_id):
        raise HTTPException(status_code=404, detail="Customer ID not found")
    mysql_handler.insert_transaction(tx.customer_id, tx.amount)
    return {"message": "Transaction added"}


@app.get("/analytics/top-spenders")
def top_spenders():
    df = mysql_handler.get_all_transactions_df()
    if df.empty:
        return []

    summary = df.groupby("customer_id", as_index=False).agg(total_spent=('amount', 'sum'))
    top2 = summary.sort_values(by="total_spent", ascending=False).head(2)
    return top2.to_dict(orient="records")

@app.get("/analytics/download-report")
def download_report():
    df = mysql_handler.get_all_transactions_df()
    if df.empty:
        raise HTTPException(status_code=404, detail="No transactions found")
    
    csv = df.to_csv(index=False)
    return StreamingResponse(StringIO(csv), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=report.csv"})
