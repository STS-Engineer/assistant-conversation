# server.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from db import get_connection

app = FastAPI(title="Conversation Logger API", version="1.0.0")

class ConversationIn(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=200)
    conversation: str = Field(..., min_length=1)
    date_conversation: datetime | None = None  # optionnel

class ConversationOut(BaseModel):
    id: int
    status: str = "ok"

@app.get("/health")
def health():
    return {"status": "up"}

@app.post("/save-conversation", response_model=ConversationOut)
def save_conversation(payload: ConversationIn):
    try:
        conn = get_connection()
        cur = conn.cursor()

        date_conv = payload.date_conversation or datetime.now()

        sql = """
            INSERT INTO conversations (user_name, conversation, date_conversation)
            VALUES (%s, %s, %s)
            RETURNING id;
        """
        cur.execute(sql, (payload.user_name.strip(), payload.conversation, date_conv))
        new_id = cur.fetchone()[0]

        conn.commit()
        cur.close()
        conn.close()

        return ConversationOut(id=new_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion échouée: {e}")
