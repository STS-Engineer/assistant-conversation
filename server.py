# server.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from db import get_connection
from typing import Optional, List
from fastapi import Query



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

class ConversationSummary(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    preview: str

@app.get("/conversations")
def list_conversations(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    user_name: Optional[str] = Query(None, max_length=200),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Build WHERE
        where_clauses = []
        params = []

        if date:
            # match sur la partie date (UTC)
            where_clauses.append("date_conversation::date = %s::date")
            params.append(date)

        if user_name:
            # recherche insensible à la casse (contains)
            where_clauses.append("LOWER(user_name) LIKE LOWER(%s)")
            params.append(f"%{user_name}%")

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Total
        cur.execute(f"SELECT COUNT(*) FROM conversations{where_sql};", params)
        total = cur.fetchone()[0]

        # Items (ordre récent → ancien)
        cur.execute(
            f"""
            SELECT id, user_name, date_conversation,
                   SUBSTRING(conversation FROM 1 FOR 300) AS preview
            FROM conversations
            {where_sql}
            ORDER BY date_conversation DESC
            LIMIT %s OFFSET %s;
            """,
            params + [limit, offset]
        )
        rows = cur.fetchall()
        cur.close(); conn.close()

        items = [
            ConversationSummary(
                id=r[0],
                user_name=r[1],
                date_conversation=r[2],
                preview=r[3] or ""
            ).model_dump()
            for r in rows
        ]
        return {"items": items, "total": total}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

