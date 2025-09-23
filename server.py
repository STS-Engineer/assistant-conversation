# server.py
from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime
from db import get_connection

app = FastAPI(title="Conversation Logger API", version="1.4.0")

# ---------------------------
# Models (conversations)
# ---------------------------
class ConversationIn(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=200)
    conversation: str = Field(..., min_length=1)
    date_conversation: Optional[datetime] = None

class ConversationOut(BaseModel):
    id: int
    status: str = "ok"

class ConversationSummary(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    preview: str

class ConversationDetail(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    conversation: str

# ---------------------------
# Models (sujet)
# ---------------------------
class SujetIn(BaseModel):
    conversation_id: int = Field(..., ge=1)
    sujet: str = Field(..., min_length=1)

class SujetOut(BaseModel):
    id: int
    conversation_id: int
    sujet: str
    created_at: datetime

class SujetSummary(BaseModel):
    id: int
    conversation_id: int
    sujet: str

# ---------------------------
# Models (actions tree)
# ---------------------------
Status = Literal["nouvelle", "en_cours", "bloquee", "terminee", "annulee"]

class SousSousActionNodeIn(BaseModel):
    task: str
    responsible: str
    due_date: str  # YYYY-MM-DD
    status: Status = "nouvelle"
    product_line: Optional[str] = None
    plant_site: Optional[str] = None

class SousActionNodeIn(BaseModel):
    task: str
    responsible: str
    due_date: str
    status: Status = "nouvelle"
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    sous_sous_actions: Optional[List[SousSousActionNodeIn]] = None

class ActionNodeIn(BaseModel):
    task: str
    responsible: str
    due_date: str
    status: Status = "nouvelle"
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    sous_actions: Optional[List[SousActionNodeIn]] = None

class ActionsBulkIn(BaseModel):
    sujet_id: int = Field(..., ge=1)
    actions: List[ActionNodeIn]

class SousSousActionNodeOut(BaseModel):
    sous_sous_action_id: int

class SousActionNodeOut(BaseModel):
    sous_action_id: int
    sous_sous_actions: Optional[List[SousSousActionNodeOut]] = None

class ActionNodeOut(BaseModel):
    action_id: int
    sous_actions: Optional[List[SousActionNodeOut]] = None

class ActionsBulkOut(BaseModel):
    sujet_id: int
    created: List[ActionNodeOut]

class SousSousActionTreeItem(BaseModel):
    sous_sous_action_id: int
    task: str
    responsible: str
    due_date: str
    status: str
    product_line: Optional[str] = None
    plant_site: Optional[str] = None

class SousActionTreeItem(BaseModel):
    sous_action_id: int
    task: str
    responsible: str
    due_date: str
    status: str
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    sous_sous_actions: Optional[List[SousSousActionTreeItem]] = None

class ActionTreeItem(BaseModel):
    action_id: int
    task: str
    responsible: str
    due_date: str
    status: str
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    sous_actions: Optional[List[SousActionTreeItem]] = None

class ActionsTreeOut(BaseModel):
    sujet_id: int
    actions: List[ActionTreeItem]

# ---------------------------
# Health
# ---------------------------
@app.get("/health")
def health():
    return {"status": "up"}

# ---------------------------
# Save conversation
# ---------------------------
@app.post("/save-conversation", response_model=ConversationOut)
def save_conversation(payload: ConversationIn):
    try:
        conn = get_connection()
        cur = conn.cursor()

        date_conv = payload.date_conversation or datetime.utcnow()

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

# ---------------------------
# List conversations
# ---------------------------
@app.get("/conversations")
def list_conversations(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (UTC)"),
    user_name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        conn = get_connection()
        cur = conn.cursor()

        where = []
        params = []
        if date:
            where.append("DATE(date_conversation AT TIME ZONE 'UTC') = %s")
            params.append(date)
        if user_name:
            where.append("LOWER(user_name) LIKE %s")
            params.append(f"%{user_name.lower()}%")

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT id, user_name, date_conversation, conversation
            FROM conversations
            {where_sql}
            ORDER BY date_conversation DESC, id DESC
            LIMIT %s OFFSET %s;
        """
        cur.execute(sql, (*params, limit, offset))
        rows = cur.fetchall()

        # total
        sql_count = f"SELECT COUNT(*) FROM conversations {where_sql};"
        cur.execute(sql_count, tuple(params))
        total = cur.fetchone()[0]

        items: List[ConversationSummary] = []
        for (cid, uname, dconv, conv) in rows:
            preview = conv[:140]
            items.append(ConversationSummary(id=cid, user_name=uname, date_conversation=dconv, preview=preview))

        cur.close(); conn.close()
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Get conversation by id
# ---------------------------
@app.get("/conversations/{id}", response_model=ConversationDetail)
def get_conversation_by_id(id: int = Path(..., ge=1)):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_name, date_conversation, conversation
            FROM conversations WHERE id=%s;
        """, (id,))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")

        return ConversationDetail(id=row[0], user_name=row[1], date_conversation=row[2], conversation=row[3])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Export TXT
# ---------------------------
@app.get("/conversations/{id}/export.txt", response_model=None)
def export_conversation_txt(id: int = Path(..., ge=1)):
    try:
        from fastapi.responses import PlainTextResponse
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT conversation FROM conversations WHERE id=%s;", (id,))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")

        # Remplacer la virgule séparatrice par des nouvelles lignes pour l'affichage "chat"
        txt = row[0].replace(" , ", "\n")
        return PlainTextResponse(content=txt, media_type="text/plain")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")

# ---------------------------
# SUJETS
# ---------------------------
@app.post("/sujets", response_model=SujetOut)
def create_sujet(payload: SujetIn):
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Check conversation exists
        cur.execute("SELECT 1 FROM conversations WHERE id=%s;", (payload.conversation_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Conversation not found")

        # Prevent duplicate subject per conversation
        cur.execute("""
            SELECT id FROM sujet
            WHERE conversation_id=%s AND sujet=%s;
        """, (payload.conversation_id, payload.sujet))
        existing = cur.fetchone()
        if existing:
            # return existing (409 or 200 – here 200 returning existing is pragmatic)
            cur.execute("SELECT id, conversation_id, sujet, created_at FROM sujet WHERE id=%s;", (existing[0],))
            s = cur.fetchone()
            cur.close(); conn.close()
            return SujetOut(id=s[0], conversation_id=s[1], sujet=s[2], created_at=s[3])

        cur.execute("""
            INSERT INTO sujet (conversation_id, sujet, created_at)
            VALUES (%s, %s, now())
            RETURNING id, conversation_id, sujet, created_at;
        """, (payload.conversation_id, payload.sujet))
        s = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        return SujetOut(id=s[0], conversation_id=s[1], sujet=s[2], created_at=s[3])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

@app.get("/sujets")
def list_sujets(
    conversation_id: Optional[int] = Query(None, ge=1),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        conn = get_connection()
        cur = conn.cursor()

        where = []
        params = []
        if conversation_id:
            where.append("conversation_id = %s")
            params.append(conversation_id)

        where_sql = "WHERE " + " AND ".join(where) if where else ""
        cur.execute(f"""
            SELECT id, conversation_id, sujet, created_at
            FROM sujet
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s;
        """, (*params, limit, offset))
        rows = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) FROM sujet {where_sql};", tuple(params))
        total = cur.fetchone()[0]

        items = [SujetSummary(id=r[0], conversation_id=r[1], sujet=r[2]) for r in rows]
        cur.close(); conn.close()
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@app.get("/conversations/{id}/sujets", response_model=List[SujetOut])
def list_sujets_by_conversation(id: int = Path(..., ge=1)):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM conversations WHERE id=%s;", (id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Conversation not found")

        cur.execute("""
            SELECT id, conversation_id, sujet, created_at
            FROM sujet
            WHERE conversation_id=%s
            ORDER BY id ASC;
        """, (id,))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return [SujetOut(id=r[0], conversation_id=r[1], sujet=r[2], created_at=r[3]) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@app.get("/sujets/{id}", response_model=SujetOut)
def get_sujet_by_id(id: int = Path(..., ge=1)):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, conversation_id, sujet, created_at
            FROM sujet WHERE id=%s;
        """, (id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Sujet not found")
        return SujetOut(id=row[0], conversation_id=row[1], sujet=row[2], created_at=row[3])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# ACTIONS (bulk insert + tree read) via sujet_id
# ---------------------------
@app.post("/actions/bulk", response_model=ActionsBulkOut)
def create_actions_bulk(payload: ActionsBulkIn):
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Check sujet exists
        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (payload.sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        created: List[ActionNodeOut] = []
        try:
            for a in payload.actions:
                cur.execute(
                    """
                    INSERT INTO action (id_sujet, task, responsible, due_date, status, product_line, plant_site)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;
                    """,
                    (payload.sujet_id, a.task, a.responsible, a.due_date, a.status, a.product_line, a.plant_site)
                )
                action_id = cur.fetchone()[0]
                sa_out: List[SousActionNodeOut] = []

                if a.sous_actions:
                    for sa in a.sous_actions:
                        cur.execute(
                            """
                            INSERT INTO sous_action (action_id, task, responsible, due_date, status, product_line, plant_site)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            RETURNING id;
                            """,
                            (action_id, sa.task, sa.responsible, sa.due_date, sa.status, sa.product_line, sa.plant_site)
                        )
                        sous_action_id = cur.fetchone()[0]
                        ssa_out: List[SousSousActionNodeOut] = []

                        if sa.sous_sous_actions:
                            for ssa in sa.sous_sous_actions:
                                cur.execute(
                                    """
                                    INSERT INTO sous_sous_action (sous_action_id, task, responsible, due_date, status, product_line, plant_site)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                                    RETURNING id;
                                    """,
                                    (sous_action_id, ssa.task, ssa.responsible, ssa.due_date, ssa.status, ssa.product_line, ssa.plant_site)
                                )
                                ssa_out.append(SousSousActionNodeOut(sous_sous_action_id=cur.fetchone()[0]))

                        sa_out.append(SousActionNodeOut(sous_action_id=sous_action_id, sous_sous_actions=ssa_out or None))

                created.append(ActionNodeOut(action_id=action_id, sous_actions=sa_out or None))

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        cur.close(); conn.close()
        return ActionsBulkOut(sujet_id=payload.sujet_id, created=created)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

class ActionsTreeOutResp(BaseModel):
    sujet_id: int
    actions: List[ActionTreeItem]

@app.get("/actions", response_model=ActionsTreeOutResp)
def list_actions_by_sujet(sujet_id: int = Query(..., ge=1)):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        # Actions
        cur.execute("""
            SELECT id, task, responsible, due_date, status, product_line, plant_site
            FROM action
            WHERE id_sujet=%s
            ORDER BY id ASC;
        """, (sujet_id,))
        actions_rows = cur.fetchall()

        actions: List[ActionTreeItem] = []
        for (action_id, task, responsible, due_date, status, product_line, plant_site) in actions_rows:
            # Sous-actions
            cur.execute("""
                SELECT id, task, responsible, due_date, status, product_line, plant_site
                FROM sous_action
                WHERE action_id=%s
                ORDER BY id ASC;
            """, (action_id,))
            sous_rows = cur.fetchall()

            sous_items: List[SousActionTreeItem] = []
            for (sid, stask, sresp, sdue, sstatus, sprod, splant) in sous_rows:
                # Sous-sous-actions
                cur.execute("""
                    SELECT id, task, responsible, due_date, status, product_line, plant_site
                    FROM sous_sous_action
                    WHERE sous_action_id=%s
                    ORDER BY id ASC;
                """, (sid,))
                ssa_rows = cur.fetchall()
                ssa_items = [
                    SousSousActionTreeItem(
                        sous_sous_action_id=r[0], task=r[1], responsible=r[2],
                        due_date=str(r[3]), status=r[4], product_line=r[5], plant_site=r[6]
                    ) for r in ssa_rows
                ]

                sous_items.append(SousActionTreeItem(
                    sous_action_id=sid, task=stask, responsible=sresp, due_date=str(sdue),
                    status=sstatus, product_line=sprod, plant_site=splant,
                    sous_sous_actions=ssa_items or None
                ))

            actions.append(ActionTreeItem(
                action_id=action_id, task=task, responsible=responsible,
                due_date=str(due_date), status=status, product_line=product_line, plant_site=plant_site,
                sous_actions=sous_items or None
            ))

        cur.close(); conn.close()
        return ActionsTreeOutResp(sujet_id=sujet_id, actions=actions)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
