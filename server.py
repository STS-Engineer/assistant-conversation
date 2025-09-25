# server.py
from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Dict, Any
from datetime import datetime
from db import get_connection

app = FastAPI(title="Conversation Logger API", version="2.0.0")

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
# Models (sujet tree - self-referencing)
# ---------------------------
class SujetIn(BaseModel):
    conversation_id: int = Field(..., ge=1)
    sujet: str = Field(..., min_length=1)
    parent_id: Optional[int] = Field(None, ge=1)  # NEW: recursive parent

class SujetOut(BaseModel):
    id: int
    conversation_id: int
    sujet: str
    parent_id: Optional[int] = None
    created_at: datetime

class SujetSummary(BaseModel):
    id: int
    conversation_id: int
    sujet: str
    parent_id: Optional[int] = None

class SujetTreeNode(BaseModel):
    id: int
    sujet: str
    children: Optional[List["SujetTreeNode"]] = None
SujetTreeNode.model_rebuild()

class SujetTreeOut(BaseModel):
    root_id: int
    tree: SujetTreeNode

# ---------------------------
# Models (action tree - self-referencing)
# ---------------------------
Status = Literal["nouvelle", "en_cours", "bloquee", "terminee", "annulee"]

class ActionNodeIn(BaseModel):
    sujet_id: int = Field(..., ge=1)                     # link to the current subject node
    task: str
    responsible: str
    due_date: str                                        # YYYY-MM-DD
    status: Status = "nouvelle"
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    parent_action_id: Optional[int] = Field(None, ge=1)  # NEW: recursive parent

class ActionOut(BaseModel):
    id: int
    sujet_id: int
    task: str
    responsible: str
    due_date: str
    status: str
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    parent_action_id: Optional[int] = None

class ActionTreeItem(BaseModel):
    id: int
    sujet_id: int
    task: str
    responsible: str
    due_date: str
    status: str
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    children: Optional[List["ActionTreeItem"]] = None
ActionTreeItem.model_rebuild()

class ActionsTreeOut(BaseModel):
    sujet_id: int
    items: List[ActionTreeItem]

# Bulk insertion for recursive actions (optional convenience)
class ActionTreeIn(BaseModel):
    task: str
    responsible: str
    due_date: str
    status: Status = "nouvelle"
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    children: Optional[List["ActionTreeIn"]] = None
ActionTreeIn.model_rebuild()

class ActionsBulkIn(BaseModel):
    sujet_id: int = Field(..., ge=1)
    items: List[ActionTreeIn]

class ActionTreeOutNode(BaseModel):
    id: int
    children: Optional[List["ActionTreeOutNode"]] = None
ActionTreeOutNode.model_rebuild()

class ActionsBulkOut(BaseModel):
    sujet_id: int
    created: List[ActionTreeOutNode]

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

        cur.execute(
            """
            INSERT INTO conversations (user_name, conversation, date_conversation)
            VALUES (%s, %s, %s)
            RETURNING id;
            """,
            (payload.user_name.strip(), payload.conversation, date_conv)
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close(); conn.close()
        return ConversationOut(id=new_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

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
        cur.execute(
            f"""
            SELECT id, user_name, date_conversation, conversation
            FROM conversations
            {where_sql}
            ORDER BY date_conversation DESC, id DESC
            LIMIT %s OFFSET %s;
            """,
            (*params, limit, offset)
        )
        rows = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) FROM conversations {where_sql};", tuple(params))
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
        cur.execute(
            """
            SELECT id, user_name, date_conversation, conversation
            FROM conversations WHERE id=%s;
            """,
            (id,)
        )
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

        txt = row[0].replace(" , ", "\n")
        return PlainTextResponse(content=txt, media_type="text/plain")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")

# ---------------------------
# SUJETS (self-referencing)
# ---------------------------
@app.post("/sujets", response_model=SujetOut)
def create_sujet(payload: SujetIn):
    try:
        conn = get_connection(); cur = conn.cursor()

        # Check conversation exists
        cur.execute("SELECT 1 FROM conversations WHERE id=%s;", (payload.conversation_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Conversation not found")

        # If parent specified, validate it belongs to same conversation
        if payload.parent_id:
            cur.execute("SELECT conversation_id FROM sujet WHERE id=%s;", (payload.parent_id,))
            row = cur.fetchone()
            if row is None:
                cur.close(); conn.close()
                raise HTTPException(status_code=404, detail="Parent subject not found")
            if row[0] != payload.conversation_id:
                cur.close(); conn.close()
                raise HTTPException(status_code=400, detail="Parent subject belongs to a different conversation")

        cur.execute(
            """
            INSERT INTO sujet (conversation_id, sujet, parent_id, created_at)
            VALUES (%s, %s, %s, now())
            RETURNING id, conversation_id, sujet, parent_id, created_at;
            """,
            (payload.conversation_id, payload.sujet, payload.parent_id)
        )
        r = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        return SujetOut(id=r[0], conversation_id=r[1], sujet=r[2], parent_id=r[3], created_at=r[4])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

@app.get("/sujets")
def list_sujets(
    conversation_id: Optional[int] = Query(None, ge=1),
    parent_id: Optional[int] = Query(None, ge=1),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        conn = get_connection(); cur = conn.cursor()

        where = []
        params = []
        if conversation_id:
            where.append("conversation_id = %s"); params.append(conversation_id)
        if parent_id:
            where.append("parent_id = %s"); params.append(parent_id)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"""
            SELECT id, conversation_id, sujet, parent_id, created_at
            FROM sujet
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s;
            """,
            (*params, limit, offset)
        )
        rows = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) FROM sujet {where_sql};", tuple(params))
        total = cur.fetchone()[0]

        items = [SujetSummary(id=r[0], conversation_id=r[1], sujet=r[2], parent_id=r[3]) for r in rows]
        cur.close(); conn.close()
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@app.get("/sujets/{id}", response_model=SujetOut)
def get_sujet_by_id(id: int = Path(..., ge=1)):
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(
            "SELECT id, conversation_id, sujet, parent_id, created_at FROM sujet WHERE id=%s;",
            (id,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Sujet not found")
        return SujetOut(id=row[0], conversation_id=row[1], sujet=row[2], parent_id=row[3], created_at=row[4])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@app.get("/sujets/tree", response_model=SujetTreeOut)
def get_sujet_tree(root_id: int = Query(..., ge=1)):
    """
    Return the recursive subject tree starting at root_id.
    """
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SELECT id, sujet FROM sujet WHERE id=%s;", (root_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        # Load all nodes of the same conversation to rebuild tree efficiently
        cur.execute("""
            WITH root_conv AS (
              SELECT conversation_id FROM sujet WHERE id=%s
            )
            SELECT id, sujet, parent_id
            FROM sujet
            WHERE conversation_id = (SELECT conversation_id FROM root_conv)
            ORDER BY id ASC;
        """, (root_id,))
        rows = cur.fetchall()
        cur.close(); conn.close()

        nodes: Dict[int, Dict[str, Any]] = {
            r[0]: {"id": r[0], "sujet": r[1], "parent_id": r[2], "children": []}
            for r in rows
        }
        for n in nodes.values():
            pid = n["parent_id"]
            if pid and pid in nodes:
                nodes[pid]["children"].append(n)

        root = nodes[root_id]
        def to_model(n: Dict[str, Any]) -> SujetTreeNode:
            return SujetTreeNode(
                id=n["id"],
                sujet=n["sujet"],
                children=[to_model(ch) for ch in n["children"]] if n["children"] else None
            )

        return SujetTreeOut(root_id=root_id, tree=to_model(root))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tree build failed: {e}")

# ---------------------------
# ACTIONS (self-referencing)
# ---------------------------
@app.post("/actions", response_model=ActionOut)
def create_action(payload: ActionNodeIn):
    try:
        conn = get_connection(); cur = conn.cursor()

        # Validate subject
        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (payload.sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        # If parent action specified, validate it belongs to same subject (same sujet_id)
        if payload.parent_action_id:
            cur.execute("SELECT sujet_id FROM action WHERE id=%s;", (payload.parent_action_id,))
            row = cur.fetchone()
            if row is None:
                cur.close(); conn.close()
                raise HTTPException(status_code=404, detail="Parent action not found")
            if row[0] != payload.sujet_id:
                cur.close(); conn.close()
                raise HTTPException(status_code=400, detail="Parent action belongs to a different sujet")

        cur.execute("""
            INSERT INTO action (sujet_id, task, responsible, due_date, status, product_line, plant_site, parent_action_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id, sujet_id, task, responsible, due_date, status, product_line, plant_site, parent_action_id;
        """, (payload.sujet_id, payload.task, payload.responsible, payload.due_date, payload.status, payload.product_line, payload.plant_site, payload.parent_action_id))
        r = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        return ActionOut(
            id=r[0], sujet_id=r[1], task=r[2], responsible=r[3], due_date=str(r[4]),
            status=r[5], product_line=r[6], plant_site=r[7], parent_action_id=r[8]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

@app.post("/actions/bulk", response_model=ActionsBulkOut)
def create_actions_bulk(payload: ActionsBulkIn):
    """
    Optional: insert a full recursive action tree under one sujet.
    """
    try:
        conn = get_connection(); cur = conn.cursor()

        # Validate subject
        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (payload.sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        def insert_node(node: ActionTreeIn, sujet_id: int, parent_id: Optional[int]) -> ActionTreeOutNode:
            cur.execute("""
                INSERT INTO action (sujet_id, task, responsible, due_date, status, product_line, plant_site, parent_action_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id;
            """, (sujet_id, node.task, node.responsible, node.due_date, node.status, node.product_line, node.plant_site, parent_id))
            new_id = cur.fetchone()[0]
            children_out: List[ActionTreeOutNode] = []
            if node.children:
                for ch in node.children:
                    children_out.append(insert_node(ch, sujet_id, new_id))
            return ActionTreeOutNode(id=new_id, children=children_out or None)

        created: List[ActionTreeOutNode] = []
        try:
            for root in payload.items:
                created.append(insert_node(root, payload.sujet_id, None))
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

@app.get("/actions/tree", response_model=ActionsTreeOut)
def list_actions_tree_by_sujet(sujet_id: int = Query(..., ge=1)):
    """
    Return the recursive action tree for a given subject (all action roots with their nested children).
    """
    try:
        conn = get_connection(); cur = conn.cursor()

        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        cur.execute("""
            SELECT id, sujet_id, task, responsible, due_date, status, product_line, plant_site, parent_action_id
            FROM action
            WHERE sujet_id=%s
            ORDER BY id ASC;
        """, (sujet_id,))
        rows = cur.fetchall()
        cur.close(); conn.close()

        nodes: Dict[int, Dict[str, Any]] = {
            r[0]: {
                "id": r[0], "sujet_id": r[1], "task": r[2], "responsible": r[3],
                "due_date": str(r[4]), "status": r[5], "product_line": r[6], "plant_site": r[7],
                "parent_action_id": r[8], "children": []
            } for r in rows
        }
        roots: List[Dict[str, Any]] = []
        for n in nodes.values():
            pid = n["parent_action_id"]
            if pid and pid in nodes:
                nodes[pid]["children"].append(n)
            else:
                roots.append(n)

        def to_model(n: Dict[str, Any]) -> ActionTreeItem:
            return ActionTreeItem(
                id=n["id"], sujet_id=n["sujet_id"], task=n["task"], responsible=n["responsible"],
                due_date=n["due_date"], status=n["status"], product_line=n["product_line"], plant_site=n["plant_site"],
                children=[to_model(ch) for ch in n["children"]] if n["children"] else None
            )

        return ActionsTreeOut(sujet_id=sujet_id, items=[to_model(r) for r in roots])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
