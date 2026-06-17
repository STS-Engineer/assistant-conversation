# server.py
import os
from typing import Any, Optional, List, Literal
from datetime import datetime, date

import httpx
from fastapi import FastAPI, HTTPException, Query, Path, Request, APIRouter
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware

from db import get_connection

app = FastAPI(title="Conversation Logger API", version="1.6.0")

origins = [
    "https://meeting-minute-ia.azurewebsites.net"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===========================================================================
# PROXY ACTION PLAN  ->  API externe
#   https://sales-feedback.azurewebsites.net
#
# Remplace les anciennes routes /v2/... qui écrivaient directement dans la
# base "Action Plan". Ici on ne fait que relayer les requêtes HTTP : le corps
# JSON du front est transmis tel quel, la réponse de l'API externe est renvoyée
# sans transformation. Pas besoin de connaître le format exact du payload.
# ===========================================================================

ACTION_PLAN_API_BASE = os.environ.get(
    "ACTION_PLAN_API_BASE",
    "https://sales-feedback.azurewebsites.net",
).rstrip("/")

# Timeout généreux (Azure App Service peut avoir un cold start).
HTTP_TIMEOUT = httpx.Timeout(30.0, connect=10.0)

action_plan_router = APIRouter(prefix="/action-plan", tags=["action-plan (proxy)"])


def _forward_headers(request: Request) -> dict:
    """Transmet quelques en-têtes utiles (auth) vers l'API externe."""
    out: dict = {}
    for name in ("authorization", "x-api-key", "accept-language"):
        val = request.headers.get(name)
        if val:
            out[name] = val
    return out


async def _proxy(method: str, path: str, *, request: Request,
                 json_body: Any = None, params: dict = None):
    """Appelle l'API externe et renvoie son JSON (ou propage son erreur)."""
    url = f"{ACTION_PLAN_API_BASE}{path}"
    headers = _forward_headers(request)
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.request(method, url, json=json_body,
                                        params=params, headers=headers)
    except httpx.ConnectError:
        raise HTTPException(status_code=502,
                            detail=f"Impossible de joindre l'API Action Plan ({ACTION_PLAN_API_BASE}).")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504,
                            detail="L'API Action Plan n'a pas répondu à temps.")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erreur réseau vers Action Plan: {e}")

    try:
        payload = resp.json()
    except ValueError:
        payload = {"raw": resp.text}

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=payload)
    return payload


@action_plan_router.post("/plans")
async def create_plan(request: Request):
    """POST -> POST {API}/api/v2/plans (corps JSON transmis tel quel)."""
    body = await request.json()
    return await _proxy("POST", "/api/v2/plans", request=request, json_body=body)


@action_plan_router.get("/plans")
async def list_plans(request: Request):
    """GET -> GET {API}/api/v2/plans (query params transmis)."""
    params = dict(request.query_params)
    return await _proxy("GET", "/api/v2/plans", request=request, params=params)


@action_plan_router.get("/plans/{sujet_id}")
async def get_plan(sujet_id: int, request: Request):
    """GET -> GET {API}/api/v2/plans/{sujet_id} (arbre complet d'un plan)."""
    return await _proxy("GET", f"/api/v2/plans/{sujet_id}", request=request)


@action_plan_router.post("/actions")
async def create_action(request: Request):
    """
    POST -> POST {API}/api/v2/actions (ajoute UNE action à un sujet existant).
    Corps attendu : { "sujet_id": int, "parent_action_id": int|null,
    "titre": str, "status": "open"|"closed"|"blocked", "due_date": "YYYY-MM-DD", ... }
    """
    body = await request.json()
    return await _proxy("POST", "/api/v2/actions", request=request, json_body=body)


@action_plan_router.patch("/actions/{action_id}/status")
async def update_action_status(action_id: int, request: Request):
    """PATCH -> PATCH {API}/api/v2/actions/{id}/status. Corps : { "status": "open|closed|blocked" }."""
    body = await request.json()
    return await _proxy("PATCH", f"/api/v2/actions/{action_id}/status",
                        request=request, json_body=body)


@action_plan_router.get("/schema")
async def get_schema(request: Request):
    """GET -> GET {API}/api/v2/schema (format attendu + exemple de payload)."""
    return await _proxy("GET", "/api/v2/schema", request=request)


@action_plan_router.get("/_ping")
async def ping_action_plan(request: Request):
    """Vérifie que l'API externe répond -> GET {API}/health."""
    return await _proxy("GET", "/health", request=request)


# Branche les routes /action-plan/* dans l'app
app.include_router(action_plan_router)


# ---------------------------
# Models (supplier conversations)
# ---------------------------
class SupplierConversationIn(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=200)
    conversation: str = Field(..., min_length=1)
    date_conversation: Optional[datetime] = None
    supplier_name: str = Field(..., min_length=1, max_length=255)
    assistant_name: str = Field(..., min_length=1, max_length=255)

class SupplierConversationOut(BaseModel):
    id: int
    status: str = "ok"

class SupplierConversationSummary(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    preview: str
    supplier_name: str
    assistant_name: str

class SupplierConversationDetail(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    conversation: str
    supplier_name: str
    assistant_name: str

# ---------------------------
# Models (conversations)
# ---------------------------
class ConversationIn(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=200)
    conversation: str = Field(..., min_length=1)
    date_conversation: Optional[datetime] = None
    client_name: Optional[str] = None
    assistant_name: Optional[str] = None

class ConversationOut(BaseModel):
    id: int
    status: str = "ok"

class ConversationSummary(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    preview: str
    client_name: Optional[str] = None
    assistant_name: Optional[str] = None

class ConversationDetail(BaseModel):
    id: int
    user_name: str
    date_conversation: datetime
    conversation: str
    client_name: Optional[str] = None
    assistant_name: Optional[str] = None

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
# Models (sous-sujet)
# ---------------------------
class SousSujetIn(BaseModel):
    sujet_id: int = Field(..., ge=1)
    titre: str = Field(..., min_length=1)

class SousSujetOut(BaseModel):
    id: int
    sujet_id: int
    titre: str
    created_at: datetime

class SousSujetSummary(BaseModel):
    id: int
    sujet_id: int
    titre: str

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
    sous_sujet_id: int = Field(..., ge=1)
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

class SousSujetTreeItem(BaseModel):
    sous_sujet_id: int
    titre: str
    actions: Optional[List[ActionTreeItem]] = None

class SujetTreeOut(BaseModel):
    sujet_id: int
    sous_sujets: List[SousSujetTreeItem]

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
            INSERT INTO conversations (user_name, conversation, date_conversation, client_name, assistant_name)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (payload.user_name.strip(), payload.conversation, date_conv, payload.client_name, payload.assistant_name),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close(); conn.close()
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
    client_name: Optional[str] = None,
    assistant_name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        conn = get_connection()
        cur = conn.cursor()
        where, params = [], []
        if date:
            where.append("DATE(date_conversation AT TIME ZONE 'UTC') = %s")
            params.append(date)
        if user_name:
            where.append("LOWER(user_name) LIKE %s")
            params.append(f"%{user_name.lower()}%")
        if client_name:
            where.append("LOWER(client_name) LIKE %s")
            params.append(f"%{client_name.lower()}%")
        if assistant_name:
            where.append("LOWER(assistant_name) = %s")
            params.append(assistant_name.lower())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"""
            SELECT id, user_name, date_conversation, conversation, client_name, assistant_name
            FROM conversations
            {where_sql}
            ORDER BY date_conversation DESC, id DESC
            LIMIT %s OFFSET %s;
            """,
            (*params, limit, offset),
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM conversations {where_sql};", tuple(params))
        total = cur.fetchone()[0]
        items: List[ConversationSummary] = []
        for (cid, uname, dconv, conv, cname, aname) in rows:
            preview = conv[:140]
            items.append(ConversationSummary(id=cid, user_name=uname, date_conversation=dconv, preview=preview, client_name=cname, assistant_name=aname))
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
            SELECT id, user_name, date_conversation, conversation, client_name, assistant_name
            FROM conversations WHERE id=%s;
            """,
            (id,),
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")
        return ConversationDetail(id=row[0], user_name=row[1], date_conversation=row[2], conversation=row[3], client_name=row[4], assistant_name=row[5])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Get all conversations by client_name (case-insensitive LIKE)
# ---------------------------
@app.get("/conversations/client/{client_name}")
def get_conversations_by_client(
    client_name: str = Path(..., min_length=1),
):
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        user_name,
                        assistant_name,
                        client_name,
                        date_conversation,
                        conversation,
                        COUNT(*) OVER() AS total_count
                    FROM conversations
                    WHERE LOWER(client_name) LIKE %s
                    ORDER BY date_conversation DESC, id DESC;
                    """,
                    (f"%{client_name.lower()}%",),
                )
                rows = cur.fetchall()

        items = []
        total = 0
        for (cid, uname, aname, cname, dconv, conv, tot) in rows:
            total = tot
            preview = (conv[:160] + "…") if isinstance(conv, str) and len(conv) > 160 else conv
            items.append({
                "id": cid,
                "user_name": uname,
                "assistant_name": aname,
                "client_name": cname,
                "date_conversation": dconv,
                "preview": preview,
            })

        return {"items": items, "total": total if rows else 0}

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
# SUJETS
# ---------------------------
@app.post("/sujets", response_model=SujetOut)
def create_sujet(payload: SujetIn):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM conversations WHERE id=%s;", (payload.conversation_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Conversation not found")

        # one subject per conversation (return existing if same text)
        cur.execute(
            """
            SELECT id FROM sujet
            WHERE conversation_id=%s AND sujet=%s;
            """,
            (payload.conversation_id, payload.sujet),
        )
        existing = cur.fetchone()
        if existing:
            cur.execute("SELECT id, conversation_id, sujet, created_at FROM sujet WHERE id=%s;", (existing[0],))
            s = cur.fetchone()
            cur.close(); conn.close()
            return SujetOut(id=s[0], conversation_id=s[1], sujet=s[2], created_at=s[3])

        cur.execute(
            """
            INSERT INTO sujet (conversation_id, sujet, created_at)
            VALUES (%s, %s, now())
            RETURNING id, conversation_id, sujet, created_at;
            """,
            (payload.conversation_id, payload.sujet),
        )
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
    # FIXED: lit bien la table `sujet` (et non `sous_sujet`) et renvoie SujetSummary.
    try:
        conn = get_connection()
        cur = conn.cursor()
        where, params = [], []
        if conversation_id:
            where.append("conversation_id = %s")
            params.append(conversation_id)
        where_sql = "WHERE " + " AND ".join(where) if where else ""
        cur.execute(
            f"""
            SELECT id, conversation_id, sujet, created_at
            FROM sujet
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s;
            """,
            (*params, limit, offset),
        )
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
        cur.execute(
            """
            SELECT id, conversation_id, sujet, created_at
            FROM sujet
            WHERE conversation_id=%s
            ORDER BY id ASC;
            """,
            (id,),
        )
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
        cur.execute(
            """
            SELECT id, conversation_id, sujet, created_at
            FROM sujet WHERE id=%s;
            """,
            (id,),
        )
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
# SOUS-SUJETS
# ---------------------------
@app.post("/sous-sujets", response_model=SousSujetOut)
def create_sous_sujet(payload: SousSujetIn):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (payload.sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        cur.execute(
            """
            INSERT INTO sous_sujet (sujet_id, titre, created_at)
            VALUES (%s, %s, now())
            RETURNING id, sujet_id, titre, created_at;
            """,
            (payload.sujet_id, payload.titre),
        )
        r = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        return SousSujetOut(id=r[0], sujet_id=r[1], titre=r[2], created_at=r[3])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

@app.get("/sous-sujets")
def list_sous_sujets(
    sujet_id: Optional[int] = Query(None, ge=1),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        conn = get_connection()
        cur = conn.cursor()
        where, params = [], []
        if sujet_id:
            where.append("sujet_id = %s")
            params.append(sujet_id)
        where_sql = "WHERE " + " AND ".join(where) if where else ""
        cur.execute(
            f"""
            SELECT id, sujet_id, titre, created_at
            FROM sous_sujet
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s;
            """,
            (*params, limit, offset),
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM sous_sujet {where_sql};", tuple(params))
        total = cur.fetchone()[0]
        items = [SousSujetSummary(id=r[0], sujet_id=r[1], titre=r[2]) for r in rows]
        cur.close(); conn.close()
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# ACTIONS (bulk insert) via sous_sujet_id
# ---------------------------
@app.post("/actions/bulk", response_model=ActionsBulkOut)
def create_actions_bulk(payload: ActionsBulkIn):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT sujet_id FROM sous_sujet WHERE id=%s;", (payload.sous_sujet_id,))
        row = cur.fetchone()
        if row is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sous-sujet not found")
        sujet_id_for_response = row[0]

        created: List[ActionNodeOut] = []
        try:
            for a in payload.actions:
                cur.execute(
                    """
                    INSERT INTO action (id_sous_sujet, task, responsible, due_date, status, product_line, plant_site)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;
                    """,
                    (payload.sous_sujet_id, a.task, a.responsible, a.due_date, a.status, a.product_line, a.plant_site),
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
                            (action_id, sa.task, sa.responsible, sa.due_date, sa.status, sa.product_line, sa.plant_site),
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
                                    (sous_action_id, ssa.task, ssa.responsible, ssa.due_date, ssa.status, ssa.product_line, ssa.plant_site),
                                )
                                ssa_out.append(SousSousActionNodeOut(sous_sous_action_id=cur.fetchone()[0]))

                        sa_out.append(SousActionNodeOut(sous_action_id=sous_action_id, sous_sous_actions=ssa_out or None))

                created.append(ActionNodeOut(action_id=action_id, sous_actions=sa_out or None))

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        cur.close(); conn.close()
        return ActionsBulkOut(sujet_id=sujet_id_for_response, created=created)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion failed: {e}")

# ---------------------------
# LEGACY: list actions by sujet (kept for compatibility)
# ---------------------------
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

        cur.execute("SELECT id FROM sous_sujet WHERE sujet_id=%s ORDER BY id ASC;", (sujet_id,))
        ss_ids = [r[0] for r in cur.fetchall()]

        actions: List[ActionTreeItem] = []
        for ss_id in ss_ids:
            cur.execute(
                """
                SELECT id, task, responsible, due_date, status, product_line, plant_site
                FROM action
                WHERE id_sous_sujet=%s
                ORDER BY id ASC;
                """,
                (ss_id,),
            )
            actions_rows = cur.fetchall()

            for (action_id, task, responsible, due_date, status, product_line, plant_site) in actions_rows:
                cur.execute(
                    """
                    SELECT id, task, responsible, due_date, status, product_line, plant_site
                    FROM sous_action
                    WHERE action_id=%s
                    ORDER BY id ASC;
                    """,
                    (action_id,),
                )
                sous_rows = cur.fetchall()

                sous_items: List[SousActionTreeItem] = []
                for (sid, stask, sresp, sdue, sstatus, sprod, splant) in sous_rows:
                    cur.execute(
                        """
                        SELECT id, task, responsible, due_date, status, product_line, plant_site
                        FROM sous_sous_action
                        WHERE sous_action_id=%s
                        ORDER BY id ASC;
                        """,
                        (sid,),
                    )
                    ssa_rows = cur.fetchall()
                    ssa_items = [
                        SousSousActionTreeItem(
                            sous_sous_action_id=r[0],
                            task=r[1],
                            responsible=r[2],
                            due_date=str(r[3]),
                            status=r[4],
                            product_line=r[5],
                            plant_site=r[6],
                        )
                        for r in ssa_rows
                    ]

                    sous_items.append(
                        SousActionTreeItem(
                            sous_action_id=sid,
                            task=stask,
                            responsible=sresp,
                            due_date=str(sdue),
                            status=sstatus,
                            product_line=sprod,
                            plant_site=splant,
                            sous_sous_actions=ssa_items or None,
                        )
                    )

                actions.append(
                    ActionTreeItem(
                        action_id=action_id,
                        task=task,
                        responsible=responsible,
                        due_date=str(due_date),
                        status=status,
                        product_line=product_line,
                        plant_site=plant_site,
                        sous_actions=sous_items or None,
                    )
                )

        cur.close(); conn.close()
        return ActionsTreeOutResp(sujet_id=sujet_id, actions=actions)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# full tree by sujet (preferred)
# ---------------------------
@app.get("/tree/sujet", response_model=SujetTreeOut)
def get_full_tree_by_sujet(sujet_id: int = Query(..., ge=1)):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (sujet_id,))
        if cur.fetchone() is None:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Sujet not found")

        cur.execute(
            """
            SELECT id, titre
            FROM sous_sujet
            WHERE sujet_id=%s
            ORDER BY id ASC;
            """,
            (sujet_id,),
        )
        ss_rows = cur.fetchall()

        sous_sujets: List[SousSujetTreeItem] = []
        for (ss_id, titre) in ss_rows:
            cur.execute(
                """
                SELECT id, task, responsible, due_date, status, product_line, plant_site
                FROM action
                WHERE id_sous_sujet=%s
                ORDER BY id ASC;
                """,
                (ss_id,),
            )
            actions_rows = cur.fetchall()

            actions: List[ActionTreeItem] = []
            for (action_id, task, responsible, due_date, status, product_line, plant_site) in actions_rows:
                cur.execute(
                    """
                    SELECT id, task, responsible, due_date, status, product_line, plant_site
                    FROM sous_action
                    WHERE action_id=%s
                    ORDER BY id ASC;
                    """,
                    (action_id,),
                )
                sous_rows = cur.fetchall()

                sous_items: List[SousActionTreeItem] = []
                for (sid, stask, sresp, sdue, sstatus, sprod, splant) in sous_rows:
                    cur.execute(
                        """
                        SELECT id, task, responsible, due_date, status, product_line, plant_site
                        FROM sous_sous_action
                        WHERE sous_action_id=%s
                        ORDER BY id ASC;
                        """,
                        (sid,),
                    )
                    ssa_rows = cur.fetchall()
                    ssa_items = [
                        SousSousActionTreeItem(
                            sous_sous_action_id=r[0],
                            task=r[1],
                            responsible=r[2],
                            due_date=str(r[3]),
                            status=r[4],
                            product_line=r[5],
                            plant_site=r[6],
                        )
                        for r in ssa_rows
                    ]

                    sous_items.append(
                        SousActionTreeItem(
                            sous_action_id=sid,
                            task=stask,
                            responsible=sresp,
                            due_date=str(sdue),
                            status=sstatus,
                            product_line=sprod,
                            plant_site=splant,
                            sous_sous_actions=ssa_items or None,
                        )
                    )

                actions.append(
                    ActionTreeItem(
                        action_id=action_id,
                        task=task,
                        responsible=responsible,
                        due_date=str(due_date),
                        status=status,
                        product_line=product_line,
                        plant_site=plant_site,
                        sous_actions=sous_items or None,
                    )
                )

            sous_sujets.append(SousSujetTreeItem(sous_sujet_id=ss_id, titre=titre, actions=actions or None))

        cur.close(); conn.close()
        return SujetTreeOut(sujet_id=sujet_id, sous_sujets=sous_sujets)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ===========================================================================
# NOTE: Les anciennes routes /v2/sujets/tree et /v2/actions/tree (qui
# écrivaient directement dans la base "Action Plan" via get_connection_1)
# ont été SUPPRIMÉES. La gestion des plans d'action passe désormais par le
# proxy vers l'API externe : voir le bloc "PROXY ACTION PLAN" en haut de
# ce fichier, routes /action-plan/*.
# ===========================================================================

# ---------------------------
# Save Supplier Conversation
# ---------------------------
@app.post("/supplier/save-conversation", response_model=SupplierConversationOut)
def save_supplier_conversation(payload: SupplierConversationIn):
    try:
        from db import get_connection_supplier
        conn = get_connection_supplier()
        cur = conn.cursor()
        date_conv = payload.date_conversation or datetime.utcnow()

        cur.execute(
            """
            INSERT INTO conversation (user_name, conversation, date_conversation, supplier_name, assistant_name)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                payload.user_name.strip(),
                payload.conversation,
                date_conv,
                payload.supplier_name.strip(),
                payload.assistant_name.strip()
            ),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return SupplierConversationOut(id=new_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insertion échouée: {e}")

# ---------------------------
# List Supplier Conversations
# ---------------------------
@app.get("/supplier/conversations")
def list_supplier_conversations(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (UTC)"),
    user_name: Optional[str] = None,
    supplier_name: Optional[str] = None,
    assistant_name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        from db import get_connection_supplier
        conn = get_connection_supplier()
        cur = conn.cursor()
        where, params = [], []

        if date:
            where.append("DATE(date_conversation AT TIME ZONE 'UTC') = %s")
            params.append(date)
        if user_name:
            where.append("LOWER(user_name) LIKE %s")
            params.append(f"%{user_name.lower()}%")
        if supplier_name:
            where.append("LOWER(supplier_name) LIKE %s")
            params.append(f"%{supplier_name.lower()}%")
        if assistant_name:
            where.append("LOWER(assistant_name) = %s")
            params.append(assistant_name.lower())

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cur.execute(
            f"""
            SELECT id, user_name, date_conversation, conversation, supplier_name, assistant_name
            FROM conversation
            {where_sql}
            ORDER BY date_conversation DESC, id DESC
            LIMIT %s OFFSET %s;
            """,
            (*params, limit, offset),
        )
        rows = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) FROM conversation {where_sql};", tuple(params))
        total = cur.fetchone()[0]

        items: List[SupplierConversationSummary] = []
        for (cid, uname, dconv, conv, sname, aname) in rows:
            preview = conv[:140]
            items.append(
                SupplierConversationSummary(
                    id=cid,
                    user_name=uname,
                    date_conversation=dconv,
                    preview=preview,
                    supplier_name=sname,
                    assistant_name=aname
                )
            )

        cur.close()
        conn.close()
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Get Supplier Conversation by ID
# ---------------------------
@app.get("/supplier/conversations/{id}", response_model=SupplierConversationDetail)
def get_supplier_conversation_by_id(id: int = Path(..., ge=1)):
    try:
        from db import get_connection_supplier
        conn = get_connection_supplier()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, user_name, date_conversation, conversation, supplier_name, assistant_name
            FROM conversation WHERE id=%s;
            """,
            (id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")

        return SupplierConversationDetail(
            id=row[0],
            user_name=row[1],
            date_conversation=row[2],
            conversation=row[3],
            supplier_name=row[4],
            assistant_name=row[5]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Get Supplier Conversations by Supplier Name
# ---------------------------
@app.get("/supplier/conversations/supplier/{supplier_name}")
def get_supplier_conversations_by_name(
    supplier_name: str = Path(..., min_length=1),
):
    try:
        from db import get_connection_supplier
        conn = get_connection_supplier()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                id,
                user_name,
                assistant_name,
                supplier_name,
                date_conversation,
                conversation,
                COUNT(*) OVER() AS total_count
            FROM conversation
            WHERE LOWER(supplier_name) LIKE %s
            ORDER BY date_conversation DESC, id DESC;
            """,
            (f"%{supplier_name.lower()}%",),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        items = []
        total = 0
        for (cid, uname, aname, sname, dconv, conv, tot) in rows:
            total = tot
            preview = (conv[:160] + "…") if isinstance(conv, str) and len(conv) > 160 else conv
            items.append({
                "id": cid,
                "user_name": uname,
                "assistant_name": aname,
                "supplier_name": sname,
                "date_conversation": dconv,
                "preview": preview,
            })

        return {"items": items, "total": total if rows else 0}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Export Supplier Conversation as TXT
# ---------------------------
@app.get("/supplier/conversations/{id}/export.txt", response_model=None)
def export_supplier_conversation_txt(id: int = Path(..., ge=1)):
    try:
        from fastapi.responses import PlainTextResponse
        from db import get_connection_supplier

        conn = get_connection_supplier()
        cur = conn.cursor()
        cur.execute("SELECT conversation FROM conversation WHERE id=%s;", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")

        txt = row[0].replace(" , ", "\n")
        return PlainTextResponse(content=txt, media_type="text/plain")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")
