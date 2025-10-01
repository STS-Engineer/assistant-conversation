# server.py
from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime , date
from db import get_connection , get_connection_1
import base64, binascii

from fastapi.responses import Response, PlainTextResponse

app = FastAPI(title="Conversation Logger API", version="1.7.0")

# ---------------------------
# Models (conversations)
# ---------------------------
class ConversationIn(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=200)
    conversation: str = Field(..., min_length=1)
    date_conversation: Optional[datetime] = None
    # NEW (methode 2): image binaire en base ------------------------------------------------
    # image_base64 : contenu b64 (pas l'URL) ; optionnel
    image_base64: Optional[str] = Field(
        default=None,
        description="Image encodée en base64 (corps binaire)",
    )
    image_mime: Optional[str] = Field(
        default=None,
        description="MIME type de l'image (ex: image/png, image/jpeg)",
        max_length=100,
    )
    image_filename: Optional[str] = Field(
        default=None,
        description="Nom de fichier proposé au téléchargement",
        max_length=255,
    )

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
    # NEW: on expose des métadonnées, pas l'image binaire
    has_image: bool
    image_mime: Optional[str] = None
    image_filename: Optional[str] = None

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
# NEW: Models (sous-sujet)
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

# CHANGED: payload now expects sous_sujet_id (not sujet_id)
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
    # kept for compatibility; we return parent sujet_id for context
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

# legacy response for GET /actions?sujet_id=...
class ActionsTreeOut(BaseModel):
    sujet_id: int
    actions: List[ActionTreeItem]

# NEW: tree by sujet including sous-sujets
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
# Models (SUJETS RÉCURSIFS)
# ---------------------------
class SujetNodeIn(BaseModel):
    titre: str = Field(..., min_length=1)
    description: Optional[str] = None
    code: Optional[str] = None
    children: Optional[List["SujetNodeIn"]] = None

SujetNodeIn.model_rebuild()

class SujetNodeOut(BaseModel):
    id: int
    titre: str
    description: Optional[str] = None
    code: Optional[str] = None
    children: Optional[List["SujetNodeOut"]] = None

# ---------------------------
# Models (ACTIONS RÉCURSIVES)
# ---------------------------
Status = Literal["nouvelle","en_cours","bloquee","terminee","annulee"]

class ActionV2In(BaseModel):
    task: str = Field(..., min_length=1)
    responsible: Optional[str] = None
    due_date: Optional[date] = None     # conseillé: vraie date
    status: Status = "nouvelle"
    # facultatifs (pas en DB2, gardés si ton app les utilise côté front)
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    children: Optional[List["ActionV2In"]] = None

ActionV2In.model_rebuild()

class ActionV2Out(BaseModel):
    id: int
    task: str
    responsible: Optional[str] = None
    due_date: Optional[str] = None
    status: str
    product_line: Optional[str] = None
    plant_site: Optional[str] = None
    children: Optional[List["ActionV2Out"]] = None

# ---------------------------
# Mapping status app -> DB
# ---------------------------
STATUS_MAP_APP_TO_DB = {
    "nouvelle": "nouveau",
    "en_cours": "en_cours",
    "bloquee": "bloque",
    "terminee": "termine",
    "annulee": "termine",  # ou élargis la CHECK pour accepter 'annulee'
}

# ---------------------------
# Save conversation
# ---------------------------
@app.post("/save-conversation", response_model=ConversationOut)
def save_conversation(payload: ConversationIn):
    """
    Accepte une image en base64 (optionnelle). On la stocke en BYTEA (image_data),
    plus image_mime et image_filename si fournis.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        date_conv = payload.date_conversation or datetime.utcnow()

        image_bytes = None
        if payload.image_base64:
            try:
                # Sécurisé: validate=True rejette les chars non base64
                image_bytes = base64.b64decode(payload.image_base64, validate=True)
            except (binascii.Error, ValueError):
                raise HTTPException(status_code=422, detail="image_base64 is not valid base64")

            # (Optionnel) limite applicative 10 Mo
            if len(image_bytes) > 10 * 1024 * 1024:
                raise HTTPException(status_code=413, detail="Image too large (max 10 MB)")

        cur.execute(
            """
            INSERT INTO conversations (user_name, conversation, date_conversation, image_data, image_mime, image_filename)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                payload.user_name.strip(),
                payload.conversation,
                date_conv,
                image_bytes,
                (payload.image_mime or None),
                (payload.image_filename or None),
            ),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close(); conn.close()
        return ConversationOut(id=new_id)
    except HTTPException:
        raise
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
        where, params = [], []
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
            (*params, limit, offset),
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
# Get conversation by id (sans l'image binaire)
# ---------------------------
@app.get("/conversations/{id}", response_model=ConversationDetail)
def get_conversation_by_id(id: int = Path(..., ge=1)):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_name, date_conversation, conversation,
                   (image_data IS NOT NULL) AS has_image,
                   image_mime, image_filename
            FROM conversations WHERE id=%s;
            """,
            (id,),
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")
        return ConversationDetail(
            id=row[0],
            user_name=row[1],
            date_conversation=row[2],
            conversation=row[3],
            has_image=bool(row[4]),
            image_mime=row[5],
            image_filename=row[6],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

# ---------------------------
# Download image (binaire)
# ---------------------------
@app.get("/conversations/{id}/image")
def download_conversation_image(id: int = Path(..., ge=1)):
    """
    Renvoie l'image binaire si présente.
    Content-Type = image_mime (ou application/octet-stream)
    Content-Disposition = attachment; filename="<image_filename>" (si défini)
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT image_data, image_mime, image_filename FROM conversations WHERE id=%s;",
            (id,),
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Conversation not found")
        image_data, image_mime, image_filename = row
        if image_data is None:
            raise HTTPException(status_code=404, detail="No image associated to this conversation")

        headers = {}
        if image_filename:
            headers["Content-Disposition"] = f'attachment; filename="{image_filename}"'
        return Response(
            content=bytes(image_data),
            media_type=(image_mime or "application/octet-stream"),
            headers=headers,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {e}")

# ---------------------------
# Export TXT
# ---------------------------
@app.get("/conversations/{id}/export.txt", response_model=None)
def export_conversation_txt(id: int = Path(..., ge=1)):
    try:
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
# NEW: SOUS-SUJETS
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

        # Check sous-sujet exists & get parent sujet_id for response context
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

        # fetch actions for all sous-sujets and flatten (legacy behavior)
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
                # Sous-actions
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
                    # Sous-sous-actions
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
# NEW: full tree by sujet (preferred)
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

# ---------------------------
# NEW: Recursive Subjects on DB secondaire (via get_connection_1)
# ---------------------------
def _insert_sujet_recursive(cur, parent_id: Optional[int], node: SujetNodeIn) -> SujetNodeOut:
    titre = (node.titre or "").strip()
    if not titre:
        raise HTTPException(status_code=422, detail="Field 'titre' is required")

    cur.execute("""
        INSERT INTO sujet (parent_sujet_id, code, titre, description)
        VALUES (%s,%s,%s,%s)
        RETURNING id;
    """, (parent_id, node.code, titre, node.description))
    sid = cur.fetchone()[0]

    children_out: List[SujetNodeOut] = []
    for ch in (node.children or []):
        child_out = _insert_sujet_recursive(cur, sid, ch)
        children_out.append(child_out)

    return SujetNodeOut(
        id=sid,
        titre=titre,
        description=node.description,
        code=node.code,
        children=children_out or None,
    )

@app.post("/v2/sujets/tree", response_model=SujetNodeOut)
def create_sujet_tree_v2(root: SujetNodeIn):
    conn = get_connection_1()
    try:
        with conn, conn.cursor() as cur:
            root_out = _insert_sujet_recursive(cur, None, root)
            return root_out
    finally:
        conn.close()

@app.get("/v2/sujets/tree", response_model=SujetNodeOut)
def get_sujet_tree_v2(root_id: int = Query(..., ge=1)):
    conn = get_connection_1()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT id, parent_sujet_id, code, titre, description FROM sujet WHERE id=%s;", (root_id,))
            head = cur.fetchone()
            if not head:
                raise HTTPException(status_code=404, detail="Sujet root not found")

            cur.execute("""
                WITH RECURSIVE tree AS (
                  SELECT id, parent_sujet_id, code, titre, description
                  FROM sujet
                  WHERE id = %s
                  UNION ALL
                  SELECT s.id, s.parent_sujet_id, s.code, s.titre, s.description
                  FROM sujet s
                  JOIN tree t ON s.parent_sujet_id = t.id
                )
                SELECT id, parent_sujet_id, code, titre, description
                FROM tree ORDER BY id;
            """, (root_id,))
            rows = cur.fetchall()

        by_parent = {}
        def mk(r):
            sid, parent, code, titre, desc = r
            return SujetNodeOut(id=sid, titre=titre, description=desc, code=code, children=[])
        for r in rows:
            sid, parent, *_ = r
            by_parent.setdefault(parent, []).append(mk(r))

        def attach(node: SujetNodeOut):
            for ch in by_parent.get(node.id, []):
                node.children.append(ch); attach(ch)
            if not node.children:
                node.children = None

        root = mk(head)
        attach(root)
        return root
    finally:
        conn.close()

# ---------------------------
# NEW: Recursive Actions on DB secondaire (via get_connection_1)
# ---------------------------
def _insert_action_recursive(cur, parent_action_id: Optional[int], sujet_id: int, node: ActionV2In) -> ActionV2Out:
    cur.execute("""
        INSERT INTO action (sujet_id, parent_action_id, type, titre, description, responsable, due_date, statut)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """, (
        sujet_id,
        parent_action_id,
        "action",
        node.task.strip(),
        None,
        (node.responsible or None),
        node.due_date,
        STATUS_MAP_APP_TO_DB.get(node.status, "nouveau"),
    ))
    aid = cur.fetchone()[0]

    children_out: List[ActionV2Out] = []
    for ch in (node.children or []):
        child_out = _insert_action_recursive(cur, aid, sujet_id, ch)
        children_out.append(child_out)

    return ActionV2Out(
        id=aid,
        task=node.task,
        responsible=node.responsible,
        due_date=(node.due_date.isoformat() if node.due_date else None),
        status=node.status,
        product_line=node.product_line,
        plant_site=node.plant_site,
        children=children_out or None,
    )

@app.post("/v2/actions/tree", response_model=ActionV2Out)
def create_action_tree_v2(sujet_id: int = Query(..., ge=1), root: ActionV2In = ...):
    conn = get_connection_1()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (sujet_id,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Sujet not found")

            root_out = _insert_action_recursive(cur, None, sujet_id, root)
            return root_out
    finally:
        conn.close()

@app.get("/v2/actions/tree", response_model=List[ActionV2Out])
def get_actions_tree_v2(sujet_id: int = Query(..., ge=1)):
    conn = get_connection_1()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM sujet WHERE id=%s;", (sujet_id,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Sujet not found")

            cur.execute("""
                SELECT id, parent_action_id, type, titre, description, responsable, due_date, statut
                FROM action
                WHERE sujet_id=%s
                ORDER BY id;
            """, (sujet_id,))
            rows = cur.fetchall()

        by_parent = {}
        def mk(r) -> ActionV2Out:
            aid, parent, type_, titre, descr, resp, due, statut = r
            return ActionV2Out(
                id=aid,
                task=titre,
                responsible=resp,
                due_date=(str(due) if due is not None else None),
                status=statut,
                product_line=None,
                plant_site=None,
                children=[],
            )
        for r in rows:
            aid, parent, *_ = r
            by_parent.setdefault(parent, []).append(mk(r))

        def attach(node: ActionV2Out):
            for ch in by_parent.get(node.id, []):
                node.children.append(ch); attach(ch)
            if not node.children:
                node.children = None

        roots: List[ActionV2Out] = []
        for root in by_parent.get(None, []):
            attach(root); roots.append(root)
        return roots
    finally:
        conn.close()
