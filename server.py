# server.py
import os
from typing import Any, Optional, List, Literal
from datetime import datetime, date

from fastapi import FastAPI, HTTPException, Query, Path, APIRouter
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
# ACTION PLAN  ->  ÉCRITURE DIRECTE dans la base "Action Plan"
#
# Remplace l'ancien proxy vers sales-feedback. Ici on se connecte directement
# à la base PostgreSQL "Action Plan" (via SQLAlchemy) et on reproduit la
# logique métier de l'API v2 (priorité, escalation, normalisation, héritage
# du responsable, récursivité des sous-actions).
#
# Configuration (variables d'environnement) :
#   ACTION_PLAN_DATABASE_URL                (recommandé, prioritaire)
#   ou bien :  ACTION_PLAN_DB_HOST / _PORT / _USER / _PASSWORD / _NAME
#
# Routes exposées (préfixe /action-plan) :
#   POST   /action-plan/plans               créer un plan complet
#   POST   /action-plan/actions             ajouter une action à un sujet
#   PATCH  /action-plan/actions/{id}/status changer le statut d'une action
#   GET    /action-plan/plans               lister les plans racines
#   GET    /action-plan/plans/{sujet_id}    arbre complet d'un plan
#   GET    /action-plan/schema              format attendu + colonnes détectées
#   GET    /action-plan/_check              diagnostic connexion base
# ===========================================================================

from typing import Dict, Set
from decimal import Decimal
from pydantic import ConfigDict, field_validator
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    BigInteger, Text, Integer, Date, TIMESTAMP, ForeignKey, func, text,
    Boolean, FetchedValue, inspect,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select


# --- Engine : réutilise get_connection_1() de db.py --------------------------
# La base "Action Plan" est déjà accessible via get_connection_1() dans db.py.
# On crée l'engine SQLAlchemy avec un "creator" qui appelle cette fonction :
# pas besoin de variables d'environnement, c'est la même connexion qui marche
# déjà pour les conversations.
def _make_action_plan_engine():
    try:
        from db import get_connection_1
    except Exception:
        return None
    try:
        return create_engine(
            "postgresql+psycopg2://",
            creator=get_connection_1,
            future=True,
            pool_pre_ping=True,
        )
    except Exception:
        return None


_ap_engine = _make_action_plan_engine()
_ap_metadata = MetaData(schema="public")


def get_action_plan_engine():
    if _ap_engine is None:
        raise HTTPException(
            status_code=503,
            detail="Base Action Plan indisponible : get_connection_1() introuvable dans db.py.",
        )
    return _ap_engine


# --- Définition des tables (identique au schéma réel) ------------------------
ap_sujet = Table(
    "sujet", _ap_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("code", Text, unique=True),
    Column("titre", Text, nullable=False),
    Column("description", Text),
    Column("inserted_by", Text),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now()),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()),
    Column("parent_sujet_id", BigInteger, ForeignKey("public.sujet.id", onupdate="CASCADE", ondelete="SET NULL")),
)

ap_action = Table(
    "action", _ap_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("sujet_id", BigInteger, ForeignKey("public.sujet.id", ondelete="CASCADE"), nullable=False),
    Column("parent_action_id", BigInteger, ForeignKey("public.action.id", ondelete="CASCADE")),
    Column("type", Text, nullable=False),
    Column("titre", Text, nullable=False),
    Column("description", Text),
    Column("status", Text, server_default=text("'open'")),
    Column("priorite", Integer),
    Column("responsable", Text),
    Column("email_responsable", Text),
    Column("kpi", Text),
    Column("demandeur", Text),
    Column("email_demandeur", Text),
    Column("due_date", Date),
    Column("estimated_duration_days", Integer),
    Column("importance", Text),
    Column("urgency", Text),
    Column("escalation_level", Integer),
    Column("priority_index", Integer),
    Column("last_reminder_sent_at", TIMESTAMP(timezone=True)),
    Column("ordre", Integer),
    Column("depth", Integer, server_default=FetchedValue(), server_onupdate=FetchedValue()),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now()),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()),
    Column("closed_date", Date),
    Column("is_deleted", Boolean, nullable=False, server_default=text("false")),
    Column("deleted_at", TIMESTAMP(timezone=True)),
    Column("deleted_by", Text),
)

# --- Constantes & logique métier (reprises de l'API v2) ----------------------
VALID_STATUSES = {"open", "closed", "blocked"}
DEFAULT_IMPORTANCE = "moyenne"
DEFAULT_URGENCY = "Flexible"
CLOSED_URGENCY = "Secondaire"
CLOSED_PRIORITY_INDEX = 0
GENERATED_ACTION_COLUMNS = {"depth"}
_AP_COL_CACHE: Dict[str, Set[str]] = {}

IMPORTANCE_SCORE = {"critique": 10, "haute": 10, "high": 10, "important": 10,
                    "moyenne": 5, "medium": 5, "moyen": 5, "faible": 2, "low": 2, "basse": 2}
URGENCY_SCORE = {"urgent": 6, "urgente": 6, "flexible": 3, "secondaire": 1, "secondary": 1}


def _ap_existing_columns(conn: Connection, table: Table) -> Set[str]:
    key = f"{table.schema or ''}.{table.name}"
    if key in _AP_COL_CACHE:
        return _AP_COL_CACHE[key]
    try:
        cols = {c["name"] for c in inspect(conn).get_columns(table.name, schema=table.schema)}
        _AP_COL_CACHE[key] = cols
        return cols
    except Exception:
        return set(table.c.keys())


def _ap_safe_values(conn, table, values, generated=None):
    generated = generated or set()
    existing = _ap_existing_columns(conn, table)
    return {k: v for k, v in values.items()
            if k in table.c and k in existing and k not in generated}


def _ap_existing_select_cols(conn, names):
    existing = _ap_existing_columns(conn, ap_action)
    return [ap_action.c[n] for n in names if n in ap_action.c and n in existing]


def _ap_json_ready(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _ap_norm_status(value):
    s = str(value or "open").strip().lower()
    if s not in VALID_STATUSES:
        raise ValueError(f"status must be one of {sorted(VALID_STATUSES)}")
    return s


def _ap_norm_importance(value):
    n = str(value or DEFAULT_IMPORTANCE).strip().lower()
    if n in {"high", "haute", "important", "critique"}:
        return "haute"
    if n in {"low", "faible", "basse"}:
        return "faible"
    return DEFAULT_IMPORTANCE


def _ap_norm_urgency(value):
    n = str(value or DEFAULT_URGENCY).strip().lower()
    if n in {"urgent", "urgente"}:
        return "Urgent"
    if n in {"secondaire", "secondary"}:
        return CLOSED_URGENCY
    return DEFAULT_URGENCY


def _ap_days_until(due):
    if isinstance(due, str):
        due = date.fromisoformat(due)
    if not due:
        return None
    return (due - date.today()).days


def _ap_escalation(due):
    d = _ap_days_until(due)
    if d is None or d >= 0:
        return 0
    od = abs(d)
    return 3 if od >= 14 else 2 if od >= 7 else 1


def _ap_overdue_bonus(due):
    d = _ap_days_until(due)
    if d is None or d >= 0:
        return 0
    od = abs(d)
    return 10 if od > 14 else 5 if od > 7 else 2


def _ap_raw_priority(status, due, importance, urgency):
    if status == "closed":
        return CLOSED_PRIORITY_INDEX
    return max(IMPORTANCE_SCORE.get(importance.lower(), 0)
               + URGENCY_SCORE.get(urgency.lower(), 0)
               + _ap_escalation(due) * 3 + _ap_overdue_bonus(due), 0)


def _ap_validate_priority(provided, calculated, status):
    if status == "closed":
        return CLOSED_PRIORITY_INDEX
    if provided is None:
        return calculated
    try:
        c = int(provided)
    except (TypeError, ValueError):
        return calculated
    if c < 0 or c > 100:
        return calculated
    if calculated > 0 and c == 0:
        return calculated
    return c


def _ap_priority_fields(status, due, importance, urgency, provided=None):
    st = _ap_norm_status(status)
    imp = _ap_norm_importance(importance)
    if st == "closed":
        return {"importance": imp, "urgency": CLOSED_URGENCY,
                "escalation_level": 0, "priority_index": CLOSED_PRIORITY_INDEX}
    urg = _ap_norm_urgency(urgency)
    calc = _ap_raw_priority(st, due, imp, urg)
    return {"importance": imp, "urgency": urg,
            "escalation_level": _ap_escalation(due),
            "priority_index": _ap_validate_priority(provided, calc, st)}


def _ap_derive_priorite(pi):
    if pi <= 0:
        return 0
    if pi >= 18:
        return 1
    if pi >= 10:
        return 2
    return 3


def _ap_type_for_level(level):
    return "action" if level <= 0 else "sub_action" if level == 1 else "sub_sub_action"


# --- Insertion récursive sujet -----------------------------------------------
def _ap_upsert_sujet(conn, titre, parent_id, code, description, inserted_by):
    if code:
        stmt = pg_insert(ap_sujet).values(
            code=code, titre=titre, description=description,
            parent_sujet_id=parent_id, inserted_by=inserted_by,
        ).on_conflict_do_update(
            index_elements=["code"],
            set_=dict(titre=titre, description=description,
                      parent_sujet_id=parent_id, inserted_by=inserted_by, updated_at=func.now()),
        ).returning(ap_sujet.c.id)
        return conn.execute(stmt).scalar_one()
    if parent_id is None:
        sel = select(ap_sujet.c.id).where(ap_sujet.c.parent_sujet_id.is_(None), ap_sujet.c.titre == titre)
    else:
        sel = select(ap_sujet.c.id).where(ap_sujet.c.parent_sujet_id == parent_id, ap_sujet.c.titre == titre)
    existing = conn.execute(sel).first()
    if existing:
        upd = ap_sujet.update().where(ap_sujet.c.id == existing[0]).values(
            description=description, updated_at=func.now(), inserted_by=inserted_by,
        ).returning(ap_sujet.c.id)
        return conn.execute(upd).scalar_one()
    ins = ap_sujet.insert().values(
        titre=titre, description=description, parent_sujet_id=parent_id, inserted_by=inserted_by,
    ).returning(ap_sujet.c.id)
    return conn.execute(ins).scalar_one()


# --- Insertion récursive action ----------------------------------------------
def _ap_insert_action(conn, sujet_id, parent_action_id, node, level,
                      d_resp, d_email_resp, d_dem, d_email_dem, created_ids):
    status = _ap_norm_status(node.get("status"))
    pf = _ap_priority_fields(status, node.get("due_date"),
                             node.get("importance"), node.get("urgency"),
                             node.get("priority_index"))
    pi = pf["priority_index"]
    raw = {
        "sujet_id": sujet_id,
        "parent_action_id": parent_action_id,
        "type": _ap_type_for_level(level),
        "titre": node["titre"],
        "description": node.get("description"),
        "status": status,
        "priorite": node.get("priorite") if node.get("priorite") is not None else _ap_derive_priorite(pi),
        "responsable": node.get("responsable") or d_resp,
        "email_responsable": node.get("email_responsable") or d_email_resp,
        "kpi": node.get("kpi"),
        "demandeur": node.get("demandeur") or d_dem,
        "email_demandeur": node.get("email_demandeur") or d_email_dem,
        "due_date": node.get("due_date"),
        "estimated_duration_days": node.get("estimated_duration_days"),
        "importance": pf["importance"],
        "urgency": pf["urgency"],
        "escalation_level": pf["escalation_level"],
        "priority_index": pi,
        "ordre": node.get("ordre"),
        "closed_date": date.today() if status == "closed" else None,
        "is_deleted": False,
    }
    safe = _ap_safe_values(conn, ap_action, raw, GENERATED_ACTION_COLUMNS)
    new_id = int(conn.execute(ap_action.insert().values(**safe).returning(ap_action.c.id)).first()[0])
    created_ids.append(new_id)
    for child in (node.get("sous_actions") or []):
        _ap_insert_action(conn, sujet_id, new_id, child, min(level + 1, 2),
                          raw.get("responsable"), raw.get("email_responsable"),
                          d_dem, d_email_dem, created_ids)
    return new_id


def _ap_ingest_sujet_tree(conn, node, parent_id, plan, created_s, created_a):
    inserted_by = node.get("inserted_by") or plan.get("inserted_by")
    sid = _ap_upsert_sujet(conn, node["titre"], parent_id, node.get("code"),
                           node.get("description"), inserted_by)
    created_s.append(int(sid))
    d_dem = plan.get("demandeur") or plan.get("inserted_by")
    d_email_dem = plan.get("email_demandeur") or plan.get("inserted_by")
    d_resp = node.get("responsable") or plan.get("responsable")
    d_email_resp = node.get("email_responsable") or plan.get("email_responsable")
    for a in (node.get("actions") or []):
        _ap_insert_action(conn, sid, None, a, 0, d_resp, d_email_resp, d_dem, d_email_dem, created_a)
    for child in (node.get("sous_sujets") or []):
        _ap_ingest_sujet_tree(conn, child, sid, plan, created_s, created_a)
    return int(sid)


def _ap_child_level(conn, parent_action_id):
    if not parent_action_id:
        return 0
    cols = _ap_existing_select_cols(conn, ["type", "depth"])
    parent = conn.execute(select(*cols).where(ap_action.c.id == parent_action_id)).first()
    if not parent:
        return 1
    pd = dict(parent._mapping)
    if pd.get("depth") is not None:
        return min(int(pd["depth"]) + 1, 2)
    return 1 if pd.get("type") == "action" else 2


# --- Lecture arbre -----------------------------------------------------------
AP_ACTION_READ_COLS = [
    "id", "sujet_id", "parent_action_id", "type", "titre", "description", "status",
    "priorite", "responsable", "email_responsable", "kpi", "demandeur", "email_demandeur",
    "due_date", "estimated_duration_days", "importance", "urgency", "escalation_level",
    "priority_index", "last_reminder_sent_at", "ordre", "depth", "created_at", "updated_at",
    "closed_date", "is_deleted", "deleted_at", "deleted_by",
]


def _ap_fetch_action_payload(conn, action_id):
    cols = _ap_existing_select_cols(conn, AP_ACTION_READ_COLS)
    row = conn.execute(select(*cols).where(ap_action.c.id == action_id)).first()
    return _ap_json_ready(dict(row._mapping)) if row else None


def _ap_fetch_tree(conn, sujet_id):
    s_row = conn.execute(select(ap_sujet).where(ap_sujet.c.id == sujet_id)).first()
    if not s_row:
        return None
    s = _ap_json_ready(dict(s_row._mapping))
    s["sous_sujets"], s["actions"] = [], []

    def fetch_actions(parent_action_id):
        cols = _ap_existing_select_cols(conn, AP_ACTION_READ_COLS)
        stmt = select(*cols).where(ap_action.c.sujet_id == sujet_id,
                                   ap_action.c.parent_action_id == parent_action_id)
        existing = _ap_existing_columns(conn, ap_action)
        stmt = stmt.order_by(ap_action.c.ordre.nullslast(), ap_action.c.id) if "ordre" in existing \
            else stmt.order_by(ap_action.c.id)
        res = []
        for a_row in conn.execute(stmt).fetchall():
            a = _ap_json_ready(dict(a_row._mapping))
            a["sous_actions"] = fetch_actions(a["id"])
            res.append(a)
        return res

    s["actions"] = fetch_actions(None)
    for child in conn.execute(select(ap_sujet.c.id).where(ap_sujet.c.parent_sujet_id == sujet_id)).fetchall():
        ct = _ap_fetch_tree(conn, child.id)
        if ct:
            s["sous_sujets"].append(ct)
    return s


# --- Modèles Pydantic (entrée souple + exemple Swagger) ----------------------
class PlanV2In(BaseModel):
    model_config = ConfigDict(extra="allow", json_schema_extra={"example": {
        "version": "2.0", "plan_code": "AP-2026-001", "plan_title": "Plan de la réunion",
        "conversation_id": 116,
        "inserted_by": "ali", "demandeur": "ali", "email_demandeur": "ali@example.com",
        "sujets": [{
            "titre": "Sujet principal", "code": "SUJ-1", "description": "Description",
            "sous_sujets": [],
            "actions": [{
                "titre": "Faire X", "status": "open", "due_date": "2026-07-01",
                "importance": "haute", "urgency": "Flexible", "responsable": "jane.doe",
                "sous_actions": [{"titre": "Sous-tâche", "status": "open"}],
            }],
        }],
    }})
    plan_title: str = Field(..., min_length=1)


class ActionV2CreateIn(BaseModel):
    model_config = ConfigDict(extra="allow", json_schema_extra={"example": {
        "sujet_id": 1, "parent_action_id": None, "titre": "Nouvelle action",
        "status": "open", "due_date": "2026-07-01", "importance": "moyenne", "responsable": "jane.doe",
    }})
    sujet_id: int = Field(..., ge=1)
    titre: str = Field(..., min_length=1)


class StatusUpdateIn(BaseModel):
    model_config = ConfigDict(extra="allow", json_schema_extra={"example": {"status": "closed"}})
    status: str = Field(..., description="open | closed | blocked")


# --- Routes ------------------------------------------------------------------
action_plan_router = APIRouter(prefix="/action-plan", tags=["action-plan (DB directe)"])


@action_plan_router.get("/_check")
def ap_check():
    """Diagnostic : teste la connexion à la base Action Plan."""
    if _ap_engine is None:
        return {"ok": False, "reason": "Engine indisponible (get_connection_1 introuvable dans db.py)."}
    try:
        with _ap_engine.connect() as conn:
            cols = sorted(_ap_existing_columns(conn, ap_action))
        return {"ok": True, "action_columns_detected": cols}
    except Exception as e:
        return {"ok": False, "reason": str(e)}


@action_plan_router.post("/plans")
def ap_create_plan(payload: PlanV2In):
    """Crée un plan complet (sujet racine + sujets + actions récursives).

    Astuce de liaison : si le corps contient un champ "conversation_id",
    un marqueur [conversation_id=N] est ajouté à la description du sujet
    racine, sans modifier la base. On peut ensuite retrouver le plan via
    GET /action-plan/plans/by-conversation/{conversation_id}.
    """
    plan = payload.model_dump(exclude_none=False)
    engine = get_action_plan_engine()

    # Construit la description racine, avec marqueur de conversation si fourni.
    root_desc = "Action plan root (v2)"
    conv_id = plan.get("conversation_id")
    if conv_id is not None:
        root_desc = f"[conversation_id={conv_id}] {root_desc}"

    try:
        with engine.begin() as conn:
            created_s, created_a = [], []
            root_id = _ap_upsert_sujet(conn, plan["plan_title"], None,
                                       plan.get("plan_code"), root_desc,
                                       plan.get("inserted_by"))
            created_s.append(int(root_id))
            for s in (plan.get("sujets") or []):
                _ap_ingest_sujet_tree(conn, s, int(root_id), plan, created_s, created_a)
            return {"root_sujet_id": int(root_id),
                    "created_sujet_ids": created_s, "created_action_ids": created_a}
    except IntegrityError as ie:
        raise HTTPException(status_code=409, detail=f"db_integrity_error: {ie.orig}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"server_error: {e}")


@action_plan_router.post("/actions")
def ap_create_action(payload: ActionV2CreateIn):
    """Ajoute une action (et ses sous-actions) à un sujet existant."""
    data = payload.model_dump(exclude_none=False)
    sujet_id = data["sujet_id"]
    parent_action_id = data.get("parent_action_id")
    engine = get_action_plan_engine()
    try:
        with engine.begin() as conn:
            if conn.execute(select(ap_sujet.c.id).where(ap_sujet.c.id == sujet_id)).first() is None:
                raise HTTPException(status_code=404, detail=f"Sujet ID {sujet_id} not found")
            parent_defaults = {}
            if parent_action_id:
                p = conn.execute(select(ap_action.c.id, ap_action.c.responsable, ap_action.c.email_responsable)
                                 .where(ap_action.c.id == parent_action_id,
                                        ap_action.c.sujet_id == sujet_id)).first()
                if not p:
                    raise HTTPException(status_code=404,
                                        detail=f"Parent Action {parent_action_id} not in Sujet {sujet_id}")
                parent_defaults = dict(p._mapping)
            created = []
            _ap_insert_action(
                conn, sujet_id, parent_action_id, data,
                _ap_child_level(conn, parent_action_id),
                data.get("responsable") or parent_defaults.get("responsable"),
                data.get("email_responsable") or parent_defaults.get("email_responsable"),
                data.get("demandeur") or data.get("inserted_by"),
                data.get("email_demandeur") or data.get("inserted_by"),
                created,
            )
            return {"created_action_ids": created}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"server_error: {e}")


@action_plan_router.patch("/actions/{action_id}/status")
def ap_update_status(action_id: int, payload: StatusUpdateIn):
    """Change le statut d'une action et recalcule sa priorité."""
    data = payload.model_dump(exclude_none=False)
    try:
        new_status = _ap_norm_status(data.get("status"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    engine = get_action_plan_engine()
    try:
        with engine.begin() as conn:
            current = _ap_fetch_action_payload(conn, action_id)
            if not current:
                raise HTTPException(status_code=404, detail="Action not found")
            pf = _ap_priority_fields(new_status, current.get("due_date"),
                                     current.get("importance") or DEFAULT_IMPORTANCE,
                                     current.get("urgency") or DEFAULT_URGENCY,
                                     data.get("priority_index"))
            pi = pf["priority_index"]
            raw = {
                "status": new_status, "updated_at": func.now(),
                "closed_date": date.today() if new_status == "closed" else None,
                "importance": pf["importance"], "urgency": pf["urgency"],
                "escalation_level": pf["escalation_level"], "priority_index": pi,
                "priorite": _ap_derive_priorite(pi),
            }
            updates = _ap_safe_values(conn, ap_action, raw, GENERATED_ACTION_COLUMNS)
            conn.execute(ap_action.update().where(ap_action.c.id == action_id).values(**updates))
            return _ap_fetch_action_payload(conn, action_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"server_error: {e}")


@action_plan_router.get("/plans")
def ap_list_plans():
    """Liste les plans racines (sujets sans parent)."""
    engine = get_action_plan_engine()
    try:
        with engine.connect() as conn:
            stmt = select(ap_sujet.c.id, ap_sujet.c.titre, ap_sujet.c.code,
                          ap_sujet.c.description, ap_sujet.c.inserted_by, ap_sujet.c.created_at) \
                .where(ap_sujet.c.parent_sujet_id.is_(None))
            return [_ap_json_ready(dict(r._mapping)) for r in conn.execute(stmt).fetchall()]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@action_plan_router.get("/plans/by-conversation/{conversation_id}")
def ap_plans_by_conversation(conversation_id: int = Path(..., ge=1), full: bool = Query(False)):
    """Retrouve les plans liés à une conversation via le marqueur
    [conversation_id=N] présent dans la description du sujet racine.

    - full=false (défaut) : renvoie la liste des plans racines correspondants.
    - full=true            : renvoie aussi l'arbre complet de chaque plan.
    """
    engine = get_action_plan_engine()
    marker = f"[conversation_id={conversation_id}]"
    try:
        with engine.connect() as conn:
            stmt = select(ap_sujet.c.id, ap_sujet.c.titre, ap_sujet.c.code,
                          ap_sujet.c.description, ap_sujet.c.inserted_by, ap_sujet.c.created_at) \
                .where(ap_sujet.c.parent_sujet_id.is_(None),
                       ap_sujet.c.description.like(f"%{marker}%"))
            roots = [_ap_json_ready(dict(r._mapping)) for r in conn.execute(stmt).fetchall()]
            if full:
                roots = [_ap_fetch_tree(conn, r["id"]) for r in roots]
            return {"conversation_id": conversation_id, "marker": marker,
                    "count": len(roots), "plans": roots}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@action_plan_router.get("/plans/{sujet_id}")
def ap_get_plan(sujet_id: int):
    """Récupère l'arbre complet d'un plan par l'id du sujet."""
    engine = get_action_plan_engine()
    try:
        with engine.connect() as conn:
            tree = _ap_fetch_tree(conn, sujet_id)
            if not tree:
                raise HTTPException(status_code=404, detail="Sujet not found")
            return tree
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@action_plan_router.get("/schema")
def ap_schema():
    """Format attendu + colonnes réellement détectées en base."""
    report = {"detected_action_columns": None}
    try:
        with get_action_plan_engine().connect() as conn:
            report["detected_action_columns"] = sorted(_ap_existing_columns(conn, ap_action))
    except Exception as e:
        report["detected_action_columns"] = None
        report["error"] = str(e)
    return {
        "version": "2.0",
        "database_columns": report,
        "valid_statuses": sorted(VALID_STATUSES),
        "example": PlanV2In.model_config["json_schema_extra"]["example"],
    }


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
