# db.py
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

def get_connection():
    """
    Établit une connexion sécurisée avec PostgreSQL sur Azure.
    """
    return psycopg2.connect(
        host="avo-adb-001.postgres.database.azure.com",
        port=5432,
        database="Customer_IA",
        user="adminavo",
        password="$#fKcdXPg4@ue8AW",
        sslmode="require"
    )

def get_connection_1():
    """
    Connexion au deuxième serveur PostgreSQL.
    """
    return psycopg2.connect(
        host="avo-adb-002.postgres.database.azure.com",
        port=5432,
        database="Action Plan",
        user="administrationSTS",
        password="St$@0987",
        sslmode="require"
    )

def get_connection_supplier():
    """
    Connexion à la base de données des conversations fournisseurs.
    """
    return psycopg2.connect(
        host="avo-adb-001.postgres.database.azure.com",  # Ajustez si différent
        port=5432,
        database="supplier_conversation",
        user="adminavo",  # Ajustez selon vos credentials
        password="$#fKcdXPg4@ue8AW",  # Ajustez selon vos credentials
        sslmode="require"
    )
def get_connection_Meeting():
    """
    Connexion à la base de données Meeting (conversations fournisseurs - meetings).
    """
    return psycopg2.connect(
        host="avo-adb-002.postgres.database.azure.com"  # adapte si différent
        port=5432,
        database="Conversation_MeetingDB",
        user="administrationSTS",
        password="St$@0987",
        sslmode="require"
    )
