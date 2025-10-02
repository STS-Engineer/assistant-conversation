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
        user="adminavo",  # utilisateur PostgreSQL, pas le compte Azure
        password="$#fKcdXPg4@ue8AW",  # mot de passe de l'utilisateur PostgreSQL
        sslmode="require"  # obligatoire sur Azure
    )

def get_connection_1():
    """
    Connexion au deuxième serveur PostgreSQL .
    """
    return psycopg2.connect(
        host="avo-adb-002.postgres.database.azure.com",
        port=5432,
        database="Action Plan",
        user="administrationSTS",
        password="St$@0987",
        sslmode="require"
    )


