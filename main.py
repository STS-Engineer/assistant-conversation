# main.py
import requests
from datetime import datetime

API_URL = "http://localhost:8000/save-conversation"

def test_send_conversation():
    data = {
        "user_name": "majed",
        "conversation": "Q: Quel est le problème ?\nR: Problème de connexion\nQ: Depuis quand ?\nR: Ce matin",
        "date_conversation": datetime.now().isoformat()
    }

    response = requests.post(API_URL, json=data)

    if response.status_code == 200:
        print("✅ Conversation enregistrée :", response.json())
    else:
        print("❌ Erreur :", response.status_code, response.text)

if __name__ == "__main__":
    test_send_conversation()
