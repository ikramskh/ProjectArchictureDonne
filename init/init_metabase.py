import time
import requests
import sys

METABASE_URL = "http://metabase:3000"

print(" Attente du démarrage de Metabase...")
while True:
    try:
        response = requests.get(f"{METABASE_URL}/api/health")
        if response.status_code == 200:
            print("Metabase est en ligne !")
            break
    except requests.exceptions.ConnectionError:
        pass
    time.sleep(5)

print(" Récupération du token d'installation...")
session_props = requests.get(f"{METABASE_URL}/api/session/properties").json()
setup_token = session_props.get("setup-token")

# --- ÉTAPE 1 : SETUP INITIAL (USER ADMIN) ---
if setup_token:
    print(" Création du compte Admin...")
    setup_payload = {
        "token": setup_token,
        "user": {
            "first_name": "Admin",
            "last_name": "Admin",
            "email": "admin@admin.com", 
            "password": "AdminPassword123!",
            "site_name": "News BigData Dashboard"
        },
        "prefs": {"allow_tracking": False, "site_name": "News BigData Dashboard"}
    }
    res_setup = requests.post(f"{METABASE_URL}/api/setup", json=setup_payload)
    if res_setup.status_code == 200:
        print("Compte Admin créé avec succès !")
    else:
        print(f"Erreur setup (peut-être déjà fait) : {res_setup.text}")
else:
    print("Metabase déjà configuré, passage à la vérification DB.")

# --- ÉTAPE 2 : CONNEXION ET AJOUT DE LA DB ---
print(" Authentification pour l'ajout de la DB...")
auth_res = requests.post(f"{METABASE_URL}/api/session", json={
    "username": "admin@admin.com",
    "password": "AdminPassword123!"
})
session_id = auth_res.json().get("id")
headers = {"X-Metabase-Session": session_id}

response = requests.get(f"{METABASE_URL}/api/database", headers=headers)

if response.status_code == 200:
    res_json = response.json()
    
    # On récupère la liste des DBs, qu'elle soit dans 'data' ou directe
    existing_dbs = res_json.get('data', res_json) if isinstance(res_json, dict) else res_json

    if isinstance(existing_dbs, list):
        # On vérifie si notre Warehouse est là
        if any(db.get('name') == "Warehouse (Gold Layer)" for db in existing_dbs):
            print("La base 'Warehouse' est déjà présente dans Metabase.")
        else:
            print(" Ajout forcé de la base de données Postgres 'warehouse'...")
            db_payload = {
                "name": "Warehouse (Gold Layer)",
                "engine": "postgres",
                "details": {
                    "host": "postgres",
                    "port": 5432,
                    "dbname": "warehouse",
                    "user": "admin",
                    "password": "admin123",
                    "ssl": False
                }
            }
            res_db = requests.post(f"{METABASE_URL}/api/database", json=db_payload, headers=headers)
            if res_db.status_code == 200:
                print("Base de données connectée avec succès !")
            else:
                print(f"Erreur lors de l'ajout de la DB : {res_db.text}")