import os
from shared import get_drive_uploader, EPHY_DRIVE_FOLDER_ID

print(f"Tentative de partage du dossier {EPHY_DRIVE_FOLDER_ID}...")
uploader = get_drive_uploader()
if not uploader:
    print("Échec de l'initialisation du Drive Uploader.")
    exit(1)

service = uploader.service
user_email = 'sebastienibert.pro@gmail.com'

try:
    print("Partage du dossier principal...")
    service.permissions().create(
        fileId=EPHY_DRIVE_FOLDER_ID,
        body={'type': 'user', 'role': 'writer', 'emailAddress': user_email},
        fields='id'
    ).execute()
    print("Dossier partagé avec succès.")
except Exception as e:
    print(f"Erreur dossier: {e}")

try:
    print("Recherche des fichiers dans le dossier...")
    results = service.files().list(q=f"'{EPHY_DRIVE_FOLDER_ID}' in parents and trashed = false").execute()
    files = results.get('files', [])
    if not files:
        print("Aucun fichier trouvé dans le dossier EPhy.")
    for f in files:
        print(f"Partage de {f['name']} ({f['id']})...")
        try:
            service.permissions().create(
                fileId=f['id'],
                body={'type': 'user', 'role': 'writer', 'emailAddress': user_email},
                fields='id'
            ).execute()
        except Exception as file_e:
            print(f"Erreur fichier {f['name']}: {file_e}")
except Exception as e:
    print(f"Erreur listage fichiers: {e}")

print("Opérations terminées.")
