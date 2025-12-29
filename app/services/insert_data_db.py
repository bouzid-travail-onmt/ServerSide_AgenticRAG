import gridfs
from typing import List, AsyncGenerator, Union
from fastapi import UploadFile
from pymongo.database import Database
from app.db import get_mongo_client


def get_collections(db: Database):
    """Retourne les instances GridFS séparées pour PDF, Excel, JSON, TXT + collection URLs."""
    return {
        "pdfs": gridfs.GridFS(db, collection="pdfs"),
        "excels": gridfs.GridFS(db, collection="excels"),
        "training_data_json": gridfs.GridFS(db, collection="training_data_json"),
        "training_data_txt": gridfs.GridFS(db, collection="training_data_txt"),
        "urls_scraping": db["urls_scraping"],
    }

async def process_and_store_data(
    files: List[Union[UploadFile, dict]],
    urls: List[str]
) -> AsyncGenerator[str, None]:
    """
    Traite et stocke les fichiers et URLs dans MongoDB avec logs en temps réel.
    PDFs, Excels, JSON et TXT sont stockés via GridFS dans leurs collections respectives.
    """
    db = get_mongo_client()
    collections = get_collections(db)

    yield "🔄 Démarrage du traitement...\n"

    inserted_files = 0
    existing_files = 0
    errors = 0

    for file in files:
        try:
            filename = None
            content = None

            # Lecture du contenu selon le type
            if isinstance(file, UploadFile):
                filename = file.filename
                content = await file.read()
            elif isinstance(file, dict):
                filename = file.get("filename") or file.get("name")
                content = file.get("content")
                if isinstance(content, str):
                    content = content.encode("utf-8")

            if not filename or not content:
                yield f"⚠️ Fichier invalide : {file}\n"
                continue

            ext = filename.lower().split(".")[-1]

            # --- Gestion des PDFs via GridFS ---
            if ext == "pdf":
                fs_pdf = collections["pdfs"]
                if not fs_pdf.exists({"filename": filename}):
                    fs_pdf.put(content, filename=filename)
                    inserted_files += 1
                    yield f"✅ PDF inséré dans GridFS : {filename}\n"
                else:
                    existing_files += 1
                    yield f"⚠️ PDF déjà existant : {filename}\n"

            # --- Gestion des fichiers Excel via GridFS ---
            elif ext in ["xls", "xlsx"]:
                fs_excel = collections["excels"]
                if not fs_excel.exists({"filename": filename}):
                    fs_excel.put(content, filename=filename)
                    inserted_files += 1
                    yield f"✅ Excel inséré dans GridFS : {filename}\n"
                else:
                    existing_files += 1
                    yield f"⚠️ Excel déjà existant : {filename}\n"

            # --- Gestion des fichiers JSON via GridFS ---
            elif ext == "json":
                fs_json = collections["training_data_json"]
                if not fs_json.exists({"filename": filename}):
                    fs_json.put(content, filename=filename)
                    inserted_files += 1
                    yield f"✅ JSON inséré dans GridFS : {filename}\n"
                else:
                    existing_files += 1
                    yield f"⚠️ JSON déjà existant : {filename}\n"

            # --- Gestion des fichiers TXT via GridFS ---
            elif ext == "txt":
                fs_txt = collections["training_data_txt"]
                if not fs_txt.exists({"filename": filename}):
                    fs_txt.put(content, filename=filename)
                    inserted_files += 1
                    yield f"✅ TXT inséré dans GridFS : {filename}\n"
                else:
                    existing_files += 1
                    yield f"⚠️ TXT déjà existant : {filename}\n"

            else:
                yield f"⚠️ Type de fichier non pris en charge : {filename}\n"

        except Exception as e:
            errors += 1
            yield f"❌ Erreur lors du traitement de {getattr(file, 'filename', 'inconnu')} : {e}\n"

    # --- Traitement des URLs ---
    if urls:
        yield "🌐 Traitement des URLs...\n"
        for url in urls:
            try:
                if collections["urls_scraping"].count_documents({"url": url}) == 0:
                    collections["urls_scraping"].insert_one({"url": url})
                    inserted_files += 1
                    yield f"✅ URL ajoutée : {url}\n"
                else:
                    existing_files += 1
                    yield f"⚠️ URL déjà présente : {url}\n"
            except Exception as e:
                errors += 1
                yield f"❌ Erreur lors de l'ajout de l'URL {url} : {e}\n"

    # --- Résumé final ---
    summary = f"✅ {inserted_files} insérés | ⚠️ {existing_files} déjà existants | ❌ {errors} erreurs"
    yield f"🚀 Traitement terminé — {summary}\n"
