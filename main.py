import os
from datetime import datetime
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_to_google_sheets
import json

METADATA_PATH = os.path.join(os.path.dirname(__file__), "../data/metadata.json")

def update_metadata(date):
    with open(METADATA_PATH, "r") as f:
        metadata = json.load(f)
    metadata["ultima_coleta"] = date.strftime("%Y-%m-%d")
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=4)
    print(f"🗓️ Metadados atualizados com a nova coleta ({date})")

def main():
    print("\n🚀 Iniciando automação ETL SNIC...\n")

    # 1️⃣ EXTRAÇÃO
    raw_file = extract_data()
    if not raw_file:
        print("❌ Erro na extração — abortando pipeline.")
        return

    # 2️⃣ TRANSFORMAÇÃO
    transform_data()

    # 3️⃣ ARMAZENAMENTO → Google Sheets
    load_to_google_sheets()

    print("\n🎯 ETL finalizado com sucesso!")

if __name__ == "__main__":
    main()
