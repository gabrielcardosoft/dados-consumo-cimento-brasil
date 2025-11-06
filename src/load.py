import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# === 1️⃣ Carregar variáveis do arquivo .env ===
load_dotenv()

CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")
WORKSHEET_NAME = os.getenv("GOOGLE_SHEETS_TAB")

# Caminho do CSV processado
CSV_PATH = os.path.join(os.path.dirname(__file__), "../data/processed/consumo_cimento_tratado.csv")

def load_to_google_sheets():
    """Envia os dados tratados para o Google Sheets."""
    if not all([CREDENTIALS_PATH, SHEET_ID, WORKSHEET_NAME]):
        raise ValueError("❌ Variáveis de ambiente ausentes. Verifique o arquivo .env")

    print(f"📂 Lendo CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype=str)

    print(f"🔐 Autenticando com credenciais: {CREDENTIALS_PATH}")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    client = gspread.authorize(creds)

    print(f"🔗 Conectando à planilha: {SHEET_ID}")
    sheet = client.open_by_key(SHEET_ID)

    try:
        worksheet = sheet.worksheet(WORKSHEET_NAME)
        worksheet.clear()
        print(f"🧹 Aba '{WORKSHEET_NAME}' limpa com sucesso.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=WORKSHEET_NAME, rows=str(len(df)+10), cols=str(len(df.columns)+5))
        print(f"🆕 Aba '{WORKSHEET_NAME}' criada.")


    # Substitui NaN antes do envio
    df = df.fillna("")

    # Enviar cabeçalho + dados
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    

    print(f"✅ Dados enviados com sucesso para aba '{WORKSHEET_NAME}'!")
    print(f"📊 Total de linhas: {len(df)}")

if __name__ == "__main__":
    load_to_google_sheets()
