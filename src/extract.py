import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Caminhos importantes
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
METADATA_DIR = os.path.join(os.path.dirname(__file__), "../data/metadata.json")

# URL utilizadas
CBIC_URL = "http://www.cbicdados.com.br/menu/materiais-de-construcao/cimento"
CBIC_FILE_URL = "http://www.cbicdados.com.br/media/anexos/tabela_07.A.03_Consumo_cimento_"

def load_metadata():
    """
    Carrega o arquivo de metadados, criando-o vazio caso não exista.
    Garante a criação da pasta 'data' e do arquivo 'metadata.json'.
    """
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    if not os.path.exists(METADATA_DIR):
        print("🆕 Criando novo arquivo de metadados vazio...")
        metadata = {
            "ultima_coleta": None,
            "ultima_url": None,
            "ultima_versao": None,
            "ultima_data_atualizacao": None
        }
        with open(METADATA_DIR, "w") as f:
            json.dump(metadata, f, indent=4)
        print(f"✅ Arquivo criado em: {METADATA_DIR}")
        return metadata

    with open(METADATA_DIR, "r") as f:
        metadata = json.load(f)
        print(f"📄 Metadados carregados de: {METADATA_DIR}")
        return metadata


def save_metadata(metadata):
    """Salva os metadados atualizados"""
    with open(METADATA_DIR, "w") as f:
        json.dump(metadata, f, indent=4)

def get_update_date():
    """
    Obtém a data de atualização do site CBIC de forma contextualizada,
    procurando a tag <h3>Cimento</h3> e o primeiro <span class='date-time'> após ela.
    """
    response = requests.get(CBIC_URL)
    soup = BeautifulSoup(response.text, "lxml")

    # Localiza o título "Cimento"
    titulo_cimento = soup.find("h3", string=lambda s: s and "Cimento" in s)
    if not titulo_cimento:
        raise ValueError("Título 'Cimento' não encontrado na página.")

    # Procura o primeiro <span class='date-time'> após o título
    span_data = titulo_cimento.find_next("span", class_="date-time")
    if not span_data:
        raise ValueError("Data de atualização (span.date-time) não encontrada após o título 'Cimento'.")

    # Extrai e converte o texto da data
    data_texto = span_data.text.strip()
    try:
        data_atualizacao = datetime.strptime(data_texto, "%d/%m/%Y").date()
        return data_atualizacao
    except ValueError:
        raise ValueError(f"Erro ao converter a data: {data_texto}")

def find_latest_file(start_num=54, limit=1000):
    """Verifica URLs incrementais para achar o arquivo Excel mais recente"""
    latest_url = None
    num = start_num
    while num < limit:
        url = f"{CBIC_FILE_URL}{num}.xlsx"
        response = requests.head(url)
        if response.status_code == 200:
            latest_url = url
            num += 1
        else:
            break
    return latest_url

def download_file(url, update_date):
    """Baixa o arquivo Excel mais recente"""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    filename = f"consumo_cimento_{update_date}.xlsx"
    filepath = os.path.join(RAW_DATA_DIR, filename)

    response = requests.get(url)
    with open(filepath, "wb") as f:
        f.write(response.content)
    return filepath

def extract_data():
    """Função principal de extração"""
    print("🔍 Iniciando verificação de atualização...")
    metadata = load_metadata()
    ultima_coleta = metadata.get("ultima_coleta")

    try:
        data_atualizacao = get_update_date()
        print(f"📅 Data mais recente no site: {data_atualizacao}")
    except Exception as e:
        print("Erro ao obter data de atualização: ", e)
        return None
    
    if isinstance(ultima_coleta, str):
        ultima_coleta = datetime.strptime(ultima_coleta, "%Y-%m-%d").date()
    
    if not ultima_coleta or data_atualizacao > ultima_coleta:
        print("📦 Nova versão detectada! Buscando arquivo mais recente...")
        latest_url = find_latest_file()
        if latest_url:
            print(f"⬇️ Baixando: {latest_url}")
            filepath = download_file(latest_url, data_atualizacao)
            metadata["ultima_coleta"] = str(data_atualizacao)
            metadata["ultima_url"] = latest_url
            save_metadata(metadata)
            print(f"✅ Arquivo salvo em: {filepath}")
            return filepath
        else:
            print("Nenhum novo arquivo encontrado.")
    else:
        print("✅ Nenhuma atualização detectada. Dados já estão atualizados.")
    return None

if __name__ == "__main__":
    extract_data()
