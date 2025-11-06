"""
transform.py
-------------
Módulo responsável por transformar os dados brutos do SNIC
(Extraídos do portal da CBIC) em um formato tabular limpo e padronizado.

Etapas executadas:
- Limpeza das linhas e colunas extras.
- Normalização de espaços e textos.
- Criação e preenchimento da coluna REGIAO.
- Renomeação da coluna ESTADO.
- Remoção de linhas agregadas (REGIÃO).
- Pivoteamento das colunas de meses -> coluna MES.
- Inclusão da coluna ANO (a partir do nome da aba).
- Exportação para /data/processed/consumo_cimento_tratado.csv
"""

import os
import re
import pandas as pd


def transform_data():
    """
    Transforma o arquivo Excel bruto do SNIC em formato tabular limpo.

    Executa:
      - limpeza de linhas e colunas extras
      - criação e preenchimento da coluna REGIAO
      - renomeação da coluna ESTADO
      - remoção de duplicatas de região
      - normalização de nomes
      - pivoteamento (colunas de meses -> coluna MES)
      - criação automática da coluna ANO (a partir do nome da aba)
    """

    # Caminhos
    RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
    PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "../data/processed")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Selecionar o arquivo mais recente
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".xlsx")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo Excel encontrado em data/raw/")
    files.sort(key=lambda f: os.path.getmtime(os.path.join(RAW_DATA_DIR, f)), reverse=True)
    latest_file = os.path.join(RAW_DATA_DIR, files[0])

    print(f"📂 Lendo arquivo: {latest_file}")

    # Ler todas as abas do Excel
    xls = pd.ExcelFile(latest_file)
    all_data = []

    for sheet_name in xls.sheet_names:
        print(f"\n🔹 Processando aba: {sheet_name}")
        df = pd.read_excel(latest_file, sheet_name=sheet_name)

        # Remover linhas e colunas extras
        if len(df) <= 8:
            print(f"⚠️ Aba {sheet_name} ignorada (poucas linhas).")
            continue

        # Remover linhas e colunas extras
        if len(df) <= 8:
            print(f"⚠️ Aba {sheet_name} ignorada (poucas linhas).")
            continue

        # Remover as 3 primeiras linhas e a última coluna
        df = df.iloc[3:, :-1].reset_index(drop=True)

        # 🧼 Normalizar texto da primeira coluna (remove apóstrofos, espaços e caixa alta)
        df.iloc[:, 0] = (
            df.iloc[:, 0]
            .astype(str)
            .str.replace("'", "", regex=False)
            .str.strip()
            .str.upper()
        )

        # 🔍 Procurar qualquer linha que contenha "BRASIL"
        idx_brasil = df[df.iloc[:, 0].str.contains("BRASIL", na=False)].index

        if not idx_brasil.empty:
            corte = idx_brasil[0]
            df = df.iloc[:corte]  # mantém apenas até antes do "BRASIL"

        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

        # --- Limpar espaços na primeira coluna antes de criar REGIAO ---
        primeira_coluna = df.columns[0]
        df[primeira_coluna] = (
            df[primeira_coluna]
            .astype(str)
            .str.strip()
            .apply(lambda x: re.sub(r"\s+", " ", x))
        )

        # Criar coluna REGIAO
        df.insert(1, "REGIAO", None)
        regioes = [
            "REGIÃO NORTE",
            "REGIÃO NORDESTE",
            "REGIÃO SUDESTE",
            "REGIÃO SUL",
            "CENTRO-OESTE"
        ]

        # Identificar linhas de região
        for i in range(len(df)):
            valor = str(df.iloc[i, 0]).strip().upper()
            if valor in regioes:
                df.loc[i, "REGIAO"] = valor

        # Fill up (bfill = preencher para cima)
        df["REGIAO"] = df["REGIAO"].bfill()

        # Renomear primeira coluna
        df.rename(columns={df.columns[0]: "ESTADO"}, inplace=True)

        # Remover linhas duplicadas de região
        df = df[df["ESTADO"].str.strip().str.upper() != df["REGIAO"].str.strip().str.upper()]
        df.reset_index(drop=True, inplace=True)

        # Remover palavra "REGIÃO"
        df["REGIAO"] = (
            df["REGIAO"]
            .astype(str)
            .str.replace("REGIÃO", "", case=False, regex=False)
            .str.strip()
        )

        # Pivoteamento (colunas de meses -> coluna MES)
        colunas_fixas = ["ESTADO", "REGIAO"]
        colunas_meses = [c for c in df.columns if c not in colunas_fixas]

        df_long = df.melt(
            id_vars=colunas_fixas,
            value_vars=colunas_meses,
            var_name="MES",
            value_name="CONSUMO_T"
        )

        # Criar coluna ANO = nome da aba
        df_long["ANO"] = str(sheet_name).strip()

        # --- Limpeza e conversão da coluna CONSUMO_T ---
        df_long["CONSUMO_T"] = (
            df_long["CONSUMO_T"]
            .astype(str)
            .str.replace(".", ",", case=False, regex=False)
            .str.strip()
        )

        df_long["CONSUMO_T"] = (
            df_long["CONSUMO_T"]
            .astype(str)
            .str.replace("nan", "0", case=False, regex=False)
            .str.strip()
        )

        # Normalização final
        df_long["MES"] = df_long["MES"].str.strip().str.upper()

        # Reordenar colunas
        df_long = df_long[["ANO", "MES", "ESTADO", "REGIAO", "CONSUMO_T"]]
        all_data.append(df_long)

    # Consolidar todas as abas
    if not all_data:
        print("⚠️ Nenhuma aba processada.")
        return None

    final_df = pd.concat(all_data, ignore_index=True)
    output_path = os.path.join(PROCESSED_DIR, "consumo_cimento_tratado.csv")
    final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # --- Consolida todas as abas ---
    final_df = pd.concat(all_data, ignore_index=True)

    # --- Caminho de saída ---
    output_path = os.path.join(PROCESSED_DIR, "consumo_cimento_tratado.csv")

    # --- Salva CSV ---
    final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ Arquivo consolidado salvo em: {output_path}")
    print(final_df.head(10))
    
    return final_df

# ✅ Permite rodar diretamente via terminal
if __name__ == "__main__":
    transform_data()
