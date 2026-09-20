import os
import requests
import zipfile
import pandas as pd
import urllib3
import pandas_gbq

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. CONFIGURAÇÕES GERAIS E BIGQUERY
# ==========================================
URL_INPI = "https://dadosabertos.inpi.gov.br/download/marcas/marcas.zip" 
DIR_TRABALHO = "./dados_inpi_local"
os.makedirs(DIR_TRABALHO, exist_ok=True)
caminho_zip = os.path.join(DIR_TRABALHO, "marcas.zip")

# Configurações do seu Google Cloud
PROJECT_ID = 'gen-lang-client-0182432496'
DATASET_ID = 'inpi_dados'

# ==========================================
# 2. EXTRAÇÃO
# ==========================================
print("📥 Iniciando download...")
response = requests.get(URL_INPI, stream=True, verify=False)
with open(caminho_zip, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
        
print("📦 Descompactando...")
with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
    zip_ref.extractall(DIR_TRABALHO)

# ==========================================
# 3. CARREGAMENTO DOS ARQUIVOS (LOAD)
# ==========================================
print("📂 Carregando arquivos CSV...")
csv_depositantes = os.path.join(DIR_TRABALHO, "MARCAS_DEPOSITANTES.csv")
csv_bibliograficos = os.path.join(DIR_TRABALHO, "MARCAS_DADOS_BIBLIOGRAFICOS.csv")
csv_nice = os.path.join(DIR_TRABALHO, "MARCAS_CLASSIFICACOES_NICE.csv")
csv_viena = os.path.join(DIR_TRABALHO, "MARCAS_CLASSIFICACOES_VIENA.csv")

df_depositantes_raw = pd.read_csv(csv_depositantes, sep=';', encoding='utf-8', low_memory=False, on_bad_lines='skip')
df_biblio_raw = pd.read_csv(csv_bibliograficos, sep=';', encoding='utf-8', low_memory=False, on_bad_lines='skip')
df_nice_raw = pd.read_csv(csv_nice, sep=';', encoding='utf-8', low_memory=False, on_bad_lines='skip')
df_viena_raw = pd.read_csv(csv_viena, sep=';', encoding='utf-8', low_memory=False, on_bad_lines='skip')

# ==========================================
# 4. TRANSFORMAÇÃO E CRUZAMENTO
# ==========================================
print("⚙️ Aplicando filtros e cruzando tabelas...")
df_pr = df_depositantes_raw[df_depositantes_raw['estado'] == 'PR'].copy()

# Limpeza e Padronização
df_pr['cnpj_limpo'] = df_pr['cnpj_cpf_titular'].apply(lambda x: ''.join(filter(str.isdigit, str(x))))
tipo_titular = df_pr['tipo_pfpj_titular'].astype('string').str.strip().str.upper()
mask_pf = tipo_titular.eq('PESSOA FÍSICA')
mask_pj = tipo_titular.eq('PESSOA JURÍDICA')
df_pr.loc[mask_pf, 'nome'] = df_pr.loc[mask_pf, 'nome'].str.title()
df_pr.loc[mask_pj, 'nome'] = df_pr.loc[mask_pj, 'nome'].str.upper()

codigos_validos = df_pr['codigo_interno'].unique()

df_biblio_filtrado = df_biblio_raw[df_biblio_raw['codigo_interno'].isin(codigos_validos)].copy()
df_nice_filtrado = df_nice_raw[df_nice_raw['codigo_interno'].isin(codigos_validos)].copy()
df_viena_filtrado = df_viena_raw[df_viena_raw['codigo_interno'].isin(codigos_validos)].copy()

del df_depositantes_raw, df_biblio_raw, df_nice_raw, df_viena_raw

dict_agrupamento_nice = {col: lambda x: ' | '.join(x.dropna().astype(str).unique()) for col in df_nice_filtrado.columns if col != 'codigo_interno'}
df_nice_agrupado = df_nice_filtrado.groupby('codigo_interno').agg(dict_agrupamento_nice).reset_index()

df_final = pd.merge(df_pr, df_biblio_filtrado, on='codigo_interno', how='left')
df_final = pd.merge(df_final, df_nice_agrupado, on='codigo_interno', how='left')

# ==========================================
# 5. REGRAS DE NEGÓCIO
# ==========================================
print("🧹 Criando regras de negócio...")
if 'numero_inpi_x' in df_final.columns:
    df_final.rename(columns={'numero_inpi_x': 'numero_processo_inpi'}, inplace=True)

colunas_para_remover = ['numero_inpi_y', 'numero_inpi', 'edicao_nice', 'especificacao_trad']
colunas_presentes = [col for col in colunas_para_remover if col in df_final.columns]
if colunas_presentes:
    df_final.drop(columns=colunas_presentes, inplace=True)

colunas_de_data = ['data_deposito', 'data_publicacao', 'data_concessao', 'data_vigencia']
for col in colunas_de_data:
    if col in df_final.columns:
        df_final[col] = pd.to_datetime(df_final[col], errors='coerce').dt.strftime('%Y-%m-%d')

def classificar_status(status):
    status_str = str(status).lower()
    if 'vigor' in status_str:
        return '🟢 Ativo'
    elif 'arquivado' in status_str or 'extinto' in status_str or 'indeferido' in status_str:
        return '🔴 Atenção / Recuperação'
    elif 'oposição' in status_str:
        return '🟡 Risco (Oposição)'
    else:
        return '🔵 Em Andamento Normal'

if 'descricao_situacao' in df_final.columns:
    df_final['macro_status'] = df_final['descricao_situacao'].apply(classificar_status)

hoje = pd.Timestamp.now()
def classificar_vencimento(data_vig):
    if pd.isna(data_vig):
        return '⚪ Não Aplicável'
    try:
        data_vig_dt = pd.to_datetime(data_vig)
        dias_restantes = (data_vig_dt - hoje).days
        if dias_restantes < -180: return '⚫ Extinto (Perda Definitiva)'
        elif -180 <= dias_restantes < 0: return '🔴 Prazo Extraordinário (Vencido, sujeito a multa)'
        elif 0 <= dias_restantes <= 180: return '🟠 Urgência (Expira em menos de 6 meses)'
        elif 180 < dias_restantes <= 365: return '🟡 Janela Aberta (Expira em 6 a 12 meses)'
        else: return '🟢 Vigente (Mais de 1 ano)'
    except:
        return '⚪ Erro na Data'

if 'data_vigencia' in df_final.columns:
    df_final['alerta_vencimento'] = df_final['data_vigencia'].apply(classificar_vencimento)

df_juridica = df_final[mask_pj].copy()
df_nao_juridica = df_final[mask_pf].copy()

# ==========================================
# 6. CARGA PARA O BIGQUERY
# ==========================================
print("\n🚀 Enviando dados para o Google BigQuery...")

# Envia Pessoas Jurídicas
pandas_gbq.to_gbq(df_juridica, f'{DATASET_ID}.fat_marcas_pr_pj', project_id=PROJECT_ID, if_exists='replace')
print(f"-> 🏢 Base PJ salva com sucesso no BigQuery!")

# Envia Pessoas Físicas
pandas_gbq.to_gbq(df_nao_juridica, f'{DATASET_ID}.fat_marcas_pr_pf', project_id=PROJECT_ID, if_exists='replace')
print(f"-> 👤 Base PF salva com sucesso no BigQuery!")

# Envia Dimensão Viena
pandas_gbq.to_gbq(df_viena_filtrado, f'{DATASET_ID}.dim_viena_pr', project_id=PROJECT_ID, if_exists='replace')
print(f"-> 🎨 Dimensão Viena salva com sucesso no BigQuery!")

print("\n✅ Processo totalmente concluído!")