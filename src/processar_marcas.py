import os
import requests
import pandas as pd
import urllib3
import pandas_gbq
import logging
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Descobre o caminho absoluto do projeto de forma dinâmica
PASTA_ATUAL = os.path.dirname(os.path.abspath(__file__)) # Pasta src/
RAIZ_PROJETO = os.path.dirname(PASTA_ATUAL) # Volta uma pasta para APP - INPI/

# Carrega o .env apontando para a raiz exata
load_dotenv(os.path.join(RAIZ_PROJETO, ".env"))

# Silencia os avisos de requisições HTTPS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. CONFIGURAÇÕES E LOGS
# ==========================================
DIR_RAW = os.path.join(RAIZ_PROJETO, "data", "raw")
DIR_OUTPUT = os.path.join(RAIZ_PROJETO, "outputs")
os.makedirs(DIR_RAW, exist_ok=True)
os.makedirs(DIR_OUTPUT, exist_ok=True)

# Configuração do ficheiro de Log (.txt)
arquivo_log = os.path.join(DIR_OUTPUT, "pipeline_execucao.txt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(arquivo_log, encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)

URLS_INPI = {
    "MARCAS_DEPOSITANTES.csv": "https://dadosabertos.inpi.gov.br/download/marcas/MARCAS_DEPOSITANTES.csv",
    "MARCAS_DADOS_BIBLIOGRAFICOS.csv": "https://dadosabertos.inpi.gov.br/download/marcas/MARCAS_DADOS_BIBLIOGRAFICOS.csv",
    "MARCAS_CLASSIFICACOES_VIENA.csv": "https://dadosabertos.inpi.gov.br/download/marcas/MARCAS_CLASSIFICACOES_VIENA.csv",
    "MARCAS_CLASSIFICACOES_NICE.csv": "https://dadosabertos.inpi.gov.br/download/marcas/MARCAS_CLASSIFICACOES_NICE.csv"
}

PROJECT_ID = 'gen-lang-client-0182432496'
DATASET_ID = 'inpi_dados'

# ==========================================
# 2. FUNÇÕES AUXILIARES
# ==========================================
def enviar_mensagem_telegram(mensagem):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logging.warning("Aviso: Token ou Chat ID do Telegram não configurado nas variáveis de ambiente.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {
        'chat_id': chat_id,
        'text': mensagem
    }

    try:
        response = requests.post(url, data=data, timeout=10, verify=False)
        if response.status_code == 200:
            logging.info("Notificação enviada para o Telegram com sucesso!")
        else:
            logging.error(f"Erro ao enviar mensagem para o Telegram: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logging.error(f"Erro ao conectar ao Telegram: {e}")

def classificar_status(status):
    status_str = str(status).lower()
    if 'vigor' in status_str: return '🟢 Ativo'
    elif 'arquivado' in status_str or 'extinto' in status_str or 'indeferido' in status_str: return '🔴 Atenção / Recuperação'
    elif 'oposição' in status_str: return '🟡 Risco (Oposição)'
    else: return '🔵 Em Andamento Normal'

def classificar_vencimento(data_vig, hoje):
    if pd.isna(data_vig): return '⚪ Não Aplicável'
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

# ==========================================
# 3. PIPELINE PRINCIPAL
# ==========================================
def main():
    try:
        logging.info("🚀 Iniciando o pipeline automatizado do INPI...")

        # Autenticação Google Cloud
        caminho_credencial = os.path.join(RAIZ_PROJETO, "gcp_key.json")
        if os.path.exists(caminho_credencial):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = caminho_credencial
            logging.info("Credenciais do GCP carregadas com sucesso.")
        else:
            logging.warning("Ficheiro gcp_key.json não encontrado. O envio para o BigQuery poderá falhar se não estiver noutro ambiente autenticado.")

        # EXTRAÇÃO
        logging.info(f"📥 A iniciar a transferência de CSVs diretos para {DIR_RAW}...")
        for nome_arquivo, url in URLS_INPI.items():
            caminho_destino = os.path.join(DIR_RAW, nome_arquivo)
            logging.info(f"   -> A transferir {nome_arquivo}...")
            
            response = requests.get(url, stream=True, verify=False)
            response.raise_for_status() 
            
            with open(caminho_destino, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info("✅ Etapa de extração finalizada.")

        # LOAD
        logging.info("📂 A carregar os ficheiros CSV para memória...")
        csv_depositantes = os.path.join(DIR_RAW, "MARCAS_DEPOSITANTES.csv")
        csv_bibliograficos = os.path.join(DIR_RAW, "MARCAS_DADOS_BIBLIOGRAFICOS.csv")
        csv_nice = os.path.join(DIR_RAW, "MARCAS_CLASSIFICACOES_NICE.csv")
        csv_viena = os.path.join(DIR_RAW, "MARCAS_CLASSIFICACOES_VIENA.csv")

        df_depositantes_raw = pd.read_csv(csv_depositantes, sep=',', encoding='utf-8', low_memory=False, on_bad_lines='skip')
        df_biblio_raw = pd.read_csv(csv_bibliograficos, sep=',', encoding='utf-8', low_memory=False, on_bad_lines='skip')
        df_nice_raw = pd.read_csv(csv_nice, sep=',', encoding='utf-8', low_memory=False, on_bad_lines='skip')
        df_viena_raw = pd.read_csv(csv_viena, sep=',', encoding='utf-8', low_memory=False, on_bad_lines='skip')

        # TRANSFORMAÇÃO E CRUZAMENTO
        logging.info("⚙️ A aplicar filtros e a cruzar tabelas...")
        df_pr = df_depositantes_raw[(df_depositantes_raw['estado'] == 'PR')].copy()
        df_pr['cnpj_limpo'] = df_pr['cnpj_cpf_titular'].apply(lambda x: ''.join(filter(str.isdigit, str(x))))

        tipo_titular = df_pr['tipo_pfpj_titular'].astype('string').str.strip().str.upper()
        mask_pf = tipo_titular.eq('PESSOA FÍSICA')
        mask_pj = tipo_titular.eq('PESSOA JURÍDICA')
        df_pr.loc[mask_pf, 'nome'] = df_pr.loc[mask_pf, 'nome'].str.title()
        df_pr.loc[mask_pj, 'nome'] = df_pr.loc[mask_pj, 'nome'].str.upper()

        codigos_validos = df_pr['codigo_interno'].unique()
        logging.info(f"   -> Encontrados {len(codigos_validos)} códigos internos únicos no PR.")

        df_biblio_filtrado = df_biblio_raw[df_biblio_raw['codigo_interno'].isin(codigos_validos)].copy()
        df_nice_filtrado = df_nice_raw[df_nice_raw['codigo_interno'].isin(codigos_validos)].copy()
        df_viena_filtrado = df_viena_raw[df_viena_raw['codigo_interno'].isin(codigos_validos)].copy()

        del df_depositantes_raw, df_biblio_raw, df_nice_raw, df_viena_raw

        dict_agrupamento_nice = {col: lambda x: ' | '.join(x.dropna().astype(str).unique()) for col in df_nice_filtrado.columns if col != 'codigo_interno'}
        df_nice_agrupado = df_nice_filtrado.groupby('codigo_interno').agg(dict_agrupamento_nice).reset_index()

        df_final = pd.merge(df_pr, df_biblio_filtrado, on='codigo_interno', how='left')
        df_final = pd.merge(df_final, df_nice_agrupado, on='codigo_interno', how='left')

        # REGRAS DE NEGÓCIO
        logging.info("🧹 A aplicar regras de negócio...")
        if 'numero_inpi_x' in df_final.columns:
            df_final.rename(columns={'numero_inpi_x': 'numero_processo_inpi'}, inplace=True)

        colunas_para_remover = ['numero_inpi_y', 'numero_inpi', 'edicao_nice', 'especificacao_trad']
        colunas_presentes = [col for col in colunas_para_remover if col in df_final.columns]
        if colunas_presentes:
            df_final.drop(columns=colunas_presentes, inplace=True)

        colunas_de_data = ['data_deposito', 'data_publicacao', 'data_concessao', 'data_vigencia']
        for col in colunas_de_data:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], errors='coerce')

        if 'descricao_situacao' in df_final.columns:
            df_final['macro_status'] = df_final['descricao_situacao'].apply(classificar_status)

        hoje = pd.Timestamp.now()
        if 'data_vigencia' in df_final.columns:
            df_final['alerta_vencimento'] = df_final['data_vigencia'].apply(lambda x: classificar_vencimento(x, hoje))

        tipo_titular_final = df_final['tipo_pfpj_titular'].astype('string').str.strip().str.upper()
        df_juridica = df_final[tipo_titular_final == 'PESSOA JURÍDICA'].copy()
        df_nao_juridica = df_final[tipo_titular_final == 'PESSOA FÍSICA'].copy()

        logging.info(f"Base higienizada. PJ: {len(df_juridica)} registos | PF: {len(df_nao_juridica)} registos.")

        # CARGA PARA O BIGQUERY
        logging.info("☁️ A enviar dados para o Google BigQuery...")
        pandas_gbq.to_gbq(df_juridica, f'{DATASET_ID}.fat_marcas_pr_pj', project_id=PROJECT_ID, if_exists='replace')
        pandas_gbq.to_gbq(df_nao_juridica, f'{DATASET_ID}.fat_marcas_pr_pf', project_id=PROJECT_ID, if_exists='replace')
        pandas_gbq.to_gbq(df_viena_filtrado, f'{DATASET_ID}.dim_viena_pr', project_id=PROJECT_ID, if_exists='replace')
        logging.info("✅ Dados guardados com sucesso no BigQuery!")

        # Notificação de Sucesso
        enviar_mensagem_telegram(
            f"✅ *Pipeline INPI Concluído!*\n\n"
            f"Bases atualizadas no BigQuery:\n"
            f"🏢 Empresas (PJ): {len(df_juridica)}\n"
            f"👤 Particulares (PF): {len(df_nao_juridica)}"
        )

    except Exception as e:
        msg_erro = f"❌ *Erro Crítico no Pipeline INPI:*\n{str(e)}"
        logging.error(msg_erro)
        logging.error(traceback.format_exc())
        enviar_mensagem_telegram(msg_erro)

if __name__ == "__main__":
    main()