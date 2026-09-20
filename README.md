# 📊 Radar de Marcas INPI - Paraná

Projeto de extensão acadêmica focado na criação de um pipeline de dados de custo zero para alimentar um dashboard de consulta e monitoramento de marcas do INPI, filtrado para titulares do estado do Paraná.

O sistema extrai bases de dados abertas, aplica regras de negócio (alertas de vencimento e farol de status) e orquestra a carga diretamente para a nuvem, garantindo que o painel no Looker Studio seja atualizado sem intervenção humana.

## 🏗️ Stack Tecnológica
- **Processamento:** Python (Pandas, Requests)
- **Armazenamento:** Google BigQuery (Data Warehouse)
- **Visualização:** Looker Studio
- **Orquestração:** GitHub Actions (Rotina semanal)
- **Monitoramento:** Telegram Bot API (Logs de execução)

## 📁 Estrutura do Repositório

- `src/processar_marcas.py`: Script principal de ETL executado localmente ou pelo GitHub Actions.
- `notebooks/`: Ambientes isolados para testes e homologação de regras de negócio.
- `data/raw/`: Diretório temporário para download das bases brutas (ignorado no versionamento).
- `outputs/`: Armazena os logs textuais de execução (`pipeline_execucao.txt`).
- `.github/workflows/processar-inpi.yml`: Arquivo de configuração da automação (CI/CD).

## ⚙️ Automação (GitHub Actions)
- O workflow está configurado para ser disparado automaticamente toda segunda-feira às 03:00 UTC.
- Ele constrói um ambiente limpo (Ubuntu), injeta de forma segura as Secrets do repositório (GCP_CREDENTIALS, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) e realiza a carga replace no BigQuery, atualizando os dados do Looker Studio instantaneamente. Também é possível forçar a execução manual pela aba Actions > Run workflow.