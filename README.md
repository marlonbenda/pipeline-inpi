# Consulta automatizada de marcas do INPI

Projeto Python para baixar os dados abertos de marcas do INPI, filtrar titulares do Parana, cruzar as tabelas e gerar bases para pessoas fisicas e juridicas.

## Estrutura

- `src/processar_marcas.py`: script executado localmente e pelo GitHub Actions.
- `notebooks/`: notebook original para exploracao.
- `data/raw/`: dados baixados durante a execucao; nao sao versionados.
- `outputs/`: CSVs gerados; nao sao versionados.
- `.github/workflows/processar-inpi.yml`: execucao manual ou semanal e upload dos resultados como artefato.

## Execucao local

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python src/processar_marcas.py
```

Os resultados ficam em `outputs/`.

## GitHub Actions

Abra a aba **Actions**, escolha **Processar dados do INPI** e use **Run workflow**. O workflow tambem esta agendado para segunda-feira as 06:00 UTC.

O resultado pode ser baixado na secao **Artifacts** da execucao. Este projeto atualmente nao usa BigQuery nem a secret `GCP_CREDENTIALS`; essa secret so sera necessaria quando uma etapa de envio ao BigQuery for implementada.

Nunca versione arquivos JSON de credenciais.
