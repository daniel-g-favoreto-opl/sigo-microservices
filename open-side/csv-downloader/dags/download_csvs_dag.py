from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import requests
import os
from pathlib import Path

# Configurações básicas
DEFAULT_CSV_PATH = "/opt/airflow/data/csvs"


def criar_dag_cliente(cliente_nome: str, schedule: str = "@daily"):
    """
    Factory function para criar DAGs específicas por cliente
    """

    @dag(
        dag_id=f"download_csvs_{cliente_nome.replace('-', '_')}",
        start_date=datetime(2024, 1, 1),
        schedule=schedule,  # Execução periódica
        catchup=False,
        tags=["csv", "download", cliente_nome],
        description=f"Download de CSVs para o cliente {cliente_nome}",
        default_args={
            "retries": 2,  # Tenta 2 vezes se falhar
            "retry_delay": timedelta(minutes=5),
        },
    )
    def download_csvs_dag():

        @task
        def obter_token():
            """
            Obtém token OAuth2 do endpoint de autenticação
            """
            print(f"🔐 Obtendo token OAuth2 para cliente: {cliente_nome}...")

            # Busca configurações do cliente nas Airflow Variables
            try:
                config = Variable.get(
                    f"csv_config_{cliente_nome}", deserialize_json=True
                )

            except KeyError:
                print(f"❌ ERRO: Configuração não encontrada para {cliente_nome}")
                print(f"⚠️ Crie a variável 'csv_config_{cliente_nome}' no Airflow!")
                raise

            url = f"{config['oauth_url']}/oauth2/token"

            headers = {"Content-Type": "application/x-www-form-urlencoded"}

            data = {
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "grant_type": "client_credentials",
            }

            try:
                response = requests.post(url, headers=headers, data=data, timeout=30)
                response.raise_for_status()

                token_data = response.json()
                access_token = token_data.get("access_token")

                print(f"✅ Token obtido com sucesso para {cliente_nome}!")

                return {
                    "token": access_token,
                    "cliente": cliente_nome,
                    "config": config,
                }

            except requests.exceptions.RequestException as e:
                print(f"❌ Erro ao obter token: {e}")
                raise

        @task
        def listar_csvs(dados: dict):
            """
            Lista todos os CSVs disponíveis na API
            """
            token = dados["token"]
            cliente = dados["cliente"]
            config = dados["config"]

            print(f"📋 Listando CSVs disponíveis para {cliente}...")

            url = f"{config['api_url']}/api/csvs"

            headers = {"Authorization": f"Bearer {token}"}

            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                resultado = response.json()
                total = resultado.get("total", 0)
                arquivos = resultado.get("arquivos", [])

                print(f"✅ Total de CSVs disponíveis: {total}")
                print(f"📄 Arquivos: {arquivos}")

                return {
                    "token": token,
                    "cliente": cliente,
                    "config": config,
                    "arquivos": arquivos,
                }

            except requests.exceptions.RequestException as e:
                print(f"❌ Erro ao listar CSVs: {e}")
                raise

        @task
        def verificar_e_baixar_csvs(dados: dict):
            """
            Verifica quais CSVs já foram baixados e faz download apenas dos novos
            """
            token = dados["token"]
            cliente = dados["cliente"]
            config = dados["config"]
            arquivos = dados["arquivos"]

            print(f"⬇️ Iniciando download de CSVs para {cliente}...")

            # Cria pasta específica do cliente
            csv_path = os.path.join(DEFAULT_CSV_PATH, cliente)
            Path(csv_path).mkdir(parents=True, exist_ok=True)

            print(f"📁 Pasta de destino: {csv_path}")

            # Verifica quais arquivos já existem
            arquivos_existentes = set(os.listdir(csv_path))
            arquivos_baixados = []
            arquivos_pulados = []
            arquivos_com_erro = []

            headers = {"Authorization": f"Bearer {token}"}

            for arquivo in arquivos:
                nome_arquivo = f"{arquivo}.csv"
                caminho_completo = os.path.join(csv_path, nome_arquivo)

                # Verifica se já foi baixado
                if nome_arquivo in arquivos_existentes:
                    print(f"⏭️ Arquivo {nome_arquivo} já existe. Pulando...")
                    arquivos_pulados.append(arquivo)
                    continue

                # Faz o download
                url = f"{config['api_url']}/api/csv/{arquivo}"

                try:
                    print(f"📥 Baixando {nome_arquivo}...")
                    response = requests.get(url, headers=headers, timeout=60)
                    response.raise_for_status()

                    # Salva o arquivo
                    with open(caminho_completo, "wb") as f:
                        f.write(response.content)

                    print(f"✅ Arquivo {nome_arquivo} baixado com sucesso!")
                    arquivos_baixados.append(arquivo)

                except requests.exceptions.RequestException as e:
                    print(f"❌ Erro ao baixar {nome_arquivo}: {e}")
                    arquivos_com_erro.append(arquivo)
                    continue

            # Relatório final
            print("\n" + "=" * 60)
            print(f"📊 RELATÓRIO DE DOWNLOAD - Cliente: {cliente}")
            print("=" * 60)
            print(f"✅ Arquivos baixados: {len(arquivos_baixados)}")
            print(f"⏭️ Arquivos pulados (já existiam): {len(arquivos_pulados)}")
            print(f"❌ Arquivos com erro: {len(arquivos_com_erro)}")
            print(f"📁 Pasta de destino: {csv_path}")
            print("=" * 60)

            # Lança exceção se houver erros críticos
            if len(arquivos_com_erro) > 0 and len(arquivos_baixados) == 0:
                raise Exception(f"Falha ao baixar todos os arquivos para {cliente}")

            return {
                "cliente": cliente,
                "baixados": arquivos_baixados,
                "pulados": arquivos_pulados,
                "erros": arquivos_com_erro,
                "total_processados": len(arquivos),
            }

        # Fluxo da DAG
        dados_auth = obter_token()
        dados_lista = listar_csvs(dados_auth)
        resultado = verificar_e_baixar_csvs(dados_lista)

    return download_csvs_dag()


# Importa timedelta para retry_delay
from datetime import timedelta

# ============================================================
# CRIE UMA DAG PARA CADA CLIENTE
# ============================================================

# Cliente 1: i-systems - Executa todo dia às 2h da manhã
dag_isystems = criar_dag_cliente(
    cliente_nome="i-systems", schedule="0 2 * * *"  # Cron: 2h AM todos os dias
)

# Cliente 2: fibrasil - Executa todo dia às 3h da manhã
dag_fibrasil = criar_dag_cliente(
    cliente_nome="fibrasil", schedule="0 3 * * *"  # Cron: 3h AM todos os dias
)

# ============================================================
# ADICIONE MAIS CLIENTES AQUI CONFORME NECESSÁRIO
# ============================================================

# Exemplo de outros schedules úteis:
# "@hourly"           - A cada hora
# "@daily"            - Todo dia à meia-noite
# "*/30 * * * *"      - A cada 30 minutos
# "0 */6 * * *"       - A cada 6 horas
# "0 9-17 * * 1-5"    - De hora em hora, das 9h às 17h, segunda a sexta
