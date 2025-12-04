from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os
from pathlib import Path

# Configurações
DEFAULT_CSV_PATH = "/opt/airflow/data/csvs"
POSTGRES_CONN_ID = "postgres_default"  # Configure esta conexão no Airflow


@dag(
    dag_id="processar_ttks_para_postgres",
    start_date=datetime(2024, 1, 1),
    schedule="0 4 * * *",  # Todo dia às 4h AM (após download dos CSVs)
    catchup=False,
    tags=["ttk", "postgres", "etl"],
    description="Processa CSVs de TTKs e insere/atualiza no PostgreSQL",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def processar_ttks_dag():

    @task
    def criar_tabelas():
        """
        Cria as tabelas de clientes e TTKs se não existirem
        """
        print("🗄️ Criando estrutura de tabelas no PostgreSQL...")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # SQL para criar tabela de clientes
        sql_clientes = """
        CREATE TABLE IF NOT EXISTS clientes (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100) UNIQUE NOT NULL,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ativo BOOLEAN DEFAULT TRUE
        );
        
        -- Cria índice para melhor performance
        CREATE INDEX IF NOT EXISTS idx_clientes_nome ON clientes(nome);
        """

        # SQL para criar tabela de TTKs - TODAS as 66 colunas do CSV
        sql_ttks = """
        CREATE TABLE IF NOT EXISTS ttks (
            id BIGINT PRIMARY KEY,
            cliente_id INTEGER NOT NULL,
            reference VARCHAR(255),
            title TEXT,
            type VARCHAR(100),
            state VARCHAR(100),
            priority VARCHAR(50),
            category VARCHAR(255),
            subcategory_id INTEGER,
            description TEXT,
            creation_date TIMESTAMP,
            resolution_due_date TIMESTAMP,
            category_id INTEGER,
            symptom_id INTEGER,
            previous_state_id INTEGER,
            creation_team VARCHAR(255),
            change_id_n VARCHAR(255),
            public_flag INTEGER,
            resolution_sla_minute INTEGER,
            creation_team_public_id VARCHAR(255),
            urgency_id INTEGER,
            urgency VARCHAR(100),
            impact_id INTEGER,
            origin VARCHAR(255),
            cause_type VARCHAR(255),
            cause_type_id INTEGER,
            subcategory_public_id VARCHAR(255),
            resolution_estimated_date TIMESTAMP,
            change_user_internal_flag INTEGER,
            sla_minute INTEGER,
            previous_state VARCHAR(100),
            service_id_count_by_client TEXT,
            original_ticket_id VARCHAR(255),
            start_date TIMESTAMP,
            tag VARCHAR(255),
            automatic_flag INTEGER,
            resolution_sla_unity VARCHAR(100),
            creation_user_name VARCHAR(255),
            priority_id INTEGER,
            state_id INTEGER,
            change_user_id INTEGER,
            state_date TIMESTAMP,
            change_date TIMESTAMP,
            previous_resolution_estimated_date TIMESTAMP,
            impact VARCHAR(100),
            type_id INTEGER,
            symptom_public_id VARCHAR(255),
            impact_public_id VARCHAR(255),
            creation_user_id INTEGER,
            creation_user_internal_flag INTEGER,
            automatic_creation_flag INTEGER,
            change_team_id INTEGER,
            subcategory VARCHAR(255),
            priority_public_id VARCHAR(255),
            entity VARCHAR(100),
            max_id BIGINT,
            hierarq_escalation INTEGER,
            change_id VARCHAR(255),
            origin_id INTEGER,
            observations TEXT,
            change_user_name VARCHAR(255),
            symptom VARCHAR(255),
            previous_service_id_count_by_client TEXT,
            category_public_id VARCHAR(255),
            change_team VARCHAR(255),
            tim_associated_tickets TEXT,
            data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_cliente FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
        );
        
        -- Cria índices para melhor performance
        CREATE INDEX IF NOT EXISTS idx_ttks_cliente_id ON ttks(cliente_id);
        CREATE INDEX IF NOT EXISTS idx_ttks_creation_date ON ttks(creation_date);
        CREATE INDEX IF NOT EXISTS idx_ttks_state ON ttks(state);
        CREATE INDEX IF NOT EXISTS idx_ttks_priority ON ttks(priority);
        """

        try:
            # Executa criação das tabelas
            hook.run(sql_clientes)
            print("✅ Tabela 'clientes' criada/verificada com sucesso!")

            hook.run(sql_ttks)
            print("✅ Tabela 'ttks' criada/verificada com sucesso!")

            return {"status": "success"}

        except Exception as e:
            print(f"❌ Erro ao criar tabelas: {e}")
            raise

    @task
    def listar_csvs_por_cliente():
        """
        Lista todos os CSVs disponíveis organizados por cliente
        """
        print("📋 Listando CSVs por cliente...")

        clientes_csvs = {}

        # Percorre as pastas de clientes
        csv_base_path = Path(DEFAULT_CSV_PATH)

        if not csv_base_path.exists():
            print(f"⚠️ Pasta {DEFAULT_CSV_PATH} não existe!")
            return {}

        for cliente_path in csv_base_path.iterdir():
            if cliente_path.is_dir():
                cliente_nome = cliente_path.name
                csvs = list(cliente_path.glob("*.csv"))

                if csvs:
                    clientes_csvs[cliente_nome] = [str(csv) for csv in csvs]
                    print(f"📁 {cliente_nome}: {len(csvs)} CSVs encontrados")

        print(f"✅ Total de clientes com CSVs: {len(clientes_csvs)}")
        return clientes_csvs

    @task
    def processar_cliente(cliente_nome: str, csv_files: list):
        """
        Processa todos os CSVs de um cliente específico
        """
        print(f"\n{'='*60}")
        print(f"🔄 Processando cliente: {cliente_nome}")
        print(f"{'='*60}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # 1. Garante que o cliente existe na tabela
        sql_insert_cliente = """
        INSERT INTO clientes (nome) 
        VALUES (%s) 
        ON CONFLICT (nome) DO NOTHING
        RETURNING id;
        """

        sql_get_cliente = "SELECT id FROM clientes WHERE nome = %s;"

        try:
            # Tenta inserir o cliente
            result = hook.get_first(sql_insert_cliente, parameters=(cliente_nome,))

            if result:
                cliente_id = result[0]
                print(
                    f"✅ Novo cliente '{cliente_nome}' cadastrado com ID: {cliente_id}"
                )
            else:
                # Cliente já existe, busca o ID
                cliente_id = hook.get_first(
                    sql_get_cliente, parameters=(cliente_nome,)
                )[0]
                print(f"ℹ️ Cliente '{cliente_nome}' já cadastrado com ID: {cliente_id}")

        except Exception as e:
            print(f"❌ Erro ao processar cliente: {e}")
            raise

        # 2. Processa cada CSV do cliente
        total_inseridos = 0
        total_atualizados = 0
        total_erros = 0

        for csv_file in csv_files:
            print(f"\n📄 Processando arquivo: {os.path.basename(csv_file)}")

            try:
                # Lê o CSV
                df = pd.read_csv(csv_file, low_memory=False)
                print(f"   📊 Total de registros no CSV: {len(df)}")

                # Processa cada linha
                for index, row in df.iterrows():
                    try:
                        # Prepara os dados para inserção/atualização
                        ttk_id = int(row["ID"]) if pd.notna(row["ID"]) else None

                        if ttk_id is None:
                            print(f"   ⚠️ Registro {index+1}: ID inválido, pulando...")
                            total_erros += 1
                            continue

                        # Converte datas (formato ISO com timezone)
                        def parse_date(date_str):
                            if pd.isna(date_str) or date_str == "":
                                return None
                            try:
                                return pd.to_datetime(date_str).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                            except:
                                return None

                        # SQL para inserir ou atualizar (UPSERT)
                        # IMPORTANTE: São exatamente 66 colunas e 66 valores
                        sql_upsert = """
                        INSERT INTO ttks (
                            id, cliente_id, reference, title, type, state, priority, category,
                            subcategory_id, description, creation_date, resolution_due_date,
                            category_id, symptom_id, previous_state_id, creation_team,
                            change_id_n, public_flag, resolution_sla_minute, creation_team_public_id,
                            urgency_id, urgency, impact_id, origin, cause_type, cause_type_id,
                            subcategory_public_id, resolution_estimated_date, change_user_internal_flag,
                            sla_minute, previous_state, service_id_count_by_client, original_ticket_id,
                            start_date, tag, automatic_flag, resolution_sla_unity, creation_user_name,
                            priority_id, state_id, change_user_id, state_date, change_date,
                            previous_resolution_estimated_date, impact, type_id, symptom_public_id,
                            impact_public_id, creation_user_id, creation_user_internal_flag,
                            automatic_creation_flag, change_team_id, subcategory, priority_public_id,
                            entity, max_id, hierarq_escalation, change_id, origin_id, observations,
                            change_user_name, symptom, previous_service_id_count_by_client,
                            category_public_id, change_team, tim_associated_tickets
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            cliente_id = EXCLUDED.cliente_id,
                            reference = EXCLUDED.reference,
                            title = EXCLUDED.title,
                            type = EXCLUDED.type,
                            state = EXCLUDED.state,
                            priority = EXCLUDED.priority,
                            category = EXCLUDED.category,
                            subcategory_id = EXCLUDED.subcategory_id,
                            description = EXCLUDED.description,
                            resolution_due_date = EXCLUDED.resolution_due_date,
                            category_id = EXCLUDED.category_id,
                            symptom_id = EXCLUDED.symptom_id,
                            previous_state_id = EXCLUDED.previous_state_id,
                            change_id_n = EXCLUDED.change_id_n,
                            public_flag = EXCLUDED.public_flag,
                            resolution_sla_minute = EXCLUDED.resolution_sla_minute,
                            urgency_id = EXCLUDED.urgency_id,
                            urgency = EXCLUDED.urgency,
                            impact_id = EXCLUDED.impact_id,
                            origin = EXCLUDED.origin,
                            cause_type = EXCLUDED.cause_type,
                            cause_type_id = EXCLUDED.cause_type_id,
                            subcategory_public_id = EXCLUDED.subcategory_public_id,
                            resolution_estimated_date = EXCLUDED.resolution_estimated_date,
                            change_user_internal_flag = EXCLUDED.change_user_internal_flag,
                            sla_minute = EXCLUDED.sla_minute,
                            previous_state = EXCLUDED.previous_state,
                            service_id_count_by_client = EXCLUDED.service_id_count_by_client,
                            original_ticket_id = EXCLUDED.original_ticket_id,
                            start_date = EXCLUDED.start_date,
                            tag = EXCLUDED.tag,
                            automatic_flag = EXCLUDED.automatic_flag,
                            resolution_sla_unity = EXCLUDED.resolution_sla_unity,
                            creation_user_name = EXCLUDED.creation_user_name,
                            priority_id = EXCLUDED.priority_id,
                            state_id = EXCLUDED.state_id,
                            change_user_id = EXCLUDED.change_user_id,
                            state_date = EXCLUDED.state_date,
                            change_date = EXCLUDED.change_date,
                            previous_resolution_estimated_date = EXCLUDED.previous_resolution_estimated_date,
                            impact = EXCLUDED.impact,
                            type_id = EXCLUDED.type_id,
                            symptom_public_id = EXCLUDED.symptom_public_id,
                            impact_public_id = EXCLUDED.impact_public_id,
                            creation_user_id = EXCLUDED.creation_user_id,
                            creation_user_internal_flag = EXCLUDED.creation_user_internal_flag,
                            automatic_creation_flag = EXCLUDED.automatic_creation_flag,
                            change_team_id = EXCLUDED.change_team_id,
                            subcategory = EXCLUDED.subcategory,
                            priority_public_id = EXCLUDED.priority_public_id,
                            entity = EXCLUDED.entity,
                            max_id = EXCLUDED.max_id,
                            hierarq_escalation = EXCLUDED.hierarq_escalation,
                            change_id = EXCLUDED.change_id,
                            origin_id = EXCLUDED.origin_id,
                            observations = EXCLUDED.observations,
                            change_user_name = EXCLUDED.change_user_name,
                            symptom = EXCLUDED.symptom,
                            previous_service_id_count_by_client = EXCLUDED.previous_service_id_count_by_client,
                            category_public_id = EXCLUDED.category_public_id,
                            change_team = EXCLUDED.change_team,
                            tim_associated_tickets = EXCLUDED.tim_associated_tickets,
                            data_atualizacao = CURRENT_TIMESTAMP;
                        """

                        # Prepara os valores (converte NaN para None)
                        def safe_value(val):
                            if pd.isna(val):
                                return None
                            return val

                        # Exatamente 64 valores para 64 colunas
                        valores = (
                            ttk_id,  # 1
                            cliente_id,  # 2
                            safe_value(row.get("REFERENCE")),  # 3
                            safe_value(row.get("TITLE")),  # 4
                            safe_value(row.get("TYPE")),  # 5
                            safe_value(row.get("STATE")),  # 6
                            safe_value(row.get("PRIORITY")),  # 7
                            safe_value(row.get("CATEGORY")),  # 8
                            safe_value(row.get("SUBCATEGORY_ID")),  # 9
                            safe_value(row.get("DESCRIPTION")),  # 10
                            parse_date(row.get("CREATION_DATE")),  # 11
                            parse_date(row.get("RESOLUTION_DUE_DATE")),  # 12
                            safe_value(row.get("CATEGORY_ID")),  # 13
                            safe_value(row.get("SYMPTOM_ID")),  # 14
                            safe_value(row.get("PREVIOUS_STATE_ID")),  # 15
                            safe_value(row.get("CREATION_TEAM")),  # 16
                            safe_value(row.get("CHANGE_ID_N")),  # 17
                            safe_value(row.get("PUBLIC_FLAG")),  # 18
                            safe_value(row.get("RESOLUTION_SLA_MINUTE")),  # 19
                            safe_value(row.get("CREATION_TEAM_PUBLIC_ID")),  # 20
                            safe_value(row.get("URGENCY_ID")),  # 21
                            safe_value(row.get("URGENCY")),  # 22
                            safe_value(row.get("IMPACT_ID")),  # 23
                            safe_value(row.get("ORIGIN")),  # 24
                            safe_value(row.get("CAUSE_TYPE")),  # 25
                            safe_value(row.get("CAUSE_TYPE_ID")),  # 26
                            safe_value(row.get("SUBCATEGORY_PUBLIC_ID")),  # 27
                            parse_date(row.get("RESOLUTION_ESTIMATED_DATE")),  # 28
                            safe_value(row.get("CHANGE_USER_INTERNAL_FLAG")),  # 29
                            safe_value(row.get("SLA_MINUTE")),  # 30
                            safe_value(row.get("PREVIOUS_STATE")),  # 31
                            safe_value(row.get("SERVICE_ID_COUNT_BY_CLIENT")),  # 32
                            safe_value(row.get("ORIGINAL_TICKET_ID")),  # 33
                            parse_date(row.get("START_DATE")),  # 34
                            safe_value(row.get("TAG")),  # 35
                            safe_value(row.get("AUTOMATIC_FLAG")),  # 36
                            safe_value(row.get("RESOLUTION_SLA_UNITY")),  # 37
                            safe_value(row.get("CREATION_USER_NAME")),  # 38
                            safe_value(row.get("PRIORITY_ID")),  # 39
                            safe_value(row.get("STATE_ID")),  # 40
                            safe_value(row.get("CHANGE_USER_ID")),  # 41
                            parse_date(row.get("STATE_DATE")),  # 42
                            parse_date(row.get("CHANGE_DATE")),  # 43
                            parse_date(
                                row.get("PREVIOUS_RESOLUTION_ESTIMATED_DATE")
                            ),  # 44
                            safe_value(row.get("IMPACT")),  # 45
                            safe_value(row.get("TYPE_ID")),  # 46
                            safe_value(row.get("SYMPTOM_PUBLIC_ID")),  # 47
                            safe_value(row.get("IMPACT_PUBLIC_ID")),  # 48
                            safe_value(row.get("CREATION_USER_ID")),  # 49
                            safe_value(row.get("CREATION_USER_INTERNAL_FLAG")),  # 50
                            safe_value(row.get("AUTOMATIC_CREATION_FLAG")),  # 51
                            safe_value(row.get("CHANGE_TEAM_ID")),  # 52
                            safe_value(row.get("SUBCATEGORY")),  # 53
                            safe_value(row.get("PRIORITY_PUBLIC_ID")),  # 54
                            safe_value(row.get("ENTITY")),  # 55
                            safe_value(row.get("MAX_ID")),  # 56
                            safe_value(row.get("HIERARQ_ESCALATION")),  # 57
                            safe_value(row.get("CHANGE_ID")),  # 58
                            safe_value(row.get("ORIGIN_ID")),  # 59
                            safe_value(row.get("OBSERVATIONS")),  # 60
                            safe_value(row.get("CHANGE_USER_NAME")),  # 61
                            safe_value(row.get("SYMPTOM")),  # 62
                            safe_value(
                                row.get("PREVIOUS_SERVICE_ID_COUNT_BY_CLIENT")
                            ),  # 63
                            safe_value(row.get("CATEGORY_PUBLIC_ID")),  # 64
                            safe_value(row.get("CHANGE_TEAM")),  # 65
                            safe_value(row.get("TIM_ASSOCIATED_TICKETS")),  # 66
                        )

                        # Debug
                        print(f"   🔍 Número de valores na tupla: {len(valores)}")
                        print(f"   🔍 Número de %s no SQL: {sql_upsert.count('%s')}")

                        # Executa o UPSERT
                        hook.run(sql_upsert, parameters=valores)

                        # Verifica se foi inserção ou atualização
                        check_sql = "SELECT data_importacao, data_atualizacao FROM ttks WHERE id = %s;"
                        result = hook.get_first(check_sql, parameters=(ttk_id,))

                        if result and result[0] == result[1]:
                            total_inseridos += 1
                        else:
                            total_atualizados += 1

                    except Exception as e:
                        print(
                            f"   ❌ Erro ao processar registro {index+1} (ID: {ttk_id}): {e}"
                        )
                        total_erros += 1
                        continue

                print(f"   ✅ Arquivo processado com sucesso!")

            except Exception as e:
                print(f"   ❌ Erro ao ler arquivo CSV: {e}")
                total_erros += 1
                continue

        # Relatório final do cliente
        print(f"\n{'='*60}")
        print(f"📊 RELATÓRIO - Cliente: {cliente_nome}")
        print(f"{'='*60}")
        print(f"✅ Registros inseridos: {total_inseridos}")
        print(f"🔄 Registros atualizados: {total_atualizados}")
        print(f"❌ Erros: {total_erros}")
        print(f"{'='*60}\n")

        return {
            "cliente": cliente_nome,
            "inseridos": total_inseridos,
            "atualizados": total_atualizados,
            "erros": total_erros,
        }

    @task
    def relatorio_geral(resultados: list):
        """
        Gera relatório geral de todos os clientes processados
        """
        print("\n" + "=" * 60)
        print("📊 RELATÓRIO GERAL DE PROCESSAMENTO")
        print("=" * 60)

        total_inseridos = sum(r["inseridos"] for r in resultados)
        total_atualizados = sum(r["atualizados"] for r in resultados)
        total_erros = sum(r["erros"] for r in resultados)

        print(f"🏢 Total de clientes processados: {len(resultados)}")
        print(f"✅ Total de registros inseridos: {total_inseridos}")
        print(f"🔄 Total de registros atualizados: {total_atualizados}")
        print(f"❌ Total de erros: {total_erros}")
        print("=" * 60)

        for resultado in resultados:
            print(f"\n  Cliente: {resultado['cliente']}")
            print(f"    Inseridos: {resultado['inseridos']}")
            print(f"    Atualizados: {resultado['atualizados']}")
            print(f"    Erros: {resultado['erros']}")

        print("\n" + "=" * 60)
        print("✅ PROCESSAMENTO CONCLUÍDO!")
        print("=" * 60 + "\n")

    @task
    def preparar_processamento(clientes_csvs: dict):
        """
        Prepara lista de tasks para processar cada cliente
        """
        # Transforma o dicionário em lista de tuplas para o expand
        return [
            {"cliente_nome": cliente, "csv_files": csvs}
            for cliente, csvs in clientes_csvs.items()
        ]

    # Fluxo da DAG
    criar_tabelas_task = criar_tabelas()
    clientes_csvs = listar_csvs_por_cliente()
    clientes_preparados = preparar_processamento(clientes_csvs)

    # Usa expand para processar cada cliente em paralelo
    resultados = processar_cliente.expand_kwargs(clientes_preparados)

    # Define dependências
    (
        criar_tabelas_task
        >> clientes_csvs
        >> clientes_preparados
        >> resultados
        >> relatorio_geral(resultados)
    )


# Instancia a DAG
processar_ttks_dag()
