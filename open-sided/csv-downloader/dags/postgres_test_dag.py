from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    dag_id="teste_conexao_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Execução manual
    catchup=False,
    tags=["teste", "postgres"],
)
def teste_conexao():

    @task
    def testar_conexao():
        """
        Testa a conexão com PostgreSQL
        """
        print("🔌 Testando conexão com PostgreSQL...")

        try:
            # Cria o hook
            hook = PostgresHook(postgres_conn_id="postgres_default")

            # Teste 1: Verificar versão
            print("\n📊 Teste 1: Verificar versão do PostgreSQL")
            version = hook.get_first("SELECT version();")
            print(f"✅ Versão: {version[0]}")

            # Teste 2: Listar databases
            print("\n📊 Teste 2: Listar databases")
            databases = hook.get_records(
                "SELECT datname FROM pg_database WHERE datistemplate = false;"
            )
            print(f"✅ Databases disponíveis:")
            for db in databases:
                print(f"   - {db[0]}")

            # Teste 3: Listar tabelas do schema public
            print("\n📊 Teste 3: Listar tabelas")
            tables = hook.get_records(
                "SELECT tablename FROM pg_tables WHERE schemaname='public';"
            )
            if tables:
                print(f"✅ Tabelas encontradas:")
                for table in tables:
                    print(f"   - {table[0]}")
            else:
                print("ℹ️ Nenhuma tabela encontrada no schema 'public'")

            # Teste 4: Data e hora do servidor
            print("\n📊 Teste 4: Data e hora do servidor")
            now = hook.get_first("SELECT NOW();")
            print(f"✅ Data/Hora do servidor: {now[0]}")

            # Teste 5: Verificar se consegue criar uma tabela de teste
            print("\n📊 Teste 5: Criar tabela de teste")
            hook.run(
                """
                CREATE TABLE IF NOT EXISTS teste_airflow (
                    id SERIAL PRIMARY KEY,
                    mensagem TEXT,
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )
            print("✅ Tabela 'teste_airflow' criada com sucesso!")

            # Teste 6: Inserir dados de teste
            print("\n📊 Teste 6: Inserir dados")
            hook.run(
                """
                INSERT INTO teste_airflow (mensagem) 
                VALUES ('Conexão funcionando perfeitamente!');
            """
            )
            print("✅ Dados inseridos com sucesso!")

            # Teste 7: Ler dados
            print("\n📊 Teste 7: Ler dados inseridos")
            dados = hook.get_records(
                "SELECT * FROM teste_airflow ORDER BY id DESC LIMIT 5;"
            )
            print(f"✅ Últimos registros:")
            for registro in dados:
                print(
                    f"   ID: {registro[0]}, Mensagem: {registro[1]}, Data: {registro[2]}"
                )

            print("\n" + "=" * 60)
            print("🎉 TODOS OS TESTES PASSARAM COM SUCESSO!")
            print("=" * 60)

            return "✅ Conexão OK!"

        except Exception as e:
            print(f"\n❌ ERRO ao conectar: {e}")
            print("\n💡 Verifique:")
            print("   1. A conexão 'postgres_default' existe no Airflow")
            print("   2. Host, porta, usuário e senha estão corretos")
            print("   3. PostgreSQL está rodando")
            print("   4. Firewall permite conexão na porta 5432")
            raise

    testar_conexao()


# Instancia a DAG
teste_conexao()
