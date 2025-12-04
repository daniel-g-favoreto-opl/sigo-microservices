# Pre-requisites

- You will need to download and install ballerina
- You will need to download and install docker

## This repository has the following folders:

- client-side
- open-side

### 1) client-side folder:
- kafka-docker-simulator : This folder contains a single docker-compose.yml file for launching a kafka simulator within a docker container. Use it to simulate a kafka in Sigo environmnets
- kafka-nossis-producer : This folder contains a ballerina project that will connect to our kafka simulator and emit Sigo Payloads containing TTK's. Here is an example of payload:

```json
{
            "ID": incidentId,
            "TITLE": string `ALARME DE REDE - Incidente ${i}`,
            "TYPE": "Incidente",
            "STATE": i % 3 == 0 ? "Fechado" : (i % 2 == 0 ? "Em Progresso" : "Aberto"),
            "PRIORITY": string `P${(i % 4) + 1}`,
            "CATEGORY": "Avaria Cliente",
            "SUBCATEGORY_ID": 3,
            "DESCRIPTION": string `Teste Open Labs - Mock ${i}`,
            "CREATION_DATE": "2025-10-10T11:05:12.000-03:00",
            "RESOLUTION_DUE_DATE": "2025-10-11T11:05:11.000-03:00",
            "CATEGORY_ID": 4,
            "SYMPTOM_ID": 7,
            "PREVIOUS_STATE_ID": 2,
            "CREATION_TEAM": "TIM",
            "CHANGE_ID_N": string `${i}/1000`,
            "PUBLIC_FLAG": 1,
            "RESOLUTION_SLA_MINUTE": 1440,
            "CREATION_TEAM_PUBLIC_ID": "TIM",
            "URGENCY_ID": (i % 4) + 1,
            "URGENCY": i % 4 == 0 ? "1-Crítico" : (i % 4 == 1 ? "2-Alto" : (i % 4 == 2 ? "3-Médio" : "4-Baixo")),
            "IMPACT_ID": 5,
            "ORIGIN": "API Management",
            "CAUSE_TYPE": "Teste",
            "CAUSE_TYPE_ID": 1,
            "SUBCATEGORY_PUBLIC_ID": "Avaria Cliente - FTTH",
            "RESOLUTION_ESTIMATED_DATE": "2025-10-11T11:05:11.000-03:00",
            "CHANGE_USER_INTERNAL_FLAG": 1,
            "SLA_MINUTE": 1440,
            "PREVIOUS_STATE": "Aberto",
            "SERVICE_ID_COUNT_BY_CLIENT": "{}",
            "ORIGINAL_TICKET_ID": "Cliente",
            "START_DATE": "2025-10-10T11:05:11.000-03:00",
            "TAG": "BLUEPHONE",
            "AUTOMATIC_FLAG": 1,
            "RESOLUTION_SLA_UNITY": "horas lineares",
            "CREATION_USER_NAME": "API Management",
            "PRIORITY_ID": (i % 4) + 1,
            "STATE_ID": 2,
            "CHANGE_USER_ID": 395,
            "STATE_DATE": "2025-10-10T11:05:12.000-03:00",
            "CHANGE_DATE": "2025-10-10T11:05:12.000-03:00",
            "PREVIOUS_RESOLUTION_ESTIMATED_DATE": "2025-10-11T11:05:11.000-03:00",
            "IMPACT": "4-Menor",
            "TYPE_ID": 1,
            "SYMPTOM_PUBLIC_ID": "Outro",
            "IMPACT_PUBLIC_ID": "4-Menor",
            "CREATION_USER_ID": 395,
            "CREATION_USER_INTERNAL_FLAG": 1,
            "AUTOMATIC_CREATION_FLAG": 1,
            "CHANGE_TEAM_ID": 38,
            "CREATION_TEAM_ID": 38,
            "SUBCATEGORY": "Avaria Cliente - FTTH",
            "PRIORITY_PUBLIC_ID": string `P${(i % 4) + 1}`,
            "ENTITY": "TTK",
            "MAX_ID": incidentId + 1,
            "HIERARQ_ESCALATION": 0,
            "CHANGE_ID": 1219529 + i,
            "ORIGIN_ID": 9,
            "OBSERVATIONS": string `Teste Open Labs - Mock ${i}`,
            "CHANGE_USER_NAME": "API Management",
            "SYMPTOM": "Outro",
            "PREVIOUS_SERVICE_ID_COUNT_BY_CLIENT": "{}",
            "CATEGORY_PUBLIC_ID": "ALARME DE REDE",
            "CHANGE_TEAM": "TIM",
            "TIM_ASSOCIATED_TICKETS": [
                {
                    "TIM_ASSOCIATED_TICKET_REFERENCE": string `EVE25100000${i.toString().padZero(4)}`,
                    "TIM_ASSOCIATED_TICKET_STATUS": i % 2 == 0 ? "NOVO" : "EM_ANDAMENTO"
                }
            ]
};
```
- kafka-nossis-consumer: A consumer that will hear all payloads coming from our producer and save those payloads as csv files inside a local folder.
- ttks_ms: An api gateway for listing our csvs and download each one individually. It is protected by oauth, using a client_id and client_secret, that can be defined using system variables. Inside this, you will also find a swagger and a postman-collection for helping with documentation.

For launching ttks_ms and kafka-nossis-consumer simultaneously, there is a single docker-compose.yml file in the root of this repository. You can check which variables can be set before launching those containers.

#### Kafka-nossis-consumer variables:
- kafka_url: this is where you can find the kafka ( in our case it's simulated inside a docker image. I suppose each client will have a different kafka_url )
- topic_name: this is where you will subscribe to our kafka messages. Defaults to TTK.
- group_id: the group for our kafka messages. Defaults to ttk-consumer-group.
- folder_path: Path where the csv's files will be stored.
- days_to_keep: number of days before csv's are deleted from our folder
- check_interval: will check every x and x seconds for old csv's and delete them.
  
#### ttks_ms variables:
- folder_path: path where the csv's files are stored. Should be the same of kafka-nossis-consumer
- client_id: id used for oauth ( you define this for each client )
- client_secret: secret used for oauth ( you define this for each client )
- expires_in: time in seconds before oauth token expiration

### open-side folder:

This contains the apache-airflow project inside csv-downloader folder. You can launch the apache-airflow using a single docker-compose.yml for testing purposes. Inside csv-downloader you will find 3 dag's:

- postgres_test_dag.py: This is used for testing your connection with postgres. This sould pass before attempting to download and insert client csv's in the database. Add a conection inside apache-airflow and then test it.
- 
- download_csvs_dag.py: 
- postgre_insert_csvs_dag.py




