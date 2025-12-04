import ballerinax/kafka;
import ballerina/io;
import ballerina/lang.runtime;

// Configuração do Kafka
const string KAFKA_URL = "localhost:9092";
const string TOPIC_NAME = "TTK";

// Cria o producer do Kafka
kafka:Producer kafkaProducer = check new (KAFKA_URL);
public function main() returns error? {
    io:println("🚀 Producer Ballerina iniciado...");
    io:println("📦 Preparando envio de 1000 incidentes mock...\n");
    
    int successCount = 0;
    int errorCount = 0;
    
    // Loop para enviar 1000 incidentes
    foreach int i in 1...1000 {
        // Gera dados únicos para cada incidente
        int incidentId = 3736089 + i;
        string reference = string `TT_${(41484 + i).toString().padZero(8)}/${2025}`;
        
        // JSON do incidente com dados variados
        json incidentData = {
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
        
        // Converte o JSON para string
        string jsonMessage = incidentData.toJsonString();
        
        // Exibe progresso a cada 100 mensagens
        if i % 100 == 0 || i == 1 {
            io:println(string `📤 Enviando incidente ${i}/1000...`);
            io:println(string `   ID: ${incidentId}, Ref: ${reference}`);
        }
        
        // Envia a mensagem para o Kafka
        kafka:Error? result = kafkaProducer->send({
            topic: TOPIC_NAME,
            value: jsonMessage.toBytes()
        });
        
        if result is kafka:Error {
            errorCount += 1;
            if i % 100 == 0 {
                io:println(string `❌ Erro no incidente ${i}: ${result.message()}`);
            }
        } else {
            successCount += 1;
        }
        
        // Pequeno delay a cada 50 mensagens para não sobrecarregar
        if i % 50 == 0 {
            runtime:sleep(0.1);
        }
    }
    
    // Aguarda para garantir que todas as mensagens foram enviadas
    runtime:sleep(2);
    
    // Relatório final
    io:println("\n" + "================");
    io:println("🎯 Processo finalizado!");
    io:println(string `✅ Mensagens enviadas com sucesso: ${successCount}`);
    io:println(string `❌ Mensagens com erro: ${errorCount}`);
    io:println(string `📊 Total processado: ${successCount + errorCount}/1000`);
    io:println("="+ "================");
}