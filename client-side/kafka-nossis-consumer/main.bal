import ballerinax/kafka;
import ballerina/log;
import ballerina/io;
import ballerina/file;
import ballerina/data.jsondata;
import ballerina/time;
import ballerina/task;
import ballerina/os;

// ===============================
// Configuração do Kafka
// ===============================z

// Configurações de ambiente com valores padrão
final string KAFKA_URL = getEnvOrDefault("KAFKA_URL", "localhost:9092");
final string TOPIC_NAME = getEnvOrDefault("TOPIC_NAME", "TTK");
final string GROUP_ID = getEnvOrDefault("GROUP_ID", "ttk-consumer-group");
final string FOLDER_PATH = getEnvOrDefault("FOLDER_PATH", "ttks"); // Caminho da pasta com os arquivos
final decimal DAYS_TO_KEEP = getDecimalEnvOrDefault("DAYS_TO_KEEP", 5.0); // Dias para manter os arquivos
final decimal CHECK_INTERVAL = getDecimalEnvOrDefault("CHECK_INTERVAL", 3600.0);  // Intervalo de verificação em segundos (1 hora

// Função auxiliar para obter variável de ambiente string ou valor padrão
function getEnvOrDefault(string key, string defaultValue) returns string {
    string|error value = trap os:getEnv(key);

    if value is string && value != "" {
        return value;
    }
    return defaultValue;
}

// Função auxiliar para obter variável de ambiente decimal ou valor padrão
function getDecimalEnvOrDefault(string key, decimal defaultValue) returns decimal {
    string|error value = trap os:getEnv(key);
    if value is string && value != "" {
        decimal|error decimalValue = decimal:fromString(value);
        if decimalValue is decimal {
            return decimalValue;
        }
    }
    return defaultValue;
}
// ===============================
// Tipos
// ===============================
type AssociatedTicket record {|
    string TIM_ASSOCIATED_TICKET_REFERENCE;
    string TIM_ASSOCIATED_TICKET_STATUS;
|};

type Incident record {|
    int? CATEGORY_ID?;
    int? SYMPTOM_ID?;
    int? PREVIOUS_STATE_ID?;
    string? CREATION_TEAM?;
    int? ID?;
    string? TYPE?;
    string? CHANGE_ID_N?;
    int? PUBLIC_FLAG?;
    int? RESOLUTION_SLA_MINUTE?;
    string? CREATION_TEAM_PUBLIC_ID?;
    int? URGENCY_ID?;
    string? URGENCY?;
    int? IMPACT_ID?;
    string? ORIGIN?;
    string? CAUSE_TYPE?;
    int? CAUSE_TYPE_ID?;
    string? SUBCATEGORY_PUBLIC_ID?;
    string? DESCRIPTION?;
    string? CATEGORY?;
    string? RESOLUTION_ESTIMATED_DATE?;
    int? CHANGE_USER_INTERNAL_FLAG?;
    int? SLA_MINUTE?;
    string? PREVIOUS_STATE?;
    string? SERVICE_ID_COUNT_BY_CLIENT?;
    string? ORIGINAL_TICKET_ID?;
    string? START_DATE?;
    string? RESOLUTION_DUE_DATE?;
    string? TAG?;
    int? AUTOMATIC_FLAG?;
    string? RESOLUTION_SLA_UNITY?;
    string? CREATION_USER_NAME?;
    string? PRIORITY?;
    int? PRIORITY_ID?;
    int? STATE_ID?;
    int? CHANGE_USER_ID?;
    string? STATE_DATE?;
    string? CREATION_DATE?;
    string? CHANGE_DATE?;
    string? PREVIOUS_RESOLUTION_ESTIMATED_DATE?;
    string? IMPACT?;
    int? TYPE_ID?;
    string? SYMPTOM_PUBLIC_ID?;
    string? IMPACT_PUBLIC_ID?;
    int? CREATION_USER_ID?;
    int? CREATION_USER_INTERNAL_FLAG?;
    int? AUTOMATIC_CREATION_FLAG?;
    int? CHANGE_TEAM_ID?;
    string? STATE?;
    int? CREATION_TEAM_ID?;
    string? SUBCATEGORY?;
    string? PRIORITY_PUBLIC_ID?;
    string? ENTITY?;
    int? MAX_ID?;
    int? HIERARQ_ESCALATION?;
    string? REFERENCE?;
    int? CHANGE_ID?;
    int? ORIGIN_ID?;
    string? OBSERVATIONS?;
    string? CHANGE_USER_NAME?;
    string? SYMPTOM?;
    int? SUBCATEGORY_ID?;
    string? PREVIOUS_SERVICE_ID_COUNT_BY_CLIENT?;
    string? CATEGORY_PUBLIC_ID?;
    string? CHANGE_TEAM?;
    AssociatedTicket[]? TIM_ASSOCIATED_TICKETS?;
    string? TITLE?;
|};

// ===============================
// Listener do Kafka
// ===============================
listener kafka:Listener kafkaListener = new (KAFKA_URL, {
    groupId: GROUP_ID,
    topics: [TOPIC_NAME]
});


// ===============================
// Service que processa mensagens
// ===============================
service on kafkaListener {

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        time:Utc now = time:utcNow();
        int timestampMs = <int>(now[0] * 1000);
        string fileName = string`${timestampMs}.csv`;

        foreach var rec in records {
            string message = check string:fromBytes(rec.value);

            log:printInfo("═══════════════════════════════════════════");
            log:printInfo("✅ Nova mensagem recebida!");
            log:printInfo("───────────────────────────────────────────");

            json|error parsedJson = message.fromJsonString();

            if parsedJson is json {
                Incident|error incident = <@untainted> check parsedJson.cloneWithType(Incident);

                if incident is Incident{

                    if incident?.ID is null {
                        log:printWarn("⚠️ Ignorando incidente sem ID.");
                        continue; // Pula para o próximo registro do foreach
                    }

                    log:printInfo("📄 DADOS DO INCIDENTE:");
                    log:printInfo(string `  • ID: ${<int>incident?.ID}`);
                    log:printInfo(string `  • Referência: ${incident?.REFERENCE ?: ""}`);
                    log:printInfo(string `  • Título: ${incident?.TITLE ?: "" }`);
                    log:printInfo(string `  • Tipo: ${incident?.TYPE?: "" }`);
                    log:printInfo(string `  • Estado: ${incident?.STATE ?: ""}`);
                    log:printInfo(string `  • Prioridade: ${incident?.PRIORITY ?: ""}`);
                    log:printInfo(string `  • Categoria: ${incident?.CATEGORY ?: ""}`);
                    log:printInfo(string `  • Subcategoria: ${incident?.SUBCATEGORY ?: ""}`);
                    log:printInfo(string `  • Descrição: ${incident?.DESCRIPTION ?: ""}`);
                    log:printInfo(string `  • Criado em: ${incident?.CREATION_DATE ?: ""}`);
                    log:printInfo(string `  • Prazo resolução: ${incident?.RESOLUTION_DUE_DATE ?: ""}`);

                    final var tickets = incident?.TIM_ASSOCIATED_TICKETS ?: [];

                  
                    if tickets.length() > 0 {
                        log:printInfo("  • Tickets Associados:");
                        foreach var ticket in tickets {
                            log:printInfo(string `    - ${ticket.TIM_ASSOCIATED_TICKET_REFERENCE} (${ticket.TIM_ASSOCIATED_TICKET_STATUS})`);
                        }
                    }

                    log:printInfo("\n📦 JSON Completo:");
                    log:printInfo(parsedJson.toJsonString());

                    final var result = saveIncidentAsCsv(incident, FOLDER_PATH, fileName);

                    if(result is error) {
                        log:printError("Erro durante a criacao do CSV");
                        continue;
                    }

                } else {
                    log:printWarn("⚠️ Estrutura de incidente inválida.");
                }

            } else {
                log:printWarn("📄 MENSAGEM (texto):");
                log:printInfo(message);
            }

            log:printInfo("═══════════════════════════════════════════\n");

            
          
        }

        // Renomear o arquivo após processar todos os registros
        string oldFilePath = string`${FOLDER_PATH}/${fileName}`;
        string newFileName = string`${timestampMs}_ok.csv`;
        string newFilePath = string`${FOLDER_PATH}/${newFileName}`;

        error? renameResult = file:rename(oldFilePath, newFilePath);
        if renameResult is error {
            log:printError(string`Erro ao renomear arquivo: ${renameResult.message()}`);
            return renameResult;
        }
        log:printInfo(string`✅ Arquivo renomeado para: ${newFileName}`);
    }

    // Log de erros
    remote function onError(kafka:Error err) {
        log:printError("Erro no Kafka Consumer", 'error = err);
    }
}

function saveIncidentAsCsv(Incident incident, string folderPath, string fileName) returns error? {
    int id = incident?.ID ?: 0;

    // Garante que a pasta exista
    if !(check file:test(folderPath, file:EXISTS)) {
        check file:createDir(folderPath);
    }

    // Timestamp no formato YYYYMMDD_HHmmss_SSS


    string filePath = check file:joinPath(folderPath, fileName);

    // Cabeçalho (todos os campos)
    string header = "ID,REFERENCE,TITLE,TYPE,STATE,PRIORITY,CATEGORY,SUBCATEGORY_ID,DESCRIPTION,CREATION_DATE," +
        "RESOLUTION_DUE_DATE,CATEGORY_ID,SYMPTOM_ID,PREVIOUS_STATE_ID,CREATION_TEAM,CHANGE_ID_N," +
        "PUBLIC_FLAG,RESOLUTION_SLA_MINUTE,CREATION_TEAM_PUBLIC_ID,URGENCY_ID,URGENCY,IMPACT_ID," +
        "ORIGIN,CAUSE_TYPE,CAUSE_TYPE_ID,SUBCATEGORY_PUBLIC_ID,RESOLUTION_ESTIMATED_DATE," +
        "CHANGE_USER_INTERNAL_FLAG,SLA_MINUTE,PREVIOUS_STATE,SERVICE_ID_COUNT_BY_CLIENT," +
        "ORIGINAL_TICKET_ID,START_DATE,TAG,AUTOMATIC_FLAG,RESOLUTION_SLA_UNITY,CREATION_USER_NAME," +
        "PRIORITY_ID,STATE_ID,CHANGE_USER_ID,STATE_DATE,CHANGE_DATE,PREVIOUS_RESOLUTION_ESTIMATED_DATE," +
        "IMPACT,TYPE_ID,SYMPTOM_PUBLIC_ID,IMPACT_PUBLIC_ID,CREATION_USER_ID,CREATION_USER_INTERNAL_FLAG," +
        "AUTOMATIC_CREATION_FLAG,CHANGE_TEAM_ID,SUBCATEGORY,PRIORITY_PUBLIC_ID,ENTITY,MAX_ID," +
        "HIERARQ_ESCALATION,CHANGE_ID,ORIGIN_ID,OBSERVATIONS,CHANGE_USER_NAME,SYMPTOM," +
        "PREVIOUS_SERVICE_ID_COUNT_BY_CLIENT,CATEGORY_PUBLIC_ID,CHANGE_TEAM," +
        "TIM_ASSOCIATED_TICKETS\n";

    // Converte lista de tickets para JSON
    string ticketsJson = "";
    if incident?.TIM_ASSOCIATED_TICKETS is AssociatedTicket[] {
        AssociatedTicket[] tickets = <AssociatedTicket[]>incident?.TIM_ASSOCIATED_TICKETS;
        ticketsJson = jsondata:toJson(tickets).toString();
    }

    string[] fields = [
        toEscapedString(id),
        toEscapedString(incident?.REFERENCE),
        toEscapedString(incident?.TITLE),
        toEscapedString(incident?.TYPE),
        toEscapedString(incident?.STATE),
        toEscapedString(incident?.PRIORITY),
        toEscapedString(incident?.CATEGORY),
        toEscapedString(incident?.SUBCATEGORY_ID),
        toEscapedString(incident?.DESCRIPTION),
        toEscapedString(incident?.CREATION_DATE),
        toEscapedString(incident?.RESOLUTION_DUE_DATE),
        toEscapedString(incident?.CATEGORY_ID),
        toEscapedString(incident?.SYMPTOM_ID),
        toEscapedString(incident?.PREVIOUS_STATE_ID),
        toEscapedString(incident?.CREATION_TEAM),
        toEscapedString(incident?.CHANGE_ID_N),
        toEscapedString(incident?.PUBLIC_FLAG),
        toEscapedString(incident?.RESOLUTION_SLA_MINUTE),
        toEscapedString(incident?.CREATION_TEAM_PUBLIC_ID),
        toEscapedString(incident?.URGENCY_ID),
        toEscapedString(incident?.URGENCY),
        toEscapedString(incident?.IMPACT_ID),
        toEscapedString(incident?.ORIGIN),
        toEscapedString(incident?.CAUSE_TYPE),
        toEscapedString(incident?.CAUSE_TYPE_ID),
        toEscapedString(incident?.SUBCATEGORY_PUBLIC_ID),
        toEscapedString(incident?.RESOLUTION_ESTIMATED_DATE),
        toEscapedString(incident?.CHANGE_USER_INTERNAL_FLAG),
        toEscapedString(incident?.SLA_MINUTE),
        toEscapedString(incident?.PREVIOUS_STATE),
        toEscapedString(incident?.SERVICE_ID_COUNT_BY_CLIENT),
        toEscapedString(incident?.ORIGINAL_TICKET_ID),
        toEscapedString(incident?.START_DATE),
        toEscapedString(incident?.TAG),
        toEscapedString(incident?.AUTOMATIC_FLAG),
        toEscapedString(incident?.RESOLUTION_SLA_UNITY),
        toEscapedString(incident?.CREATION_USER_NAME),
        toEscapedString(incident?.PRIORITY_ID),
        toEscapedString(incident?.STATE_ID),
        toEscapedString(incident?.CHANGE_USER_ID),
        toEscapedString(incident?.STATE_DATE),
        toEscapedString(incident?.CHANGE_DATE),
        toEscapedString(incident?.PREVIOUS_RESOLUTION_ESTIMATED_DATE),
        toEscapedString(incident?.IMPACT),
        toEscapedString(incident?.TYPE_ID),
        toEscapedString(incident?.SYMPTOM_PUBLIC_ID),
        toEscapedString(incident?.IMPACT_PUBLIC_ID),
        toEscapedString(incident?.CREATION_USER_ID),
        toEscapedString(incident?.CREATION_USER_INTERNAL_FLAG),
        toEscapedString(incident?.AUTOMATIC_CREATION_FLAG),
        toEscapedString(incident?.CHANGE_TEAM_ID),
        toEscapedString(incident?.SUBCATEGORY),
        toEscapedString(incident?.PRIORITY_PUBLIC_ID),
        toEscapedString(incident?.ENTITY),
        toEscapedString(incident?.MAX_ID),
        toEscapedString(incident?.HIERARQ_ESCALATION),
        toEscapedString(incident?.CHANGE_ID),
        toEscapedString(incident?.ORIGIN_ID),
        toEscapedString(incident?.OBSERVATIONS),
        toEscapedString(incident?.CHANGE_USER_NAME),
        toEscapedString(incident?.SYMPTOM),
        toEscapedString(incident?.PREVIOUS_SERVICE_ID_COUNT_BY_CLIENT),
        toEscapedString(incident?.CATEGORY_PUBLIC_ID),
        toEscapedString(incident?.CHANGE_TEAM),
        toEscapedString(ticketsJson)
    ];

    string newRow = string:'join(",", ...fields) + "\n";

    // Se o arquivo não existe, cria com cabeçalho
    if !(check file:test(filePath, file:EXISTS)) {
        check io:fileWriteString(filePath, header + newRow);
        log:printInfo(string `✅ CSV criado e salvo: ${filePath}`);
        return;
    }

    // Lê conteúdo existente
    string currentContent = check io:fileReadString(filePath);
    string[] lines = re `\n`.split(currentContent);

    // Atualiza ou adiciona
    boolean updated = false;
    string[] updatedLines = [];

    // Preserva o cabeçalho
    updatedLines.push(lines[0]);

    foreach string line in lines.slice(1) {
        if line.trim().length() == 0 {
            continue;
        }
        string[] cols = re `,`.split(line);
        if cols.length() > 0 && cols[0] == id.toString() {
            updatedLines.push(trimEnd(newRow, "\n"));
            updated = true;
        } else {
            updatedLines.push(line);
        }
    }

    if !updated {
        updatedLines.push(trimEnd(newRow, "\n"));
    }

    // Reescreve o arquivo completo
    string finalContent = string:'join("\n", ...updatedLines) + "\n";
    check io:fileWriteString(filePath, finalContent);

    log:printInfo(updated ? 
        string `✏️  Incidente ${id} atualizado em ${filePath}` :
        string `🆕 Incidente ${id} adicionado em ${filePath}`);
}

function toEscapedString(any value) returns string {
    if value is () {
        return escapeCsv(());
    } else if value is int {
        return escapeCsv(value.toString());
    } else if value is string {
        return escapeCsv(value);
    }
    return "";
}

function trimEnd(string str, string suffix) returns string {
    if str.endsWith(suffix) {
        return str.substring(0, str.length() - suffix.length());
    }
    return str;
}


// ==================================================
// Função utilitária para escapar strings CSV
// ==================================================
function escapeCsv(string? value) returns string {
    // Verifica se é nulo
    if value is () {
        return "";
    }
    
    string val = value;
    
    // Verifica se precisa de aspas (contém vírgula, aspas ou quebra de linha)
    boolean needsQuotes = val.includes(",") || val.includes("\"") || val.includes("\n");
    
    if needsQuotes {
        // Duplica aspas duplas internas
        string escaped = re `"`.replaceAll(val, "\"\"");
        return string `"${escaped}"`;
    }
    
    return val;
}


// Classe do Job para o scheduler
class Job {
    *task:Job;
    
    public function execute() {
        error? result = cleanupOldFiles();
        if result is error {
            log:printError("Erro ao executar limpeza", 'error = result);
        }
    }
}

// Função que faz a limpeza dos arquivos antigos
function cleanupOldFiles() returns error? {
    log:printInfo("Iniciando verificação de arquivos antigos...");
    
    // Obter o timestamp atual em milliseconds
    time:Utc now = time:utcNow();
    int currentTimestampMs = <int>(now[0] * 1000);
    
    // Calcular o limite de tempo (5 dias em milliseconds)
    int fiveDaysInMs = <int>(DAYS_TO_KEEP * 24 * 60 * 60 * 1000);
    int cutoffTimestamp = currentTimestampMs - fiveDaysInMs;
    
    log:printInfo(string `Timestamp atual: ${currentTimestampMs}`);
    log:printInfo(string `Limite (${DAYS_TO_KEEP} dias atrás): ${cutoffTimestamp}`);
    
    // Verificar se a pasta existe
    boolean folderExists = check file:test(FOLDER_PATH, file:EXISTS);
    if !folderExists {
        log:printWarn(string `Pasta não encontrada: ${FOLDER_PATH}`);
        return;
    }
    
    // Listar todos os arquivos na pasta
    file:MetaData[] files = check file:readDir(FOLDER_PATH);
    
    int filesDeleted = 0;
    int filesChecked = 0;
    
    foreach file:MetaData fileInfo in files {
        // Ignorar diretórios
        if fileInfo.dir {
            continue;
        }
        
        filesChecked += 1;
        string filePath = fileInfo.absPath;
        string fileName = getFileNameFromPath(filePath);

        time:Utc lastModified = fileInfo.modifiedTime;
        int lastModifiedMs = <int>(lastModified[0] * 1000);

        // Só deletar se não foi modificado recentemente (ex: últimos 10 minutos)
        int tenMinutesMs = 10 * 60 * 1000;
        if !((currentTimestampMs - lastModifiedMs) > tenMinutesMs) {
            continue;
        }
                
        // Extrair o timestamp do nome do arquivo (assumindo formato: timestamp.csv)
        string[] parts = re `.csv`.split(fileName);
        if parts.length() < 1 {
            log:printWarn(string `Arquivo com formato inválido: ${fileName}`);
            continue;
        }
        
        // Tentar converter o nome do arquivo para timestamp
        int|error fileTimestamp = int:fromString(parts[0]);
        
        if fileTimestamp is error {
            log:printWarn(string `Não foi possível extrair timestamp de: ${fileName}`);
            continue;
        }
        
        // Verificar se o arquivo é mais antigo que 5 dias
        if fileTimestamp < cutoffTimestamp {
            int ageInDays = (currentTimestampMs - fileTimestamp) / (24 * 60 * 60 * 1000);
            log:printInfo(string `Deletando arquivo antigo: ${fileName} (${ageInDays} dias)`);
            
            error? deleteResult = file:remove(filePath);
            if deleteResult is error {
                log:printError(string `Erro ao deletar ${fileName}`, 'error = deleteResult);
            } else {
                filesDeleted += 1;
            }
        }
    }
    
    log:printInfo(string `Limpeza concluída. Arquivos verificados: ${filesChecked}, Arquivos deletados: ${filesDeleted}`);
}

// Função auxiliar para extrair o nome do arquivo do path completo
function getFileNameFromPath(string path) returns string {
    string[] parts = re `/`.split(path);
    if parts.length() > 0 {
        return parts[parts.length() - 1];
    }
    return path;
}

// ===============================
// Função principal
// ===============================
public function main() {
    log:printInfo("🎧 Consumer Ballerina iniciado...");
    log:printInfo(string `📡 Conectando ao Kafka: ${KAFKA_URL}`);
    log:printInfo(string `📥 Consumindo do tópico: ${TOPIC_NAME}`);
    log:printInfo(string `👥 Group ID: ${GROUP_ID}`);
    log:printInfo("⏳ Aguardando mensagens...\n");

    // Executa a limpeza imediatamente ao iniciar
    error? cleanupResult = cleanupOldFiles();
    if cleanupResult is error {
        log:printError("Erro na limpeza inicial", 'error = cleanupResult);
    }
    
    // Configura o timer para executar periodicamente
    task:JobId|error jobId = task:scheduleJobRecurByFrequency(
        new Job(),
        CHECK_INTERVAL
    );
    
    if jobId is error {
        log:printError("Erro ao criar timer", 'error = jobId);
        return;
    }
    
    log:printInfo(string `Timer iniciado com ID: ${jobId.id}`);
    
}
