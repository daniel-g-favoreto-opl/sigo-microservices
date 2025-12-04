import ballerina/http;
import ballerina/file;
import ballerina/io;
import ballerina/time;
import ballerina/crypto;
import ballerina/uuid;
import ballerina/url;
import ballerina/os;

// Configurações com variáveis de ambiente
final string FOLDER_PATH = getEnvOrDefault("FOLDER_PATH", "/Users/DanielFavoreto/projects/kafka-nossis-consumer/ttks");
final string CLIENT_ID = getEnvOrDefault("CLIENT_ID", "sigo-admin");
final string CLIENT_SECRET = getEnvOrDefault("CLIENT_SECRET", "j{2P|j<Mzl0qoE==>Z40");
final int EXPIRES_IN = getIntEnvOrDefault("EXPIRES_IN", 3600 * 24); // Tempo de expiração do token em segundos

// Função auxiliar para obter variável de ambiente string ou valor padrão
function getEnvOrDefault(string key, string defaultValue) returns string {
    string|error value = trap os:getEnv(key);

    if value is string && value != "" {
        return value;
    }
    return defaultValue;
}

// Função auxiliar para obter variável de ambiente int ou valor padrão
function getIntEnvOrDefault(string key, int defaultValue) returns int {
    string|error value = trap os:getEnv(key);
    if value is string && value != "" {
        int|error intValue = int:fromString(value);
        if intValue is int {
            return intValue;
        }
    }
    return defaultValue;
}

// Armazenamento de tokens em memória (em produção, use Redis ou BD)
map<TokenInfo> tokenStore = {};

// Estrutura do Token
type TokenInfo record {
    string accessToken;
    string clientId;
    int expiresAt;
    string[] scopes;
};

// Gerar token JWT simples
function generateAccessToken(string clientId, string[] scopes) returns string {
    string tokenData = string `${clientId}:${time:utcNow()[0]}:${uuid:createType1AsString()}`;
    byte[] hash = crypto:hashSha256(tokenData.toBytes());
    return hash.toBase16();
}

// Validar token
function validateToken(string token) returns TokenInfo|error {
    if !tokenStore.hasKey(token) {
        return error("Token inválido");
    }
    
    TokenInfo tokenInfo = tokenStore.get(token);
    int currentTime = time:utcNow()[0];
    
    if currentTime > tokenInfo.expiresAt {
        _ = tokenStore.remove(token);
        return error("Token expirado");
    }
    
    return tokenInfo;
}

// Interceptor customizado para validação de token
service class TokenAuthInterceptor {
    *http:RequestInterceptor;

    resource function 'default [string... path](http:RequestContext ctx, http:Request req) 
            returns http:NextService|http:Unauthorized|error? {
        
        string|http:HeaderNotFoundError authHeader = req.getHeader("Authorization");
        
        if authHeader is http:HeaderNotFoundError {
            return <http:Unauthorized>{
                body: {
                    "error": "unauthorized",
                    "error_description": "Token de autenticação não fornecido"
                }
            };
        }
        
        if !authHeader.startsWith("Bearer ") {
            return <http:Unauthorized>{
                body: {
                    "error": "invalid_token",
                    "error_description": "Formato de token inválido. Use: Bearer <token>"
                }
            };
        }
        
        string token = authHeader.substring(7);
        TokenInfo|error tokenInfo = validateToken(token);
        
        if tokenInfo is error {
            return <http:Unauthorized>{
                body: {
                    "error": "invalid_token",
                    "error_description": tokenInfo.message()
                }
            };
        }
        
        return ctx.next();
    }
}

// Serviço OAuth2 - Geração de Tokens
service /oauth2 on new http:Listener(9091) {
    
    // Endpoint para obter token (Client Credentials Flow)
    resource function post token(http:Request req) returns json|http:Unauthorized|http:BadRequest|error {
        
        // Ler form data
        string|error payload = req.getTextPayload();
        
        if payload is error {
            return <http:BadRequest>{
                body: {
                    "error": "invalid_request",
                    "error_description": "Corpo da requisição inválido"
                }
            };
        }
        
        // Parse dos parâmetros
        map<string> params = {};
        string[] pairs = re `&`.split(payload);
        foreach string pair in pairs {
            string[] keyValue = re `=`.split(pair);
            if keyValue.length() == 2 {
                params[keyValue[0]] = keyValue[1];
            }
        }
        
        // Validar grant_type
        string? grantType = params["grant_type"];
        if grantType != "client_credentials" {
            return <http:BadRequest>{
                body: {
                    "error": "unsupported_grant_type",
                    "error_description": "Apenas 'client_credentials' é suportado"
                }
            };
        }
        
        // Validar credenciais
        string clientId = params["client_id"] ?: "";
        string clientSecret = params["client_secret"] ?: "";

        string decodedClientId = check url:decode(clientId, "UTF-8");
        string decodedClientSecret = check url:decode(clientSecret, "UTF-8");
        
        if decodedClientId != CLIENT_ID || decodedClientSecret != CLIENT_SECRET {
            return <http:Unauthorized>{
                body: {
                    "error": "invalid_client",
                    "error_description": "Credenciais inválidas"
                }
            };
        }
        
        // Gerar token
        string accessToken = generateAccessToken(CLIENT_ID, ["read"]);
        int expiresIn = EXPIRES_IN; // 1 hora
        int expiresAt = time:utcNow()[0] + expiresIn;
        
        // Armazenar token
        tokenStore[accessToken] = {
            accessToken: accessToken,
            clientId: CLIENT_ID,
            expiresAt: expiresAt,
            scopes: ["read"]
        };
        
        return {
            "access_token": accessToken,
            "token_type": "Bearer",
            "expires_in": expiresIn,
            "scope": "read"
        };
    }
    
    // Endpoint para validar token (Token Introspection)
    resource function post introspect(http:Request req) returns json {
        
        string|error payload = req.getTextPayload();
        
        if payload is error {
            return {
                "active": false
            };
        }
        
        // Parse token
        map<string> params = {};
        string[] pairs = re `&`.split(payload);
        foreach string pair in pairs {
            string[] keyValue = re `=`.split(pair);
            if keyValue.length() == 2 {
                params[keyValue[0]] = keyValue[1];
            }
        }
        
        string? token = params["token"];
        
        if token is () {
            return {
                "active": false
            };
        }
        
        TokenInfo|error tokenInfo = validateToken(token);
        
        if tokenInfo is error {
            return {
                "active": false
            };
        }
        
        return {
            "active": true,
            "client_id": tokenInfo.clientId,
            "scope": string:'join(" ", ...tokenInfo.scopes),
            "exp": tokenInfo.expiresAt
        };
    }
}

// Serviço de API protegido com OAuth2
service http:InterceptableService /api on new http:Listener(9090) {
    
    public function createInterceptors() returns TokenAuthInterceptor {
        return new TokenAuthInterceptor();
    }

    // API 1: Listar todos os arquivos CSV
    resource function get csvs() returns json|http:InternalServerError {
        file:MetaData[]|error files = file:readDir(FOLDER_PATH);
        if files is error {
            return <http:InternalServerError>{
                body: {
                    "error": "Erro ao ler pasta",
                    "message": files.message()
                }
            };
        }
        
        string[] csvFiles = [];
        foreach var fileInfo in files {
            string fullPath = fileInfo.absPath;
            int lastSlashIndex = fullPath.lastIndexOf("/") ?: -1;
            string fileName = fullPath.substring(lastSlashIndex + 1);
            
            // Filtrar apenas arquivos que contêm "_ok" e terminam com ".csv"
            if fileName.endsWith("_ok.csv") {
                // Remover "_ok.csv" (7 caracteres)
                string fileNameWithoutExtension = fileName.substring(0, fileName.length() - 7);
                csvFiles.push(fileNameWithoutExtension);
            }
        }
        
        return {
            "total": csvFiles.length(),
            "arquivos": csvFiles
        };
    }

    // API 2: Buscar conteúdo de um CSV específico
  resource function get csv/[string fileName]() returns http:Response|http:NotFound|http:InternalServerError|error {
        // Adicionar extensão _ok.csv ao fileName
        string fullFileName = string`${fileName}_ok.csv`;
        string filePath = FOLDER_PATH + "/" + fullFileName;
        
        // Verificar se o arquivo existe
        boolean|error fileExists = file:test(filePath, file:EXISTS);
        if fileExists is error || !fileExists {
            return <http:NotFound>{
                body: {
                    "error": "Arquivo não encontrado",
                    "message": string`O arquivo ${fullFileName} não existe`
                }
            };
        }
        
        // Ler o conteúdo do arquivo como bytes
        byte[]|io:Error fileContent = io:fileReadBytes(filePath);
        if fileContent is io:Error {
            return <http:InternalServerError>{
                body: {
                    "error": "Erro ao ler arquivo",
                    "message": fileContent.message()
                }
            };
        }
        
        // Criar resposta HTTP com o arquivo para download
        http:Response response = new;
        response.setBinaryPayload(fileContent);
        _ = check response.setContentType("text/csv");
        response.setHeader("Content-Disposition", string`attachment; filename="${fullFileName}"`);
        
        return response;
    }
}

// Função principal
public function main() returns error? {
    // Criar pasta de CSVs se não existir
    boolean|error folderExists = file:test(FOLDER_PATH, file:EXISTS);
    if folderExists is error || !folderExists {
        check file:createDir(FOLDER_PATH);
        io:println("✅ Pasta 'csv_files' criada com sucesso!");
    }
    
    io:println("🚀 Servidor OAuth2 iniciado na porta 9091");
    io:println("🚀 Servidor API iniciado na porta 9090 (protegido)");
    io:println("\n📋 Credenciais OAuth2:");
    io:println("   Client ID: " + CLIENT_ID);
    io:println("   Client Secret: " + CLIENT_SECRET);
    
    io:println("\n💡 Fluxo completo de uso:");
    
    io:println("\n1️⃣  Obter token de acesso:");
    io:println("   curl -X POST http://localhost:9091/oauth2/token \\");
    io:println("        -H \"Content-Type: application/x-www-form-urlencoded\" \\");
    io:println("        -d \"grant_type=client_credentials&client_id=" + CLIENT_ID + "&client_secret=" + CLIENT_SECRET + "\"");
    
    io:println("\n2️⃣  Listar CSVs (substituir <TOKEN> pelo access_token recebido):");
    io:println("   curl -H \"Authorization: Bearer <TOKEN>\" \\");
    io:println("        http://localhost:9090/api/csvs");
    
    io:println("\n3️⃣  Buscar CSV específico:");
    io:println("   curl -H \"Authorization: Bearer <TOKEN>\" \\");
    io:println("        http://localhost:9090/api/csv/exemplo.csv");
    
    io:println("\n4️⃣  Validar token (introspection):");
    io:println("   curl -X POST http://localhost:9091/oauth2/introspect \\");
    io:println("        -H \"Content-Type: application/x-www-form-urlencoded\" \\");
    io:println("        -d \"token=<TOKEN>\"");
}