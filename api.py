from flask import Flask, jsonify, request, abort
import json
import os
from flask_cors import CORS
import sqlite3
from datetime import datetime, timedelta
import pytz
import re
import requests
from hashlib import md5

# Configuração da API
API_KEY = "123456789"  # Substitua por uma chave segura
JSON_PATH = "https://headdira.github.io/api-rastreamento/enriched_devices_data.json"  # sua URL real aqui  # Atualizado para o novo formato
SIMCARDS_PATH = "data/endpoints_filtrados.json"
DB_PATH = "data/devices_data.sqlite"
TIMEZONE = pytz.timezone('America/Sao_Paulo')

# Verifica se os arquivos JSON existem
if not JSON_PATH.startswith("http") and not os.path.exists(JSON_PATH):
    raise FileNotFoundError(f"O arquivo JSON '{JSON_PATH}' não foi encontrado. Execute o script de logística para gerá-lo.")

if not os.path.exists(SIMCARDS_PATH):
    raise FileNotFoundError(f"O arquivo JSON '{SIMCARDS_PATH}' não foi encontrado. Verifique a pasta de dados.")

# Função para carregar dados do JSON
def carregar_dados(caminho_ou_url):
    if caminho_ou_url.startswith("http"):
        response = requests.get(caminho_ou_url)
        response.raise_for_status()
        return response.json()
    else:
        with open(caminho_ou_url, "r", encoding='utf-8') as json_file:
            return json.load(json_file)


def format_timestamp_to_sp(timestamp_ms):
    if timestamp_ms is None or timestamp_ms <= 0:
        return None
    try:
        timestamp_sec = timestamp_ms / 1000.0
        utc_dt = datetime.utcfromtimestamp(timestamp_sec).replace(tzinfo=pytz.utc)
        sp_dt = utc_dt.astimezone(TIMEZONE)
        return sp_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        app.logger.error(f"Erro ao converter timestamp: {e}")
        return None

# Iniciar o Flask
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

# Middleware para verificar a API Key
@app.before_request
def verificar_api_key():
    if request.method == "OPTIONS":
        return "", 200
    if request.headers.get("x-api-key") != API_KEY:
        abort(401, description="Unauthorized: Missing or invalid x-api-key.")

# Rota para obter todos os dados do arquivo principal
@app.route("/api/devices", methods=["GET"])
def obter_dados():
    dados = carregar_dados(JSON_PATH)
    return jsonify(dados)

# Rota para obter dados de um email específico
@app.route("/api/devices/<email>", methods=["GET"])
def obter_dados_por_email(email):
    dados = carregar_dados(JSON_PATH)
    if email not in dados:
        abort(404, description="Email não encontrado.")
    return jsonify(dados[email])

# Rota para obter todos os dados do novo arquivo
@app.route("/api/simcards", methods=["GET"])
def obter_simcards():
    dados = carregar_dados(SIMCARDS_PATH)
    return jsonify(dados)

# Rota para buscar por imei_with_luhn ou sim_iccid_with_luhn
@app.route("/api/simcards/<identifier>", methods=["GET"])
def buscar_simcard(identifier):
    dados = carregar_dados(SIMCARDS_PATH)
    resultados = [
        item for item in dados 
        if item.get("imei_with_luhn") == identifier or item.get("sim_iccid_with_luhn") == identifier
    ]

    if not resultados:
        abort(404, description="Nenhum resultado encontrado para o identificador fornecido.")
    
    return jsonify(resultados)

# Buscar dispositivo por config.name ou device_key
@app.route("/api/devices/search/<identifier>", methods=["GET"])
def buscar_device(identifier):
    dados = carregar_dados(JSON_PATH)
    resultados = []

    for email, info in dados.items():
        devices = info.get("devices", [])
        for device in devices:
            # Acesso seguro aos campos
            config_name = device.get("config", {}).get("name", "")
            device_key = device.get("device_key", "")
            
            if config_name == identifier or device_key == identifier:
                resultados.append(device)

    if not resultados:
        abort(404, description="Nenhum dispositivo encontrado para o identificador fornecido.")

    return jsonify(resultados)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_today_range():
    now = datetime.now(TIMEZONE)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_ms = int(start_of_day.timestamp() * 1000)
    end_ms = int(end_of_day.timestamp() * 1000)
    return start_ms, end_ms

# Rota para verificar o status da API
@app.route("/api/status", methods=["GET"])
def status():
    conn = get_db_connection()
    start_of_day, end_of_day = get_today_range()
    
    try:
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='devices_data'"
        ).fetchone()
        
        if not table_check:
            return jsonify({"error": "Tabela 'devices_data' não encontrada"}), 404
        
        total_devices = conn.execute(
            """SELECT COUNT(*) FROM devices_data 
            WHERE location_date BETWEEN ? AND ?""",
            (start_of_day, end_of_day)
        ).fetchone()[0]
        
        if total_devices == 0:
            return jsonify({
                "status": "API está funcionando",
                "aviso": "Nenhum dado encontrado para o dia atual",
                "data_referencia": datetime.now(TIMEZONE).strftime('%Y-%m-%d'),
                "fuso_horario": "America/Sao_Paulo"
            })
        
        gps_devices = conn.execute(
            """SELECT COUNT(*) FROM devices_data 
            WHERE lat != 0 AND lng != 0 AND latlng_valid = 1
            AND location_date BETWEEN ? AND ?""",
            (start_of_day, end_of_day)
        ).fetchone()[0]
        
        lbs_devices = conn.execute(
            """SELECT COUNT(*) FROM devices_data 
            WHERE lbs_lat IS NOT NULL AND lbs_lng IS NOT NULL
            AND location_date BETWEEN ? AND ?""",
            (start_of_day, end_of_day)
        ).fetchone()[0]
        
        no_location_devices = conn.execute(
            """SELECT COUNT(*) FROM devices_data 
            WHERE (lat = 0 OR lng = 0 OR latlng_valid = 0) 
            AND (lbs_lat IS NULL OR lbs_lng IS NULL)
            AND location_date BETWEEN ? AND ?""",
            (start_of_day, end_of_day)
        ).fetchone()[0]
        
        two_am_start = datetime.now(TIMEZONE).replace(hour=2, minute=0, second=0, microsecond=0)
        two_am_end = two_am_start + timedelta(hours=1)
        two_am_start_ms = int(two_am_start.timestamp() * 1000)
        two_am_end_ms = int(two_am_end.timestamp() * 1000)
        
        two_am_devices = conn.execute(
            """SELECT COUNT(*) FROM devices_data 
            WHERE location_date BETWEEN ? AND ?""",
            (two_am_start_ms, two_am_end_ms)
        ).fetchone()[0]
        
        return jsonify({
            "status": "API está funcionando",
            "tabela": "devices_data",
            "data_referencia": datetime.now(TIMEZONE).strftime('%Y-%m-%d'),
            "fuso_horario": "America/Sao_Paulo",
            "total_dispositivos_hoje": total_devices,
            "dispositivos_com_gps_valido": gps_devices,
            "dispositivos_com_lbs": lbs_devices,
            "dispositivos_sem_localizacao": no_location_devices,
            "dispositivos_localizacao_2am": two_am_devices,
            "percentuais": {
                "com_gps": f"{(gps_devices/total_devices*100):.2f}%" if total_devices else "0%",
                "com_lbs": f"{(lbs_devices/total_devices*100):.2f}%" if total_devices else "0%",
                "sem_localizacao": f"{(no_location_devices/total_devices*100):.2f}%" if total_devices else "0%",
                "localizacao_2am": f"{(two_am_devices/total_devices*100):.2f}%" if total_devices else "0%"
            }
        })
    
    except sqlite3.Error as e:
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    finally:
        conn.close()

def get_time_ranges():
    now = datetime.now(TIMEZONE)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    two_am_start = now.replace(hour=2, minute=0, second=0, microsecond=0)
    two_am_end = two_am_start + timedelta(hours=1)
    
    return {
        'today_start_ms': int(start_of_day.timestamp() * 1000),
        'today_end_ms': int(end_of_day.timestamp() * 1000),
        'two_am_start_ms': int(two_am_start.timestamp() * 1000),
        'two_am_end_ms': int(two_am_end.timestamp() * 1000),
    }

def to_int(value, default=0):
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default

def calculate_stock_status():
    dados = carregar_dados(JSON_PATH)
    time_ranges = get_time_ranges()
    stock_data = {
        'total_devices': 0,
        'comunicaram_2am': [],
        'comunicaram_2am_gps': [],
        'comunicaram_2am_lbs': [],
        'nao_comunicaram': [],
        'comunicaram_sem_atualizacao': [],
    }
    
    for email, user_data in dados.items():
        for device in user_data.get('devices', []):
            stock_data['total_devices'] += 1
            status = device.get('status', {})
            
            location_date = to_int(status.get('location_date'))
            heartbeat_time = to_int(status.get('heartbeat_time'))
            status_date = to_int(status.get('date'))

            # Verificar comunicação às 2AM
            if time_ranges['two_am_start_ms'] <= location_date <= time_ranges['two_am_end_ms']:
                stock_data['comunicaram_2am'].append(device)
                if status.get('lat', 0) != 0 and status.get('lng', 0) != 0 and status.get('latlng_valid', 0) == 1:
                    stock_data['comunicaram_2am_gps'].append(device)
                if device.get('lbs_position', {}).get('lat') and device.get('lbs_position', {}).get('lng'):
                    stock_data['comunicaram_2am_lbs'].append(device)

            # Verificar não comunicação
            if location_date < time_ranges['today_start_ms']:
                if location_date <= 0:
                    days_diff = "Nunca"
                else:
                    last_time = datetime.fromtimestamp(location_date / 1000, TIMEZONE)
                    days_diff = str((datetime.now(TIMEZONE) - last_time).days)
                
                device_copy = device.copy()
                device_copy['days_no_comm'] = days_diff
                stock_data['nao_comunicaram'].append(device_copy)

            # Verificar comunicação sem atualização
            has_activity_today = (
                (time_ranges['today_start_ms'] <= heartbeat_time <= time_ranges['today_end_ms']) or
                (time_ranges['today_start_ms'] <= status_date <= time_ranges['today_end_ms'])
            )
            if has_activity_today and location_date < time_ranges['today_start_ms']:
                stock_data['comunicaram_sem_atualizacao'].append(device)
    
    return stock_data

# Rota principal para status de estoque
@app.route("/api/status/stock", methods=["GET"])
def stock_status():
    stock_data = calculate_stock_status()
    
    days_no_comm = {}
    for device in stock_data['nao_comunicaram']:
        days = device['days_no_comm']
        days_no_comm[days] = days_no_comm.get(days, 0) + 1
    
    return jsonify({
        'total_devices': stock_data['total_devices'],
        'comunicaram_2am': {
            'total': len(stock_data['comunicaram_2am']),
            'com_gps': len(stock_data['comunicaram_2am_gps']),
            'com_lbs': len(stock_data['comunicaram_2am_lbs'])
        },
        'nao_comunicaram': {
            'total': len(stock_data['nao_comunicaram']),
            'dias_sem_comunicar': days_no_comm
        },
        'comunicaram_sem_atualizacao': {
            'total': len(stock_data['comunicaram_sem_atualizacao'])
        }
    })

# Rotas para listar dispositivos por categoria
@app.route("/api/status/stock/comunicaram_2am", methods=["GET"])
def list_comunicaram_2am():
    stock_data = calculate_stock_status()
    return jsonify([{
        'imei': d['imei'],
        'device_key': d['device_key'],
        'name': d['config'].get('name'),
        'logistica': d.get('logistica', {})  # Incluir dados de logística
    } for d in stock_data['comunicaram_2am']])

@app.route("/api/status/stock/comunicaram_2am/gps", methods=["GET"])
def list_comunicaram_2am_gps():
    stock_data = calculate_stock_status()
    return jsonify([{
        'imei': d['imei'],
        'device_key': d['device_key'],
        'name': d['config'].get('name'),
        'logistica': d.get('logistica', {})  # Incluir dados de logística
    } for d in stock_data['comunicaram_2am_gps']])

@app.route("/api/status/stock/comunicaram_2am/lbs", methods=["GET"])
def list_comunicaram_2am_lbs():
    stock_data = calculate_stock_status()
    return jsonify([{
        'imei': d['imei'],
        'device_key': d['device_key'],
        'name': d['config'].get('name'),
        'logistica': d.get('logistica', {})  # Incluir dados de logística
    } for d in stock_data['comunicaram_2am_lbs']])

@app.route("/api/status/stock/nao_comunicaram", methods=["GET"])
def list_nao_comunicaram():
    stock_data = calculate_stock_status()
    result = []
    for d in stock_data['nao_comunicaram']:
        timestamp_ms = d['status'].get('location_date')
        formatted_date = format_timestamp_to_sp(timestamp_ms)
        
        result.append({
            'imei': d['imei'],
            'device_key': d['device_key'],
            'name': d['config'].get('name'),
            'days_no_comm': d['days_no_comm'],
            'last_location_date': timestamp_ms,
            'last_location_date_br': formatted_date,
            'logistica': d.get('logistica', {})  # Incluir dados de logística
        })
    return jsonify(result)

@app.route("/api/status/stock/comunicaram_sem_atualizacao", methods=["GET"])
def list_comunicaram_sem_atualizacao():
    stock_data = calculate_stock_status()
    return jsonify([{
        'imei': d['imei'],
        'device_key': d['device_key'],
        'name': d['config'].get('name'),
        'heartbeat_time': d['status'].get('heartbeat_time'),
        'status_date': d['status'].get('date'),
        'logistica': d.get('logistica', {})  # Incluir dados de logística
    } for d in stock_data['comunicaram_sem_atualizacao']])

# Configuração para envio de comandos
COMMAND_EMAILS = [
    "loovirj@loovi.com.br",
    "loovirj2@loovi.com.br",
    "loovimg@loovi.com.br",
    "loovidf@loovi.com.br",
    "loovice@loovi.com.br",
    "loovies@loovi.com.br",
    "loovirs@loovi.com.br",
    "loovigo@loovi.com.br",
    "looviac@loovi.com.br",
    "loovipe@loovi.com.br",
    "loovisp1@loovi.com.br",
    "loovisp2@loovi.com.br",
    "loovisp3@loovi.com.br",
    "loovisp4@loovi.com.br",
    "loovisp5@loovi.com.br",
    "loovisp6@loovi.com.br",
    "loovisp7@loovi.com.br",
    "loovisp8@loovi.com.br",
    "loovisp9@loovi.com.br",
    "loovial@loovi.com.br",
    "looviam@loovi.com.br",
    "looviap@loovi.com.br",
    "looviba@loovi.com.br",
    "loovima@loovi.com.br",
    "loovims@loovi.com.br",
    "loovimt@loovi.com.br",
    "loovipa@loovi.com.br",
    "loovipb@loovi.com.br",
    "loovipe@loovi.com.br",
    "loovipi@loovi.com.br",
    "loovipr@loovi.com.br",
    "loovirj2@loovi.com.br",
    "loovirn@loovi.com.br",
    "looviro@loovi.com.br",
    "loovirr@loovi.com.br",
    "loovisc@loovi.com.br",
    "loovise@loovi.com.br",
    "loovito@loovi.com.br"
]
COMMAND_PASSWORD = "123456"
COMMAND_API_AUTH_URL = "http://openapi.tftiot.com/v2/auth/action"
COMMAND_API_URL = "https://openapi.tftiot.com/v2/device-waiting-send-cmds"
IMEI_PATTERN = re.compile(r"^\d{15,16}$")

def validar_imei(imei):
    return bool(IMEI_PATTERN.match(imei))

def gerar_token_comando(email):
    payload = {
        "getAccessToken": {
            "account": email,
            "password-md5": md5(COMMAND_PASSWORD.encode()).hexdigest(),
            "client-type": "web",
        }
    }
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(COMMAND_API_AUTH_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("access-token")
    except:
        return None

def obter_token_valido_comando():
    for email in COMMAND_EMAILS:
        token = gerar_token_comando(email)
        if token:
            return token
    return None

@app.route("/api/send-command-by-imei", methods=["POST"])
def send_command_by_imei():
    data = request.json
    if not data or "imeis" not in data or "command" not in data:
        abort(400, description="Requisição inválida. Campos 'imeis' e 'command' são obrigatórios.")

    imeis = data["imeis"]
    command = data["command"]
    
    valid_imeis = [imei for imei in imeis if validar_imei(imei)]
    invalid_imeis = set(imeis) - set(valid_imeis)
    
    if not valid_imeis:
        abort(400, description="Nenhum IMEI válido fornecido")
    
    token = obter_token_valido_comando()
    if not token:
        abort(500, description="Não foi possível obter um token válido para envio de comandos")
    
    url = f"{COMMAND_API_URL}?access-token={token}"
    payload = {"imeis": valid_imeis, "message": command}
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=45)
        response_data = response.json()
        
        if response.status_code == 200 and response_data.get("code") == 0:
            return jsonify({
                "success": True,
                "message": "Comandos enviados com sucesso",
                "tft_response": response_data
            })
        else:
            error_msg = response_data.get("msg", "Erro desconhecido na API TFT")
            return jsonify({
                "success": False,
                "error": error_msg,
                "code": response_data.get("code"),
                "status_code": response.status_code
            }), 400
            
    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "error": "Timeout ao comunicar com a API TFT"
        }), 504
    except requests.exceptions.RequestException as e:
        return jsonify({
            "success": False,
            "error": f"Erro de rede: {str(e)}"
        }), 503
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro inesperado: {str(e)}"
        }), 500

# Executar o servidor
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5004, debug=True)