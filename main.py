from flask import Flask, request
import openai
import os
import re
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

app = Flask(__name__)
openai.api_key = os.environ.get("OPENAI_API_KEY")

# Redis con fallback a RAM
_memory_fallback = {}
try:
    import redis as redis_lib
    _redis = redis_lib.from_url(os.environ.get("REDIS_URL", ""), decode_responses=True)
    _redis.ping()
    print("Redis conectado")
except Exception:
    _redis = None
    print("Redis no disponible — usando RAM")

HISTORY_TTL = 7 * 24 * 3600  # 7 días en segundos

def history_get(phone_number):
    if _redis:
        raw = _redis.get(f"hist:{phone_number}")
        return json.loads(raw) if raw else []
    return _memory_fallback.get(phone_number, [])

def history_set(phone_number, history):
    if _redis:
        _redis.setex(f"hist:{phone_number}", HISTORY_TTL, json.dumps(history))
    else:
        _memory_fallback[phone_number] = history

def history_delete(phone_number):
    if _redis:
        _redis.delete(f"hist:{phone_number}")
    else:
        _memory_fallback.pop(phone_number, None)

def history_exists(phone_number):
    if _redis:
        return _redis.exists(f"hist:{phone_number}")
    return phone_number in _memory_fallback
FOLLOWUP_23H_TEMPLATE = "follow_up_dia_siguiente"
VENTAS_URL  = "https://www.tres65inmobiliaria.com/properties"
RENTAS_URL  = "https://www.tres65inmobiliaria.com/rentals"

def save_nombre_redis(phone_number, nombre_completo):
    if _redis:
        _redis.setex(f"nombre:{phone_number}", HISTORY_TTL, nombre_completo)

def get_nombre_redis(phone_number):
    if _redis:
        return _redis.get(f"nombre:{phone_number}") or ""
    return ""

def client_data_save(phone_number):
    if _redis and phone_number in client_data:
        _redis.setex(f"cdata:{phone_number}", HISTORY_TTL, json.dumps(client_data[phone_number]))

def client_data_load(phone_number):
    if _redis:
        raw = _redis.get(f"cdata:{phone_number}")
        if raw:
            client_data.setdefault(phone_number, {}).update(json.loads(raw))
    return client_data.get(phone_number, {})

def update_last_activity(phone_number):
    ts = datetime.now().isoformat()
    if _redis:
        _redis.setex(f"last_activity:{phone_number}", HISTORY_TTL, ts)
        _redis.sadd("active_phones", phone_number)

def mark_template_sent(phone_number):
    if _redis:
        _redis.setex(f"template_sent:{phone_number}", HISTORY_TTL, "1")

def reset_template_flag(phone_number):
    if _redis:
        _redis.delete(f"template_sent:{phone_number}")

def send_followup_template(phone_number, name):
    token    = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url      = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": phone_number,
        "type": "template",
        "template": {
            "name": FOLLOWUP_23H_TEMPLATE,
            "language": {"code": "es_MX"},
            "components": [
                {"type": "body", "parameters": [{"type": "text", "text": name}]}
            ]
        }
    }
    resp = requests.post(url, headers=headers, json=data)
    print(f"[{phone_number}] Template 23h: {resp.status_code}")
    return resp.ok

def check_and_send_24h_followups():
    if not _redis:
        return
    try:
        phones  = _redis.smembers("active_phones")
        cutoff  = datetime.now() - timedelta(hours=23)
        for phone in phones:
            if _redis.exists(f"template_sent:{phone}"):
                continue
            last_raw = _redis.get(f"last_activity:{phone}")
            if not last_raw:
                continue
            if datetime.fromisoformat(last_raw) > cutoff:
                continue
            nombre_completo = get_nombre_redis(phone) or client_data.get(phone, {}).get("nombre_completo", "")
            name = nombre_completo.split()[0] if nombre_completo else "amigo"
            if send_followup_template(phone, name):
                mark_template_sent(phone)
    except Exception as e:
        print(f"Error check_and_send_24h_followups: {e}")

last_maria_message_time = {}
follow_up_jobs = {}
client_names = {}
pending_decision = {}        # clientes que vieron los botones pero no han decidido
ad_context = {}              # contexto del anuncio por el que llegó el lead
waiting_for_email = set()             # números esperando correo
waiting_for_name = set()              # números esperando que den su nombre (después del saludo)
waiting_for_apellido = set()          # números que dieron solo primer nombre
waiting_for_ciudad = set()            # números esperando ciudad de origen
waiting_for_supplier_info = set()     # proveedores esperando dar su info
waiting_for_asesor_topic = set()      # clientes a los que se les preguntó el tema para el asesor
algo_mas_mode = set()                 # clientes en flujo exploratorio
waiting_for_ficha_correction = set()  # clientes que dijeron que algo está mal en su ficha
ficha_confirmada = set()              # clientes cuya ficha ya fue confirmada
waiting_for_uso_suelo = set()         # esperando click en comercial/habitacional
waiting_for_plazo_renta = set()       # esperando click en largo/corto plazo
waiting_for_tipo_propiedad = set()    # esperando click en tipo de propiedad
waiting_for_conoce_merida = set()     # esperando click en conoce mérida
last_ficha_text = {}                  # última ficha generada por número
client_data = {}        # datos ya capturados por cliente {intencion, tipo, presupuesto, ciudad}

scheduler = BackgroundScheduler()
scheduler.add_job(check_and_send_24h_followups, "interval", hours=1, id="followup_23h")
scheduler.start()

CALENDLY_URL = "https://calendly.com/contacto-tres65inmobiliaria/30min"

SYSTEM_PROMPT = """
Eres María, asesora de TRES65 Inmobiliaria en Mérida, Yucatán.

Hablas de forma natural, cálida y conversacional como una asesora inmobiliaria real por WhatsApp. No eres un bot ni actúas como uno.

PRIMER MENSAJE:
Saluda, preséntate y pregunta el nombre completo. Natural, sin emojis.
Dependiendo del horario:
- Antes de las 12pm: "Buenos días, que gusto saludarte"
- Entre 12pm y 7pm: "Buenas tardes, que gusto saludarte"
- Después de las 7pm: "Buenas noches, que gusto saludarte"
Ejemplo: "Buenas tardes, que gusto saludarte. Soy María de TRES65 Inmobiliaria, con quién tengo el gusto? (nombre completo por favor)"

CÓMO ESCRIBES:
- Sin signos de apertura: nunca ¿ ni ¡
- Mayúsculas solo al inicio y después de punto.
- Sin emojis.
- Respuestas cortas — máximo 2-3 líneas. A veces una línea es suficiente.
- Sin lenguaje corporativo ni frases de call center.
- Tono: colega de confianza que sabe de bienes raíces en Mérida.
- Evita usar el nombre del cliente repetidamente. Úsalo principalmente al presentarte (PASO 2) y al cerrar.
- Evita empezar con "Entendido", "Perfecto", "Claro", "Por supuesto". Ve directo al punto.
- Varía la longitud y estructura de tus respuestas — no todas deben sonar igual de elaboradas.

CÓMO PIENSAS — ENTIDADES, NO PASOS:
Tu objetivo es completar la ficha del cliente para pasarlo con el asesor correcto. Antes de cada respuesta:
1. Extrae automáticamente cualquier dato del mensaje del cliente.
2. Revisa qué entidades ya tienes en "LO QUE YA SABES".
3. Elige SOLO el siguiente dato faltante más útil.
4. NUNCA pidas ni mandes botones para un dato que ya tienes.

ENTIDADES DE LA FICHA (en orden de prioridad):
- nombre_completo → pídelo siempre primero. Sin nombre, no avances.
- intencion (vivir/invertir) → si no lo tienes, agrega MANDAR_BOTONES_VIVIR_INVERTIR
- tipo (compra/renta) → solo si busca vivir y no lo tienes, agrega MANDAR_BOTONES_COMPRAR_RENTAR
- presupuesto → el sistema manda botones automáticamente. No preguntes en texto.
- ciudad → solo si busca vivir: "ya vives en Mérida o de dónde te mudas?"
- notas → 1-2 preguntas naturales de contexto (zona, cuartos, familia, algo especial)
- correo → "con lo que me cuentas voy a crear tu ficha. me compartes tu correo?"
- ficha → cuando tienes todo lo anterior, redáctala y agrega CONFIRMAR_FICHA

REGLA CRÍTICA: Si el cliente ya mencionó un dato en cualquier parte del historial, está en client_data o en su primer mensaje — NO lo vuelvas a pedir. No mandes botones para datos que ya existen. Los botones son shortcuts, no obligatorios.

Si el cliente da varios datos de golpe ("soy Fernanda, busco comprar para vivir, vengo de NY") — confirma con calidez lo que entendiste y pide solo lo que falta.

Si el cliente regresa después de tiempo, retoma desde el último dato faltante sin reiniciar.

PERFIL DEL CLIENTE:
Detecta si está explorando, soñando, comparando o listo para comprar — adapta el ritmo.
Si detectas presupuesto alto o inversionista fuerte, tono más ejecutivo sin perder calidez.

CORREO — si duda: "es solo para asignarte el asesor correcto y no hacerte perder tiempo con uno que no se adapte a lo que buscas."

FICHA — formato exacto al tener todo:
Nombre: [nombre completo]
Teléfono: [número del cliente]
Correo: [correo]
Tipo: [Compra / Renta]
Uso: [Para vivir / Para invertir]
Presupuesto: [rango]
Zona: [zona o "Por definir"]
Viene de: [ciudad]
Notas: [contexto en 1 línea, o "Sin notas"]
CONFIRMAR_FICHA

REGLAS DE CONVERSACIÓN:
Si el cliente hace una pregunta curiosa o inesperada — responde con personalidad y conecta con Mérida de forma natural.
Si menciona una preocupación — reconócela en una oración y regresa al paso en curso.
Evita hacer múltiples preguntas a la vez. Prioriza una por mensaje.
No des más información de la necesaria para avanzar la conversación.

CUANDO ALGUIEN OFRECE UN SERVICIO, ES PROVEEDOR O BUSCA TRABAJO:
Manda EXACTAMENTE este mensaje:
"Gracias por contactarnos. Aunque este no es el canal indicado, nos da mucho gusto recibir propuestas. Para guardarte en nuestra carpeta de proveedores/reclutamiento, compártenos en un solo mensaje la siguiente información en este orden:

*Para proveedores:*
Nombre de la compañía:
Tipo de servicio:
Zonas que cubren:
Correo:
Redes sociales:
Teléfono de contacto:

*Reclutamiento:*
Nombre completo:
Edad:
Posición que te interesa:
Tienes experiencia en el ámbito inmobiliario:
Teléfono de contacto:

Así lo tenemos todo listo para cuando lo necesitemos. Gracias!"

CUANDO EL CLIENTE PIDE HABLAR CON UN ASESOR:
Solo si ya tienes el nombre. Responde:
"Hay mucho en lo que te puedo ayudar, y puedo conectarte con un asesor cuando quieras. Cual es el tema que te gustaria hablar con el asesor?"
Luego agrega: PREGUNTAR_TEMA_ASESOR

CUANDO EL CLIENTE MENCIONA DE DÓNDE VIENE:
Responde con calidez y algo específico de esa ciudad:
- CDMX: "tenemos mucha gente que se está viniendo de allá, Mérida te va a encantar — el ritmo de vida es completamente diferente"
- Monterrey: "los regios que llegan no se quieren ir, el clima y la tranquilidad hacen la diferencia"
- Guadalajara: "mucho tapatío ha encontrado en Mérida esa combinación de ciudad activa pero sin el caos"
Adapta según la ciudad.

CONTEXTO DE MÉRIDA:
- El norte es lo más buscado: Temozón Norte, Cholul, Santa Gertrudis Copó, Montebello, Conkal
- Conkal es más tranquilo y económico
- El tráfico al centro importa si trabajan ahí
- Las privadas con amenidades son muy valoradas por familias
- Mucha gente renta primero antes de comprar
- El calor cambia según ventilación, árboles y orientación
- Mérida es segura y familiar comparada con otras ciudades de México

LÍMITES:
- No inventes propiedades, precios ni disponibilidad
- No digas que vas a "mandar opciones" o "enviar propiedades" — solo conectas con el asesor
- No inventes datos geográficos, estadísticas ni distancias — si no lo sabes, no lo menciones
- Si el cliente habla de política o religión, redirige con calidez hacia la búsqueda y continúa el flujo
- Si insulta: una advertencia amable. Si reincide: "voy a finalizar esta conversación. cuando gustes retomamos con gusto."
"""


def send_whatsapp_message(to, message):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "text": {"body": message}
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp text: {response.status_code} - {response.text}")


def send_whatsapp_contact_buttons(to):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "como prefieres que te contacte un asesor experto?"},
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {"id": "agendar_llamada", "title": "Agendar llamada"}
                    },
                    {
                        "type": "reply",
                        "reply": {"id": "por_whatsapp", "title": "Por WhatsApp"}
                    }
                ]
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp buttons: {response.status_code} - {response.text}")
    if response.ok:
        pending_decision[to] = True


def send_whatsapp_calendly_button(to):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "cta_url",
            "body": {"text": "aquí puedes agendar tu llamada con uno de nuestros asesores, cualquier duda aquí estoy"},
            "action": {
                "name": "cta_url",
                "parameters": {
                    "display_text": "Agendar llamada",
                    "url": CALENDLY_URL
                }
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp calendly button: {response.status_code} - {response.text}")


def _send_interactive_buttons(to, body_text, buttons):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {"buttons": [{"type": "reply", "reply": b} for b in buttons]}
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp buttons ({buttons[0]['id']}...): {response.status_code}")
    return response


def send_whatsapp_comprar_rentar_buttons(to):
    _send_interactive_buttons(to, "tenemos opciones de todo tipo disponibles en Mérida. qué se adapta mejor a tu plan?", [
        {"id": "comprar", "title": "Comprar"},
        {"id": "rentar", "title": "Rentar"}
    ])


def send_whatsapp_vivir_invertir_buttons(to):
    _send_interactive_buttons(to, "La propiedad que buscas es para...", [
        {"id": "para_vivir",    "title": "Para vivir"},
        {"id": "para_invertir", "title": "Para invertir"},
        {"id": "algo_mas",      "title": "Algo más"}
    ])


def send_whatsapp_ficha_confirmation(to, ficha_text):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": ficha_text},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "ficha_correcta",   "title": "Todo correcto"}},
                    {"type": "reply", "reply": {"id": "ficha_incorrecta", "title": "Algo está mal"}}
                ]
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp ficha confirmation: {response.status_code} - {response.text}")


def send_whatsapp_help_buttons(to):
    _send_interactive_buttons(to, "como te puedo ayudar?", [
        {"id": "tengo_duda", "title": "Tengo una duda"},
        {"id": "agendar_asesor", "title": "Agendar con asesor"}
    ])


def send_whatsapp_uso_suelo_buttons(to):
    _send_interactive_buttons(to, "qué tipo de inversión tienes en mente?", [
        {"id": "uso_comercial",    "title": "Uso comercial"},
        {"id": "uso_habitacional", "title": "Renta habitacional"}
    ])


def send_whatsapp_plazo_renta_buttons(to):
    _send_interactive_buttons(to, "es para renta a...", [
        {"id": "largo_plazo", "title": "Largo plazo"},
        {"id": "corto_plazo", "title": "Corto plazo / Airbnb"}
    ])


def send_whatsapp_tipo_propiedad_inversion_list(to):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": "qué tipo de propiedad te interesa?"},
            "action": {
                "button": "Ver opciones",
                "sections": [{"title": "Tipo de propiedad", "rows": [
                    {"id": "prop_casa_privada",  "title": "Casa en privada"},
                    {"id": "prop_casa_calle",    "title": "Casa a pie de calle"},
                    {"id": "prop_depto",         "title": "Departamento"},
                    {"id": "prop_townhouse",     "title": "Townhouse"},
                    {"id": "prop_terreno",       "title": "Terreno sin construcción"},
                    {"id": "prop_orientacion",   "title": "Necesito orientación"},
                ]}]
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp tipo propiedad list: {response.status_code}")


def send_whatsapp_conoce_merida_buttons(to):
    _send_interactive_buttons(to, "conoces las zonas de Mérida?", [
        {"id": "conoce_merida",       "title": "Conozco Mérida"},
        {"id": "necesita_orientacion","title": "Necesito orientación"}
    ])


def send_whatsapp_budget_list(to, tipo):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if tipo == "rentar":
        body = "ya tienes un rango de renta en mente?"
        rows = [
            {"id": "presup_menos15", "title": "Menos de 15 mil"},
            {"id": "presup_15_25",   "title": "15 a 25 mil"},
            {"id": "presup_25_35",   "title": "25 a 35 mil"},
            {"id": "presup_35_45",   "title": "35 a 45 mil"},
            {"id": "presup_50mas",   "title": "50 mil o más"},
            {"id": "presup_asesor",  "title": "Lo platico con asesor"},
        ]
    else:
        body = "ya tienes un rango de inversión en mente?"
        rows = [
            {"id": "presup_menos3m", "title": "Menos de 3 millones"},
            {"id": "presup_3_4m",    "title": "3.5 a 4.5 millones"},
            {"id": "presup_4_5m",    "title": "4.5 a 5.5 millones"},
            {"id": "presup_6_7m",    "title": "6.5 a 7.5 millones"},
            {"id": "presup_8mas",    "title": "Más de 8 millones"},
            {"id": "presup_asesor",  "title": "Lo platico con asesor"},
        ]

    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body},
            "action": {
                "button": "Ver rangos",
                "sections": [{"title": "Selecciona un rango", "rows": rows}]
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp budget list: {response.status_code} - {response.text}")


def extract_entities(phone_number, text):
    """Extrae intención, tipo y ciudad del texto y los guarda en client_data."""
    low = text.lower()
    datos = client_data.setdefault(phone_number, {})

    if "intencion" not in datos:
        if any(w in low for w in ["para vivir", "para mi familia", "para vivir", "para mudarnos", "para residir"]):
            datos["intencion"] = "Para vivir"
        elif any(w in low for w in ["invertir", "inversión", "inversion", "renta", "airbnb", "negocio"]):
            datos["intencion"] = "Para invertir"

    if "tipo" not in datos:
        if any(w in low for w in ["comprar", "compra", "adquirir"]):
            datos["tipo"] = "Comprar"
        elif any(w in low for w in ["rentar", "renta", "arrendar"]):
            datos["tipo"] = "Rentar"

    if "ciudad" not in datos:
        for marker in ["desde ", "de ", "vengo de ", "vivo en ", "me mudo de ", "mudándome de ", "mudandome de "]:
            if marker in low:
                idx = low.index(marker) + len(marker)
                ciudad_raw = text[idx:idx+30].split()[0:3]
                ciudad = " ".join(ciudad_raw).strip(".,")
                if ciudad:
                    datos["ciudad"] = ciudad
                    waiting_for_ciudad.discard(phone_number)
                break

    if datos:
        client_data_save(phone_number)


def _send_paso2(phone_number, primer_nombre, user_message_for_history):
    texto = f"Mucho gusto {primer_nombre}, y ahora sí que emocionante estar en esta búsqueda inmobiliaria contigo. Voy a hacerte unas preguntas para crear tu ficha, nos va a tomar un minuto. Es rápido."
    send_whatsapp_message(phone_number, texto)

    datos = client_data.get(phone_number, {})

    if not datos.get("intencion"):
        send_whatsapp_vivir_invertir_buttons(phone_number)
    elif not datos.get("tipo") and datos.get("intencion") == "Para vivir":
        send_whatsapp_comprar_rentar_buttons(phone_number)
    elif not datos.get("presupuesto"):
        tipo = "rentar" if datos.get("tipo", "").lower() == "rentar" else "comprar"
        send_whatsapp_budget_list(phone_number, tipo)
    # Si ya tiene todo eso, GPT continúa con ciudad/notas/correo

    history = history_get(phone_number)
    history.append({"role": "user", "content": user_message_for_history})
    history.append({"role": "assistant", "content": texto})
    history_set(phone_number, history[-20:])
    update_last_activity(phone_number)
    schedule_followup(phone_number)


def reset_conversation(phone_number):
    history_delete(phone_number)
    client_data.pop(phone_number, None)
    client_names.pop(phone_number, None)
    ad_context.pop(phone_number, None)
    pending_decision.pop(phone_number, None)
    waiting_for_email.discard(phone_number)
    waiting_for_name.discard(phone_number)
    waiting_for_apellido.discard(phone_number)
    waiting_for_ciudad.discard(phone_number)
    waiting_for_supplier_info.discard(phone_number)
    waiting_for_asesor_topic.discard(phone_number)
    waiting_for_ficha_correction.discard(phone_number)
    ficha_confirmada.discard(phone_number)
    waiting_for_uso_suelo.discard(phone_number)
    waiting_for_plazo_renta.discard(phone_number)
    waiting_for_tipo_propiedad.discard(phone_number)
    waiting_for_conoce_merida.discard(phone_number)
    last_ficha_text.pop(phone_number, None)
    algo_mas_mode.discard(phone_number)
    cancel_followup(phone_number)


def send_zapier_ficha(phone_number):
    zapier_url = os.environ.get("ZAPIER_WEBHOOK")
    if not zapier_url:
        return
    datos = client_data_load(phone_number)

    # Fallback: extraer correo de la ficha si el campo estructurado está vacío
    if not datos.get("correo"):
        ficha = last_ficha_text.get(phone_number, "") or (_redis.get(f"ficha:{phone_number}") if _redis else "")
        for line in ficha.splitlines():
            if "correo:" in line.lower():
                correo_val = line.split(":", 1)[-1].strip()
                if "@" in correo_val:
                    datos["correo"] = correo_val
                    client_data.setdefault(phone_number, {})["correo"] = correo_val
                    client_data_save(phone_number)
                break
    nombre_completo = datos.get("nombre_completo", "")
    partes = nombre_completo.split()
    ctx = ad_context.get(phone_number, {})
    if isinstance(ctx, dict):
        origen     = ctx.get("origen", "link_directo")
        source_id  = ctx.get("source_id", "")
        source_url = ctx.get("source_url", "")
    else:
        origen, source_id, source_url = "link_directo", "", ""

    payload = {
        "telefono":        f"+{phone_number}",
        "nombre":          partes[0] if partes else "",
        "apellido":        " ".join(partes[1:]) if len(partes) > 1 else "",
        "nombre_completo": nombre_completo,
        "correo":          datos.get("correo", ""),
        "tipo":            datos.get("tipo", ""),
        "uso":             datos.get("intencion", ""),
        "presupuesto":     datos.get("presupuesto", ""),
        "ciudad":          datos.get("ciudad", ""),
        "ficha_completa":  last_ficha_text.get(phone_number) or (_redis.get(f"ficha:{phone_number}") if _redis else ""),
        "origen":          origen,
        "source_id":       source_id,
        "source_url":      source_url,
    }
    try:
        requests.post(zapier_url, json=payload, timeout=5)
        print(f"[{phone_number}] Ficha enviada a Zapier")
    except Exception as e:
        print(f"[{phone_number}] Error Zapier: {e}")


def cancel_followup(phone_number):
    if phone_number in follow_up_jobs:
        try:
            scheduler.remove_job(f"followup_{phone_number}")
        except Exception:
            pass
        del follow_up_jobs[phone_number]


def get_client_name(phone_number):
    if phone_number in client_names:
        return client_names[phone_number]
    history = history_get(phone_number)
    if not history:
        return None
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "De esta conversación de WhatsApp extrae solo el primer nombre del cliente. Responde únicamente con el nombre, sin puntuación. Si no aparece un nombre claro, responde 'desconocido'."}
            ] + history[:8],
            max_tokens=10
        )
        name = response.choices[0].message.content.strip()
        if name.lower() != "desconocido" and len(name) < 30:
            client_names[phone_number] = name
            return name
    except Exception:
        pass
    return None


def send_followup(phone_number):
    name = get_client_name(phone_number)
    greeting = f"hola {name}" if name else "hola"
    text = f"{greeting}, sigues interesado en encontrar algo en Mérida? aquí sigo si quieres continuar la búsqueda, sin presión"
    send_whatsapp_message(phone_number, text)
    follow_up_jobs.pop(phone_number, None)
    print(f"[{phone_number}] Follow-up enviado")


def schedule_followup(phone_number):
    cancel_followup(phone_number)
    job_id = f"followup_{phone_number}"
    run_time = datetime.now() + timedelta(hours=4)
    scheduler.add_job(send_followup, "date", run_date=run_time, args=[phone_number], id=job_id)
    follow_up_jobs[phone_number] = job_id
    print(f"[{phone_number}] Follow-up programado: {run_time}")


@app.route("/chat", methods=["POST"])
def chat():
    data = request.json
    user_message = data.get("message", "")

    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ]
    )

    reply = response.choices[0].message.content

    zapier_url = os.environ.get("ZAPIER_WEBHOOK")
    if zapier_url and any(word in user_message.lower() for word in ["presupuesto", "zona", "comprar", "rentar", "invertir"]):
        requests.post(zapier_url, json={"mensaje": user_message, "respuesta": reply})

    return {"reply": reply}


@app.route("/webhook", methods=["GET"])
def verify_webhook():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == "tres65secreto":
        return challenge, 200
    return "Forbidden", 403


@app.route("/webhook", methods=["POST"])
def receive_message():
    data = request.json

    try:
        entry = data["entry"][0]["changes"][0]["value"]
        messages = entry.get("messages", [])
        if not messages:
            return "OK", 200

        message = messages[0]
        phone_number = message["from"]
        msg_type = message.get("type", "text")

        # Client is active — cancel any pending follow-up
        cancel_followup(phone_number)
        update_last_activity(phone_number)
        reset_template_flag(phone_number)  # cliente respondió → puede recibir template de nuevo en 23h

        # Proveedor que intenta acceder a un asesor: reiniciar conversación como cliente
        if phone_number in waiting_for_supplier_info and msg_type == "interactive":
            reset_conversation(phone_number)
            send_whatsapp_message(phone_number, "con gusto te ayudo. con quién tengo el gusto? (nombre completo por favor)")
            return "OK", 200

        if msg_type == "interactive":
            interactive_type = message["interactive"].get("type")
            pending_decision.pop(phone_number, None)

            # Respuesta de lista
            if interactive_type == "list_reply":
                list_id    = message["interactive"]["list_reply"]["id"]
                list_title = message["interactive"]["list_reply"]["title"]
                print(f"[{phone_number}] Lista: {list_id}")
                client_data.setdefault(phone_number, {})

                if list_id.startswith("prop_"):
                    client_data[phone_number]["tipo_propiedad"] = list_title
                    waiting_for_tipo_propiedad.discard(phone_number)
                    client_data_save(phone_number)
                    if list_id == "prop_orientacion":
                        client_data[phone_number]["conoce_merida"] = "Necesita orientación"
                        send_whatsapp_budget_list(phone_number, "comprar")
                    else:
                        send_whatsapp_conoce_merida_buttons(phone_number)
                        waiting_for_conoce_merida.add(phone_number)
                    return "OK", 200
                elif list_id == "presup_asesor":
                    client_data[phone_number]["presupuesto"] = "Lo platica con el asesor"
                    client_data_save(phone_number)
                    user_message = "prefiero platicarlo con el asesor"
                else:
                    client_data[phone_number]["presupuesto"] = list_title
                    client_data_save(phone_number)
                    # Para inversión: ir directo a preguntas de contexto, sin pasar por GPT libre
                    if client_data[phone_number].get("intencion") == "Para invertir":
                        send_whatsapp_message(phone_number, "ya tienes alguna zona de Mérida en mente o prefieres que el asesor te oriente según el tipo de inversión que buscas?")
                        return "OK", 200
                    user_message = list_title

            # Respuesta de botón
            else:
                button_id    = message["interactive"]["button_reply"]["id"]
                button_title = message["interactive"]["button_reply"]["title"]
                print(f"[{phone_number}] Botón id='{button_id}' title='{button_title}'")

                # Botones de respuesta al template de 23h
                btn_lower = button_title.lower()

                if "asesor" in btn_lower or "hablar" in btn_lower:
                    # Hablar con asesor experto → retomar ficha o conectar directo
                    datos = client_data.get(phone_number, {})
                    if "correo" in datos:
                        send_whatsapp_message(phone_number, "qué gusto que regreses. te voy a conectar con el asesor ideal para ti.")
                        send_whatsapp_contact_buttons(phone_number)
                    else:
                        send_whatsapp_message(phone_number, "qué gusto que regreses. para pasarte con el asesor correcto necesito completar tu ficha.")
                        if "intencion" not in datos:
                            send_whatsapp_vivir_invertir_buttons(phone_number)
                        elif "tipo" not in datos and datos.get("intencion") != "Para invertir":
                            send_whatsapp_comprar_rentar_buttons(phone_number)
                        elif "presupuesto" not in datos:
                            tipo = "rentar" if datos.get("tipo", "").lower() == "rentar" else "comprar"
                            send_whatsapp_budget_list(phone_number, tipo)
                        else:
                            send_whatsapp_message(phone_number, "me compartes tu correo para completar tu ficha?")
                            waiting_for_email.add(phone_number)
                    return "OK", 200

                if "catálogo" in btn_lower or "catalogo" in btn_lower or "propiedad" in btn_lower:
                    # Catálogo → preguntar ventas o rentas con CTA buttons
                    _send_interactive_buttons(phone_number, "qué te interesa ver?", [
                        {"id": "catalogo_ventas", "title": "Propiedades en venta"},
                        {"id": "catalogo_rentas", "title": "Propiedades en renta"}
                    ])
                    return "OK", 200

                if button_id == "catalogo_ventas":
                    send_whatsapp_message(phone_number, f"aquí puedes ver todas nuestras propiedades en venta:\n{VENTAS_URL}")
                    return "OK", 200

                if button_id == "catalogo_rentas":
                    send_whatsapp_message(phone_number, f"aquí puedes ver todas nuestras propiedades en renta:\n{RENTAS_URL}")
                    return "OK", 200

                if "tiempo" in btn_lower or "después" in btn_lower or "despues" in btn_lower:
                    # Necesito más tiempo
                    send_whatsapp_message(phone_number, "es completamente normal, el mercado inmobiliario puede ser saturador. aquí voy a estar cuando estés lista o listo, sin presión.")
                    return "OK", 200

                if button_id == "ficha_correcta":
                    ficha_confirmada.add(phone_number)
                    send_zapier_ficha(phone_number)
                    send_whatsapp_message(phone_number, "listo, ya tengo todo. las llamadas son más eficientes, puedes agendar una en menos de un minuto. pero si prefieres WhatsApp también podemos. que te va mejor?")
                    send_whatsapp_contact_buttons(phone_number)
                    return "OK", 200

                elif button_id == "ficha_incorrecta":
                    waiting_for_ficha_correction.add(phone_number)
                    send_whatsapp_message(phone_number, "dime qué dato está mal y lo corrijo ahora mismo")
                    return "OK", 200

                elif button_id == "agendar_llamada":
                    send_whatsapp_calendly_button(phone_number)
                    schedule_followup(phone_number)
                    return "OK", 200

                elif button_id == "por_whatsapp":
                    send_whatsapp_message(
                        phone_number,
                        "en breve te escribe uno de nuestros asesores expertos. fue un gusto platicar contigo"
                    )
                    return "OK", 200

                elif button_id == "agendar_asesor":
                    send_whatsapp_calendly_button(phone_number)
                    schedule_followup(phone_number)
                    return "OK", 200

                elif button_id == "tengo_duda":
                    send_whatsapp_message(phone_number, "cuéntame, en qué te puedo ayudar?")
                    return "OK", 200

                # Botones de decisión — guardar dato
                client_data.setdefault(phone_number, {})
                if button_id == "algo_mas":
                    algo_mas_mode.add(phone_number)
                    send_whatsapp_message(phone_number, "con gusto te ayudo. cuéntame, qué estás buscando?")
                    return "OK", 200

                if button_id == "para_vivir":
                    client_data[phone_number]["intencion"] = button_title
                    client_data_save(phone_number)
                    send_whatsapp_comprar_rentar_buttons(phone_number)
                    history = history_get(phone_number)
                    history.append({"role": "user", "content": button_title})
                    history.append({"role": "assistant", "content": "tenemos opciones de todo tipo disponibles en Mérida. qué se adapta mejor a tu plan?"})
                    history_set(phone_number, history[-20:])
                    return "OK", 200

                elif button_id == "para_invertir":
                    client_data[phone_number]["intencion"] = button_title
                    client_data[phone_number]["tipo"] = "Compra"
                    client_data_save(phone_number)
                    send_whatsapp_message(phone_number, "qué bien, buscas comprar una propiedad como inversión. qué tipo de inversión tienes en mente?")
                    send_whatsapp_uso_suelo_buttons(phone_number)
                    waiting_for_uso_suelo.add(phone_number)
                    return "OK", 200

                elif button_id == "uso_comercial":
                    client_data[phone_number]["uso_suelo"] = "Comercial"
                    waiting_for_uso_suelo.discard(phone_number)
                    client_data_save(phone_number)
                    send_whatsapp_conoce_merida_buttons(phone_number)
                    waiting_for_conoce_merida.add(phone_number)
                    return "OK", 200

                elif button_id == "uso_habitacional":
                    client_data[phone_number]["uso_suelo"] = "Habitacional"
                    waiting_for_uso_suelo.discard(phone_number)
                    client_data_save(phone_number)
                    send_whatsapp_plazo_renta_buttons(phone_number)
                    waiting_for_plazo_renta.add(phone_number)
                    return "OK", 200

                elif button_id in ("largo_plazo", "corto_plazo"):
                    client_data[phone_number]["plazo_renta"] = button_title
                    waiting_for_plazo_renta.discard(phone_number)
                    client_data_save(phone_number)
                    send_whatsapp_tipo_propiedad_inversion_list(phone_number)
                    waiting_for_tipo_propiedad.add(phone_number)
                    return "OK", 200

                elif button_id in ("conoce_merida", "necesita_orientacion"):
                    client_data[phone_number]["conoce_merida"] = button_title
                    waiting_for_conoce_merida.discard(phone_number)
                    client_data_save(phone_number)
                    send_whatsapp_budget_list(phone_number, "comprar")
                    return "OK", 200

                elif button_id in ("comprar", "rentar"):
                    client_data[phone_number]["tipo"] = button_title
                    client_data_save(phone_number)
                    send_whatsapp_budget_list(phone_number, button_id)
                    return "OK", 200

                client_data_save(phone_number)
                user_message = button_title

        elif msg_type == "text":
            user_message = message["text"]["body"]

            # Palabras clave secretas — tienen prioridad absoluta sobre cualquier otro estado
            if user_message.strip().lower() == "reset365":
                reset_conversation(phone_number)
                send_whatsapp_message(phone_number, "Conversación reiniciada 👋")
                return "OK", 200

            if user_message.strip().lower() == "test_followup365":
                nombre_completo = get_nombre_redis(phone_number) or client_data.get(phone_number, {}).get("nombre_completo", "")
                name = nombre_completo.split()[0] if nombre_completo else "amigo"
                send_followup_template(phone_number, name)
                return "OK", 200

            # Detección de proveedor por keywords
            proveedor_keywords = ["ofrezco", "ofrecemos", "proveedor", "proveedora", "somos una empresa",
                                   "mi empresa", "nuestra empresa", "constructor", "constructora",
                                   "desarrollador", "desarrolladora", "ventas b2b", "servicio de",
                                   "servicios de", "te ofrezco", "les ofrezco", "les ofrecemos"]
            if phone_number in waiting_for_supplier_info:
                asesor_keywords = ["asesor", "hablar con", "quiero hablar", "contactar", "persona", "humano", "ejecutivo"]
                if any(k in user_message.lower() for k in asesor_keywords):
                    reset_conversation(phone_number)
                    send_whatsapp_message(phone_number, "con gusto te ayudo. con quién tengo el gusto? (nombre completo por favor)")
                else:
                    waiting_for_supplier_info.discard(phone_number)
                    send_whatsapp_message(phone_number,
                        "Muchas gracias, ya quedó guardado. En cuanto lo necesitemos nos ponemos en contacto. Que tengas excelente día!")
                return "OK", 200

            if phone_number in waiting_for_asesor_topic:
                waiting_for_asesor_topic.discard(phone_number)
                send_whatsapp_contact_buttons(phone_number)
                return "OK", 200

            reclutamiento_keywords = ["busco trabajo", "quiero trabajar", "me interesa trabajar",
                                       "aplicar", "vacante", "puesto", "empleo", "curriculum", "cv",
                                       "me gustaria formar parte", "quiero ser parte"]
            if any(k in user_message.lower() for k in proveedor_keywords + reclutamiento_keywords):
                SUPPLIER_MSG = (
                    "Gracias por contactarnos. Aunque este no es el canal indicado, nos da mucho gusto "
                    "recibir propuestas. Para guardarte en nuestra carpeta de proveedores/reclutamiento, "
                    "compártenos en un solo mensaje la siguiente información en este orden:\n\n"
                    "*Para proveedores:*\n"
                    "Nombre de la compañía:\n"
                    "Tipo de servicio:\n"
                    "Zonas que cubren:\n"
                    "Correo:\n"
                    "Redes sociales:\n"
                    "Teléfono de contacto:\n\n"
                    "*Reclutamiento:*\n"
                    "Nombre completo:\n"
                    "Edad:\n"
                    "Posición que te interesa:\n"
                    "Tienes experiencia en el ámbito inmobiliario:\n"
                    "Teléfono de contacto:\n\n"
                    "Así lo tenemos todo listo para cuando lo necesitemos. Gracias!"
                )
                send_whatsapp_message(phone_number, SUPPLIER_MSG)
                waiting_for_supplier_info.add(phone_number)
                return "OK", 200

            # Capturar contexto del anuncio solo en el primer mensaje
            if phone_number not in ad_context:
                referral = message.get("referral", {})
                if referral:
                    parts = []
                    if referral.get("headline"):
                        parts.append(f"Anuncio: {referral['headline']}")
                    if referral.get("body"):
                        parts.append(f"Descripción: {referral['body']}")
                    ad_context[phone_number] = {
                        "texto":      " | ".join(parts),
                        "source_id":  referral.get("source_id", ""),
                        "source_url": referral.get("source_url", ""),
                        "origen":     "anuncio"
                    }
                    print(f"[{phone_number}] Lead desde anuncio: {ad_context[phone_number]}")
                else:
                    ad_context[phone_number] = {"texto": "", "source_id": "", "source_url": "", "origen": "link_directo"}

            if pending_decision.get(phone_number):
                send_whatsapp_message(phone_number, "solo dime, como prefieres que te contacte el asesor?")
                send_whatsapp_contact_buttons(phone_number)
                return "OK", 200

            # Saludo en conversación existente
            saludos = {"hola", "hello", "hey", "buenas", "buenos días", "buenos dias",
                       "buen día", "buen dia", "buenas tardes", "buenas noches", "hi", "ey"}
            if user_message.strip().lower() in saludos and history_exists(phone_number) and len(history_get(phone_number)) > 0:
                name = client_names.get(phone_number) or client_data.get(phone_number, {}).get("nombre")
                greeting = f"hola {name}, cómo te puedo ayudar?" if name else "hola, cómo te puedo ayudar?"
                send_whatsapp_message(phone_number, greeting)
                return "OK", 200

            # Si el cliente escribe texto cuando esperamos un botón de inversión, reenviar botones
            if phone_number in waiting_for_uso_suelo:
                _send_interactive_buttons(phone_number, "selecciona el tipo de inversión:", [
                    {"id": "uso_comercial",    "title": "Uso comercial"},
                    {"id": "uso_habitacional", "title": "Renta habitacional"}
                ])
                return "OK", 200
            if phone_number in waiting_for_plazo_renta:
                _send_interactive_buttons(phone_number, "selecciona el plazo de renta:", [
                    {"id": "largo_plazo", "title": "Largo plazo"},
                    {"id": "corto_plazo", "title": "Corto plazo / Airbnb"}
                ])
                return "OK", 200
            if phone_number in waiting_for_tipo_propiedad:
                send_whatsapp_tipo_propiedad_inversion_list(phone_number)
                return "OK", 200
            if phone_number in waiting_for_conoce_merida:
                _send_interactive_buttons(phone_number, "conoces las zonas de Mérida?", [
                    {"id": "conoce_merida",        "title": "Conozco Mérida"},
                    {"id": "necesita_orientacion", "title": "Necesito orientación"}
                ])
                return "OK", 200

            if phone_number in waiting_for_ficha_correction:
                waiting_for_ficha_correction.discard(phone_number)
                user_message = f"corrección de ficha: {user_message}"

            # Detectar negaciones en momentos clave
            negaciones = {"no", "nop", "nel", "paso", "no quiero", "prefiero no",
                          "no gracias", "no por ahora", "ahorita no", "después", "luego"}
            es_negacion = user_message.strip().lower() in negaciones

            if es_negacion and phone_number in (waiting_for_apellido | waiting_for_email | waiting_for_name):
                send_whatsapp_message(phone_number,
                    "entiendo, sin presión. es importante tener tu información completa para poder pasarte con el asesor experto que mejor se adapte a lo que buscas. cuando te sientas listo aquí voy a estar.")
                return "OK", 200

            # Captura de nombre después del saludo — SIN pasar por GPT
            if phone_number in waiting_for_name:
                words = [w for w in user_message.strip().split() if w.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").isalpha()]
                # Solo tratar como nombre si el mensaje es corto (máximo 4 palabras)
                if len(user_message.strip().split()) <= 4 and len(words) >= 1:
                    waiting_for_name.discard(phone_number)
                    if len(words) == 1:
                        client_data.setdefault(phone_number, {})["nombre_completo"] = words[0].capitalize()
                        save_nombre_redis(phone_number, words[0].capitalize())
                        waiting_for_apellido.add(phone_number)
                        send_whatsapp_message(phone_number, "y tu apellido?")
                        return "OK", 200
                    else:
                        full = " ".join(words[:3]).title()  # máximo 3 palabras como nombre
                        client_data.setdefault(phone_number, {})["nombre_completo"] = full
                        save_nombre_redis(phone_number, full)
                        client_data_save(phone_number)
                        _send_paso2(phone_number, words[0].capitalize(), user_message)
                        return "OK", 200
                else:
                    # Mensaje largo — extraer entidades y dejar que GPT continúe
                    waiting_for_name.discard(phone_number)
                    extract_entities(phone_number, user_message)

            # Guardar apellido — SIN pasar por GPT
            elif phone_number in waiting_for_apellido:
                waiting_for_apellido.discard(phone_number)
                existing = client_data.get(phone_number, {}).get("nombre_completo", "") or get_nombre_redis(phone_number)
                full_name = f"{existing} {user_message.strip().title()}".strip()
                client_data.setdefault(phone_number, {})["nombre_completo"] = full_name
                save_nombre_redis(phone_number, full_name)
                client_data_save(phone_number)
                _send_paso2(phone_number, full_name.split()[0], user_message)
                return "OK", 200

            if phone_number in waiting_for_ciudad:
                if client_data.get(phone_number, {}).get("intencion") == "Para invertir":
                    waiting_for_ciudad.discard(phone_number)  # ignorar para inversión
                else:
                    waiting_for_ciudad.discard(phone_number)
                    client_data.setdefault(phone_number, {})["ciudad"] = user_message.strip()
                    client_data_save(phone_number)

            # Detectar email en cualquier mensaje
            email_match = re.search(r'[^@\s]+@[^@\s]+\.[^@\s]{2,}', user_message.strip())
            if email_match:
                client_data.setdefault(phone_number, {})["correo"] = email_match.group(0)
                client_data_save(phone_number)
                waiting_for_email.discard(phone_number)
            elif phone_number in waiting_for_email:
                send_whatsapp_message(phone_number, "ese correo no parece válido, me lo puedes compartir de nuevo? por ejemplo: nombre@gmail.com")
                return "OK", 200

        else:
            return "OK", 200

        history = history_get(phone_number)
        is_first_message = len(history) == 0
        history.append({"role": "user", "content": user_message})

        hora_actual = datetime.now().hour
        if hora_actual < 12:
            momento = "mañana"
            despedida = "buen día"
        elif hora_actual < 19:
            momento = "tarde"
            despedida = "buenas tardes"
        else:
            momento = "noche"
            despedida = "buenas noches"

        system = SYSTEM_PROMPT
        system += f"\n\nHORA ACTUAL: Son las {hora_actual}:00 hrs — es de {momento}. Cuando te despidas o cierres un mensaje usa '{despedida}', nunca 'buen día' si es de tarde o noche."

        # Extraer entidades del mensaje ANTES de decidir qué instrucción dar
        if msg_type == "text":
            extract_entities(phone_number, user_message)

        datos_frescos = client_data.get(phone_number, {})

        if is_first_message:
            if datos_frescos.get("intencion") or datos_frescos.get("tipo") or datos_frescos.get("ciudad"):
                # El primer mensaje ya tiene info útil — saluda, confirma lo que entendiste y pide nombre/apellido
                system += "\n\nINSTRUCCIÓN: Es el primer mensaje y el cliente ya compartió información. Saluda como María de TRES65, confirma con calidez lo que entendiste de su mensaje (intención, tipo de propiedad, ciudad si aplica) y pide su nombre completo. No hagas más preguntas."
            else:
                system += "\n\nINSTRUCCIÓN INMEDIATA: Primer mensaje sin datos. Saluda con calidez, preséntate como María de TRES65 y pide el nombre completo. NADA MÁS."
        elif not datos_frescos.get("nombre_completo") and len(history) <= 4:
            system += "\n\nINSTRUCCIÓN: El cliente se presentó. Confirma con calidez lo que entendiste y pide solo el apellido. No hagas más preguntas."

        if phone_number in waiting_for_apellido:
            system += "\n\nINSTRUCCIÓN: El cliente dio solo su primer nombre. Tu ÚNICA respuesta es pedir el apellido de forma natural: 'y tu apellido?' — nada más."

        if phone_number in algo_mas_mode:
            system += """

MODO EXPLORATORIO — este cliente tiene una necesidad especial o no tiene claro lo que busca.
NO uses el paso a paso estándar. NO mandes botones de vivir/invertir ni comprar/rentar.
Sé comprensiva, cálida y paciente. Tu objetivo es entender su situación y guiarlos con calidez.

SI NO SABEN SI RENTAR O COMPRAR:
- Sé comprensiva: es completamente normal no tener claro. Primero pregunta: "ya vives en Mérida o vienes de fuera?"

SI VIVEN EN MÉRIDA:
- Valida con calidez: comprar es un gran paso pero es una inversión a futuro muy valiosa.
- Di algo como: "comprar puede sonar a mucho pero es una de las mejores inversiones que puedes hacer. un asesor de TRES65 te puede orientar aunque aún no tengas todo claro, para eso estamos."
- Recoge: nombre completo, correo, en cuánto tiempo quisiera tomar una decisión o mudarse, presupuesto aproximado.

SI VIENEN DE FUERA:
- Valida con calidez: es muy común querer rentar primero para conocer la ciudad antes de comprar.
- Di algo como: "muchos que llegan de fuera prefieren rentar primero para conocer bien las zonas, algunos incluso arrancan con una estadía corta tipo Airbnb. Mérida aunque está creciendo mucho sigue siendo muy fácil de descifrar, y con un asesor de TRES65 la decisión es mucho más guiada y eficiente."
- Si mencionan subarrendar, Airbnb o renta a corto plazo: "todo se puede en este mundo inmobiliario mientras se establezcan bien las cosas en el contrato. es para Airbnb u otra plataforma de renta?"
- Recoge: nombre completo, correo, de dónde vienen, tiempo de estancia o mudanza, presupuesto aproximado.

DATOS OBLIGATORIOS en modo exploratorio:
1. Nombre completo (nombre + apellido) — primero siempre
2. Contexto de situación (vive en Mérida o viene de fuera)
3. Correo
4. Tiempo estimado de mudanza o decisión
5. Presupuesto aproximado
Cuando tengas todo, genera la ficha y agrega: CONFIRMAR_FICHA"""
        ctx = ad_context.get(phone_number, {})
        if isinstance(ctx, dict) and ctx.get("texto"):
            system += f"\n\nCONTEXTO DEL ANUNCIO POR EL QUE LLEGÓ ESTE LEAD:\n{ctx['texto']}\nUsa este contexto para personalizar tu primer mensaje — menciona algo relacionado al anuncio de forma natural, sin copiar el texto exacto."
        elif isinstance(ctx, str) and ctx:
            system += f"\n\nCONTEXTO DEL ANUNCIO POR EL QUE LLEGÓ ESTE LEAD:\n{ctx}\nUsa este contexto para personalizar tu primer mensaje — menciona algo relacionado al anuncio de forma natural, sin copiar el texto exacto."

        system += f"\n\nTELÉFONO DEL CLIENTE: +{phone_number} — usa este número exacto en el campo Teléfono de la ficha."

        datos = client_data.get(phone_number, {})
        if datos:
            conocido = []
            # Inyectar primer nombre explícitamente para que GPT no use el apellido
            if "nombre_completo" in datos:
                primer_nombre = datos["nombre_completo"].split()[0]
                conocido.append(f"- Nombre completo: {datos['nombre_completo']}")
                conocido.append(f"- Primer nombre (usa SOLO este para saludar): {primer_nombre}")
            if "intencion" in datos:
                conocido.append(f"- Ya dijo que es {datos['intencion']} (NO vuelvas a preguntar esto)")
            if "tipo" in datos:
                conocido.append(f"- Ya dijo que quiere {datos['tipo']} (NO vuelvas a preguntar esto)")
            if "presupuesto" in datos:
                conocido.append(f"- Presupuesto: {datos['presupuesto']} (NO vuelvas a preguntar esto)")
            if "ciudad" in datos:
                conocido.append(f"- Viene de / vive en: {datos['ciudad']} (NO vuelvas a preguntar esto)")
            if "uso_suelo" in datos:
                conocido.append(f"- Tipo de inversión: {datos['uso_suelo']} (NO vuelvas a preguntar esto)")
            if "plazo_renta" in datos:
                conocido.append(f"- Plazo de renta: {datos['plazo_renta']} (NO vuelvas a preguntar esto)")
            if "tipo_propiedad" in datos:
                conocido.append(f"- Tipo de propiedad: {datos['tipo_propiedad']} (NO vuelvas a preguntar esto)")
            if "conoce_merida" in datos:
                conocido.append(f"- Conoce Mérida: {datos['conoce_merida']} (NO vuelvas a preguntar esto)")
            system += "\n\nLO QUE YA SABES DE ESTE CLIENTE:\n" + "\n".join(conocido)

        # Flujo de inversión — nunca preguntar ciudad ni vivir/invertir
        if datos.get("intencion") == "Para invertir":
            system += "\n\nREGLA INVERSIÓN ABSOLUTA: Este cliente es de INVERSIÓN. NUNCA preguntes 'ya vives en Mérida', 'de dónde te mudas', ni nada sobre dónde vive. No es relevante. NUNCA preguntes vivir/invertir ni comprar/rentar — ya lo sabes."
            if "presupuesto" in datos:
                if "correo" not in datos:
                    system += f"\n\nFLUJO INVERSIÓN: Tienes uso_suelo={datos.get('uso_suelo','')}, tipo_propiedad={datos.get('tipo_propiedad','')}, conoce_merida={datos.get('conoce_merida','')}, presupuesto={datos.get('presupuesto')}. Haz 1-2 preguntas de contexto para notas (zona, expectativa de retorno, plazo), luego pide el correo."
                else:
                    system += "\n\nFLUJO INVERSIÓN: Ya tienes todos los datos incluyendo correo. Genera la ficha y agrega CONFIRMAR_FICHA."

        if phone_number in ficha_confirmada:
            system += "\n\nLA FICHA DE ESTE CLIENTE YA FUE CONFIRMADA. Si en la conversación surge información nueva relevante (zona, recámaras, preferencias, preocupaciones, fechas, etc.), agrégala a las Notas, regenera la ficha completa actualizada con el mismo formato del PASO 7 y agrega CONFIRMAR_FICHA al final para que el cliente la vuelva a confirmar. Si el cliente solo platica sin dar info nueva, responde normal sin reenviar la ficha."

        if phone_number in waiting_for_ficha_correction:
            system += "\n\nEl cliente acaba de corregir un dato de su ficha. Actualiza el dato, regenera la ficha completa con el formato del PASO 7 y agrega CONFIRMAR_FICHA al final."

        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system}] + history
        )

        reply = response.choices[0].message.content

        # Limpiar tokens antes de guardar en historial para que GPT no se confunda
        all_tokens = ["MANDAR_BOTONES_CONTACTO", "MANDAR_BOTONES_COMPRAR_RENTAR",
                      "MANDAR_BOTONES_VIVIR_INVERTIR", "CONFIRMAR_FICHA",
                      "PREGUNTAR_TEMA_ASESOR"]
        reply_clean = reply
        for t in all_tokens:
            reply_clean = reply_clean.replace(t, "").strip()

        history.append({"role": "assistant", "content": reply_clean})
        history_set(phone_number, history[-20:])

        def dispatch_reply(reply_text):
            datos_actuales = client_data.get(phone_number, {})
            tokens = {
                "MANDAR_BOTONES_CONTACTO":      send_whatsapp_contact_buttons,
                "MANDAR_BOTONES_COMPRAR_RENTAR": send_whatsapp_comprar_rentar_buttons,
                "MANDAR_BOTONES_VIVIR_INVERTIR": send_whatsapp_vivir_invertir_buttons,
            }
            for token, fn in tokens.items():
                if token in reply_text:
                    text_part = reply_text.replace(token, "").strip()
                    # Saltar botones si ya tenemos esa info
                    if token == "MANDAR_BOTONES_VIVIR_INVERTIR" and "intencion" in datos_actuales:
                        if text_part:
                            send_whatsapp_message(phone_number, text_part)
                        return
                    if token == "MANDAR_BOTONES_COMPRAR_RENTAR" and "tipo" in datos_actuales:
                        if text_part:
                            send_whatsapp_message(phone_number, text_part)
                        return
                    if text_part:
                        send_whatsapp_message(phone_number, text_part)
                    fn(phone_number)
                    return
            if "CONFIRMAR_FICHA" in reply_text:
                ficha_text = reply_text.replace("CONFIRMAR_FICHA", "").strip()
                last_ficha_text[phone_number] = ficha_text
                if _redis:
                    _redis.setex(f"ficha:{phone_number}", HISTORY_TTL, ficha_text)
                send_whatsapp_ficha_confirmation(phone_number, ficha_text)
                return
            if "PREGUNTAR_TEMA_ASESOR" in reply_text:
                text_part = reply_text.replace("PREGUNTAR_TEMA_ASESOR", "").strip()
                if text_part:
                    send_whatsapp_message(phone_number, text_part)
                waiting_for_asesor_topic.add(phone_number)
                return
            send_whatsapp_message(phone_number, reply_text)
            low = reply_text.lower()
            if "de dónde te mudas" in low or "ya vives en mérida" in low:
                waiting_for_ciudad.add(phone_number)
            if any(p in low for p in ["me compartes tu correo", "me das tu correo", "tu correo", "correo electrónico", "correo para", "comparte tu correo", "correo?"]):
                waiting_for_email.add(phone_number)

        dispatch_reply(reply)

        if is_first_message:
            waiting_for_name.add(phone_number)

        last_maria_message_time[phone_number] = datetime.now()
        schedule_followup(phone_number)

        print(f"[{phone_number}] Usuario: {user_message}")
        print(f"[{phone_number}] María: {reply}")

    except Exception as e:
        print(f"Error: {e}")

    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
