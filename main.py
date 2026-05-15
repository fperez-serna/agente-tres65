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
    if es_horario_silencioso():
        print("Follow-up 23h: horario silencioso, se omite este ciclo")
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
pending_decision = {}        # clientes que vieron los botones pero no han decidido (con TTL en Redis)
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
last_ficha_text = {}                  # última ficha generada por número
client_data = {}        # datos ya capturados por cliente {intencion, tipo, presupuesto, ciudad}

scheduler = BackgroundScheduler()
scheduler.add_job(check_and_send_24h_followups, "interval", hours=1, id="followup_23h")
scheduler.start()

CALENDLY_URL = "https://calendly.com/contacto-tres65inmobiliaria/30min"

# Propiedades configuradas por anuncio
PROPERTIES = {
    "santa ana": {
        "saludo": "Hola! Santa Ana es de las colonias más cotizadas del centro de Mérida. Con quién tengo el gusto? (nombre completo por favor)",
        "contexto": "La casa en Santa Ana es una joya del centro de Mérida.",
        "url": "https://www.tres65inmobiliaria.com/property/casa-en-venta-en-merida-centro-8e06cd60-5cd3-4688-a498-b41d3bdad845",
        "datos": {
            "tipo": "comprar",
            "intencion": "Para vivir",
            "presupuesto": "5 a 6 millones",
            "notas": "Interesado en casa Santa Ana — venta amueblada $5.5M"
        }
    },
}

SYSTEM_PROMPT = """
Eres María, asesora de TRES65 Inmobiliaria en Mérida, Yucatán.

QUIÉN ERES:
Llevas años viviendo en Mérida y ayudando a familias y compradores a encontrar su lugar aquí.
No eres un bot. Eres una persona que conoce la ciudad, entiende lo que implica mudarse,
y sabe que comprar o rentar una propiedad es una decisión que pesa.

ASÍ HABLAS — ejemplos reales de tu tono:
- "ya te encuentras aquí o vienes de fuera?"
- "cuánto tiempo lleva buscando más o menos?"
- "qué es lo que más te importa de la zona?"
- "eso tiene sentido, Mérida tiene esa ventaja"
- "de acuerdo, con eso ya te puedo conectar con la persona indicada"
- "no hay prisa, cuéntame un poco más"
Nota el tono: directo, sin relleno, como habla alguien que sabe de lo que habla.

CÓMO ESCRIBES:
- Sin signos de apertura: nunca ¿ ni ¡
- Mayúsculas solo al inicio y después de punto.
- Sin emojis.
- Respuestas cortas — máximo 2-3 líneas. A veces una línea es suficiente.
- Sin lenguaje corporativo ni frases de call center.
- Evita empezar con "Entendido", "Perfecto", "Claro", "Por supuesto", "Excelente".
- Varía estructura y longitud — no todo debe sonar igual de elaborado.
- Evita usar el nombre del cliente repetidamente.
- Cuando el cliente comparte algo personal o emocional, reconócelo en una oración
  antes de continuar. Nunca ignores lo que dijeron para ir directo a la siguiente pregunta.

PRIMER MENSAJE:
Saluda según horario, preséntate, pide nombre completo. Sin emojis.
- Antes de 12pm: "Buenos días, que gusto saludarte"
- 12pm-7pm: "Buenas tardes, que gusto saludarte"
- Después de 7pm: "Buenas noches, que gusto saludarte"
Ejemplo: "Buenas tardes, que gusto saludarte. Soy María de TRES65 Inmobiliaria,
con quién tengo el gusto? (nombre completo por favor)"

CÓMO PIENSAS — ENTIDADES, NO PASOS:
Tu objetivo es completar la ficha para conectar al cliente con el asesor correcto.
Antes de cada respuesta:
1. Extrae automáticamente cualquier dato del mensaje del cliente.
2. Revisa qué entidades ya tienes en "LO QUE YA SABES".
3. Elige SOLO el siguiente dato faltante más útil.
4. NUNCA pidas ni mandes botones para un dato que ya tienes.

ENTIDADES DE LA FICHA (en orden de prioridad):
- nombre_completo → pídelo siempre primero. Sin nombre, no avances.
- intencion (vivir/invertir) → si no lo tienes, agrega MANDAR_BOTONES_VIVIR_INVERTIR
- tipo (compra/renta) → solo si busca vivir y no lo tienes, agrega MANDAR_BOTONES_COMPRAR_RENTAR
- presupuesto → el sistema manda botones automáticamente. No preguntes en texto.
- ciudad → solo si busca vivir: "ya te encuentras en Mérida o de dónde te mudas?"
- notas → 1-2 preguntas sobre la propiedad ideal: cuántos cuartos necesitan, si buscan alberca, jardín, escuelas cerca, amenidades, o algo en especial. No preguntes sobre zonas — eso lo define el asesor.
- correo → "para conectarte con el asesor que mejor se adapta a lo que buscas, me compartes tu correo?"
- ficha → cuando tienes todo, redáctala y agrega CONFIRMAR_FICHA
- contacto → ÚNICAMENTE después de que el cliente confirme la ficha, agrega MANDAR_BOTONES_CONTACTO

REGLA CRÍTICA: Si el cliente ya mencionó un dato — en cualquier parte del historial,
en client_data, o en su primer mensaje — NO lo vuelvas a pedir.
Los botones son shortcuts, no obligatorios.

REGLA DE CIERRE: NUNCA cierres sin haber mandado CONFIRMAR_FICHA y
MANDAR_BOTONES_CONTACTO. NUNCA escribas opciones de contacto como lista de texto.

Si el cliente da varios datos de golpe — confirma con calidez lo que entendiste
y pide solo lo que falta.

Si el cliente regresa después de tiempo — retoma desde el último dato faltante
sin reiniciar.

MODO ESCUCHA — cuándo pausar el flujo:
Si el cliente expresa frustración, dudas fuertes, o algo emocional
("llevamos años buscando", "no sé si es buen momento", "ya no sé qué hacer")
— primero valida en una oración, luego continúa.
Nunca saltes directo a la siguiente pregunta después de algo así.
Ejemplos:
- "dos años buscando es bastante, algo bueno va a salir de toda esa búsqueda"
- "es normal tener esa duda, muchos llegan con lo mismo y al final encuentran algo"
- "entiendo, no es una decisión fácil"

MODO EXPLORATORIO — cliente que elige "Algo más":
Si el cliente no tiene claro qué busca o tiene una necesidad especial (renta a corto plazo,
subarrendamiento, estadía temporal), sé curiosa y abierta. No uses el flujo estándar.
Pregunta una cosa a la vez: es de trabajo o vacacional, viene solo o acompañado, cuánto tiempo.
Recoge igual nombre, correo y presupuesto antes de conectar con el asesor.

PERFIL DEL CLIENTE:
Detecta si está explorando, soñando, comparando o listo para comprar.
Si detectas presupuesto alto o inversionista fuerte, tono más ejecutivo sin perder calidez.

CUANDO ALGUIEN MENCIONA DE DÓNDE VIENE:
Responde con algo específico y real, no un eslogan.
- CDMX: "mucha gente de allá se está moviendo, el ritmo aquí es completamente diferente"
- Monterrey: "los que llegan de allá generalmente se sorprenden con la tranquilidad"
- Guadalajara: "varios tapatíos han encontrado aquí ese balance de ciudad sin el caos"
- USA/exterior: "cada vez más gente de fuera está eligiendo Mérida, tiene mucho sentido"
Adapta según la ciudad. Que suene a observación real, no a pitch.

CUANDO EL CLIENTE MENCIONA UNA LONA O ANUNCIO:
Si el cliente dice que vio una lona, letrero, cartel, anuncio de facebook, publicación, post,
o algo en redes sociales de una propiedad específica:
- Pregunta qué recuerda de ella: colonia, calle, zona, nombre de la privada, o alguna característica
  (color de la fachada, número de recámaras, precio aproximado).
- Para lonas: "recuerdas en qué colonia o calle viste la lona? o alguna característica de la propiedad?"
- Para anuncios digitales: "recuerdas el nombre del anuncio, la zona que mencionaba, o algún detalle de la propiedad?"
- Con esa info el asesor puede ubicar exactamente cuál es.
- Guarda lo que recuerde como nota en la ficha.

CONTEXTO DE MÉRIDA:
- El norte es lo más buscado: Temozón Norte, Cholul, Santa Gertrudis Copó, Montebello, Conkal
- Conkal es más tranquilo y económico
- El tráfico al centro importa si trabajan ahí
- Las privadas con amenidades son muy valoradas por familias
- Mucha gente renta primero antes de comprar
- El calor cambia según ventilación, árboles y orientación
- Mérida es segura y familiar comparada con otras ciudades de México

FICHA — formato exacto al tener todo:
Nombre: [nombre completo]
Teléfono: [número del cliente]
Correo: [correo]
Tipo: [Compra / Renta]
Uso: [Para vivir / Para invertir]
Presupuesto: [rango]
Zona: [zona o "Por definir"]
Viene de: [ciudad]
Origen: [lo tienes en el contexto del sistema como ORIGEN DEL LEAD]
Notas: [contexto en 1 línea, o "Sin notas"]
CONFIRMAR_FICHA

CUANDO EL CLIENTE PIDE HABLAR CON UN ASESOR:
Solo si ya tienes el nombre. Responde:
"Hay mucho en lo que te puedo ayudar, y puedo conectarte cuando quieras.
Cual es el tema que te gustaria tratar con el asesor?"
Luego agrega: PREGUNTAR_TEMA_ASESOR

CUANDO ALGUIEN OFRECE UN SERVICIO, ES PROVEEDOR O BUSCA TRABAJO:
Manda EXACTAMENTE este mensaje:
"Gracias por contactarnos. Aunque este no es el canal indicado, nos da mucho gusto
recibir propuestas. Para guardarte en nuestra carpeta de proveedores/reclutamiento,
compártenos en un solo mensaje la siguiente información en este orden:

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

LÍMITES:
- No inventes propiedades, precios ni disponibilidad
- No digas que vas a "mandar opciones" o "enviar propiedades"
- No inventes datos geográficos, estadísticas ni distancias
- Si el cliente habla de política o religión, redirige con calidez y continúa el flujo
- Si insulta: una advertencia amable. Si reincide: "voy a finalizar esta conversación.
  cuando gustes retomamos con gusto."
"""


def send_whatsapp_message(to, message):
    if message:
        message = message[0].upper() + message[1:]
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "text": {"body": message, "preview_url": True}
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp text: {response.status_code} - {response.text}")

def send_whatsapp_image(to, image_url, caption=""):
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "image",
        "image": {"link": image_url, "caption": caption}
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp image: {response.status_code} - {response.text[:100]}")


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
        if _redis:
            _redis.setex(f"pending_decision:{to}", 2 * 3600, "1")
        chatwoot_sync_bot(to, "¿Cómo prefieres que te contacte un asesor? [Agendar llamada / Por WhatsApp]")


def _send_cta_url(to, body_text, display_text, url_dest):
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
            "body": {"text": body_text},
            "action": {
                "name": "cta_url",
                "parameters": {"display_text": display_text, "url": url_dest}
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    print(f"WhatsApp CTA: {response.status_code} - {response.text[:100]}")

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
    chatwoot_sync_bot(to, "¿Comprar o Rentar? [Comprar / Rentar]")


def send_whatsapp_vivir_invertir_buttons(to):
    _send_interactive_buttons(to, "La propiedad que buscas es para...", [
        {"id": "para_vivir",    "title": "Para vivir"},
        {"id": "para_invertir", "title": "Para invertir"},
        {"id": "algo_mas",      "title": "Algo más"}
    ])
    chatwoot_sync_bot(to, "La propiedad que buscas es para... [Para vivir / Para invertir / Algo más]")


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
        body = "¿Ya tienes un rango de renta en mente?"
        rows = [
            {"id": "presup_menos15", "title": "Menos de 15 mil"},
            {"id": "presup_15_25",   "title": "15 a 25 mil"},
            {"id": "presup_25_35",   "title": "25 a 35 mil"},
            {"id": "presup_35_45",   "title": "35 a 45 mil"},
            {"id": "presup_50mas",   "title": "50 mil o más"},
            {"id": "presup_asesor",  "title": "Lo platico con asesor"},
        ]
    else:
        body = "¿Ya tienes un rango de inversión en mente?"
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


STOPWORDS = {"y", "e", "o", "a", "en", "de", "del", "la", "el", "los", "las", "que",
             "me", "mi", "mis", "se", "su", "sus", "un", "una", "por", "para", "con",
             "no", "sé", "se", "al", "porque", "pero", "también", "tambien", "muy"}

def detect_property(text):
    """Detecta si el mensaje menciona una propiedad configurada. Retorna la clave o None."""
    low = text.lower()
    for key in PROPERTIES:
        if key in low:
            return key
    return None


def format_lead_ad_for_chatwoot(text):
    """Convierte el mensaje crudo de Lead Ad a formato legible en español."""
    if "filled out your form" not in text:
        return text
    label_map = {
        "full_name": "Nombre",
        "email": "Correo",
        "phone_number": "Teléfono",
        "city": "Ciudad",
        "¿estás_interesado_en_adquirir_una_propiedad_en_mérida,_yucatán?": "Interesado en Mérida",
        "¿cuál_es_el_presupuesto_que_tenías_contemplado_para_esta_inversión?": "Presupuesto",
        "¿cómo_te_gustaría_realizar_tu_inversión?": "Forma de inversión",
    }
    lines = ["📋 *Lead desde formulario de Meta*\n"]
    for line in text.strip().splitlines():
        if ":" not in line or "Hello!" in line:
            continue
        raw_key, _, val = line.partition(":")
        key = raw_key.strip()
        label = label_map.get(key.lower(), key.replace("_", " ").strip("¿?").strip().capitalize())
        lines.append(f"• *{label}*: {val.strip()}")
    return "\n".join(lines)


def parse_lead_ad_message(phone_number, text):
    """Detecta y procesa el mensaje automático de Meta Lead Ads.
    Retorna True si era un mensaje de formulario y ya pre-pobló client_data."""
    if "Hello! I filled out your form" not in text and "filled out your form" not in text:
        return False

    datos = client_data.setdefault(phone_number, {})
    lines = text.strip().splitlines()

    presupuesto_map = {
        "menos de $5,300,000": "Menos de 3 millones",
        "menos de": "Menos de 3 millones",
        "$5,300,000": "Menos de 3 millones",
    }

    for line in lines:
        if ":" not in line:
            continue
        raw_key, _, raw_val = line.partition(":")
        key = raw_key.strip().lower().replace("¿", "").replace("?", "").replace("_", " ").strip()
        val = raw_val.strip()
        if not val:
            continue

        if "full_name" in raw_key.lower() or "nombre" in key:
            datos["nombre_completo"] = val.strip().title()
            save_nombre_redis(phone_number, datos["nombre_completo"])
        elif "email" in raw_key.lower() or "correo" in key:
            if "@" in val:
                datos["correo"] = val.strip().lower()
        elif "city" in raw_key.lower() or "ciudad" in key:
            datos["ciudad"] = val.strip()
        elif "presupuesto" in key or "inversión" in key and "$" in val:
            val_low = val.lower()
            for k, mapped in presupuesto_map.items():
                if k in val_low:
                    datos["presupuesto"] = mapped
                    break
            else:
                datos["presupuesto"] = val  # guardar tal cual si no matchea
        elif "interesado" in key or "adquirir" in key:
            if "sí" in val.lower() or "si" in val.lower():
                datos["intencion"] = "Para vivir"  # default; bot puede refinar

    client_data_save(phone_number)
    _reconcile_states(phone_number, datos)
    print(f"[{phone_number}] Lead Ad parseado: {datos}")
    return True


def extract_entities(phone_number, text):
    """Extrae intención, tipo, ciudad y zona, y los guarda en client_data."""
    low = text.lower()
    datos = client_data.setdefault(phone_number, {})

    if "intencion" not in datos:
        vivir_patterns = ["para vivir", "para mi familia", "para mudarnos", "para residir",
                          "mudarme", "mudarse", "me mudo", "nos mudamos", "vivir en mérida",
                          "vivir allá", "vivir alla", "para establecerme", "para quedarme",
                          "conocer la ciudad", "conocer mérida", "vivir en la ciudad"]
        invertir_patterns = ["para invertir", "como inversión", "como inversion",
                             "airbnb", "negocio", "rentar a otros", "generar renta"]
        if any(w in low for w in vivir_patterns):
            datos["intencion"] = "Para vivir"
        elif any(w in low for w in invertir_patterns):
            datos["intencion"] = "Para invertir"

    if "tipo" not in datos:
        tiene_compra = any(w in low for w in ["comprar", "compra", "adquirir"])
        tiene_renta = any(w in low for w in ["rentar", "renta", "arrendar"])
        if tiene_compra and tiene_renta:
            # Ambos mencionados — dejar que GPT aclare
            pass
        elif tiene_compra:
            datos["tipo"] = "Comprar"
        elif tiene_renta:
            datos["tipo"] = "Rentar"

    if "ciudad" not in datos:
        for marker in ["desde ", "vengo de ", "me mudo de ", "mudándome de ", "mudandome de ",
                       "me vengo de ", "llego de ", "vivo en ", "actualmente en "]:
            if marker in low:
                idx = low.index(marker) + len(marker)
                words = text[idx:idx+40].split()
                ciudad_words = []
                for w in words:
                    if w.lower().strip(".,!?") in STOPWORDS:
                        break
                    ciudad_words.append(w.strip(".,!?"))
                    if len(ciudad_words) == 2:
                        break
                ciudad = " ".join(ciudad_words)
                if ciudad:
                    datos["ciudad"] = ciudad
                    waiting_for_ciudad.discard(phone_number)
                break

    # Detectar si ya dijo que no conoce las zonas
    if "zona" not in datos:
        if any(p in low for p in ["no sé de las zonas", "no se las zonas", "no conozco las zonas",
                                   "no sé las zonas", "no sé qué zona", "no tengo zona",
                                   "sin zona", "no sé de zonas"]):
            datos["zona"] = "No conoce las zonas, necesita orientación del asesor"
            waiting_for_ciudad.discard(phone_number)

    if datos:
        client_data_save(phone_number)
        _reconcile_states(phone_number, datos)


def _reconcile_states(phone_number, datos):
    """Limpia waiting_for_* cuando ya tenemos el dato por conversación natural."""
    if "ciudad" in datos:
        waiting_for_ciudad.discard(phone_number)
    if "correo" in datos:
        waiting_for_email.discard(phone_number)


def next_missing_field(phone_number):
    """Returns the next entity name missing from client_data, or None if ficha is complete."""
    datos = client_data_load(phone_number)
    if not datos.get("nombre_completo"):
        return "nombre"
    if not datos.get("intencion"):
        return "intencion"
    if datos.get("intencion") == "Para vivir":
        if not datos.get("tipo"):
            return "tipo"
    elif datos.get("intencion") == "Para invertir":
        if not datos.get("uso_suelo"):
            return "uso_suelo"
        if datos.get("uso_suelo") == "Habitacional":
            if not datos.get("plazo_renta"):
                return "plazo_renta"
            if not datos.get("tipo_propiedad"):
                return "tipo_propiedad"
        if not datos.get("conoce_merida"):
            return "conoce_merida"
    if not datos.get("presupuesto"):
        return "presupuesto"
    if not datos.get("ciudad") and datos.get("intencion") == "Para vivir":
        return "ciudad"
    if not datos.get("correo"):
        return "correo"
    return None  # ficha completa


def advance_flow(phone_number):
    """Send the appropriate button for the next missing field.
    Returns True if a button was sent, False if GPT should handle it."""
    field = next_missing_field(phone_number)
    datos = client_data.get(phone_number, {})

    if field == "intencion":
        send_whatsapp_vivir_invertir_buttons(phone_number)
        return True
    elif field == "tipo":
        send_whatsapp_comprar_rentar_buttons(phone_number)
        return True
    elif field == "uso_suelo":
        send_whatsapp_uso_suelo_buttons(phone_number)
        return True
    elif field == "plazo_renta":
        send_whatsapp_plazo_renta_buttons(phone_number)
        return True
    elif field == "tipo_propiedad":
        send_whatsapp_tipo_propiedad_inversion_list(phone_number)
        return True
    elif field == "conoce_merida":
        send_whatsapp_conoce_merida_buttons(phone_number)
        return True
    elif field == "presupuesto":
        tipo = "rentar" if datos.get("tipo", "").lower() == "rentar" else "comprar"
        send_whatsapp_budget_list(phone_number, tipo)
        return True
    # ciudad, correo, None → let GPT handle
    return False


def _send_paso2(phone_number, primer_nombre, user_message_for_history):
    texto = f"Mucho gusto {primer_nombre}, y ahora sí que emocionante estar en esta búsqueda inmobiliaria contigo. Voy a hacerte unas preguntas para crear tu ficha, nos va a tomar un minuto. Es rápido."
    send_whatsapp_message(phone_number, texto)

    datos = client_data.get(phone_number, {})
    print(f"[{phone_number}] _send_paso2 datos: intencion={datos.get('intencion')} tipo={datos.get('tipo')} presupuesto={datos.get('presupuesto')}")

    boton_enviado = advance_flow(phone_number)

    if not boton_enviado:
        # Ya tenemos entidades básicas — pedir lo siguiente via GPT
        # Agregar solo el mensaje al historial y dejar que el siguiente mensaje del cliente active GPT
        print(f"[{phone_number}] _send_paso2: entidades completas, GPT seguirá en próximo mensaje")

    history = history_get(phone_number)
    history.append({"role": "user", "content": user_message_for_history})
    history.append({"role": "assistant", "content": texto})
    history_set(phone_number, history[-20:])
    update_last_activity(phone_number)
    schedule_followup(phone_number)


def _chatwoot_headers():
    return {
        "api_access_token": os.environ.get("CHATWOOT_TOKEN", ""),
        "Content-Type": "application/json"
    }

def chatwoot_base():
    url  = os.environ.get("CHATWOOT_URL", "")
    acct = os.environ.get("CHATWOOT_ACCOUNT_ID", "")
    return f"{url}/api/v1/accounts/{acct}"

def chatwoot_get_or_create_contact(phone_number, datos):
    base = chatwoot_base()
    nombre = datos.get("nombre_completo", "")
    correo = datos.get("correo", "")
    # Buscar contacto existente por teléfono
    r = requests.get(f"{base}/contacts/search",
                     params={"q": phone_number, "page": 1},
                     headers=_chatwoot_headers(), timeout=5)
    if r.ok:
        payload = r.json().get("payload", [])
        contacts = payload if isinstance(payload, list) else payload.get("contacts", [])
        if contacts:
            return contacts[0]["id"]
    # Crear nuevo contacto
    payload = {
        "name":         nombre or phone_number,
        "phone_number": f"+{phone_number}",
        "email":        correo or None,
    }
    r = requests.post(f"{base}/contacts", json=payload,
                      headers=_chatwoot_headers(), timeout=5)
    print(f"[Chatwoot] crear contacto status={r.status_code} body={r.text[:200]}")
    return r.json().get("id") if r.ok else None

def chatwoot_get_or_create_conversation(phone_number, contact_id):
    base     = chatwoot_base()
    inbox_id = os.environ.get("CHATWOOT_INBOX_ID", "")
    redis_key = f"cw_conv:{phone_number}"
    # Revisar si ya existe en Redis y validar que sigue existiendo en Chatwoot
    if _redis:
        conv_id = _redis.get(redis_key)
        if conv_id:
            check = requests.get(f"{base}/conversations/{conv_id}",
                                 headers=_chatwoot_headers(), timeout=5)
            if check.ok:
                return int(conv_id)
            # Conv no existe en Chatwoot — limpiar caché
            _redis.delete(redis_key)
            print(f"[Chatwoot] conv {conv_id} ya no existe, creando nueva")
    # Crear nueva conversación
    payload = {
        "contact_id":       contact_id,
        "inbox_id":         int(inbox_id),
        "additional_attributes": {"phone": f"+{phone_number}"}
    }
    r = requests.post(f"{base}/conversations", json=payload,
                      headers=_chatwoot_headers(), timeout=5)
    if r.ok:
        conv_id = r.json().get("id")
        if _redis and conv_id:
            _redis.setex(redis_key, HISTORY_TTL, str(conv_id))
        return conv_id
    return None

def chatwoot_sync_bot(phone_number, text):
    """Atajo para sincronizar mensajes del bot como nota privada."""
    chatwoot_sync_message(phone_number, f"🤖 {text}", "outgoing", private=True)

def chatwoot_send_message(conv_id, text, message_type="outgoing", private=False):
    base = chatwoot_base()
    requests.post(f"{base}/conversations/{conv_id}/messages",
                  json={"content": text, "message_type": message_type, "private": private},
                  headers=_chatwoot_headers(), timeout=5)

def chatwoot_add_label(conv_id, label):
    """Agrega una etiqueta preservando las existentes."""
    base = chatwoot_base()
    # Obtener etiquetas actuales
    r = requests.get(f"{base}/conversations/{conv_id}/labels",
                     headers=_chatwoot_headers(), timeout=5)
    existing = r.json().get("payload", []) if r.ok else []
    if label not in existing:
        existing.append(label)
    requests.post(f"{base}/conversations/{conv_id}/labels",
                  json={"labels": existing},
                  headers=_chatwoot_headers(), timeout=5)


def chatwoot_add_labels(conv_id, labels):
    """Agrega múltiples etiquetas a la vez preservando las existentes."""
    base = chatwoot_base()
    r = requests.get(f"{base}/conversations/{conv_id}/labels",
                     headers=_chatwoot_headers(), timeout=5)
    existing = set(r.json().get("payload", [])) if r.ok else set()
    merged = list(existing | set(labels))
    requests.post(f"{base}/conversations/{conv_id}/labels",
                  json={"labels": merged},
                  headers=_chatwoot_headers(), timeout=5)

def chatwoot_sync_message(phone_number, text, message_type="incoming", private=False):
    """Sincroniza un mensaje a Chatwoot para monitoreo."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        print("[Chatwoot] Sin token — sync omitido")
        return
    try:
        datos   = client_data_load(phone_number)
        c_id    = chatwoot_get_or_create_contact(phone_number, datos)
        print(f"[Chatwoot] contact_id={c_id}")
        if not c_id:
            return
        conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
        print(f"[Chatwoot] conv_id={conv_id}")
        if not conv_id:
            return
        base = chatwoot_base()
        r = requests.post(f"{base}/conversations/{conv_id}/messages",
                          json={"content": text, "message_type": message_type, "private": private},
                          headers=_chatwoot_headers(), timeout=5)
        # Si la conversación fue borrada en Chatwoot, limpiar caché y crear nueva
        if r.status_code in (404, 422):
            if _redis:
                _redis.delete(f"cw_conv:{phone_number}")
            conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
            print(f"[Chatwoot] conv_id recuperado={conv_id}")
            if conv_id:
                requests.post(f"{base}/conversations/{conv_id}/messages",
                              json={"content": text, "message_type": message_type, "private": private},
                              headers=_chatwoot_headers(), timeout=5)
    except Exception as e:
        print(f"Chatwoot sync error: {e}")

def chatwoot_get_or_create_team(team_name):
    """Busca un equipo por nombre o lo crea si no existe. Retorna el team_id."""
    base = chatwoot_base()
    # Buscar equipos existentes
    r = requests.get(f"{base}/teams", headers=_chatwoot_headers(), timeout=5)
    if r.ok:
        for team in r.json():
            if team.get("name", "").lower() == team_name.lower():
                return team["id"]
    # No existe — crear
    r = requests.post(f"{base}/teams",
                      json={"name": team_name},
                      headers=_chatwoot_headers(), timeout=5)
    if r.ok:
        team_id = r.json().get("id")
        print(f"Chatwoot team creado: {team_name} (id={team_id})")
        return team_id
    return None


def chatwoot_assign_team(conv_id, team_id):
    """Asigna una conversación a un equipo."""
    base = chatwoot_base()
    requests.patch(f"{base}/conversations/{conv_id}/assignments",
                   json={"team_id": team_id},
                   headers=_chatwoot_headers(), timeout=5)


def chatwoot_update_contact_name(phone_number, nombre_completo):
    """Actualiza el nombre del contacto en Chatwoot."""
    if not os.environ.get("CHATWOOT_TOKEN") or not nombre_completo:
        return
    try:
        datos = client_data_load(phone_number)
        c_id  = chatwoot_get_or_create_contact(phone_number, datos)
        if not c_id:
            return
        base = chatwoot_base()
        requests.put(f"{base}/contacts/{c_id}",
                     json={"name": nombre_completo},
                     headers=_chatwoot_headers(), timeout=5)
    except Exception as e:
        print(f"Chatwoot update name error: {e}")


def chatwoot_mark_qualified(phone_number, ficha_text):
    """Etiqueta la conversación como lista para asesor y añade la ficha."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        return
    try:
        datos   = client_data_load(phone_number)
        c_id    = chatwoot_get_or_create_contact(phone_number, datos)
        if not c_id:
            return
        conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
        if not conv_id:
            return
        # Etiquetas según datos de la ficha (todas de una sola vez)
        labels = ["listo-para-asesor"]
        # Etiqueta del anuncio si viene de Meta
        ctx_orig = ad_context.get(phone_number, {})
        if isinstance(ctx_orig, dict) and ctx_orig.get("origen") == "anuncio":
            anuncio_titulo = ctx_orig.get("texto", "").split("|")[0].replace("Anuncio:", "").strip()
            if anuncio_titulo:
                slug = anuncio_titulo.lower()[:30].replace(" ", "-")
                labels.append(f"ad-{slug}")
        chatwoot_add_labels(conv_id, labels)
        print(f"[{phone_number}] Chatwoot labels: {labels}")
        chatwoot_send_message(conv_id, f"✅ LEAD CALIFICADO\n\n{ficha_text}", "activity")
    except Exception as e:
        print(f"Chatwoot qualify error: {e}")


def reset_conversation(phone_number):
    # RAM
    history_delete(phone_number)
    client_data.pop(phone_number, None)
    client_names.pop(phone_number, None)
    ad_context.pop(phone_number, None)
    pending_decision.pop(phone_number, None)
    last_ficha_text.pop(phone_number, None)
    waiting_for_email.discard(phone_number)
    waiting_for_name.discard(phone_number)
    waiting_for_apellido.discard(phone_number)
    waiting_for_ciudad.discard(phone_number)
    waiting_for_supplier_info.discard(phone_number)
    waiting_for_asesor_topic.discard(phone_number)
    waiting_for_ficha_correction.discard(phone_number)
    ficha_confirmada.discard(phone_number)
    algo_mas_mode.discard(phone_number)
    cancel_followup(phone_number)
    # Redis
    if _redis:
        for key in [f"nombre:{phone_number}", f"cdata:{phone_number}",
                    f"ficha:{phone_number}", f"last_activity:{phone_number}",
                    f"cw_conv:{phone_number}", f"agent_active:{phone_number}",
                    f"template_sent:{phone_number}", f"followup_{phone_number}",
                    f"pending_decision:{phone_number}"]:
            _redis.delete(key)


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


def hora_merida():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=-6))).hour

def es_horario_silencioso():
    h = hora_merida()
    return h < 9 or h >= 21  # silencio entre 9pm y 9am

def send_followup(phone_number):
    if es_horario_silencioso():
        # Reprogramar para las 9am siguiente
        from datetime import timezone, timedelta
        merida_tz = timezone(timedelta(hours=-6))
        ahora = datetime.now(merida_tz)
        manana_9am = ahora.replace(hour=9, minute=0, second=0, microsecond=0)
        if ahora.hour >= 9:
            manana_9am = manana_9am + timedelta(days=1)
        delay = (manana_9am - ahora).total_seconds()
        job_id = f"followup_{phone_number}"
        scheduler.add_job(send_followup, "date",
                          run_date=datetime.now() + timedelta(seconds=delay),
                          args=[phone_number], id=job_id + "_retry",
                          replace_existing=True)
        print(f"[{phone_number}] Follow-up diferido a las 9am Mérida")
        return
    text = "Buenos días! Si sigues buscando propiedad, aquí estoy para ayudarte."
    _send_interactive_buttons(phone_number, text, [
        {"id": "ver_catalogo",   "title": "Catálogo Propiedades"},
        {"id": "no_listo",       "title": "Aún no estoy listo"},
        {"id": "hablar_asesor",  "title": "Hablar con asesor"},
    ])
    follow_up_jobs.pop(phone_number, None)
    print(f"[{phone_number}] Follow-up enviado")


def schedule_followup(phone_number):
    cancel_followup(phone_number)
    job_id = f"followup_{phone_number}"
    run_time = datetime.now() + timedelta(hours=4)
    scheduler.add_job(send_followup, "date", run_date=run_time, args=[phone_number], id=job_id)
    follow_up_jobs[phone_number] = job_id
    print(f"[{phone_number}] Follow-up programado: {run_time}")


@app.route("/chatwoot-webhook", methods=["POST"])
def chatwoot_webhook():
    data = request.json
    try:
        event = data.get("event")
        if event != "message_created":
            return "OK", 200

        msg  = data.get("message", data)

        # Ignorar notas privadas — no van al cliente
        if msg.get("private", False):
            return "OK", 200

        # Solo reenviar mensajes de agentes humanos (no del bot ni del sistema)
        sender_type = msg.get("sender", {}).get("type", "")
        if sender_type not in ("agent", "user"):
            return "OK", 200

        content = msg.get("content", "").strip()
        if not content:
            return "OK", 200

        # Obtener número de WhatsApp del contacto
        conversation = data.get("conversation", {})
        phone_raw = (
            conversation.get("meta", {}).get("sender", {}).get("phone_number", "") or
            conversation.get("additional_attributes", {}).get("phone", "")
        )
        phone = phone_raw.lstrip("+").strip() if phone_raw else None

        if phone:
            send_whatsapp_message(phone, content)
            # Pausar el bot para este número — agente humano tomó el control
            if _redis:
                _redis.setex(f"agent_active:{phone}", 2 * 3600, "1")  # bot pausa 2h
            print(f"[Chatwoot→WA] {phone}: {content[:60]}")

    except Exception as e:
        print(f"Chatwoot webhook error: {e}")

    return "OK", 200


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
        reset_template_flag(phone_number)

        # Sincronizar mensaje entrante a Chatwoot
        if msg_type == "text":
            _body = message.get("text", {}).get("body", "")
            if _body:
                _body_display = format_lead_ad_for_chatwoot(_body)
                chatwoot_sync_message(phone_number, _body_display, "incoming")
                # Etiqueta de origen + equipo automático en el primer mensaje
                if not history_exists(phone_number):
                    referral = message.get("referral", {})
                    TEXTO_LINK_DIRECTO = "hola! necesito ayuda en mi búsqueda inmobiliaria"
                    es_link_directo = _body.strip().lower() == TEXTO_LINK_DIRECTO
                    origen_label = "link-directo" if es_link_directo else "ad-meta"
                    try:
                        datos_orig = client_data_load(phone_number)
                        c_id_orig  = chatwoot_get_or_create_contact(phone_number, datos_orig)
                        if c_id_orig:
                            conv_orig = chatwoot_get_or_create_conversation(phone_number, c_id_orig)
                            if conv_orig:
                                chatwoot_add_label(conv_orig, origen_label)
                                # Si viene de anuncio, crear/buscar equipo con el nombre del anuncio
                                if referral.get("source_type") == "ad" and referral.get("headline"):
                                    team_name = referral["headline"][:50]
                                    team_id = chatwoot_get_or_create_team(team_name)
                                    if team_id:
                                        chatwoot_assign_team(conv_orig, team_id)
                    except Exception as e:
                        print(f"Chatwoot origen error: {e}")

        # Sincronizar botones e interacciones a Chatwoot
        if msg_type == "interactive":
            try:
                interactive = message.get("interactive", {})
                if interactive.get("type") == "button_reply":
                    btn_title = interactive["button_reply"].get("title", "")
                    chatwoot_sync_message(phone_number, f"[Botón] {btn_title}", "incoming")
                elif interactive.get("type") == "list_reply":
                    list_title = interactive["list_reply"].get("title", "")
                    chatwoot_sync_message(phone_number, f"[Lista] {list_title}", "incoming")
            except Exception:
                pass

        # Proveedor que intenta acceder a un asesor: reiniciar conversación como cliente
        if phone_number in waiting_for_supplier_info and msg_type == "interactive":
            reset_conversation(phone_number)
            send_whatsapp_message(phone_number, "con gusto te ayudo. con quién tengo el gusto? (nombre completo por favor)")
            return "OK", 200

        if msg_type == "interactive":
            interactive_type = message["interactive"].get("type")
            pending_decision.pop(phone_number, None)
            if _redis:
                _redis.delete(f"pending_decision:{phone_number}")

            # Respuesta de lista
            if interactive_type == "list_reply":
                list_id    = message["interactive"]["list_reply"]["id"]
                list_title = message["interactive"]["list_reply"]["title"]
                print(f"[{phone_number}] Lista: {list_id}")
                client_data.setdefault(phone_number, {})

                if list_id.startswith("prop_"):
                    client_data[phone_number]["tipo_propiedad"] = list_title
                    client_data_save(phone_number)
                    if list_id == "prop_orientacion":
                        client_data[phone_number]["conoce_merida"] = "Necesita orientación"
                        client_data_save(phone_number)
                        advance_flow(phone_number)
                    else:
                        advance_flow(phone_number)
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
                    datos = client_data_load(phone_number)
                    # Si ya tiene todo → conectar directo
                    if datos.get("correo"):
                        send_whatsapp_message(phone_number, "Qué gusto que regreses. Te voy a conectar con el asesor ideal para ti.")
                        send_whatsapp_contact_buttons(phone_number)
                    else:
                        send_whatsapp_message(phone_number, "Qué gusto! Retomemos. Para pasarte con el asesor ideal, solo necesito completar tu ficha.")
                        send_whatsapp_message(phone_number, "¿Cuál es tu nombre? (completo por favor)")
                        waiting_for_name.add(phone_number)
                    return "OK", 200

                if button_id == "ver_catalogo":
                    _send_cta_url(phone_number, "Aquí tienes nuestro catálogo completo:", "Ver propiedades", VENTAS_URL)
                    return "OK", 200

                if button_id == "catalogo_ventas":
                    _send_cta_url(phone_number, "Aquí están todas nuestras propiedades en venta:", "Ver propiedades en venta", VENTAS_URL)
                    return "OK", 200

                if button_id == "catalogo_rentas":
                    _send_cta_url(phone_number, "Aquí están todas nuestras propiedades en renta:", "Ver propiedades en renta", RENTAS_URL)
                    return "OK", 200

                if button_id == "no_listo":
                    send_whatsapp_message(phone_number, "Sin presión, aquí voy a estar cuando estés lista o listo.")
                    return "OK", 200

                if "catálogo" in btn_lower or "catalogo" in btn_lower or "propiedad" in btn_lower:
                    _send_interactive_buttons(phone_number, "¿Qué te interesa ver?", [
                        {"id": "catalogo_ventas", "title": "En venta"},
                        {"id": "catalogo_rentas", "title": "En renta"}
                    ])
                    return "OK", 200

                if "tiempo" in btn_lower or "después" in btn_lower or "despues" in btn_lower:
                    # Necesito más tiempo
                    send_whatsapp_message(phone_number, "es completamente normal, el mercado inmobiliario puede ser saturador. aquí voy a estar cuando estés lista o listo, sin presión.")
                    return "OK", 200

                if button_id == "ficha_correcta":
                    ficha_confirmada.add(phone_number)
                    send_zapier_ficha(phone_number)
                    chatwoot_mark_qualified(phone_number, last_ficha_text.get(phone_number, "") or (_redis.get(f"ficha:{phone_number}") if _redis else ""))
                    send_whatsapp_message(phone_number, "listo, ya tengo todo. las llamadas son más eficientes, puedes agendar una en menos de un minuto. pero si prefieres WhatsApp también podemos. que te va mejor?")
                    send_whatsapp_contact_buttons(phone_number)
                    return "OK", 200

                elif button_id == "ficha_incorrecta":
                    waiting_for_ficha_correction.add(phone_number)
                    ficha_confirmada.discard(phone_number)  # clear confirmed flag so ficha can be re-confirmed
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
                    history = history_get(phone_number)
                    history.append({"role": "user", "content": button_title})
                    advance_flow(phone_number)
                    history_set(phone_number, history[-20:])
                    return "OK", 200

                elif button_id == "para_invertir":
                    client_data[phone_number]["intencion"] = button_title
                    client_data[phone_number]["tipo"] = "Compra"
                    client_data_save(phone_number)
                    send_whatsapp_message(phone_number, "qué bien, buscas comprar una propiedad como inversión. qué tipo de inversión tienes en mente?")
                    advance_flow(phone_number)
                    return "OK", 200

                elif button_id == "uso_comercial":
                    client_data[phone_number]["uso_suelo"] = "Comercial"
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return "OK", 200

                elif button_id == "uso_habitacional":
                    client_data[phone_number]["uso_suelo"] = "Habitacional"
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return "OK", 200

                elif button_id in ("largo_plazo", "corto_plazo"):
                    client_data[phone_number]["plazo_renta"] = button_title
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return "OK", 200

                elif button_id in ("conoce_merida", "necesita_orientacion"):
                    client_data[phone_number]["conoce_merida"] = button_title
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return "OK", 200

                elif button_id in ("comprar", "rentar"):
                    client_data[phone_number]["tipo"] = button_title
                    client_data_save(phone_number)
                    send_whatsapp_budget_list(phone_number, button_id)
                    return "OK", 200

                client_data_save(phone_number)
                user_message = button_title

        elif msg_type in ("audio", "text"):
            if msg_type == "audio":
                try:
                    import io
                    media_id = message["audio"]["id"]
                    token = os.environ.get("WHATSAPP_TOKEN")
                    url_resp = requests.get(
                        f"https://graph.facebook.com/v17.0/{media_id}",
                        headers={"Authorization": f"Bearer {token}"}, timeout=10
                    )
                    media_url = url_resp.json().get("url", "")
                    audio_resp = requests.get(
                        media_url,
                        headers={"Authorization": f"Bearer {token}"}, timeout=30
                    )
                    audio_file = io.BytesIO(audio_resp.content)
                    audio_file.name = "audio.ogg"
                    transcript = openai.audio.transcriptions.create(
                        model="whisper-1", file=audio_file, language="es"
                    )
                    user_message = transcript.text.strip()
                    print(f"[{phone_number}] Audio transcrito: {user_message[:80]}")
                    send_whatsapp_message(phone_number, f"escuché: _{user_message}_")
                except Exception as e:
                    print(f"Error transcribiendo audio: {e}")
                    send_whatsapp_message(phone_number, "no pude escuchar bien el audio, puedes escribirlo?")
                    return "OK", 200
            else:
                user_message = message["text"]["body"]

            # Detectar propiedad específica en primer mensaje
            is_first_message = not history_exists(phone_number)
            prop_key = detect_property(user_message) if is_first_message else None
            if prop_key:
                prop = PROPERTIES[prop_key]
                referral_early = message.get("referral", {})
                ad_image_url = referral_early.get("image_url", "")
                # Pre-poblar datos conocidos de la propiedad
                if prop.get("datos"):
                    client_data.setdefault(phone_number, {}).update(prop["datos"])
                    client_data_save(phone_number)
                msg_unico = prop["saludo"]
                if ad_image_url:
                    send_whatsapp_image(phone_number, ad_image_url, msg_unico)
                else:
                    send_whatsapp_message(phone_number, msg_unico)
                history = history_get(phone_number)
                history.append({"role": "user", "content": user_message})
                history.append({"role": "assistant", "content": msg_unico})
                history_set(phone_number, history[-20:])
                update_last_activity(phone_number)
                waiting_for_name.add(phone_number)
                schedule_followup(phone_number)
                return "OK", 200

            # Detectar formulario de Meta Lead Ad y pre-poblar datos
            if parse_lead_ad_message(phone_number, user_message):
                datos_lead = client_data.get(phone_number, {})
                nombre = datos_lead.get("nombre_completo", "")
                primer_nombre = nombre.split()[0] if nombre else ""
                if primer_nombre:
                    chatwoot_update_contact_name(phone_number, nombre)
                    _send_paso2(phone_number, primer_nombre, user_message)
                else:
                    send_whatsapp_message(phone_number, "Gracias por tu interés. Con quién tengo el gusto? (nombre completo)")
                    waiting_for_name.add(phone_number)
                return "OK", 200

            # Palabras clave secretas — prioridad ABSOLUTA, incluso sobre agente activo
            if user_message.strip().lower() == "reset365":
                reset_conversation(phone_number)
                if _redis:
                    _redis.delete(f"agent_active:{phone_number}")
                send_whatsapp_message(phone_number, "Conversación reiniciada 👋")
                return "OK", 200

            if user_message.strip().lower() == "nextday365":
                send_followup(phone_number)
                return "OK", 200

            if user_message.strip().lower() == "test_followup365":
                nombre_completo = get_nombre_redis(phone_number) or client_data.get(phone_number, {}).get("nombre_completo", "")
                name = nombre_completo.split()[0] if nombre_completo else "amigo"
                send_followup_template(phone_number, name)
                return "OK", 200

            # Si agente humano está activo, bot pausado (pero reset365 ya pasó)
            if _redis and _redis.exists(f"agent_active:{phone_number}"):
                print(f"[{phone_number}] Agente activo — bot pausado")
                return "OK", 200

            # ── PASO 0: Extraer entidades y reconciliar estados ANTES de cualquier check ──
            extract_entities(phone_number, user_message)

            # Detectar proveedor por keywords
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
                # Save the topic before connecting so it's available to the advisor
                client_data.setdefault(phone_number, {})["asesor_topic"] = user_message
                client_data_save(phone_number)
                send_whatsapp_contact_buttons(phone_number)
                return "OK", 200

            reclutamiento_keywords = ["busco trabajo", "quiero trabajar", "me interesa trabajar",
                                       "aplicar", "vacante", "puesto", "empleo", "curriculum", "cv",
                                       "me gustaria formar parte", "quiero ser parte"]
            ya_en_conversacion = bool(client_data.get(phone_number, {}).get("nombre_completo") or len(history_get(phone_number)) > 2)
            if not ya_en_conversacion and any(k in user_message.lower() for k in proveedor_keywords + reclutamiento_keywords):
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

            # pending_decision: check RAM first, then Redis (for persistence across restarts)
            _in_pending = pending_decision.get(phone_number) or (
                _redis and _redis.exists(f"pending_decision:{phone_number}")
            )
            if _in_pending:
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

            if phone_number in waiting_for_ficha_correction:
                waiting_for_ficha_correction.discard(phone_number)
                user_message = f"corrección de ficha: {user_message}"

            # Detectar frustración con el bot (máquina/robot/no persona)
            bot_frustration_keywords = ["máquina", "maquina", "robot", "bot ", "no es una persona",
                                        "no habla con personas", "no quiero hablar con", "inteligencia artificial",
                                        "no me entiendo con", "no tiene caso", "no es humano"]
            if any(k in user_message.lower() for k in bot_frustration_keywords):
                MSG_BOT = (
                    "Entiendo que pueda sentirse incómodo llenar tantos datos, y lo respeto. "
                    "Detrás de este sistema hay asesores reales, muchos con agendas cargadas y familias que dependen de este trabajo. "
                    "Tener la ficha aunque sea a medias les permite llegar a la conversación preparados para ayudarte mejor, sin hacerte repetir todo. "
                    "No tiene que quedar perfecta — con tu nombre y celular ya es suficiente para que alguien te contacte. "
                    "¿Le damos una oportunidad?"
                )
                send_whatsapp_message(phone_number, MSG_BOT)
                return "OK", 200

            # Detectar negaciones en momentos clave
            negaciones = {"no", "nop", "nel", "paso", "no quiero", "prefiero no",
                          "no gracias", "no por ahora", "ahorita no", "después", "luego"}
            es_negacion = user_message.strip().lower() in negaciones

            if es_negacion and phone_number in (waiting_for_apellido | waiting_for_email | waiting_for_name):
                send_whatsapp_message(phone_number,
                    "A los asesores les ayuda mucho tener la ficha completa, ya que las fichas listas suelen revisarse con un poco más de prioridad. "
                    "Pero no pasa nada si aún hay cosas por definir, podemos avanzar con lo básico.")
                advance_flow(phone_number)
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
                        chatwoot_update_contact_name(phone_number, full)
                        _send_paso2(phone_number, words[0].capitalize(), user_message)
                        return "OK", 200
                else:
                    # Mensaje largo — entidades ya extraídas en PASO 0; dejar que GPT continúe
                    waiting_for_name.discard(phone_number)

            # Guardar apellido — SIN pasar por GPT
            elif phone_number in waiting_for_apellido:
                waiting_for_apellido.discard(phone_number)
                existing = client_data.get(phone_number, {}).get("nombre_completo", "") or get_nombre_redis(phone_number)
                full_name = f"{existing} {user_message.strip().title()}".strip()
                client_data.setdefault(phone_number, {})["nombre_completo"] = full_name
                save_nombre_redis(phone_number, full_name)
                client_data_save(phone_number)
                chatwoot_update_contact_name(phone_number, full_name)
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
            elif phone_number in waiting_for_email and len(user_message.strip()) > 4:
                send_whatsapp_message(phone_number, "ese correo no parece válido, me lo puedes compartir de nuevo? por ejemplo: nombre@gmail.com")
                return "OK", 200

        else:
            return "OK", 200

        history = history_get(phone_number)
        is_first_message = len(history) == 0
        history.append({"role": "user", "content": user_message})

        from datetime import timezone, timedelta
        merida_tz = timezone(timedelta(hours=-6))
        hora_actual = datetime.now(merida_tz).hour
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

        ctx = ad_context.get(phone_number, {})
        if isinstance(ctx, dict):
            if ctx.get("origen") == "anuncio" and ctx.get("texto"):
                system += f"\n\nORIGEN DEL LEAD: Anuncio de Meta — {ctx['texto']}"
            else:
                system += "\n\nORIGEN DEL LEAD: Link directo (wa.me)"
        else:
            system += "\n\nORIGEN DEL LEAD: Link directo (wa.me)"

        # extract_entities was already called at PASO 0 for text/audio — don't call again
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
            if "zona" in datos:
                conocido.append(f"- Zona: {datos['zona']} (NO vuelvas a preguntar esto)")
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

        # Si GPT mandó la ficha sin el token, forzar CONFIRMAR_FICHA
        if ("CONFIRMAR_FICHA" not in reply and
                "Nombre:" in reply and "Correo:" in reply and "Teléfono:" in reply):
            reply = reply.strip() + "\nCONFIRMAR_FICHA"
            print(f"[{phone_number}] CONFIRMAR_FICHA inyectado — GPT olvidó el token")

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
            """Process ALL tokens in GPT reply. Each token is handled in order;
            skips button tokens when the entity already exists in client_data."""
            datos_actuales = client_data.get(phone_number, {})

            # --- Step 1: extract plain text (everything that is not a token) ---
            all_token_list = [
                "MANDAR_BOTONES_CONTACTO",
                "MANDAR_BOTONES_COMPRAR_RENTAR",
                "MANDAR_BOTONES_VIVIR_INVERTIR",
                "CONFIRMAR_FICHA",
                "PREGUNTAR_TEMA_ASESOR",
            ]
            text_part = reply_text
            for t in all_token_list:
                text_part = text_part.replace(t, "")
            text_part = text_part.strip()

            # --- Step 2: process CONFIRMAR_FICHA first (before sending text, to avoid duplicate) ---
            if "CONFIRMAR_FICHA" in reply_text:
                ficha_text = text_part  # the cleaned text IS the ficha body
                last_ficha_text[phone_number] = ficha_text
                if _redis:
                    _redis.setex(f"ficha:{phone_number}", HISTORY_TTL, ficha_text)
                send_whatsapp_ficha_confirmation(phone_number, ficha_text)
                return  # ficha confirmation is terminal — nothing else needed

            # --- Step 3: send the plain text (only if no CONFIRMAR_FICHA) ---
            if text_part:
                send_whatsapp_message(phone_number, text_part)
                low = text_part.lower()
                if "ya te encuentras en mérida" in low or "de dónde te mudas" in low or "ya vives en mérida" in low:
                    waiting_for_ciudad.add(phone_number)
                if any(p in low for p in ["me compartes tu correo", "me das tu correo", "tu correo",
                                           "correo electrónico", "correo para", "comparte tu correo", "correo?"]):
                    waiting_for_email.add(phone_number)

            if "PREGUNTAR_TEMA_ASESOR" in reply_text:
                waiting_for_asesor_topic.add(phone_number)
                # text already sent above; no button to send

            if "MANDAR_BOTONES_CONTACTO" in reply_text:
                send_whatsapp_contact_buttons(phone_number)

            if "MANDAR_BOTONES_VIVIR_INVERTIR" in reply_text:
                if "intencion" not in datos_actuales:
                    send_whatsapp_vivir_invertir_buttons(phone_number)
                else:
                    # Entity already known — use advance_flow to send what's actually missing
                    advance_flow(phone_number)

            if "MANDAR_BOTONES_COMPRAR_RENTAR" in reply_text:
                if "tipo" not in datos_actuales:
                    send_whatsapp_comprar_rentar_buttons(phone_number)
                else:
                    advance_flow(phone_number)

        dispatch_reply(reply)

        # Sincronizar respuesta de María como nota privada (visible en Chatwoot, no llega al cliente)
        chatwoot_sync_message(phone_number, f"🤖 {reply_clean}", "outgoing", private=True)

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
