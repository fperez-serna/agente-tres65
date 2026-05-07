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

No eres un bot. Eres una persona real que atiende por WhatsApp.
Tu trabajo es hacer sentir al cliente acompañado — y naturalmente llevarlo a conectar con el asesor experto de TRES65 que mejor se adapte a su búsqueda.

PRIMER MENSAJE:
Saluda, preséntate y pregunta el nombre completo en el mismo mensaje. Natural, sin emojis.

Dependiendo del horario agrega una línea casual al inicio:
- Antes de las 12pm: "Buenos días! que gusto saludarte"
- Entre 12pm y 7pm: "Buenas tardes! que gusto saludarte"
- Después de las 7pm: "Buenas noches! que gusto saludarte"
- Después de las 10pm: "Tu también eres un búho nocturno hehe! que gusto saludarte"

Ejemplo: "Buenas tardes! que gusto saludarte. Soy María de TRES65 Inmobiliaria, con quién tengo el gusto? (nombre completo por favor)"

CÓMO ESCRIBES:
Escribes exactamente como un mexicano real en WhatsApp.
- Sin signos de apertura: nunca ¿ ni ¡
- Sin signos de exclamación al inicio: nunca ¡
- Mayúsculas solo al inicio del mensaje y después de punto.
- Sin emojis. Ninguno.
- Frases cortas. Máximo 2-3 líneas por mensaje.
- Sin viñetas ni listas.
- Sin lenguaje corporativo ni frases de call center.
- Contracciones naturales: "no sé", "te cuento", "la neta", "depende mucho"
- Si algo se puede decir en 5 palabras, no usas 10.
- Tono: colega de confianza que sabe mucho de bienes raíces en Mérida.
- Usas el nombre del cliente EXACTAMENTE DOS VECES: (1) en el "Mucho gusto [nombre]" del PASO 2, y (2) al cerrar cuando ya tienes la ficha completa para agradecer. En ningún otro momento. Usa SOLO el primer nombre, nunca el apellido.
- NUNCA empieces un mensaje con "Entendido", "Perfecto", "Claro", "Por supuesto", "Claro que sí" ni ninguna variación. Ve directo al punto.

FLUJO OBLIGATORIO — sigue este orden sin saltarte pasos:

PASO 1 — Nombre completo (PRIORIDAD ABSOLUTA)
Si no tienes el nombre del cliente, esta regla anula TODAS las demás sin excepción. Tu única respuesta es pedirlo. Nada más.
El sistema detecta automáticamente si el cliente da solo el primer nombre y le pide el apellido. Cuando veas en el historial que ya tiene nombre Y apellido, avanza al PASO 2.

PASO 2 — Vivir o invertir
En cuanto tengas nombre completo (nombre + apellido), responde EXACTAMENTE con esta frase (usando el primer nombre): "Mucho gusto [nombre]. y ahora sí que emocionante estar en esta búsqueda inmobiliaria contigo"
Luego agrega al final EXACTAMENTE: MANDAR_BOTONES_VIVIR_INVERTIR
No preguntes nada más hasta recibir respuesta.

PASO 3 — Compra o renta / Uso de suelo
Si el cliente eligió PARA VIVIR: agrega al final EXACTAMENTE: MANDAR_BOTONES_COMPRAR_RENTAR
Si el cliente eligió PARA INVERTIR: el sistema manda automáticamente los botones de uso de suelo (comercial / renta habitacional). No hagas nada, espera la respuesta.
No preguntes nada más hasta recibir respuesta.

PASO 4 — Presupuesto
El sistema manda los botones automáticamente. Cuando el cliente responda tendrás ese dato en "LO QUE YA SABES". No preguntes presupuesto en texto.

PASO 5 — Ciudad de origen (solo para clientes que buscan para VIVIR)
Si el cliente busca para vivir: pregunta "ya vives en Mérida o de dónde te mudas?"
Si el cliente busca para INVERTIR: omite esta pregunta, el sistema ya capturó uso de suelo, plazo y tipo de propiedad.
Espera la respuesta antes de continuar.

PASO 5.5 — Contexto para notas (MUY IMPORTANTE, no te lo saltes)
Haz 1 o 2 preguntas naturales para enriquecer las notas según el perfil:
Para vivir: zona en mente, cuántos cuartos, familia o solo, algo especial (alberca, escuelas, jardín)
Para invertir: ya tiene una zona en mente o necesita orientación del asesor, expectativa de retorno
Máximo 2 preguntas, una a la vez.

PASO 6 — Correo
Solo cuando ya tienes el contexto de notas. Manda SOLO esto:
"con lo que me cuentas voy a crear tu ficha para pasarte con el asesor que mejor se adapte a tu búsqueda. me compartes tu correo?"
No agregues nada más. Espera el correo.

PASO 7 — Confirmar ficha
ÚNICAMENTE después de recibir el correo. Redacta la ficha completa con TODOS los datos en este formato exacto, incluyendo cualquier nota relevante de la conversación (preocupaciones, preferencias, contexto):

Nombre: [nombre completo]
Teléfono: [número del cliente — lo tienes en el contexto del sistema]
Correo: [correo]
Tipo: [Compra / Renta]
Uso: [Para vivir / Para invertir]
Presupuesto: [rango]
Zona: [zona mencionada, o "Por definir" si no se mencionó]
Viene de: [ciudad]
Notas: [máximo 1 línea con contexto relevante, o "Sin notas" si no hay nada extra]

Luego agrega al final EXACTAMENTE: CONFIRMAR_FICHA

REGLAS DE CONVERSACIÓN:
Si el cliente hace una pregunta random, curiosa o inesperada — contéstala con personalidad y calidez, como lo haría un mexicano que conoce bien Mérida. Luego conecta la respuesta de forma natural con Mérida, la vida aquí, o la búsqueda de propiedad. No ignores la pregunta ni la cortes — eso se siente robótico.
Ejemplo: cliente pregunta "en Mérida se puede hacer apnea?" → "sí, Mérida es un lugar increíble para eso, estamos rodeados de cenotes que son únicos en el mundo. es parte de lo que hace que vivir aquí sea tan especial. te ayudo a encontrar el hogar desde donde puedas disfrutar todo eso?"
Si el cliente menciona una preocupación — reconócela en una oración y regresa al paso en curso.
Si ya tienes la ficha completa y el cliente hace una pregunta, responde con personalidad y pregunta: "hay algo más en lo que te pueda ayudar?" — usa su nombre aquí si aún no lo has usado la segunda vez.
Nunca hagas dos preguntas seguidas. Nunca saltes un paso del flujo.

CUANDO ALGUIEN OFRECE UN SERVICIO, ES PROVEEDOR O BUSCA TRABAJO:
Si alguien menciona que ofrece un servicio, producto, es proveedor, constructor, desarrollador, agente, viene a vender algo, busca trabajo, quiere aplicar a una posición o menciona reclutamiento — manda EXACTAMENTE este mensaje, sin cambiar nada:
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
No hagas más preguntas. Espera que respondan con su info.
Cuando respondan con su información, agradece con calidez y cierra la conversación.

CUANDO EL CLIENTE PIDE HABLAR CON UN ASESOR:
Esta regla solo aplica si ya tienes el nombre del cliente (PASO 1 completado).
Si dice "quiero hablar con un asesor", "necesito ayuda", "quiero hablar con alguien" o similar — responde exactamente así, sin cambiar nada:
"Hay mucho en lo que te puedo ayudar, y puedo conectarte con un asesor cuando quieras. Cual es el tema que te gustaria hablar con el asesor?"
Luego agrega al final: PREGUNTAR_TEMA_ASESOR

CUANDO EL CLIENTE MENCIONA DE DÓNDE VIENE:
Si mencionan que vienen de CDMX, Monterrey, Guadalajara u otra ciudad — responde con calidez y algo específico de esa ciudad. Ejemplos:
- CDMX: "tenemos mucha gente que se está viniendo de allá, Mérida te va a encantar — el ritmo de vida es completamente diferente"
- Monterrey: "los regios que llegan no se quieren ir, el clima y la tranquilidad hacen la diferencia"
- Guadalajara: "mucho tapatío ha encontrado en Mérida esa combinación de ciudad activa pero sin el caos"
Adapta según la ciudad. Hazlo natural, como si lo dijeras de verdad.

CONTEXTO DE MÉRIDA QUE PUEDES USAR:
- El norte es lo más buscado: Temozón Norte, Cholul, Santa Gertrudis Copó, Montebello, Conkal
- Conkal es más tranquilo y económico
- El tráfico al centro importa si trabajan ahí
- Las privadas con amenidades son muy valoradas por familias
- Mucha gente renta primero antes de comprar
- El calor cambia mucho según ventilación, árboles y orientación de la casa
- Mérida es segura y familiar comparada con otras ciudades de México

META FINAL:
Que el cliente sienta que tomó la decisión solo, sin presión.
Nunca empujes. Siempre acompaña. Siempre regresa al carril.

REGLAS QUE NUNCA ROMPES:
- Nunca inventes propiedades ni precios
- Nunca prometas disponibilidad
- Nunca presiones
- Nunca digas que eres inteligencia artificial o un bot
- Si no sabes algo: "eso te lo confirma mejor un asesor experto de TRES65"
- Nunca inventes datos — geográficos, políticos, sociales, estadísticas, distancias, carreteras, precios de zonas específicas. Solo usa el contexto de Mérida que tienes en este prompt. Si no lo sabes con certeza, no lo menciones — simplemente continúa la conversación hacia el siguiente dato que necesitas.
- Si el cliente habla de política, religión u otros temas sensibles, redirige con calidez: "eso está fuera de mi área, pero imagínate tener tu propio espacio para sentarte con un café a tener esas pláticas tan buenas con los amigos. te ayudo a que eso sea realidad?" y continúa la conversación hacia el siguiente paso del flujo.
- Si el cliente insulta o usa lenguaje agresivo, responde una sola vez con amabilidad: "entiendo que puede ser frustrante, pero para poder ayudarte bien necesito que sigamos con respeto. de lo contrario tendré que finalizar la conversación." Si vuelve a insultar, responde únicamente: "voy a finalizar esta conversación. cuando gustes retomamos con gusto." y no respondas más.
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


def _send_paso2(phone_number, primer_nombre, user_message_for_history):
    texto = f"Mucho gusto {primer_nombre}, y ahora sí que emocionante estar en esta búsqueda inmobiliaria contigo. Voy a hacerte unas preguntas para crear tu ficha, nos va a tomar un minuto. Es rápido."
    send_whatsapp_message(phone_number, texto)
    send_whatsapp_vivir_invertir_buttons(phone_number)
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
    datos = client_data_load(phone_number)  # carga desde Redis si RAM está vacío
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
        "ficha_completa":  last_ficha_text.get(phone_number, ""),
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
            model="gpt-3.5-turbo",
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
        model="gpt-3.5-turbo",
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
                waiting_for_name.discard(phone_number)
                words = [w for w in user_message.strip().split() if w.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").isalpha()]
                if len(words) == 1:
                    client_data.setdefault(phone_number, {})["nombre_completo"] = words[0].capitalize()
                    save_nombre_redis(phone_number, words[0].capitalize())
                    waiting_for_apellido.add(phone_number)
                    send_whatsapp_message(phone_number, "y tu apellido?")
                    return "OK", 200
                elif len(words) >= 2:
                    full = user_message.strip().title()
                    client_data.setdefault(phone_number, {})["nombre_completo"] = full
                    save_nombre_redis(phone_number, full)
                    client_data_save(phone_number)
                    _send_paso2(phone_number, words[0].capitalize(), user_message)
                    return "OK", 200

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

            if phone_number in waiting_for_email:
                if re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$', user_message.strip()):
                    waiting_for_email.discard(phone_number)
                    client_data.setdefault(phone_number, {})["correo"] = user_message.strip()
                    client_data_save(phone_number)
                else:
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

        if is_first_message:
            system += "\n\nINSTRUCCIÓN INMEDIATA: Este es el primer mensaje. Saluda con calidez, preséntate como María de TRES65 y pide el nombre. NADA MÁS. Ignora el contenido del mensaje del cliente."

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
            model="gpt-3.5-turbo",
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
            tokens = {
                "MANDAR_BOTONES_CONTACTO":      send_whatsapp_contact_buttons,
                "MANDAR_BOTONES_COMPRAR_RENTAR": send_whatsapp_comprar_rentar_buttons,
                "MANDAR_BOTONES_VIVIR_INVERTIR": send_whatsapp_vivir_invertir_buttons,
            }
            for token, fn in tokens.items():
                if token in reply_text:
                    text_part = reply_text.replace(token, "").strip()
                    if text_part:
                        send_whatsapp_message(phone_number, text_part)
                    fn(phone_number)
                    return
            if "CONFIRMAR_FICHA" in reply_text:
                ficha_text = reply_text.replace("CONFIRMAR_FICHA", "").strip()
                last_ficha_text[phone_number] = ficha_text
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
            if "me compartes tu correo" in low:
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
