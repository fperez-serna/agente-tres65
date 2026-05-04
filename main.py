from flask import Flask, request
import openai
import os
import re
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

app = Flask(__name__)
openai.api_key = os.environ.get("OPENAI_API_KEY")

conversation_history = {}
last_maria_message_time = {}
follow_up_jobs = {}
client_names = {}
pending_decision = {}        # clientes que vieron los botones pero no han decidido
ad_context = {}              # contexto del anuncio por el que llegó el lead
waiting_for_email = set()          # números esperando correo
waiting_for_ciudad = set()         # números esperando ciudad de origen
waiting_for_supplier_info = set()  # proveedores esperando dar su info
waiting_for_asesor_topic = set()   # clientes a los que se les preguntó el tema para el asesor
algo_mas_mode = set()              # clientes en flujo exploratorio (no el paso a paso estándar)
client_data = {}        # datos ya capturados por cliente {intencion, tipo, presupuesto, ciudad}

scheduler = BackgroundScheduler()
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
- Entre 12pm y 6pm: "Buenas tardes! que gusto saludarte"
- Después de las 6pm: "Buenas noches! que gusto saludarte"
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
- NUNCA repitas el nombre del cliente en cada mensaje. Úsalo máximo una vez cada 4-5 mensajes.
- Cuando uses el nombre del cliente, usa SOLO su primer nombre, nunca el apellido.
- NUNCA empieces un mensaje con "Entendido", "Perfecto", "Claro", "Por supuesto", "Claro que sí" ni ninguna variación. Ve directo al punto.

FLUJO OBLIGATORIO — sigue este orden sin saltarte pasos:

PASO 1 — Nombre (PRIORIDAD ABSOLUTA)
Si no tienes el nombre del cliente, esta regla anula TODAS las demás sin excepción. No importa si el cliente pide un asesor, hace una pregunta, o dice cualquier otra cosa — si no tienes su nombre, tu única respuesta es pedirlo. Nada más. Ni asesor, ni botones, ni información de Mérida.

PASO 2 — Vivir o invertir
En cuanto tengas el nombre, responde con una frase corta y cálida (ej: "qué emocionante estar en esta búsqueda contigo") y agrega al final EXACTAMENTE: MANDAR_BOTONES_VIVIR_INVERTIR
No preguntes nada más hasta recibir respuesta.

PASO 3 — Compra o renta
Después de saber si es para vivir o invertir. Agrega al final EXACTAMENTE: MANDAR_BOTONES_COMPRAR_RENTAR
No preguntes nada más hasta recibir respuesta.

PASO 4 — Presupuesto
El sistema manda los botones automáticamente. Cuando el cliente responda tendrás ese dato en "LO QUE YA SABES". No preguntes presupuesto en texto.

PASO 5 — Ciudad de origen
Solo cuando tienes nombre, vivir/invertir, compra/renta y presupuesto. Pregunta en texto, sin botones:
"ya vives en Mérida o de dónde te mudas?"
Espera la respuesta antes de continuar.

PASO 6 — Correo
Solo cuando tienes todo lo anterior. Manda SOLO esto:
"con lo que me cuentas voy a crear tu ficha para pasarte con el asesor que mejor se adapte a tu búsqueda. me compartes tu correo?"
No agregues nada más. Espera el correo.

PASO 7 — Confirmar ficha y decisión de contacto
ÚNICAMENTE después de recibir el correo. Redacta un resumen natural y cálido de la ficha del cliente usando lo que sabes (nombre, vivir/invertir, compra/renta, presupuesto, ciudad de origen) y luego pregunta cómo prefiere el contacto. Ejemplo de tono:
"[nombre], ya tengo todo listo. buscas [comprar/rentar] para [vivir/invertir] en Mérida, vienes de [ciudad] y tu presupuesto es [rango]. te voy a pasar con el asesor ideal para ti. las llamadas son más eficientes, puedes agendar en menos de un minuto. pero si prefieres WhatsApp también podemos. que te va mejor?"
Luego agrega: MANDAR_BOTONES_CONTACTO

REGLAS DE CONVERSACIÓN:
Si el cliente hace una pregunta random, curiosa o inesperada — contéstala con personalidad y calidez, como lo haría un mexicano que conoce bien Mérida. Luego conecta la respuesta de forma natural con Mérida, la vida aquí, o la búsqueda de propiedad. No ignores la pregunta ni la cortes — eso se siente robótico.
Ejemplo: cliente pregunta "en Mérida se puede hacer apnea?" → "sí, Mérida es un lugar increíble para eso, estamos rodeados de cenotes que son únicos en el mundo. es parte de lo que hace que vivir aquí sea tan especial. te ayudo a encontrar el hogar desde donde puedas disfrutar todo eso?"
Si el cliente menciona una preocupación — reconócela en una oración y regresa al paso en curso.
Si ya tienes la ficha completa y el cliente hace una pregunta, llámalo por su nombre, responde con personalidad y pregunta: "hay algo más en lo que te pueda ayudar?"
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
- Si el cliente habla de política, religión u otros temas sensibles, redirige con calidez: "eso está fuera de mi área, pero imagínate tener tu propio espacio para sentarte con un café a tener esas pláticas tan buenas con los amigos. te ayudo a que eso sea realidad?" y agrega: MANDAR_BOTONES_ASESOR
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
    _send_interactive_buttons(to, "que prefieres?", [
        {"id": "comprar", "title": "Comprar"},
        {"id": "rentar", "title": "Rentar"}
    ])


def send_whatsapp_vivir_invertir_buttons(to):
    _send_interactive_buttons(to, "La propiedad que buscas es para...", [
        {"id": "para_vivir",    "title": "Para vivir"},
        {"id": "para_invertir", "title": "Para invertir"},
        {"id": "algo_mas",      "title": "Algo más"}
    ])


def send_whatsapp_help_buttons(to):
    _send_interactive_buttons(to, "como te puedo ayudar?", [
        {"id": "tengo_duda", "title": "Tengo una duda"},
        {"id": "agendar_asesor", "title": "Agendar con asesor"}
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


def reset_conversation(phone_number):
    conversation_history.pop(phone_number, None)
    client_data.pop(phone_number, None)
    client_names.pop(phone_number, None)
    ad_context.pop(phone_number, None)
    pending_decision.pop(phone_number, None)
    waiting_for_email.discard(phone_number)
    waiting_for_ciudad.discard(phone_number)
    waiting_for_supplier_info.discard(phone_number)
    waiting_for_asesor_topic.discard(phone_number)
    algo_mas_mode.discard(phone_number)
    cancel_followup(phone_number)


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
    history = conversation_history.get(phone_number, [])
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

        # Proveedor que intenta acceder a un asesor: reiniciar conversación como cliente
        if phone_number in waiting_for_supplier_info and msg_type == "interactive":
            reset_conversation(phone_number)
            send_whatsapp_message(phone_number, "con gusto te ayudo. con quién tengo el gusto? (nombre completo por favor)")
            return "OK", 200

        if msg_type == "interactive":
            interactive_type = message["interactive"].get("type")
            pending_decision.pop(phone_number, None)

            # Respuesta de lista de presupuesto
            if interactive_type == "list_reply":
                list_id    = message["interactive"]["list_reply"]["id"]
                list_title = message["interactive"]["list_reply"]["title"]
                print(f"[{phone_number}] Lista: {list_id}")
                client_data.setdefault(phone_number, {})

                if list_id == "presup_asesor":
                    client_data[phone_number]["presupuesto"] = "Lo platica con el asesor"
                    user_message = "prefiero platicarlo con el asesor"
                else:
                    client_data[phone_number]["presupuesto"] = list_title
                    user_message = list_title

            # Respuesta de botón
            else:
                button_id    = message["interactive"]["button_reply"]["id"]
                button_title = message["interactive"]["button_reply"]["title"]
                print(f"[{phone_number}] Botón: {button_id}")

                if button_id == "agendar_llamada":
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

                if button_id in ("para_vivir", "para_invertir"):
                    client_data[phone_number]["intencion"] = button_title
                elif button_id in ("comprar", "rentar"):
                    client_data[phone_number]["tipo"] = button_title
                    send_whatsapp_budget_list(phone_number, button_id)
                    return "OK", 200

                user_message = button_title

        elif msg_type == "text":
            user_message = message["text"]["body"]

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
                    if parts:
                        ad_context[phone_number] = " | ".join(parts)
                        print(f"[{phone_number}] Lead desde anuncio: {ad_context[phone_number]}")

            if pending_decision.get(phone_number):
                send_whatsapp_message(phone_number, "solo dime, como prefieres que te contacte el asesor?")
                send_whatsapp_contact_buttons(phone_number)
                return "OK", 200

            if phone_number in waiting_for_ciudad:
                waiting_for_ciudad.discard(phone_number)
                client_data.setdefault(phone_number, {})["ciudad"] = user_message.strip()

            if phone_number in waiting_for_email:
                if re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$', user_message.strip()):
                    waiting_for_email.discard(phone_number)
                else:
                    send_whatsapp_message(phone_number, "ese correo no parece válido, me lo puedes compartir de nuevo? por ejemplo: nombre@gmail.com")
                    return "OK", 200

        else:
            return "OK", 200

        is_first_message = phone_number not in conversation_history or len(conversation_history[phone_number]) == 0

        if phone_number not in conversation_history:
            conversation_history[phone_number] = []

        history = conversation_history[phone_number]
        history.append({"role": "user", "content": user_message})

        system = SYSTEM_PROMPT
        if is_first_message:
            system += "\n\nINSTRUCCIÓN INMEDIATA: Este es el primer mensaje. Saluda con calidez, preséntate como María de TRES65 y pide el nombre. NADA MÁS. Ignora el contenido del mensaje del cliente."

        if phone_number in algo_mas_mode:
            system += """

MODO EXPLORATORIO — este cliente tiene una necesidad especial, no el flujo estándar.
NO uses el paso a paso. NO mandes botones de vivir/invertir ni comprar/rentar.
Sé curiosa, cálida y abierta. Tu objetivo es entender QUÉ necesita exactamente y conectarlo con el asesor ideal.

Si mencionan renta a corto plazo, estadía temporal, Airbnb o algo vacacional/de trabajo:
- No des por hecho nada. Divaga un poco con calidez: "todo se puede en este mundo inmobiliario, cuéntame más de lo que buscas"
- Pregunta UNA cosa a la vez: es de trabajo o vacacional? vienes solo o con más gente? amigos, familia? cuánto tiempo más o menos?
- Si es vacacional: qué buscan — descansar, explorar, aventura?
- Conecta siempre con Mérida: cenotes, gastronomía, cultura, seguridad, clima

Cuando ya tengas suficiente contexto (2-3 mensajes), di:
"con todo esto ya puedo pasarte con el asesor que mejor se adapta a lo que buscas. cómo prefieres que te contacte?"
Luego agrega: MANDAR_BOTONES_CONTACTO"""
        if ad_context.get(phone_number):
            system += f"\n\nCONTEXTO DEL ANUNCIO POR EL QUE LLEGÓ ESTE LEAD:\n{ad_context[phone_number]}\nUsa este contexto para personalizar tu primer mensaje — menciona algo relacionado al anuncio de forma natural, sin copiar el texto exacto."

        datos = client_data.get(phone_number, {})
        if datos:
            conocido = []
            if "intencion" in datos:
                conocido.append(f"- Ya dijo que es {datos['intencion']} (NO vuelvas a preguntar esto)")
            if "tipo" in datos:
                conocido.append(f"- Ya dijo que quiere {datos['tipo']} (NO vuelvas a preguntar esto)")
            if "presupuesto" in datos:
                conocido.append(f"- Presupuesto: {datos['presupuesto']} (NO vuelvas a preguntar esto, ve al PASO 5)")
            if "ciudad" in datos:
                conocido.append(f"- Viene de / vive en: {datos['ciudad']} (NO vuelvas a preguntar esto, ve al PASO 6)")
            system += "\n\nLO QUE YA SABES DE ESTE CLIENTE:\n" + "\n".join(conocido)

        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": system}] + history
        )

        reply = response.choices[0].message.content
        history.append({"role": "assistant", "content": reply})

        if len(history) > 20:
            conversation_history[phone_number] = history[-20:]

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

        last_maria_message_time[phone_number] = datetime.now()
        schedule_followup(phone_number)

        print(f"[{phone_number}] Usuario: {user_message}")
        print(f"[{phone_number}] María: {reply}")

    except Exception as e:
        print(f"Error: {e}")

    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
