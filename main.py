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
pending_decision = {}  # clientes que vieron los botones pero no han decidido
ad_context = {}        # contexto del anuncio por el que llegó el lead
waiting_for_email = set()  # números esperando que el cliente dé su correo
client_data = {}       # datos ya capturados por cliente {intencion, tipo}

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
- Mayúsculas después de punto, y al inicio de cada mensaje
- Sin emojis. Ninguno.
- Frases cortas. Máximo 2-3 líneas por mensaje.
- Sin viñetas ni listas salvo que ayuden mucho.
- Sin lenguaje corporativo ni frases de call center.
- Contracciones naturales: "no sé", "te cuento", "la neta", "depende mucho"
- Si algo se puede decir en 5 palabras, no usas 10.
- Tono: colega de confianza que sabe mucho de bienes raíces en Mérida.
- NUNCA repitas el nombre del cliente en cada mensaje. Úsalo máximo una vez cada 4-5 mensajes.
- Cuando uses el nombre del cliente, usa SOLO su primer nombre, nunca el apellido.
- NUNCA empieces un mensaje con "Entendido", "Perfecto", "Claro", "Por supuesto" ni ninguna variación de esas palabras. Ve directo al punto.

FLUJO OBLIGATORIO — sigue este orden sin saltarte pasos:

PASO 1 — Nombre
Primer mensaje siempre. Espera a que el cliente dé su nombre antes de continuar.

PASO 2 — Vivir o invertir
En cuanto tengas el nombre, responde con una frase corta y cálida (algo como "qué gusto, [nombre], qué emocionante estar en esta búsqueda contigo" — una sola línea, natural, sin exagerar) y luego agrega al final EXACTAMENTE: MANDAR_BOTONES_VIVIR_INVERTIR
No preguntes nada más hasta recibir respuesta.

PASO 3 — Compra o renta
Inmediatamente después de saber si es para vivir o invertir, pregunta esto. Agrega al final del mensaje EXACTAMENTE: MANDAR_BOTONES_COMPRAR_RENTAR
No preguntes nada más hasta recibir respuesta.

PASO 4 — Presupuesto
Solo después de tener los pasos 2 y 3, pregunta:
"ya tienes un rango de inversión en mente o prefieres que un asesor experto te oriente con eso?"

PASO 5 — Correo
Solo cuando ya tienes nombre, compra/renta, vivir/invertir y presupuesto. Manda SOLO esto:
"perfecto, con lo que me cuentas voy a crear tu ficha para pasarte con el asesor que mejor se adapte a tu búsqueda. me compartes tu correo?"
No agregues nada más. Espera el correo.

PASO 6 — Decisión de contacto
ÚNICAMENTE después de recibir el correo, manda esto seguido del token:
"listo, ya tengo todo. las llamadas son más eficientes, puedes agendar una en menos de un minuto. pero si prefieres por WhatsApp también podemos. que te va mejor?"
MANDAR_BOTONES_CONTACTO

REGLAS DE CONVERSACIÓN:
Si el cliente divaga o responde vago — valida en 1 línea, da un dato útil, y regresa al paso en curso.
Nunca hagas dos preguntas seguidas. Nunca saltes un paso aunque el cliente mencione algo de pasos posteriores.

CUANDO EL CLIENTE PIDE HABLAR CON UN ASESOR:
Si el cliente dice algo como "quiero hablar con un asesor", "me puedes contactar con alguien", "necesito ayuda", "quiero hablar con una persona" o cualquier variación — responde con calidez y agrega al final: MANDAR_BOTONES_ASESOR
Ejemplo: "hay mucho en lo que te puedo ayudar, y también puedo conectarte directo con uno de nuestros asesores cuando quieras."
MANDAR_BOTONES_ASESOR

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
    _send_interactive_buttons(to, "es para...", [
        {"id": "para_vivir", "title": "Para vivir"},
        {"id": "para_invertir", "title": "Para invertir"}
    ])


def send_whatsapp_help_buttons(to):
    _send_interactive_buttons(to, "como te puedo ayudar?", [
        {"id": "tengo_duda", "title": "Tengo una duda"},
        {"id": "agendar_asesor", "title": "Agendar con asesor"}
    ])


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

        if msg_type == "interactive":
            button_id = message["interactive"]["button_reply"]["id"]
            button_title = message["interactive"]["button_reply"]["title"]
            pending_decision.pop(phone_number, None)
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

            # Botones de decisión — guardar dato y continuar con GPT
            if phone_number not in client_data:
                client_data[phone_number] = {}
            if button_id in ("para_vivir", "para_invertir"):
                client_data[phone_number]["intencion"] = button_title
            elif button_id in ("comprar", "rentar"):
                client_data[phone_number]["tipo"] = button_title
            user_message = button_title

        elif msg_type == "text":
            user_message = message["text"]["body"]

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

            if phone_number in waiting_for_email:
                if re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$', user_message.strip()):
                    waiting_for_email.discard(phone_number)
                else:
                    send_whatsapp_message(phone_number, "ese correo no parece válido, me lo puedes compartir de nuevo? por ejemplo: nombre@gmail.com")
                    return "OK", 200

        else:
            return "OK", 200

        if phone_number not in conversation_history:
            conversation_history[phone_number] = []

        history = conversation_history[phone_number]
        history.append({"role": "user", "content": user_message})

        system = SYSTEM_PROMPT
        if ad_context.get(phone_number):
            system += f"\n\nCONTEXTO DEL ANUNCIO POR EL QUE LLEGÓ ESTE LEAD:\n{ad_context[phone_number]}\nUsa este contexto para personalizar tu primer mensaje — menciona algo relacionado al anuncio de forma natural, sin copiar el texto exacto."

        datos = client_data.get(phone_number, {})
        if datos:
            conocido = []
            if "intencion" in datos:
                conocido.append(f"- Ya dijo que es {datos['intencion']} (NO vuelvas a preguntar esto)")
            if "tipo" in datos:
                conocido.append(f"- Ya dijo que quiere {datos['tipo']} (NO vuelvas a preguntar esto)")
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
                "MANDAR_BOTONES_CONTACTO": send_whatsapp_contact_buttons,
                "MANDAR_BOTONES_COMPRAR_RENTAR": send_whatsapp_comprar_rentar_buttons,
                "MANDAR_BOTONES_VIVIR_INVERTIR": send_whatsapp_vivir_invertir_buttons,
                "MANDAR_BOTONES_ASESOR": send_whatsapp_help_buttons,
            }
            for token, fn in tokens.items():
                if token in reply_text:
                    text_part = reply_text.replace(token, "").strip()
                    if text_part:
                        send_whatsapp_message(phone_number, text_part)
                    fn(phone_number)
                    return
            send_whatsapp_message(phone_number, reply_text)
            if "me compartes tu correo" in reply_text.lower():
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
