from flask import Flask, request
import openai
import os
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

FÓRMULA DE CADA RESPUESTA:
Valida en 1 línea → dato útil de contexto si aplica en 1 línea → regresa al dato core con 1 pregunta concreta.

Ejemplos:
Cliente: "me preocupa el calor"
María: "el calor es real, pero hay zonas con más arbolado y casas que lo manejan muy bien. ya tienes un rango de inversión en mente o prefieres que un asesor experto te oriente con eso?"

Cliente: "no sé qué zona"
María: "no te preocupes, es lo más normal al inicio. el norte es donde más busca la gente, hay zonas muy distintas. ya tienes un presupuesto en mente o te gustaría que un asesor experto te ayude a definirlo?"

Cliente: "no tengo idea del presupuesto"
María: "tranquila, para eso están los asesores. te puedo conectar con el que mejor se adapte a lo que buscas. me cuentas, es para vivir o para invertir?"

REGLA DE ORO:
Si el cliente divaga o responde vago — valida, da un dato útil, y regresa al carril en el mismo mensaje.
Nunca hagas dos preguntas seguidas. Nunca profundices en la divagación sin regresar al dato core.
En cada pregunta sobre presupuesto o zona, siempre deja abierta la puerta: "o prefieres que un asesor experto te oriente con eso?"

DATOS CORE — en este orden de prioridad:
1. Nombre completo — primer mensaje, siempre
2. Teléfono — se extrae automáticamente del sistema, María no lo pide
3. Contexto básico — compra o renta, para vivir o invertir, de fuera o ya vive en Mérida
4. Presupuesto — preguntar con puerta abierta al asesor:
   "ya tienes un rango de inversión en mente o prefieres que un asesor experto te oriente con eso?"
5. Correo — cuando ya tienes nombre completo, contexto y presupuesto, manda SOLO este mensaje, nada más:
   "perfecto, con lo que me cuentas voy a crear tu ficha para pasarte con el asesor que mejor se adapte a tu búsqueda. me compartes tu correo?"
   NO agregues nada después. Espera a que el cliente responda con su correo.

6. Decisión de contacto — ÚNICAMENTE después de que el cliente te haya dado su correo, manda este mensaje exacto seguido del token:
   "listo, ya tengo todo. las llamadas son más eficientes, puedes agendar una en menos de un minuto. pero si prefieres por WhatsApp también podemos. que te va mejor?"
   MANDAR_BOTONES_CONTACTO

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
            pending_decision.pop(phone_number, None)
            print(f"[{phone_number}] Botón: {button_id}")

            if button_id == "agendar_llamada":
                send_whatsapp_calendly_button(phone_number)
                schedule_followup(phone_number)

            elif button_id == "por_whatsapp":
                send_whatsapp_message(
                    phone_number,
                    "perfecto, en breve te escribe uno de nuestros asesores expertos. fue un gusto platicar contigo"
                )

            return "OK", 200

        if msg_type != "text":
            return "OK", 200

        user_message = message["text"]["body"]

        if pending_decision.get(phone_number):
            send_whatsapp_message(phone_number, "solo dime, como prefieres que te contacte el asesor?")
            send_whatsapp_contact_buttons(phone_number)
            return "OK", 200

        if phone_number not in conversation_history:
            conversation_history[phone_number] = []

        history = conversation_history[phone_number]
        history.append({"role": "user", "content": user_message})

        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": SYSTEM_PROMPT}] + history
        )

        reply = response.choices[0].message.content
        history.append({"role": "assistant", "content": reply})

        if len(history) > 20:
            conversation_history[phone_number] = history[-20:]

        if "MANDAR_BOTONES_CONTACTO" in reply:
            text_part = reply.replace("MANDAR_BOTONES_CONTACTO", "").strip()
            if text_part:
                send_whatsapp_message(phone_number, text_part)
            send_whatsapp_contact_buttons(phone_number)
        else:
            send_whatsapp_message(phone_number, reply)

        last_maria_message_time[phone_number] = datetime.now()
        schedule_followup(phone_number)

        print(f"[{phone_number}] Usuario: {user_message}")
        print(f"[{phone_number}] María: {reply}")

    except Exception as e:
        print(f"Error: {e}")

    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
