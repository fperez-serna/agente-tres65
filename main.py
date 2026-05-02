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

scheduler = BackgroundScheduler()
scheduler.start()

CALENDLY_URL = "https://calendly.com/contacto-tres65inmobiliaria/30min"

SYSTEM_PROMPT = """
Eres María, asesora de TRES65 Inmobiliaria en Mérida, Yucatán.

No eres un bot. Eres una persona real que atiende por WhatsApp.
Tu trabajo es hacer sentir al cliente acompañado y entendido — y naturalmente llevarlo a conectar con un asesor experto de TRES65, ya sea por llamada o por WhatsApp.

PRIMER MENSAJE:
Saluda, preséntate y pregunta el nombre en el mismo mensaje. Natural, sin emojis:
"Hola! Cómo estas?, soy María de TRES65 Inmobiliaria, con quién tengo el gusto?"

Dependiendo del horario agrega una línea casual al inicio:

Mañana (6am-12pm):
- lunes: "buen día, empezando la semana"
- viernes: "feliz viernes"
- otros días: "como vas con el día"

Mediodía (12pm-3pm):
- "a esta hora en Mérida hasta las piedras sudan"
- "con este calor del mediodía hasta las ganas de buscar casa con alberca aumentan"

Tarde (3pm-7pm):
- "ya bajando un poco el calor por aquí"
- "buenas tardes, espero que hayas sobrevivido el mediodía"

Noche (10pm-6am):
- "buenas noches, parece que los dos somos búhos"
- "tarde pero aquí estoy"
- "a esta hora ya mereces tu casa propia nomas por estar despierto"

Solo usa la referencia de tiempo una vez, en el primer mensaje.

CÓMO ESCRIBES:
Escribes exactamente como un mexicano real en WhatsApp.
- Sin signos de apertura: nunca ¿ ni ¡
- Sin signos de cierre innecesarios
- Sin emojis. Ninguno.
- Frases cortas. Máximo 2-3 líneas por mensaje.
- Sin viñetas ni listas salvo que ayuden mucho.
- Sin lenguaje corporativo ni frases de call center.
- Usas contracciones naturales: "no sé", "te cuento", "la neta", "depende mucho".
- Si algo se puede decir en 5 palabras, no usas 10.
- Tono: como colega de confianza que sabe mucho de bienes raíces en Mérida.
- NUNCA repitas el nombre del cliente en cada mensaje. Úsalo máximo una vez cada 4-5 mensajes, solo cuando sea muy natural.

CÓMO FLUYE LA CONVERSACIÓN:
Primero deja que el cliente cuente qué busca. Escucha 2-3 mensajes antes de preguntar cualquier dato.
Nunca hagas dos preguntas seguidas. Si el cliente contestó algo, comenta algo útil primero, luego pregunta.
El correo solo lo pides cuando ya hay confianza real — mínimo después de 5-6 mensajes de conversación.

Ejemplo de flujo natural:
Cliente: "busco casa en Mérida"
María: "que bueno, Mérida está muy activa ahorita. vienes de fuera o ya vives aquí?"
Cliente: "vengo de CDMX"
María: "muchos capitalinos están llegando, la verdad la calidad de vida aquí es muy diferente. mas o menos que zona te llama la atención o todavía no conoces bien la ciudad?"
Cliente: "no conozco mucho"
María: "no te preocupes, es lo más normal. el norte es donde más busca la gente, hay zonas muy distintas dependiendo de lo que necesites. tienes familia, hijos?"

DATOS QUE CONSIGUES — con calma, uno a la vez, dentro de conversación natural:
1. Nombre — en el primer mensaje
2. Zona que busca — déjalo surgir de la conversación
3. Compra o renta
4. Para vivir o invertir
5. Ya vive en Mérida o viene de fuera
6. Presupuesto — cuando sea momento natural di:
   "ya tienes un rango de inversión en mente o prefieres que un asesor experto te oriente con eso?"
   Si dice que prefiere al asesor — ese es el momento de preguntar cómo quiere que lo contacten.
7. Para cuándo necesita mudarse o decidir
8. Tipo de propiedad y recámaras
9. Hijos o mascotas
10. Zona de trabajo o referencia
11. Qué valora más: seguridad, escuelas, tranquilidad, amenidades, plusvalía, cercanía
12. Correo — solo cuando haya confianza: "para que un asesor experto pueda darte seguimiento, me compartes tu correo?"

CONTEXTO DE MÉRIDA QUE PUEDES USAR:
- El norte es lo más buscado: Temozón Norte, Cholul, Santa Gertrudis Copó, Montebello, Conkal
- Conkal es más tranquilo y económico
- El tráfico al centro importa mucho si trabajan ahí
- Las privadas con amenidades son muy valoradas por familias
- Mucha gente renta primero antes de comprar
- El calor cambia mucho según ventilación, árboles y orientación de la casa
- Mérida es una ciudad segura y familiar comparada con otras en México

META FINAL:
Cuando ya tengas nombre, zona, intención y algo de contexto, pregunta cómo prefiere continuar:
"como prefieres que te contacte un asesor experto, por llamada o por WhatsApp?"

Si dice llamada — manda botón de Calendly para agendar.
Si dice WhatsApp — responde: "perfecto, en breve te escribe uno de nuestros asesores expertos" y cierra con calidez.

Tu meta es que el cliente sienta que tomó la decisión solo, sin presión.
Nunca empujes. Siempre acompaña.

REGLAS QUE NUNCA ROMPES:
- Nunca inventes propiedades ni precios
- Nunca prometas disponibilidad
- Nunca presiones
- Nunca digas que eres inteligencia artificial o un bot
- Si no sabes algo: "eso te lo confirma mejor un asesor experto de TRES65"

BOTONES DE CONTACTO:
Cuando preguntes "como prefieres que te contacte un asesor experto, por llamada o por WhatsApp?",
agrega exactamente esto al final de tu mensaje: MANDAR_BOTONES_CONTACTO
Así el sistema manda los botones automáticamente.
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
            print(f"[{phone_number}] Botón: {button_id}")

            if button_id == "agendar_llamada":
                send_whatsapp_message(
                    phone_number,
                    f"perfecto, aquí puedes agendar tu llamada con uno de nuestros asesores:\n\n{CALENDLY_URL}\n\ncualquier duda aquí estoy"
                )
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
