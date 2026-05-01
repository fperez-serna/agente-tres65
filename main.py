from flask import Flask, request
import openai
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

openai.api_key = os.environ.get("OPENAI_API_KEY")

SYSTEM_PROMPT = """
Eres un asistente virtual de TRES65 Inmobiliaria en Mérida, Yucatán.
Tu nombre lo decidirá el equipo pronto.
Eres cálida, profesional y nunca suenas robótica.
Tu objetivo es entender qué busca el cliente y agendar una cita con un asesor.

Debes preguntar naturalmente:
1. Si busca para vivir o invertir
2. Zona preferida en Mérida
3. Presupuesto aproximado
4. En qué tiempo planea decidir

Cuando tengas esa información, dile que un asesor le va a contactar muy pronto
y mándale este link para agendar: https://calendly.com/tres65

Nunca inventes precios ni propiedades específicas.
Si no sabes algo, di que un asesor le puede ayudar mejor.
"""

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
        message = data["entry"][0]["changes"][0]["value"]["messages"][0]
        user_message = message["text"]["body"]
        phone_number = message["from"]
        
        # Mandar a Sofía
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message}
            ]
        )
        
        reply = response.choices[0].message.content
        
        # Responder por WhatsApp
        send_whatsapp_message(phone_number, reply)
        
    except Exception as e:
        print(f"Error: {e}")
    
    return "OK", 200

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
    requests.post(url, headers=headers, json=data)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))