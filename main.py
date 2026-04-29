from flask import Flask, request
import openai
import os

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

if __name__ == "__main__":
    app.run(debug=True)