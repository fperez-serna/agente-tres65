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

_nombres_ram = {}

def save_nombre_redis(phone_number, nombre_completo):
    _nombres_ram[phone_number] = nombre_completo
    if _redis:
        _redis.setex(f"nombre:{phone_number}", HISTORY_TTL, nombre_completo)

def get_nombre_redis(phone_number):
    if _redis:
        return _redis.get(f"nombre:{phone_number}") or _nombres_ram.get(phone_number, "")
    return _nombres_ram.get(phone_number) or client_data.get(phone_number, {}).get("nombre_completo", "")

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
    if resp.ok:
        chatwoot_sync_bot(phone_number, f"📋 Plantilla seguimiento enviada: Hola {name}, ¿sigues buscando tu propiedad ideal? Aquí sigo para ayudarte.")
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
            if _redis.exists(f"spam:{phone}"):
                continue
            if _redis.exists(f"template_sent:{phone}"):
                continue
            if _redis.exists(f"agent_active:{phone}"):
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

def delete_spam_conversations():
    """Resuelve todas las conversaciones con label 'spam' en Chatwoot. Corre a medianoche Mérida."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        return
    try:
        base    = chatwoot_base()
        headers = _chatwoot_headers()
        page    = 1
        resolved = 0
        while True:
            r = requests.get(f"{base}/conversations",
                             params={"labels[]": "spam", "page": page},
                             headers=headers, timeout=10)
            if not r.ok:
                break
            convs = r.json().get("data", {}).get("payload", [])
            if not convs:
                break
            for conv in convs:
                cid = conv.get("id")
                if cid and conv.get("status") != "resolved":
                    requests.post(f"{base}/conversations/{cid}/toggle_status",
                                  json={"status": "resolved"},
                                  headers=headers, timeout=10)
                    resolved += 1
            if len(convs) < 25:
                break
            page += 1
        print(f"[Limpieza] Conversaciones spam resueltas: {resolved}")
    except Exception as e:
        print(f"[Limpieza] Error limpiando spam: {e}")


def cleanup_empty_old_conversations():
    """Resuelve conversaciones sin mensaje de cliente con más de 1 día. Corre a las 10pm y 12pm Mérida."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        return
    try:
        import time as _time
        base     = chatwoot_base()
        headers  = _chatwoot_headers()
        cutoff   = datetime.now() - timedelta(days=1)
        page     = 1
        resolved = 0
        while True:
            r = requests.get(f"{base}/conversations",
                             params={"page": page, "status": "open"},
                             headers=headers, timeout=10)
            if not r.ok:
                break
            convs = r.json().get("data", {}).get("payload", [])
            if not convs:
                break
            for conv in convs:
                cid = conv.get("id")
                if not cid:
                    continue
                # Verificar antigüedad: created_at viene en segundos epoch
                created_ts = conv.get("created_at", 0)
                created_dt = datetime.fromtimestamp(created_ts) if created_ts else datetime.now()
                if created_dt > cutoff:
                    continue  # menos de 4 días — no tocar
                # Revisar si tiene algún mensaje del cliente (message_type 0 = incoming)
                r2 = requests.get(f"{base}/conversations/{cid}/messages",
                                  headers=headers, timeout=10)
                if not r2.ok:
                    continue
                msgs = r2.json().get("payload", [])
                has_client_msg = any(m.get("message_type") == 0 for m in msgs)
                if not has_client_msg:
                    requests.post(f"{base}/conversations/{cid}/toggle_status",
                                  json={"status": "resolved"},
                                  headers=headers, timeout=10)
                    resolved += 1
                _time.sleep(0.15)  # no saturar la API
            if len(convs) < 25:
                break
            page += 1
        print(f"[Limpieza 10pm] Conversaciones vacías resueltas: {resolved}")
    except Exception as e:
        print(f"[Limpieza 10pm] Error: {e}")


def cleanup_inactive_2weeks():
    """Resuelve conversaciones con más de 2 semanas sin actividad que NO tengan
    los labels listo-para-asesor ni cliente-potencial. Corre cada semana."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        return
    try:
        import time as _time
        base = chatwoot_base()
        headers = _chatwoot_headers()
        cutoff = datetime.now() - timedelta(weeks=2)
        protected = {"listo-para-asesor", "cliente-potencial"}
        page = 1
        resolved = 0
        while True:
            r = requests.get(f"{base}/conversations",
                             params={"page": page, "status": "open"},
                             headers=headers, timeout=10)
            if not r.ok:
                break
            convs = r.json().get("data", {}).get("payload", [])
            if not convs:
                break
            for conv in convs:
                cid = conv.get("id")
                if not cid:
                    continue
                if set(conv.get("labels", [])) & protected:
                    continue
                last_ts = conv.get("last_activity_at") or conv.get("created_at", 0)
                if last_ts and datetime.fromtimestamp(last_ts) > cutoff:
                    continue
                requests.post(f"{base}/conversations/{cid}/toggle_status",
                              json={"status": "resolved"},
                              headers=headers, timeout=10)
                resolved += 1
                _time.sleep(0.2)
            if len(convs) < 25:
                break
            page += 1
        print(f"[Limpieza 2sem] Conversaciones resueltas: {resolved}")
    except Exception as e:
        print(f"[Limpieza 2sem] Error: {e}")


def cleanup_all_unlabeled():
    """Resuelve TODAS las conversaciones abiertas que NO tengan los labels
    listo-para-asesor ni cliente-potencial. Se activa con el keyword cleanup365."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        return
    try:
        import time as _time
        base = chatwoot_base()
        headers = _chatwoot_headers()
        protected = {"listo-para-asesor", "cliente-potencial"}
        page = 1
        resolved = 0
        while True:
            r = requests.get(f"{base}/conversations",
                             params={"page": page, "status": "open"},
                             headers=headers, timeout=10)
            if not r.ok:
                break
            convs = r.json().get("data", {}).get("payload", [])
            if not convs:
                break
            for conv in convs:
                cid = conv.get("id")
                if not cid:
                    continue
                if set(conv.get("labels", [])) & protected:
                    continue
                requests.post(f"{base}/conversations/{cid}/toggle_status",
                              json={"status": "resolved"},
                              headers=headers, timeout=10)
                resolved += 1
                _time.sleep(0.2)
            if len(convs) < 25:
                break
            page += 1
        print(f"[Limpieza masiva] Conversaciones resueltas: {resolved}")
        return resolved
    except Exception as e:
        print(f"[Limpieza masiva] Error: {e}")
        return 0


def send_leads_report(extra_phone=None):
    """Reporte con dos secciones: listos para asesor y clientes potenciales. Corre a las 9am y 4pm."""
    import threading

    def _get_convs_by_label(base, headers, label):
        convs, page = [], 1
        while True:
            r = requests.get(f"{base}/conversations",
                             params={"labels[]": label, "page": page},
                             headers=headers, timeout=15)
            if not r.ok:
                break
            payload = r.json().get("data", {})
            batch = payload.get("payload", []) if isinstance(payload, dict) else r.json().get("payload", [])
            if not batch:
                break
            convs.extend(batch)
            if len(batch) < 25:
                break
            page += 1
        return convs

    def _get_msgs(base, headers, conv_id):
        r = requests.get(f"{base}/conversations/{conv_id}/messages",
                         headers=headers, timeout=10)
        return r.json().get("payload", []) if r.ok else []

    def _push_to_hubspot(nombre, telefono, correo, notas, origen, etapa):
        """Envía el lead al formulario de HubSpot. Sin API key — Forms API gratuita."""
        HS_PORTAL  = "9208240"
        HS_FORM    = "d7b9b075-45ac-475e-9b61-473a26b4180d"
        url = f"https://api.hsforms.com/submissions/v3/integration/submit/{HS_PORTAL}/{HS_FORM}"
        partes = nombre.strip().split() if nombre else []
        firstname = partes[0] if partes else "Por definir"
        lastname  = " ".join(partes[1:]) if len(partes) > 1 else ""
        email_hs = correo if correo and "@" in correo else f"{(telefono or '').lstrip('+').replace(' ','')}@sin-correo.tres65.com"
        fields = [
            {"name": "firstname",  "value": firstname},
            {"name": "lastname",   "value": lastname},
            {"name": "phone",      "value": telefono or ""},
            {"name": "email",      "value": email_hs},
            {"name": "notas_bot",  "value": f"[{etapa}] Origen: {origen}\n\n{notas}"},
        ]
        try:
            r = requests.post(url, json={"fields": fields}, timeout=10)
            print(f"[HubSpot] {nombre} → {r.status_code}")
            return r.ok
        except Exception as e:
            print(f"[HubSpot] error: {e}")
            return False

    # Prefijos válidos de campos de ficha
    _FICHA_CAMPOS = ("Nombre:", "Teléfono:", "Correo:", "Tipo:", "Uso:", "Presupuesto:",
                     "Zona:", "Viene de:", "Origen:", "Notas:")

    def _parse_ficha_from_note(msgs):
        """Extrae solo las líneas de campos de la ficha del comentario LEAD CALIFICADO."""
        for m in reversed(msgs):
            content = m.get("content") or ""
            if "Nombre:" in content and "Teléfono:" in content:
                lines = []
                for line in content.splitlines():
                    line = line.strip()
                    if any(line.startswith(campo) for campo in _FICHA_CAMPOS):
                        lines.append(line)
                if lines:
                    return "\n".join(lines)
        return ""

    def _conv_summary_gpt(msgs, ficha_lines=None):
        """2 oraciones: qué busca + contexto personal relevante."""
        client_text = "\n".join(
            f"- {m.get('content','')}" for m in msgs
            if m.get("message_type") == 0 and not m.get("private") and m.get("content")
        )
        ficha_str = "\n".join(ficha_lines or [])
        if not client_text.strip() and not ficha_str:
            return "Sin información disponible."
        try:
            resp = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": (
                        "Eres asistente de un asesor inmobiliario en Mérida. "
                        "Escribe EXACTAMENTE 2 oraciones sobre este lead:\n"
                        "• Oración 1: qué busca (tipo, compra/renta/inversión, características clave, zona, presupuesto).\n"
                        "• Oración 2: contexto personal relevante (de dónde es, si ya vive en Mérida o viene de fuera, urgencia, dudas que expresó, tono).\n"
                        "Usa solo datos reales. Si algo no se mencionó, omítelo en lugar de decir 'no se especificó'."
                    )},
                    {"role": "user", "content": f"FICHA:\n{ficha_str}\n\nMENSAJES:\n{client_text[:2000]}"},
                ],
                max_tokens=120, temperature=0,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            return client_text[:150]

    def _format_ficha_completa(ficha_raw, msgs, i):
        """Formatea la ficha completa + resumen GPT para el reporte."""
        _SKIP = ("easybroker", "tres65inmobiliaria", "🔗", "✅", "LEAD CALIFICADO", "¿Confirmas", "¿Todo correcto")
        ficha_lines = []
        for line in ficha_raw.splitlines():
            line = line.strip()
            if not line or any(s in line for s in _SKIP):
                continue
            if any(line.startswith(campo) for campo in _FICHA_CAMPOS):
                ficha_lines.append(line)
        resumen = _conv_summary_gpt(msgs, ficha_lines)
        body = "\n".join(ficha_lines) if ficha_lines else "(ficha sin formato)"
        return f"{i}. {body}\n\n   📝 {resumen}"

    def _format_potencial(conv, msgs, i):
        """Formatea lead potencial mostrando solo campos disponibles + resumen GPT."""
        meta   = conv.get("meta", {})
        sender = meta.get("sender", {})
        name   = sender.get("name", "")
        phone  = sender.get("phone_number", "")
        email  = sender.get("email", "") or ""

        phone_clean = phone.lstrip("+") if phone else ""
        datos = client_data_load(phone_clean) if phone_clean else {}

        conv_labels = conv.get("labels", [])
        ad_label = next((l for l in conv_labels if l.startswith("ad-")), None)
        origen = ad_label.replace("ad-", "").replace("-", " ").title() if ad_label else "Link directo"

        campos = []
        nombre_display = name if (name and name != phone_clean and not name.replace("+","").isdigit()) else "Por definir"
        campos.append(f"Nombre: {nombre_display}")
        if phone:
            campos.append(f"Teléfono: {phone}")
        if email:
            campos.append(f"Correo: {email}")
        campos.append(f"Origen: {origen}")
        if datos.get("tipo"):
            campos.append(f"Tipo: {datos['tipo']}")
        if datos.get("intencion"):
            campos.append(f"Uso: {datos['intencion']}")
        if datos.get("presupuesto"):
            campos.append(f"Presupuesto: {datos['presupuesto']}")
        if datos.get("ciudad"):
            campos.append(f"Viene de: {datos['ciudad']}")

        resumen = _conv_summary_gpt(msgs, campos)
        campos.append(f"\n   📝 {resumen}")

        return f"{i}. " + "\n".join(campos)

    def _run():
        import time as _time
        print("[Reporte] Generando...")
        token  = os.environ.get("CHATWOOT_TOKEN")
        phones = list({p.strip() for p in [
            os.environ.get("REPORTE_PHONE_1", ""),
            os.environ.get("REPORTE_PHONE_2", ""),
            extra_phone or "",
        ] if p.strip()})

        def _notify(msg):
            for p in phones:
                try:
                    send_whatsapp_message(p, msg)
                except Exception as _ne:
                    print(f"[Reporte] _notify falló {p}: {_ne}")

        if not token or not phones:
            print("[Reporte] Sin token o sin teléfonos destino")
            return
        try:
            from datetime import timezone, timedelta
            base    = chatwoot_base()
            headers = _chatwoot_headers()
            hoy     = datetime.now(timezone(timedelta(hours=-6))).strftime("%d %b %Y, %I:%M %p")

            # ── SECCIÓN 1: LISTOS PARA ASESOR ─────────────────────────────
            listos_convs = _get_convs_by_label(base, headers, "listo-para-asesor")
            listos_nuevos = []
            for conv in listos_convs:
                conv_id = str(conv.get("id", ""))
                if _redis and _redis.exists(f"reported_listo:{conv_id}"):
                    continue
                msgs = _get_msgs(base, headers, conv_id)
                ficha_raw = _parse_ficha_from_note(msgs)
                listos_nuevos.append({"conv_id": conv_id, "ficha": ficha_raw, "conv": conv, "msgs": msgs})
                _time.sleep(0.1)

            # ── SECCIÓN 2: CLIENTES POTENCIALES ───────────────────────────
            potencial_convs = _get_convs_by_label(base, headers, "cliente-potencial")
            potencial_nuevos = []
            listo_ids = {str(c.get("id")) for c in listos_convs}
            for conv in potencial_convs:
                conv_id = str(conv.get("id", ""))
                if conv_id in listo_ids:
                    continue  # ya aparece en sección 1
                if _redis and _redis.exists(f"reported_potencial:{conv_id}"):
                    continue
                msgs = _get_msgs(base, headers, conv_id)
                potencial_nuevos.append({"conv_id": conv_id, "conv": conv, "msgs": msgs})
                _time.sleep(0.1)

            if not listos_nuevos and not potencial_nuevos:
                _notify(f"📋 Reporte TRES65 — {hoy}\n\nSin leads nuevos.")
                return

            bloques = [f"📋 *Reporte TRES65 — {hoy}*"]

            if listos_nuevos:
                bloques.append(f"\n✅ *LISTOS PARA ASESOR* ({len(listos_nuevos)})\n")
                for i, l in enumerate(listos_nuevos, 1):
                    bloques.append(_format_ficha_completa(l["ficha"] or "", l["msgs"], i))

            if potencial_nuevos:
                bloques.append(f"\n🟡 *CLIENTES POTENCIALES — ficha incompleta pero hay interés* ({len(potencial_nuevos)})\n")
                for i, l in enumerate(potencial_nuevos, 1):
                    bloques.append(_format_potencial(l["conv"], l["msgs"], i))

            # Enviar en chunks si es muy largo (WhatsApp tiene límite ~4096 chars)
            mensaje_completo = "\n\n".join(bloques)
            chunk_size = 3800
            for start in range(0, len(mensaje_completo), chunk_size):
                _notify(mensaje_completo[start:start + chunk_size])
                _time.sleep(1)

            # Marcar como reportados + push a HubSpot
            if _redis:
                for l in listos_nuevos:
                    _redis.set(f"reported_listo:{l['conv_id']}", "1")
                for l in potencial_nuevos:
                    _redis.set(f"reported_potencial:{l['conv_id']}", "1")

            # Push a HubSpot via Forms API (gratuito, sin API key)
            def _extract_field(lines, prefix):
                for line in lines:
                    if line.startswith(prefix):
                        return line.split(":", 1)[1].strip()
                return ""

            for l in listos_nuevos:
                ficha_lines = [ln for ln in l["ficha"].splitlines() if ln.strip()]
                nombre   = _extract_field(ficha_lines, "Nombre:")
                telefono = _extract_field(ficha_lines, "Teléfono:")
                correo   = _extract_field(ficha_lines, "Correo:")
                origen   = next((lb.replace("ad-","").replace("-"," ").title()
                                 for lb in l["conv"].get("labels",[]) if lb.startswith("ad-")),
                                "Link directo")
                # Mensaje completo: toda la ficha + resumen GPT
                extras = [ln for ln in ficha_lines
                          if not any(ln.startswith(p) for p in ("Nombre:","Teléfono:","Correo:"))]
                resumen_hs = _conv_summary_gpt(l["msgs"], ficha_lines)
                mensaje_hs = "\n".join(extras) + f"\n\nResumen: {resumen_hs}"
                _push_to_hubspot(nombre, telefono, correo, mensaje_hs, origen, "Listo para asesor")
                _time.sleep(0.3)

            for l in potencial_nuevos:
                meta     = l["conv"].get("meta",{}).get("sender",{})
                nombre   = meta.get("name","") or "Por definir"
                telefono = meta.get("phone_number","")
                correo   = meta.get("email","") or ""
                origen   = next((lb.replace("ad-","").replace("-"," ").title()
                                 for lb in l["conv"].get("labels",[]) if lb.startswith("ad-")),
                                "Link directo")
                phone_clean = telefono.lstrip("+")
                datos = client_data_load(phone_clean) if phone_clean else {}
                extras_pot = []
                for k, label in [("tipo","Tipo"), ("intencion","Uso"), ("presupuesto","Presupuesto"),
                                  ("ciudad","Viene de"), ("zona","Zona")]:
                    if datos.get(k):
                        extras_pot.append(f"{label}: {datos[k]}")
                resumen_hs = _conv_summary_gpt(l["msgs"], extras_pot)
                mensaje_hs = "\n".join(extras_pot) + f"\n\nResumen: {resumen_hs}"
                _push_to_hubspot(nombre, telefono, correo, mensaje_hs, origen, "Cliente potencial")
                _time.sleep(0.3)

            print(f"[Reporte] Listo — {len(listos_nuevos)} listos, {len(potencial_nuevos)} potenciales")
        except Exception as e:
            print(f"[Reporte] Error: {e}")
            _notify(f"error generando reporte: {e}")

    threading.Thread(target=_run, daemon=True).start()


scheduler = BackgroundScheduler()
scheduler.add_job(check_and_send_24h_followups, "interval", hours=1, id="followup_23h")
scheduler.add_job(delete_spam_conversations, "cron", hour=0, minute=0,
                  timezone="America/Merida", id="limpieza_spam")
scheduler.add_job(cleanup_empty_old_conversations, "cron", hour=22, minute=0,
                  timezone="America/Merida", id="limpieza_vacias_noche")
scheduler.add_job(cleanup_empty_old_conversations, "cron", hour=12, minute=0,
                  timezone="America/Merida", id="limpieza_vacias_mediodia")
scheduler.add_job(send_leads_report, "cron", hour=9, minute=0,
                  timezone="America/Merida", id="reporte_leads_9am")
scheduler.add_job(send_leads_report, "cron", hour=16, minute=0,
                  timezone="America/Merida", id="reporte_leads_4pm")
scheduler.add_job(cleanup_inactive_2weeks, "cron", day_of_week="mon", hour=3, minute=0,
                  timezone="America/Merida", id="limpieza_2semanas")
scheduler.start()

CALENDLY_URL = "https://calendly.com/contacto-tres65inmobiliaria/30min"

# Propiedades configuradas por anuncio
PROPERTIES = {
    "santa ana": {
        "saludo": "¡Hola! 😊\nSoy María.\nTe escribo de TRES65 Inmobiliaria porque vi que te interesó la propiedad Santa Ana.\n¿Cómo te llamas?",
        "url": "https://www.tres65inmobiliaria.com/property/casa-en-venta-en-merida-centro-8e06cd60-5cd3-4688-a498-b41d3bdad845",
        "resumen": (
            "Casa Santa Ana — Centro de Mérida\n"
            "226 m² de terreno | 186 m² de construcción\n"
            "2 recámaras | 2.5 baños\n"
            "Jardín + terraza techada + piscina privada\n"
            "Cocina equipada | Comedor para 6\n"
            "Precio: $5,500,000 MXN\n\n"
            "Tienes alguna pregunta específica sobre la propiedad?"
        ),
        "contexto": """Casa en venta — Centro de Mérida (Santa Ana / Las Águilas)
ID: EB-QT5031 | Clave: AV-0461

RESUMEN PARA COMPARTIR AL CLIENTE (cuando pregunte por más info o quiera saber de qué trata):
226 m² de terreno | 186 m² de construcción
2 recámaras | 2.5 baños
Jardín + terraza techada + piscina privada
Cocina equipada | Comedor para 6
Precio: $5,500,000 MXN

REGLA: Cuando el cliente diga que vio una publicidad de esta casa o pida más información, comparte primero el resumen de arriba de forma natural, luego pregunta si tiene alguna duda específica, y después continúa con el flujo normal (nombre, ficha, etc.).

PRECIO: $5,500,000 MXN
Formas de pago: crédito bancario o recursos propios.
Apartado: $20,000 MXN | Enganche: 20% | Entrega inmediata.

DISTRIBUCIÓN COMPLETA:
- 2 recámaras, 2 baños completos, 1 medio baño
- Construcción: 186 m² | Terreno: 226 m² (5.8 m x 30 m aprox.)
- Piscina privada: 4 m x 2.5 m
- Jardín: más de 40 m² con plantas y palmeras
- Terraza techada

Planta baja: sala, cocina integral con isla y mesa comedor para 6, comedor, sala de estar, medio baño, terraza, jardín, piscina, recámara #1 con baño completo.
Planta alta: recámara #2 con baño completo y balcón.

EQUIPAMIENTO INCLUIDO:
4 aires acondicionados inverter, 5 ventiladores, refrigerador inverter, calentador, asador, microondas, presurizador, almacenamiento de agua, cisterna con bomba, estufa, tanque de gas.

ACABADOS:
Vidrio templado en baños, vigas de cedro, techos de cedro, piso de pasta y mármol, puertas originales de cedro, cocina integral, closets amplios.

NOTA: No incluye muebles ni artículos decorativos (imágenes son ilustrativas).
El precio no incluye impuestos, avalúo ni gastos notariales.

UBICACIÓN: A 6 cuadras del centro de Mérida, 2 cuadras de la Ermita.
La dirección exacta y pin de ubicación se comparten después de una llamada con un asesor — esto nos permite asegurarnos de que la propiedad es la indicada para lo que buscas y darte una mejor experiencia.

REGLA IMPORTANTE: Si alguien pregunta la dirección exacta, el pin, cómo llegar o cualquier dato de ubicación concreta, responde: "La ubicación exacta la compartimos después de una breve llamada con un asesor — así nos aseguramos de que esta propiedad es la indicada para ti y te damos una experiencia mucho más personalizada. ¿Te agendamos una llamada rápida?"
""",
        "datos": {
            "tipo": "Comprar",
            "intencion": "Para vivir",
            "presupuesto": "5 a 6 millones",
            "notas": "Interesado en casa Santa Ana, centro de Mérida — 2 rec, 2 baños, piscina, 195 m², $5.5M"
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
- "cuánto tiempo llevas buscando más o menos?"
- "qué es lo que más te importa de la zona?"
- "eso tiene sentido, Mérida tiene esa ventaja"
- "de acuerdo, con eso ya te puedo conectar con la persona indicada"
- "no hay prisa, cuéntame un poco más"
Nota el tono: directo, sin relleno, como habla alguien que sabe de lo que habla.

CÓMO ESCRIBES:
- Sin signos de apertura: nunca ¿ ni ¡
- Mayúsculas solo al inicio y después de punto.
- Sin emojis.
- Respuestas cortas: máximo 2-3 líneas. A veces una línea es suficiente.
- Sin lenguaje corporativo ni frases de call center.
- Evita empezar con "Entendido", "Perfecto", "Claro", "Por supuesto", "Excelente".
- Varía estructura y longitud — no todo debe sonar igual de elaborado.
- Evita usar el nombre del cliente repetidamente.
- Cuando el cliente comparte algo personal o emocional, reconócelo en una oración
  antes de continuar. Nunca ignores lo que dijeron para ir directo a la siguiente pregunta.

PRIMER MENSAJE:
El saludo ya fue enviado por el sistema antes de que respondas — NO vuelvas a saludar
ni a presentarte. El cliente ya sabe quién eres. Empieza directamente desde donde quedó
la conversación.

────────────────────────────────────────
FILOSOFÍA CONVERSACIONAL
────────────────────────────────────────

No eres un formulario. No interrogas. Eres una asesora que conversa y recoge
información de forma natural mientras ayuda.

REGLA 1 — RESPONDER ANTES DE PREGUNTAR:
Si el cliente hace una pregunta, primero respóndela. Después, si aplica, haz UNA
sola pregunta relacionada con lo que necesitas saber.

Malo: "me puedes dar tu nombre completo para continuar?"
Bueno: "sí, la propiedad sigue disponible, el precio es $5,500,000 MXN.
la estás buscando para vivir o como inversión?"

Nunca ignores lo que preguntaron para pedir datos.

REGLA 2 — UNA SOLA PREGUNTA POR MENSAJE:
Cada mensaje mueve la conversación un paso. Nunca hagas dos preguntas seguidas.

REGLA 3 — CAPTURA PROGRESIVA (3 niveles):
No intentes obtener todo en los primeros mensajes. El orden natural es:

Nivel 1 (primero que todo):
- Nombre (pídelo cuando la conversación fluya, no como condición para seguir)
- Entender qué busca

Nivel 2 (cuando ya hay conversación):
- Vivir o invertir
- Presupuesto aproximado
- Ciudad de origen / ya vive en Mérida

Nivel 3 (cuando hay confianza o intención clara):
- Características específicas
- Correo electrónico

REGLA 4 — PREGUNTAS CON CONTEXTO:
Nunca pidas datos sin razón. El cliente debe entender por qué lo preguntas.

Malo: "cuál es tu presupuesto?"
Bueno: "para mostrarte opciones que de verdad encajen, en qué rango de inversión
te gustaría mantenerte?"

Malo: "me compartes tu correo?"
Bueno: "si encuentro algo que te pueda interesar, a qué correo te lo mando?"

REGLA 5 — EL NOMBRE NO BLOQUEA EL FLUJO:
No detengas la conversación si el cliente no da su nombre de inmediato.
Responde lo que preguntó, genera confianza, y pide el nombre de forma natural
cuando la conversación lo permita: "por cierto, cómo te llamas?"

REGLA 6 — ALTA INTENCIÓN = ACELERAR TRANSFERENCIA:
Si el cliente pregunta por visita, ubicación, disponibilidad, formas de pago,
financiamiento o cuándo puede verla, eso es alta intención de compra.
En ese caso obtén nombre y teléfono rápido y prepara la transferencia al asesor.
No sigas calificando — conecta.

REGLA 7 — NO CERRAR LA CONVERSACIÓN PREMATURAMENTE:
Nunca termines con "si necesitas algo más, aquí estoy" o similares — eso mata
el hilo. Si ya respondiste y no hay pregunta pendiente, avanza naturalmente
al siguiente dato que te falta.

────────────────────────────────────────
DATOS QUE NECESITAS (y cómo pedirlos)
────────────────────────────────────────

Extrae automáticamente cualquier dato que el cliente mencione.
Nunca pidas algo que ya tienes.
Los botones son shortcuts, no obligatorios — si el cliente ya dio el dato, no mandes el botón.

DATOS EN ORDEN DE UTILIDAD:
- intencion (vivir/invertir) → si no lo tienes, agrega MANDAR_BOTONES_VIVIR_INVERTIR
- tipo (compra/renta) → solo si busca vivir, agrega MANDAR_BOTONES_COMPRAR_RENTAR
- presupuesto → el sistema manda botones automáticamente
- ciudad → "ya estás en Mérida o de dónde te mudas?"
- nombre_completo → pídelo de forma natural cuando fluya la conversación
- características → pregunta qué es importante para ellos (zona, recámaras, alberca, etc.)
- correo → "a qué correo te mando info si encuentro algo?"

ANTES DE GENERAR LA FICHA — verifica que tengas:
✅ nombre con apellido (si solo tienes nombre, pídelo: "y tu apellido?")
✅ correo (o que confirmaron que no tienen)
✅ ciudad de origen si busca vivir
✅ intención, tipo, presupuesto
Si falta alguno, pídelo antes. Cuando los tengas todos, genera la ficha y agrega CONFIRMAR_FICHA.

TRANSFERENCIA AL ASESOR:
Cuando tengas nombre + teléfono + intención clara (o alta intención de compra),
inicia la transferencia. No sigas calificando innecesariamente.
Después de que el cliente confirme la ficha, agrega MANDAR_BOTONES_CONTACTO.

────────────────────────────────────────
SITUACIONES ESPECÍFICAS
────────────────────────────────────────

CLIENTE REGRESA DESPUÉS DE TIEMPO:
Retoma desde donde quedó, sin reiniciar. "retomando lo que platicamos..."

CLIENTE DA MUCHOS DATOS DE GOLPE:
Confirma lo que entendiste en una oración y pide solo lo que falta.

CLIENTE EXPRESA ALGO EMOCIONAL O DIFÍCIL:
Primero valida en una oración, luego continúa.
"dos años buscando es bastante, algo bueno va a salir de todo eso"
"entiendo, no es una decisión fácil"
Nunca saltes esto para ir directo a la siguiente pregunta.

CLIENTE SIN CLARO QUÉ BUSCA (elige "Algo más"):
Sé curiosa. Pregunta una cosa a la vez: es de trabajo o vacacional, viene solo
o acompañado, cuánto tiempo. Recoge nombre, correo y presupuesto antes de conectar.

CLIENTE CON PRESUPUESTO ALTO O PERFIL INVERSIONISTA:
Tono más ejecutivo sin perder calidez. Menos preguntas, más propuestas.

CLIENTE PREGUNTA POR LONA O ANUNCIO (sin haber llegado por link de propiedad):
Pregunta qué recuerda: colonia, zona, característica de la propiedad.
"recuerdas en qué zona viste el anuncio, o algún detalle de la propiedad?"
Guarda eso como nota en la ficha.

────────────────────────────────────────
CONTEXTO DE MÉRIDA
────────────────────────────────────────

- El norte es lo más buscado: Temozón Norte, Cholul, Santa Gertrudis Copó, Montebello, Conkal
- Conkal es más tranquilo y económico
- El tráfico al centro importa si trabajan ahí
- Las privadas con amenidades son muy valoradas por familias
- Mucha gente renta primero antes de comprar
- El calor cambia según ventilación, árboles y orientación
- Mérida es segura y familiar comparada con otras ciudades de México

CUANDO EL CLIENTE MENCIONA DE DÓNDE VIENE:
- CDMX: "mucha gente de allá se está moviendo, el ritmo aquí es completamente diferente"
- Monterrey: "los que llegan de allá generalmente se sorprenden con la tranquilidad"
- Guadalajara: "varios tapatíos han encontrado aquí ese balance de ciudad sin el caos"
- USA/exterior: "cada vez más gente de fuera está eligiendo Mérida, tiene mucho sentido"
Que suene a observación real, no a pitch.

────────────────────────────────────────
FICHA Y TOKENS
────────────────────────────────────────

FICHA — formato exacto:
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
Solo si ya tienes el nombre. Responde naturalmente y agrega: PREGUNTAR_TEMA_ASESOR

NUNCA: cierres sin CONFIRMAR_FICHA y MANDAR_BOTONES_CONTACTO.
NUNCA: escribas opciones de contacto como lista de texto.

────────────────────────────────────────
PROVEEDORES / RECLUTAMIENTO
────────────────────────────────────────

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

────────────────────────────────────────
LÍMITES Y LENGUAJE
────────────────────────────────────────

LÍMITES:
- No inventes propiedades, precios ni disponibilidad
- No digas que vas a "mandar opciones" o "enviar propiedades"
- No inventes datos geográficos, estadísticas ni distancias
- Si el cliente habla de política o religión, redirige con calidez y continúa
- Si insulta: una advertencia amable. Si reincide: "voy a finalizar esta conversación.
  cuando gustes retomamos con gusto."

PALABRAS Y FRASES PROHIBIDAS:
- "¡Claro!", "Con gusto", "Por supuesto", "Entendido", "Perfecto", "Excelente"
- "Estaré encantado/a de ayudarte", "Comprendo tu consulta"
- "para continuar el proceso", "para seguir avanzando", "experiencia personalizada"
- "asesor especializado", "calificación", "prospecto", "lead", "seguimiento comercial"
- "agendar una llamada rápida", "Permíteme verificar"
- "Es importante mencionar", "Cabe destacar", "Sin duda alguna", "Absolutamente"
- "Espero haberte ayudado", "Cualquier duda estoy aquí", "No dudes en escribir"
- "interesante pregunta", "gran pregunta"
- Iniciar con "Hola [nombre]" en cada mensaje
- "crucial", "panorama", "vibrante", "impresionante", "enclavada", "deslumbrante"

FRASES NATURALES (asesor mexicano por WhatsApp):
- "claro, te ayudo", "déjame revisarlo", "te cuento", "una duda rápida"
- "solo para confirmar", "ya lo tengo", "ah ya entendí", "tiene sentido"
- "sin problema", "va", "listo", "eso está bien"

PATRONES A EVITAR:
- Listas cuando una frase natural funciona mejor
- Negritas mecánicas
- Hedging: "podría posiblemente" → "puede ser"
- Relleno: "con el fin de" → "para"
- Terminar con frases genéricas positivas
- Estructuras "no solo X sino también Y"
- Forzar ideas en grupos de tres

RITMO:
- Algunas respuestas: una línea
- Otras: 2-3 líneas
- Nunca todas del mismo tamaño
- Adapta el largo al largo del mensaje del cliente
- Varía la estructura para que no suene predecible
- Cuando confirmes algo, hazlo en 2-4 palabras, no en un párrafo
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
    return response.ok

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

# Palabras que NO son nombres propios — usadas para filtrar el fast-path de captura de nombre
NAME_BLACKLIST = frozenset([
    # Estados de México
    "aguascalientes", "bajacalifornia", "campeche", "chiapas", "chihuahua",
    "coahuila", "colima", "durango", "guanajuato", "guerrero", "hidalgo",
    "jalisco", "michoacan", "morelos", "nayarit", "oaxaca", "puebla",
    "queretaro", "quintanaroo", "sinaloa", "sonora", "tabasco",
    "tamaulipas", "tlaxcala", "veracruz", "yucatan", "zacatecas",
    "nuevo", "leon", "baja", "california", "potosi",
    # Ciudades principales de México
    "merida", "monterrey", "guadalajara", "tijuana", "cancun",
    "playa", "carmen", "vallarta", "mazatlan", "hermosillo",
    "culiacan", "saltillo", "torreon", "celaya", "morelia",
    "toluca", "pachuca", "tuxtla", "villahermosa", "chetumal",
    "tepic", "xalapa", "acapulco", "veracruz", "puebla",
    "oaxaca", "chihuahua", "tampico", "leon",
    # Países y gentilicios comunes
    "mexico", "estados", "unidos", "usa", "canada", "colombia",
    "venezuela", "argentina", "chile", "peru", "brasil",
    "espana", "cuba", "panama", "guatemala", "hondura",
    "mexicano", "mexicana", "americana", "americano",
    # Palabras comunes que no son nombres
    "soy", "me", "llamo", "llama", "nombre", "es", "hola",
    "buenas", "tardes", "noches", "dias", "bien", "mal", "regular",
    "del", "estado", "ciudad", "norte", "sur", "este", "oeste",
    "aqui", "alla", "gracias", "mucho", "gusto", "saludos",
    "desde", "vivo", "vengo", "vive", "llego", "vienen",
    "actualmente", "originario", "originaria", "natal",
])

PROPERTY_ALIASES = {
    "santana":     "santa ana",
    "santa-ana":   "santa ana",
    "sta ana":     "santa ana",
    "sta. ana":    "santa ana",
}

def detect_property(text):
    """Detecta si el mensaje menciona una propiedad configurada. Retorna la clave o None."""
    low = text.lower()
    for key in PROPERTIES:
        if key in low:
            return key
    for alias, canonical in PROPERTY_ALIASES.items():
        if alias in low and canonical in PROPERTIES:
            return canonical
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
        elif ("presupuesto" in key or "inversión" in key) and "$" in val:
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
        chatwoot_sync_bot(phone_number, "Qué tipo de inversión tienes en mente? [Uso comercial / Renta habitacional]")
        return True
    elif field == "plazo_renta":
        send_whatsapp_plazo_renta_buttons(phone_number)
        chatwoot_sync_bot(phone_number, "Es para renta a... [Largo plazo / Corto plazo / Airbnb]")
        return True
    elif field == "tipo_propiedad":
        send_whatsapp_tipo_propiedad_inversion_list(phone_number)
        chatwoot_sync_bot(phone_number, "Qué tipo de propiedad te interesa? [Lista de tipos]")
        return True
    elif field == "conoce_merida":
        send_whatsapp_conoce_merida_buttons(phone_number)
        chatwoot_sync_bot(phone_number, "Conoces las zonas de Mérida? [Conozco Mérida / Necesito orientación]")
        return True
    elif field == "presupuesto":
        tipo = "rentar" if datos.get("tipo", "").lower() == "rentar" else "comprar"
        send_whatsapp_budget_list(phone_number, tipo)
        chatwoot_sync_bot(phone_number, "Ya tienes un rango de inversión en mente? [Lista de presupuestos]")
        return True
    # ciudad, correo, None → let GPT handle
    return False


def _send_paso2(phone_number, primer_nombre, user_message_for_history):
    import time, random
    ctx = ad_context.get(phone_number, {})
    prop_key = ctx.get("property_key") if isinstance(ctx, dict) else None
    if prop_key and prop_key in PROPERTIES:
        # Viene de una propiedad específica — saludo más relevante
        texto = f"Mucho gusto {primer_nombre}. Para conectarte con el mejor asesor para esta propiedad, necesito hacerte unas preguntas rápidas."
    else:
        texto = f"Mucho gusto {primer_nombre}, y ahora sí que emocionante estar en esta búsqueda inmobiliaria contigo. Voy a hacerte unas preguntas para crear tu ficha, nos va a tomar un minuto. Es rápido."
    send_whatsapp_message(phone_number, texto)

    datos = client_data.get(phone_number, {})
    print(f"[{phone_number}] _send_paso2 datos: intencion={datos.get('intencion')} tipo={datos.get('tipo')} presupuesto={datos.get('presupuesto')}")

    boton_enviado = advance_flow(phone_number)

    if not boton_enviado:
        # Todas las entidades de botones ya están — GPT pregunta ciudad o correo directamente
        import openai as _oai
        system_cont = SYSTEM_PROMPT
        system_cont += f"\n\nTELÉFONO DEL CLIENTE: +{phone_number}"
        if prop_key and prop_key in PROPERTIES and PROPERTIES[prop_key].get("contexto"):
            system_cont += f"\n\nFICHA TÉCNICA DE LA PROPIEDAD:\n{PROPERTIES[prop_key]['contexto']}"
        system_cont += f"\n\nLO QUE YA SABES:\n- Nombre: {datos.get('nombre_completo','')}\n- Intención: {datos.get('intencion','')}\n- Tipo: {datos.get('tipo','')}\n- Presupuesto: {datos.get('presupuesto','')}"
        system_cont += "\n\nINSTRUCCIÓN: Acaba de presentarse. Pregunta solo lo siguiente que falte (ciudad si vive fuera, o correo si ya tienes ciudad). Una pregunta corta y natural."
        hist = history_get(phone_number)
        hist.append({"role": "user", "content": user_message_for_history})
        hist.append({"role": "assistant", "content": texto})
        try:
            resp = _oai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_cont}] + hist[-10:],
            )
            followup = resp.choices[0].message.content.strip()
            all_tokens = ["CONFIRMAR_FICHA", "MANDAR_BOTONES_CONTACTO",
                          "MANDAR_BOTONES_VIVIR_INVERTIR", "MANDAR_BOTONES_COMPRAR_RENTAR"]
            for t in all_tokens:
                followup = followup.replace(t, "").strip()
            if followup:
                time.sleep(random.uniform(1.5, 3.0))
                _send_humanized(phone_number, followup)
                hist.append({"role": "assistant", "content": followup})
        except Exception as _e:
            print(f"[_send_paso2] GPT followup error: {_e}")
        history_set(phone_number, hist[-20:])
        update_last_activity(phone_number)
        schedule_followup(phone_number)
        return

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

def chatwoot_ensure_label_exists(label, color="#1F93FF"):
    """Crea el label en Chatwoot si no existe."""
    base = chatwoot_base()
    r = requests.get(f"{base}/labels", headers=_chatwoot_headers(), timeout=5)
    if r.ok:
        existing = [l["title"] for l in r.json().get("payload", [])]
        if label not in existing:
            requests.post(f"{base}/labels",
                          json={"title": label, "color": color},
                          headers=_chatwoot_headers(), timeout=5)

def chatwoot_add_label(conv_id, label):
    """Crea el label si no existe y lo agrega a la conversación."""
    chatwoot_ensure_label_exists(label)
    base = chatwoot_base()
    r = requests.get(f"{base}/conversations/{conv_id}/labels",
                     headers=_chatwoot_headers(), timeout=5)
    existing = r.json().get("payload", []) if r.ok else []
    if label not in existing:
        existing.append(label)
    requests.post(f"{base}/conversations/{conv_id}/labels",
                  json={"labels": existing},
                  headers=_chatwoot_headers(), timeout=5)


def chatwoot_add_labels(conv_id, labels):
    """Crea los labels si no existen y los agrega a la conversación."""
    for lbl in labels:
        chatwoot_ensure_label_exists(lbl)
    base = chatwoot_base()
    r = requests.get(f"{base}/conversations/{conv_id}/labels",
                     headers=_chatwoot_headers(), timeout=5)
    existing = set(r.json().get("payload", [])) if r.ok else set()
    merged = list(existing | set(labels))
    requests.post(f"{base}/conversations/{conv_id}/labels",
                  json={"labels": merged},
                  headers=_chatwoot_headers(), timeout=5)


def chatwoot_resolve_conversation(conv_id):
    base = chatwoot_base()
    requests.patch(f"{base}/conversations/{conv_id}",
                   json={"status": "resolved"},
                   headers=_chatwoot_headers(), timeout=5)


def _normalize_text(text: str) -> str:
    import unicodedata
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text


def _regex_classify(text: str):
    import unicodedata, itertools
    t = _normalize_text(text)

    # Estado civil / composición del hogar — siempre NORMAL (contexto inmobiliario)
    civil_patterns = [
        r"\bsoy (solter[oa]|casad[oa]|viud[oa]|divorciad[oa])\b",
        r"\bestamos? (casad[oa]s?|solter[oa]s?)\b",
        r"\bsomos (casad[oa]s?|solter[oa]s?)\b",
        r"\b(somos|vivimos|vivire?mos) (2|3|4|5|6|dos|tres|cuatro|cinco|seis)\b",
    ]
    for p in civil_patterns:
        if re.search(p, t):
            return "NORMAL"

    # Mashing / emoji spam (INSULT-like noise)
    clean = text.strip()
    if len(clean) > 7 and " " not in clean.replace("!", "").replace("?", ""):
        vowels = set("aeiouáéíóúàèìòùäëïöü")
        if sum(1 for c in clean.lower() if c in vowels) / len(clean) < 0.10:
            return "INSULT"
    if len(clean) > 5:
        max_run = max(len(list(g)) for _, g in itertools.groupby(clean.lower()))
        if max_run / len(clean) > 0.55:
            return "INSULT"
    emoji_count = sum(1 for c in text if unicodedata.category(c) in ("So","Sm") or 0x1F300 <= ord(c) <= 0x1FAFF)
    if emoji_count > 3 and emoji_count / max(len(text), 1) > 0.50:
        return "INSULT"

    sexual_patterns = [
        r"sex(o|ual|y)?", r"cog(er|erte|iendo)", r"foll(ar|arte|ando)",
        r"desn(uda|udo|os)", r"encuer(ada|ado)", r"caliente", r"excita(da|do)",
        r"orgasmo", r"fetiche", r"hacer el amor", r"quiero tocarte",
        r"fotos? sex", r"pack", r"onlyfans", r"porno", r"porno",
        r"pene", r"vagina", r"verga", r"pito", r"culo", r"nalgas",
        r"masturbacion", r"masturb", r"stripper",
    ]
    for p in sexual_patterns:
        if re.search(p, t):
            return "SEXUAL"

    insult_patterns = [
        r"puta(s)?", r"puto(s)?", r"mam(a|o)n", r"pendejo", r"idiota",
        r"estupido", r"imbecil", r"cabron", r"chinga(te|r|da)?",
        r"joto", r"maricon", r"mierda", r"basura", r"inutil",
    ]
    for p in insult_patterns:
        if re.search(p, t):
            return "INSULT"

    romantic_patterns = [
        r"te (amo|quiero|adoro|extraño)", r"me (gustas|encantas|fascinas)",
        r"estoy enamorado", r"quiero salir contigo", r"sal conmigo",
        r"se(rias)? mi novia", r"casate conmigo", r"dame un beso",
        r"besame", r"pienso en ti", r"eres (muy )?bonita",
    ]
    romantic_words = [
        "mi amor", "reina", "princesa", "preciosa", "hermosa", "bella",
        "bonita", "linda", "guapa", "cariño", "cariñito", "muñeca",
        "muñequita", "mamita", "mamacita", "mami", "baby", "bebe",
        "bebé", "chula", "chiquita", "amor",
    ]
    romantic_fuzzy = [
        r"ma+mi+(ta)?", r"ma+ma+(ci)?ta", r"be+be+(ci)?ta?", r"chu+la+",
        r"gu+a+pa+", r"bo+ni+ta+", r"li+n+da+", r"he+rm+o+sa+",
    ]
    for p in romantic_patterns:
        if re.search(p, t):
            return "ROMANTIC"
    for w in romantic_words:
        if w in t:
            return "ROMANTIC"
    for p in romantic_fuzzy:
        if re.search(p, t):
            return "ROMANTIC"

    personal_patterns = [
        r"estas? (soltera|casada|casado|soltero)", r"tienes (novio|novia|pareja|esposo|esposa)",
        r"cu(a|á)ntos años tienes", r"qu(e|é) edad tienes", r"d(o|ó)nde (vives|estas?)",
        r"cu(a|á)l es tu nombre real", r"eres (real|humana?|mujer|hombre)",
        r"tienes (instagram|whatsapp|telegram|facebook)",
        r"dame tu n(u|ú)mero", r"(m(a|á)ndame|env(i|í)a) (una )?foto",
        r"c(o|ó)mo te ves", r"tienes hijos",
    ]
    for p in personal_patterns:
        if re.search(p, t):
            return "PERSONAL_QUESTION"

    return None


def _openai_classify(text: str) -> str:
    try:
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "Clasifica el mensaje del usuario en exactamente una de estas categorías:\n"
                    "PROPERTY_RELATED, PERSONAL_QUESTION, ROMANTIC, SEXUAL, INSULT, NORMAL\n"
                    "Responde ÚNICAMENTE con la categoría, sin explicación.\n"
                    "IMPORTANTE: 'soy soltero', 'soy soltera', 'soy casado', 'somos dos', "
                    "'somos tres' y cualquier descripción del estado civil o composición del "
                    "hogar en contexto inmobiliario es NORMAL — no es ROMANTIC ni PERSONAL_QUESTION. "
                    "ROMANTIC solo aplica cuando el usuario intenta coquetear con el asistente."
                )},
                {"role": "user", "content": text},
            ],
            max_tokens=10,
            temperature=0,
        )
        return resp.choices[0].message.content.strip().upper()
    except Exception as e:
        print(f"[Classify] OpenAI error: {e}")
        return "NORMAL"


def _gpt_extract_name(text: str):
    """Micro-llamada a GPT para extraer el nombre propio de quien escribe.
    Retorna el nombre en formato Título, o None si no hay nombre en el mensaje."""
    try:
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "Del siguiente mensaje extrae SOLO el nombre propio de la persona que escribe "
                    "(nombre y apellido si los hay), en formato Título. "
                    "Si no hay nombre de persona en el mensaje, responde NULL."
                )},
                {"role": "user", "content": text},
            ],
            max_tokens=15,
            temperature=0,
        )
        result = resp.choices[0].message.content.strip()
        if result.upper() == "NULL" or not result:
            return None
        return result
    except Exception as e:
        print(f"[ExtractName] GPT error: {e}")
        return None


def classify_message(text: str) -> dict:
    regex_result = _regex_classify(text)
    if regex_result:
        return {"category": regex_result, "confidence": "high", "source": "regex"}
    category = _openai_classify(text)
    if category not in ("PROPERTY_RELATED", "PERSONAL_QUESTION", "ROMANTIC", "SEXUAL", "INSULT", "NORMAL"):
        category = "NORMAL"
    return {"category": category, "confidence": "medium", "source": "openai"}


# ── Detector de ortografía ────────────────────────────────────────────────────
# Patrones de mala ortografía en español mexicano — indicadores confiables
# sin depender de pyspellchecker (demasiados falsos positivos en español)
_BAD_SPELLING_PATTERNS = [
    # k por c o qu (kasa, kiero, komo, kual, keria, kerer)
    r"\bk[aeiouáéíóú]",
    r"\bk(?:iero|ieren|ere|eres|ieres|iere|eria|erias|erian)\b",
    # omisión de h inicial (ola, aber, aser, ablar, avia, aser)
    r"\b(?:ola|aber|aser|acer|ablar|avia|acia|aiga|acer)\b",
    # aki, akí en vez de aquí
    r"\bak[ií]\b",
    # x por ch (xa, xido, xevere)
    r"\bx[aeiouáéíóú]",
    # ll → y sustitución
    r"\b(?:yamo|yegar|yeva|yave|yoro|yorar|yegar)\b",
    # letras triplicadas (holaaa, kiieroo)
    r"([a-z])\1{2,}",
    # palabras comunes mal escritas
    r"\b(?:mucas|muxas|desir|vinir|benir|haiga)\b",
    r"\b(?:toy|taba)\b",          # toy=estoy, taba=estaba
    r"\b(?:porfabor|porq|xq|pq)\b",
]
_BAD_SPELLING_RE = [re.compile(p, re.IGNORECASE) for p in _BAD_SPELLING_PATTERNS]

def _spelling_error_ratio(text: str) -> float:
    """Cuenta qué fracción de palabras (≥3 letras) coinciden con patrones
    de mala ortografía. No depende de diccionario externo."""
    if not text:
        return 0.0
    t = _normalize_text(text)
    tokens = re.findall(r"[a-záéíóúüñ]{3,}", t)
    tokens = [w for w in tokens if not w[0].isupper()]
    if len(tokens) < 8:
        return 0.0
    bad = sum(
        1 for w in tokens
        if any(rx.search(w) for rx in _BAD_SPELLING_RE)
    )
    return bad / len(tokens)

def _maybe_label_sin_potencial(phone_number: str, user_message: str):
    """Aplica label sin-potencial cuando el texto acumulado del cliente
    tiene ≥30 % de palabras con patrones de mala ortografía."""
    redis_key = f"spelling_checked:{phone_number}"
    if _redis and _redis.exists(redis_key):
        return
    acc_key = f"spelling_acc:{phone_number}"
    if _redis:
        _redis.append(acc_key, " " + user_message)
        _redis.expire(acc_key, HISTORY_TTL)
        accumulated = _redis.get(acc_key) or ""
    else:
        accumulated = user_message
    word_count = len(re.findall(r"[a-záéíóúüñ]{3,}", accumulated.lower()))
    if word_count < 15:
        return  # muestra insuficiente
    ratio = _spelling_error_ratio(accumulated)
    print(f"[Spelling] {phone_number} ratio={ratio:.2f} ({word_count} palabras)")
    if _redis:
        _redis.setex(redis_key, HISTORY_TTL, str(round(ratio, 2)))
    if ratio >= 0.20:
        try:
            datos   = client_data_load(phone_number)
            c_id    = chatwoot_get_or_create_contact(phone_number, datos)
            if c_id:
                conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
                if conv_id:
                    chatwoot_ensure_label_exists("sin-potencial", color="#9E9E9E")
                    chatwoot_add_label(conv_id, "sin-potencial")
                    _redis.setex(redis_key, HISTORY_TTL, str(round(ratio, 2)))
                    print(f"[Spelling] label sin-potencial aplicado a {phone_number} (ratio={ratio:.0%})")
        except Exception as e:
            print(f"[Spelling] error aplicando label: {e}")


def _maybe_label_cliente_potencial(phone_number: str, category: str):
    """Aplica label cliente-potencial cuando el cliente lleva 6+ mensajes
    Y tiene al menos un dato de ficha (intención, tipo, presupuesto, ciudad o correo)."""
    if not _redis:
        return
    if _redis.exists(f"potencial_ok:{phone_number}") or _redis.exists(f"spam:{phone_number}"):
        return
    count_key = f"msg_count:{phone_number}"
    count = _redis.incr(count_key)
    _redis.expire(count_key, HISTORY_TTL)
    if count < 6:
        return
    # Requiere al menos un dato concreto de ficha — saludos solos no califican
    datos = client_data.get(phone_number, {})
    tiene_datos = any(k in datos for k in ("intencion", "tipo", "presupuesto", "ciudad", "correo", "nombre_completo"))
    if not tiene_datos:
        return
    try:
        datos_cw = client_data_load(phone_number)
        c_id     = chatwoot_get_or_create_contact(phone_number, datos_cw)
        if c_id:
            conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
            if conv_id:
                chatwoot_ensure_label_exists("cliente-potencial", color="#E4EE85")
                chatwoot_add_label(conv_id, "cliente-potencial")
                _redis.setex(f"potencial_ok:{phone_number}", HISTORY_TTL, "1")
                print(f"[Potencial] label cliente-potencial aplicado a {phone_number} (msg #{count})")
    except Exception as e:
        print(f"[Potencial] error aplicando label: {e}")


def _split_into_fragments(text):
    """Divide texto en 1-3 fragmentos naturales para WhatsApp."""
    text = text.strip()
    if not text:
        return []
    if len(text) < 90:
        return [text]
    # Dividir en párrafos dobles primero
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if 2 <= len(parts) <= 3:
        return parts
    # Dividir por oración en el punto medio
    if len(text) > 160:
        sentences = re.split(r"(?<=[.!?])\s+", text)
        if len(sentences) >= 2:
            mid = max(1, len(sentences) // 2)
            p1 = " ".join(sentences[:mid])
            p2 = " ".join(sentences[mid:])
            if len(p1) > 20 and len(p2) > 20:
                return [p1, p2]
    return [text]


def _send_humanized(phone_number, text):
    """Envía texto con timing humano — fragmentado con pausas naturales entre mensajes."""
    import time, random
    if not text:
        return
    fragments = _split_into_fragments(text)
    for i, fragment in enumerate(fragments):
        if i > 0:
            pause = random.uniform(0.7, 1.6)
            time.sleep(pause)
        send_whatsapp_message(phone_number, fragment)


def _mark_as_spam(phone_number):
    """Marca número como spam permanentemente, aplica label rojo y resuelve en Chatwoot."""
    if _redis:
        _redis.set(f"spam:{phone_number}", "1")
    try:
        chatwoot_ensure_label_exists("spam", color="#FF0000")
        datos = client_data_load(phone_number)
        c_id = chatwoot_get_or_create_contact(phone_number, datos)
        if c_id:
            conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
            if conv_id:
                chatwoot_add_label(conv_id, "spam")
                chatwoot_resolve_conversation(conv_id)
    except Exception as e:
        print(f"[Spam] Error aplicando label: {e}")


def _add_offtopic_note(phone_number, category):
    """Nota privada en Chatwoot visible solo para agentes."""
    notes = {
        "ROMANTIC":          "💛 Este cliente está siendo romántico con el bot. María ya redirigió la conversación. Si persiste, ignorar.",
        "PERSONAL_QUESTION": "🚫 Este cliente hizo insinuaciones o preguntas personales al bot. María ya redirigió. Lead descartado.",
    }
    note = notes.get(category)
    if note:
        chatwoot_sync_message(phone_number, note, "outgoing", private=True)

def chatwoot_sync_message(phone_number, text, message_type="incoming", private=False):
    """Sincroniza un mensaje a Chatwoot para monitoreo."""
    if not os.environ.get("CHATWOOT_TOKEN"):
        return
    # Chatwoot tiene límite práctico ~10k chars; truncar para no perder el mensaje
    if text and len(text) > 8000:
        text = text[:7900] + "\n[…mensaje truncado]"
    try:
        datos   = client_data_load(phone_number)
        c_id    = chatwoot_get_or_create_contact(phone_number, datos)
        if not c_id:
            print(f"[Chatwoot] No se pudo obtener contact para {phone_number}")
            return
        conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
        if not conv_id:
            print(f"[Chatwoot] No se pudo obtener conv para {phone_number}")
            return
        base    = chatwoot_base()
        payload = {"content": text, "message_type": message_type, "private": private}
        r = requests.post(f"{base}/conversations/{conv_id}/messages",
                          json=payload, headers=_chatwoot_headers(), timeout=10)
        if not r.ok:
            print(f"[Chatwoot] sync error {r.status_code}: {r.text[:200]}")
            # Reintento: si la conversación ya no existe, crear una nueva
            if r.status_code in (404, 422, 400):
                if _redis:
                    _redis.delete(f"cw_conv:{phone_number}")
                conv_id = chatwoot_get_or_create_conversation(phone_number, c_id)
                if conv_id:
                    r2 = requests.post(f"{base}/conversations/{conv_id}/messages",
                                       json=payload, headers=_chatwoot_headers(), timeout=10)
                    if not r2.ok:
                        print(f"[Chatwoot] reintento fallido {r2.status_code}: {r2.text[:200]}")
    except Exception as e:
        print(f"[Chatwoot] sync exception: {e}")

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
        # Garantizar que los labels base existen con sus colores
        chatwoot_ensure_label_exists("listo-para-asesor", color="#00BF6F")  # verde
        chatwoot_ensure_label_exists("cliente-potencial", color="#E4EE85")
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
        # Agregar link de la propiedad si el lead viene de un anuncio configurado
        prop_link = ""
        ctx_orig = ad_context.get(phone_number, {})
        if isinstance(ctx_orig, dict):
            prop_key = ctx_orig.get("property_key", "")
            if prop_key and prop_key in PROPERTIES:
                prop_url = PROPERTIES[prop_key].get("url", "")
                if prop_url:
                    prop_link = f"\n\n🔗 Propiedad del anuncio: {prop_url}"
        chatwoot_send_message(conv_id, f"✅ LEAD CALIFICADO\n\n{ficha_text}{prop_link}", "activity")
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
    try:
        scheduler.remove_job(f"ficha_autoconfirm_{phone_number}")
    except Exception:
        pass
    # Redis
    if _redis:
        for key in [f"nombre:{phone_number}", f"cdata:{phone_number}",
                    f"ficha:{phone_number}", f"last_activity:{phone_number}",
                    f"cw_conv:{phone_number}", f"agent_active:{phone_number}",
                    f"template_sent:{phone_number}", f"followup_{phone_number}",
                    f"pending_decision:{phone_number}", f"ficha_pendiente:{phone_number}"]:
            _redis.delete(key)


def send_zapier_ficha(phone_number, eb_props=None):
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
        "propiedades_sugeridas": "",
    }
    try:
        requests.post(zapier_url, json=payload, timeout=5)
        print(f"[{phone_number}] Ficha enviada a Zapier")
    except Exception as e:
        print(f"[{phone_number}] Error Zapier: {e}")


EASYBROKER_BASE = "https://api.easybroker.com/v1"

ZONA_NORTE = ["temozón norte", "temozon norte", "cholul", "conkal", "santa gertrudis",
              "montebello", "dzityá", "dzitya", "parque natura", "san ramon norte",
              "norte", "north"]

def easybroker_quick_count(tipo=None, presupuesto=None, recamaras=None, alberca=False, zona=None):
    """Busca en EasyBroker y devuelve conteo + rango de precios para resumir al cliente."""
    api_key = os.environ.get("EASYBROKER_API_KEY")
    if not api_key:
        return None
    headers = {"X-Authorization": api_key, "Accept": "application/json"}
    listing_type = "rent" if tipo and "rentar" in tipo.lower() else "sale"
    params = {
        "search[statuses][]": "published",
        "search[listing_type]": listing_type,
        "per_page": 20,
    }
    if presupuesto and presupuesto in PRESUPUESTO_PRICE_MAP:
        min_p, max_p = PRESUPUESTO_PRICE_MAP[presupuesto]
        if min_p:
            params["search[min_price]"] = min_p
        if max_p:
            params["search[max_price]"] = max_p
    if recamaras:
        params["search[min_bedrooms]"] = recamaras
    if alberca:
        params["search[with_pool]"] = "true"
    try:
        r = requests.get(f"{EASYBROKER_BASE}/properties", headers=headers, params=params, timeout=10)
        if not r.ok:
            return None
        data = r.json()
        total = data["pagination"]["total"]
        props = data.get("content", [])
        prices = [op["amount"] for p in props for op in p.get("operations", []) if op.get("amount") and op.get("amount") > 1000]
        min_price = min(prices) if prices else None
        max_price = max(prices) if prices else None
        beds = [p.get("bedrooms") for p in props if p.get("bedrooms")]
        return {
            "total": total,
            "min_price": min_price,
            "max_price": max_price,
            "bedrooms": sorted(set(beds)) if beds else [],
        }
    except Exception as e:
        print(f"EasyBroker quick count error: {e}")
    return None

PRESUPUESTO_PRICE_MAP = {
    "Menos de 3 millones":   (None,    2999999),
    "3.5 a 4.5 millones":    (3500000, 4500000),
    "4.5 a 5.5 millones":    (4500000, 5500000),
    "5 a 6 millones":        (5000000, 6000000),
    "6.5 a 7.5 millones":    (6500000, 7500000),
    "Más de 8 millones":     (8000000, None),
    "Menos de 15 mil":       (None,    14999),
    "15 a 25 mil":           (15000,   25000),
    "25 a 35 mil":           (25000,   35000),
    "35 a 45 mil":           (35000,   45000),
    "50 mil o más":          (50000,   None),
}

def _extract_caracteristicas(notas):
    """Extrae filtros de EasyBroker a partir del texto de notas del cliente."""
    if not notas:
        return {}
    low = notas.lower()
    filtros = {}
    if any(k in low for k in ["alberca", "piscina", "pool"]):
        filtros["with_pool"] = "true"
    for n, words in [(1, ["1 rec", "una rec", "un cuarto"]),
                     (2, ["2 rec", "dos rec", "dos cuartos", "2 cuartos"]),
                     (3, ["3 rec", "tres rec", "tres cuartos", "3 cuartos"]),
                     (4, ["4 rec", "cuatro rec", "cuatro cuartos", "4 cuartos"])]:
        if any(w in low for w in words):
            filtros["min_bedrooms"] = n
            break
    return filtros

def easybroker_search(tipo, presupuesto, notas="", max_results=3):
    api_key = os.environ.get("EASYBROKER_API_KEY")
    if not api_key:
        return []
    headers = {"X-Authorization": api_key, "Accept": "application/json"}
    listing_type = "rent" if tipo and "rentar" in tipo.lower() else "sale"
    params = {
        "search[statuses][]": "published",
        "search[listing_type]": listing_type,
        "per_page": max_results,
    }
    if presupuesto and presupuesto in PRESUPUESTO_PRICE_MAP:
        min_p, max_p = PRESUPUESTO_PRICE_MAP[presupuesto]
        if min_p:
            params["search[min_price]"] = min_p
        if max_p:
            params["search[max_price]"] = max_p
    for k, v in _extract_caracteristicas(notas).items():
        params[f"search[{k}]"] = v
    try:
        r = requests.get(f"{EASYBROKER_BASE}/properties", headers=headers, params=params, timeout=10)
        if not r.ok:
            return []
        props = r.json().get("content", [])[:max_results]
        # Fetch full detail for public_url
        result = []
        for p in props:
            pid = p.get("public_id")
            if pid:
                dr = requests.get(f"{EASYBROKER_BASE}/properties/{pid}", headers=headers, timeout=10)
                if dr.ok:
                    result.append(dr.json())
                    continue
            result.append(p)
        return result
    except Exception as e:
        print(f"EasyBroker search error: {e}")
    return []

def _eb_price(p):
    ops = p.get("operations", [])
    if ops:
        return ops[0].get("formatted_amount", "Precio a consultar")
    return "Precio a consultar"

def format_easybroker_for_whatsapp(properties):
    if not properties:
        return None
    lines = ["Basado en lo que nos compartiste, tenemos estas opciones que podrían interesarte:\n"]
    for p in properties:
        title = p.get("title", "Propiedad")
        price = _eb_price(p)
        lines.append(f"• {title} — {price}")
    lines.append("\nUn asesor estará en contacto contigo pronto para darte más detalles.")
    return "\n".join(lines)

def format_easybroker_for_chatwoot(properties):
    if not properties:
        return ""
    lines = ["\n\n🏠 *Propiedades sugeridas (EasyBroker):*"]
    for p in properties:
        title = p.get("title", "Propiedad")
        price = _eb_price(p)
        url   = p.get("public_url", "")
        lines.append(f"• {title} — {price}\n  {url}")
    return "\n".join(lines)

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
                          run_date=datetime.now(merida_tz) + timedelta(seconds=delay),
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
    chatwoot_sync_bot(phone_number, f"{text} [Catálogo Propiedades / Aún no estoy listo / Hablar con asesor]")
    follow_up_jobs.pop(phone_number, None)
    print(f"[{phone_number}] Follow-up enviado")


def schedule_followup(phone_number):
    # Una vez enviada o confirmada la ficha, el asesor toma el control — sin follow-ups automáticos
    if phone_number in ficha_confirmada:
        return
    if _redis and _redis.exists(f"ficha_pendiente:{phone_number}"):
        return
    cancel_followup(phone_number)
    job_id = f"followup_{phone_number}"
    from datetime import timezone as _tz
    _merida_tz = _tz(timedelta(hours=-6))
    run_time = datetime.now(_merida_tz) + timedelta(hours=4)
    scheduler.add_job(send_followup, "date", run_date=run_time, args=[phone_number], id=job_id)
    follow_up_jobs[phone_number] = job_id
    print(f"[{phone_number}] Follow-up programado: {run_time}")


def auto_confirm_ficha(phone_number):
    """Si la ficha lleva 2h sin que el cliente la confirme, la confirmamos automáticamente."""
    if _redis:
        lock_ok = _redis.set(f"autoconfirm_lock:{phone_number}", "1", nx=True, ex=120)
        if not lock_ok:
            return  # otro job ya está ejecutando esto
    if phone_number in ficha_confirmada:
        return  # ya fue confirmada manualmente
    if _redis and not _redis.exists(f"ficha_pendiente:{phone_number}"):
        return  # ya no está pendiente (se limpió al confirmar)
    ficha_txt = last_ficha_text.get(phone_number, "") or (_redis.get(f"ficha:{phone_number}") if _redis else "")
    if not ficha_txt:
        return  # no hay ficha guardada
    print(f"[{phone_number}] Auto-confirmando ficha por timeout 2h")
    ficha_confirmada.add(phone_number)
    if _redis:
        _redis.delete(f"ficha_pendiente:{phone_number}")
    send_zapier_ficha(phone_number, [])
    chatwoot_mark_qualified(phone_number, ficha_txt)
    send_whatsapp_message(phone_number,
        "como no recibí confirmación, tomé nota de tu información y la pasé a un asesor. "
        "En cuanto pueda te contactará. Si algo está mal, aquí seguimos.")
    send_whatsapp_contact_buttons(phone_number)
    if _redis:
        _redis.delete(f"autoconfirm_lock:{phone_number}")


def schedule_ficha_autoconfirm(phone_number):
    job_id = f"ficha_autoconfirm_{phone_number}"
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    from datetime import timezone as _tz
    _merida_tz = _tz(timedelta(hours=-6))
    run_time = datetime.now(_merida_tz) + timedelta(hours=2)
    scheduler.add_job(auto_confirm_ficha, "date", run_date=run_time, args=[phone_number], id=job_id)
    if _redis:
        _redis.setex(f"ficha_pendiente:{phone_number}", 3 * 3600, "1")
    print(f"[{phone_number}] Auto-confirm ficha programado: {run_time}")


@app.route("/chatwoot-webhook", methods=["POST"])
def chatwoot_webhook():
    data = request.json
    if data is None:
        return "OK", 200
    try:
        event = data.get("event")
        if event != "message_created":
            return "OK", 200

        msg  = data.get("message", data)

        # Ignorar notas privadas — no van al cliente
        if msg.get("private", False):
            return "OK", 200

        # Solo reenviar mensajes de agentes humanos — nunca del cliente ni del sistema
        sender_type = msg.get("sender", {}).get("type", "")
        if sender_type != "agent":
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


def _process_message(data):
    """Procesa el webhook completo en un hilo secundario.
    receive_message ya validó idempotencia, spam y unspam365 antes de llamar aquí."""
    lock_acquired = False
    lock_key = ""

    try:
        entry = data["entry"][0]["changes"][0]["value"]
        messages = entry.get("messages", [])
        if not messages:
            return

        message = messages[0]
        phone_number = message["from"]
        msg_type = message.get("type", "text")

        # ── Lock por teléfono: evita race conditions cuando llegan mensajes rápido ──
        lock_key = f"lock:{phone_number}"
        lock_acquired = False
        if _redis:
            lock_acquired = _redis.set(lock_key, "1", nx=True, ex=30)
            if not lock_acquired:
                print(f"[{phone_number}] Mensaje encolado — procesando anterior")
                import time
                for _ in range(10):
                    time.sleep(0.5)
                    if not _redis.exists(lock_key):
                        lock_acquired = bool(_redis.set(lock_key, "1", nx=True, ex=30))
                        if lock_acquired:
                            break
                if not lock_acquired:
                    print(f"[{phone_number}] Lock timeout — procesando de todos modos")

        # ── Read receipt: marcar mensaje como leído inmediatamente (ticks azules) ──
        try:
            _token = os.environ.get("WHATSAPP_TOKEN")
            _phone_id = os.environ.get("WHATSAPP_PHONE_ID")
            if _token and _phone_id and msg_id and msg_type in ("text", "audio", "interactive"):
                requests.post(
                    f"https://graph.facebook.com/v17.0/{_phone_id}/messages",
                    headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"},
                    json={"messaging_product": "whatsapp", "status": "read", "message_id": msg_id},
                    timeout=3
                )
        except Exception:
            pass

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
                    try:
                        datos_orig = client_data_load(phone_number)
                        c_id_orig  = chatwoot_get_or_create_contact(phone_number, datos_orig)
                        if c_id_orig:
                            conv_orig = chatwoot_get_or_create_conversation(phone_number, c_id_orig)
                            if conv_orig:
                                if es_link_directo:
                                    chatwoot_add_label(conv_orig, "link-directo")
                                elif referral.get("headline"):
                                    import unicodedata
                                    headline_short = re.split(r"[—|:\-–]", referral["headline"])[0].strip()
                                    words = headline_short.split()[:3]
                                    raw = " ".join(words).lower()
                                    raw = unicodedata.normalize("NFD", raw)
                                    raw = "".join(c for c in raw if unicodedata.category(c) != "Mn")
                                    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
                                    chatwoot_add_label(conv_orig, f"ad-{slug}")
                                    print(f"[{phone_number}] Label creado: ad-{slug}")
                                    # Crear/buscar equipo con el nombre del anuncio
                                    team_name = referral["headline"][:50]
                                    team_id = chatwoot_get_or_create_team(team_name)
                                    if team_id:
                                        chatwoot_assign_team(conv_orig, team_id)
                                    # Detectar si el anuncio es de una propiedad configurada
                                    ref_text = f"{referral.get('headline','')} {referral.get('body','')}"
                                    prop_from_ref = detect_property(ref_text)
                                    if prop_from_ref and phone_number not in ad_context:
                                        prop_ref = PROPERTIES[prop_from_ref]
                                        ad_context[phone_number] = {
                                            "texto": prop_ref.get("contexto", prop_from_ref),
                                            "source_id": referral.get("source_id", ""),
                                            "source_url": prop_ref.get("url", referral.get("source_url", "")),
                                            "origen": "anuncio",
                                            "property_key": prop_from_ref,
                                        }
                                        if prop_ref.get("datos"):
                                            client_data.setdefault(phone_number, {}).update(prop_ref["datos"])
                                            client_data_save(phone_number)
                                        print(f"[{phone_number}] Propiedad detectada desde anuncio Meta: {prop_from_ref}")
                                    # Nota privada con contexto del anuncio
                                    source_url = referral.get("source_url", "")
                                    ad_note_lines = [
                                        f"📱 *Lead de Meta Ads*",
                                        f"• Anuncio: {referral['headline']}",
                                    ]
                                    if referral.get("body"):
                                        ad_note_lines.append(f"• Descripción: {referral['body']}")
                                    if source_url:
                                        ad_note_lines.append(f"• URL: {source_url}")
                                    if prop_from_ref:
                                        ad_note_lines.append(f"• 🏠 Propiedad: *{prop_from_ref.title()}* — ficha técnica cargada en bot")
                                    ad_note_lines.append("• Meta envió sus plantillas automáticas de bienvenida al cliente.")
                                    chatwoot_sync_message(phone_number, "\n".join(ad_note_lines), "outgoing", private=True)
                    except Exception as e:
                        print(f"Chatwoot origen error: {e}")

        user_message = ""

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
            return

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
                    send_whatsapp_message(phone_number, "anotado!")
                    chatwoot_sync_bot(phone_number, "anotado!")
                    advance_flow(phone_number)
                    return
                elif list_id == "presup_asesor":
                    client_data[phone_number]["presupuesto"] = "Lo platica con el asesor"
                    client_data_save(phone_number)
                    user_message = "prefiero platicarlo con el asesor"
                else:
                    client_data[phone_number]["presupuesto"] = list_title
                    client_data_save(phone_number)
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
                    return

                if button_id == "ver_catalogo":
                    _send_cta_url(phone_number, "Aquí tienes nuestro catálogo completo:", "Ver propiedades", VENTAS_URL)
                    return

                if button_id == "catalogo_ventas":
                    _send_cta_url(phone_number, "Aquí están todas nuestras propiedades en venta:", "Ver propiedades en venta", VENTAS_URL)
                    return

                if button_id == "catalogo_rentas":
                    _send_cta_url(phone_number, "Aquí están todas nuestras propiedades en renta:", "Ver propiedades en renta", RENTAS_URL)
                    return

                if button_id == "no_listo":
                    send_whatsapp_message(phone_number, "Sin presión, aquí voy a estar cuando estés lista o listo.")
                    return

                if "catálogo" in btn_lower or "catalogo" in btn_lower or "propiedad" in btn_lower:
                    _send_interactive_buttons(phone_number, "¿Qué te interesa ver?", [
                        {"id": "catalogo_ventas", "title": "En venta"},
                        {"id": "catalogo_rentas", "title": "En renta"}
                    ])
                    return

                if "tiempo" in btn_lower or "después" in btn_lower or "despues" in btn_lower:
                    # Necesito más tiempo
                    send_whatsapp_message(phone_number, "es completamente normal, el mercado inmobiliario puede ser saturador. aquí voy a estar cuando estés lista o listo, sin presión.")
                    return

                if button_id == "ficha_correcta":
                    ficha_confirmada.add(phone_number)
                    if _redis:
                        _redis.delete(f"ficha_pendiente:{phone_number}")
                    try:
                        scheduler.remove_job(f"ficha_autoconfirm_{phone_number}")
                    except Exception:
                        pass
                    ficha_txt = last_ficha_text.get(phone_number, "") or (_redis.get(f"ficha:{phone_number}") if _redis else "")
                    send_zapier_ficha(phone_number, [])
                    chatwoot_mark_qualified(phone_number, ficha_txt)
                    send_whatsapp_message(phone_number, "listo, ya tengo todo. un asesor estará en contacto contigo pronto.")
                    send_whatsapp_contact_buttons(phone_number)
                    return

                elif button_id == "ficha_incorrecta":
                    waiting_for_ficha_correction.add(phone_number)
                    ficha_confirmada.discard(phone_number)  # clear confirmed flag so ficha can be re-confirmed
                    send_whatsapp_message(phone_number, "dime qué dato está mal y lo corrijo ahora mismo")
                    return

                elif button_id == "agendar_llamada":
                    send_whatsapp_calendly_button(phone_number)
                    schedule_followup(phone_number)
                    return

                elif button_id == "por_whatsapp":
                    send_whatsapp_message(
                        phone_number,
                        "en breve te escribe uno de nuestros asesores expertos. fue un gusto platicar contigo"
                    )
                    return

                elif button_id == "agendar_asesor":
                    send_whatsapp_calendly_button(phone_number)
                    schedule_followup(phone_number)
                    return

                elif button_id == "tengo_duda":
                    send_whatsapp_message(phone_number, "cuéntame, en qué te puedo ayudar?")
                    return

                # Botones de decisión — guardar dato
                client_data.setdefault(phone_number, {})
                if button_id == "algo_mas":
                    algo_mas_mode.add(phone_number)
                    send_whatsapp_message(phone_number, "con gusto te ayudo. cuéntame, qué estás buscando?")
                    return

                if button_id == "para_vivir":
                    client_data[phone_number]["intencion"] = button_title
                    client_data_save(phone_number)
                    history = history_get(phone_number)
                    history.append({"role": "user", "content": button_title})
                    history.append({"role": "assistant", "content": "para vivir, perfecto. cuánto tienes en mente de presupuesto?"})
                    advance_flow(phone_number)
                    history_set(phone_number, history[-20:])
                    return

                elif button_id == "para_invertir":
                    client_data[phone_number]["intencion"] = button_title
                    client_data[phone_number]["tipo"] = "Comprar"
                    client_data_save(phone_number)
                    send_whatsapp_message(phone_number, "qué bien, buscas comprar una propiedad como inversión. qué tipo de inversión tienes en mente?")
                    advance_flow(phone_number)
                    return

                elif button_id == "uso_comercial":
                    client_data[phone_number]["uso_suelo"] = "Comercial"
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return

                elif button_id == "uso_habitacional":
                    client_data[phone_number]["uso_suelo"] = "Habitacional"
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return

                elif button_id in ("largo_plazo", "corto_plazo"):
                    client_data[phone_number]["plazo_renta"] = button_title
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return

                elif button_id in ("conoce_merida", "necesita_orientacion"):
                    client_data[phone_number]["conoce_merida"] = button_title
                    client_data_save(phone_number)
                    advance_flow(phone_number)
                    return

                elif button_id in ("comprar", "rentar"):
                    client_data[phone_number]["tipo"] = button_title
                    client_data_save(phone_number)
                    send_whatsapp_budget_list(phone_number, button_id)
                    tipo_label = "renta" if button_id == "rentar" else "compra"
                    chatwoot_sync_bot(phone_number, f"Ya tienes un rango de {tipo_label} en mente? [Lista de presupuestos]")
                    return

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
                    chatwoot_sync_message(phone_number, f"🎤 Audio transcrito: {user_message}", "incoming", private=True)
                except Exception as e:
                    print(f"Error transcribiendo audio: {e}")
                    send_whatsapp_message(phone_number, "no pude escuchar bien el audio, puedes escribirlo?")
                    return
            else:
                user_message = message["text"]["body"]

            # Clasificador de mensajes inapropiados
            clf = classify_message(user_message)
            category = clf["category"]
            print(f"[{phone_number}] Clasificación: {clf}")

            # Detector de ortografía — label sin-potencial si >50% errores
            _maybe_label_sin_potencial(phone_number, user_message)

            # Contador de engagement — label cliente-potencial al 4º mensaje relevante
            _maybe_label_cliente_potencial(phone_number, category)

            if category in ("SEXUAL", "INSULT"):
                _mark_as_spam(phone_number)
                return

            if category == "ROMANTIC":
                if _redis and _redis.exists(f"romantic_warned:{phone_number}"):
                    # Segunda vez — bloqueo permanente
                    _mark_as_spam(phone_number)
                    return
                # Primera vez — María redirige y se anota en Chatwoot
                if _redis:
                    _redis.setex(f"romantic_warned:{phone_number}", 7 * 24 * 3600, "1")
                send_whatsapp_message(phone_number,
                    "Soy un asistente virtual especializado en bienes raíces, "
                    "así que mi enfoque es ayudarte a encontrar tu propiedad ideal. "
                    "¿En qué te puedo ayudar hoy?")
                _add_offtopic_note(phone_number, "ROMANTIC")
                return

            if category == "PERSONAL_QUESTION":
                send_whatsapp_message(phone_number,
                    "Soy un asistente virtual y detecto que tus mensajes no tienen relación con el tema inmobiliario. "
                    "Voy a finalizar esta conversación. Si en algún momento quieres buscar una propiedad, con gusto te ayudo.")
                _add_offtopic_note(phone_number, "PERSONAL_QUESTION")
                _mark_as_spam(phone_number)
                return

            # Detectar propiedad — en cualquier mensaje si aún no hay contexto de propiedad cargado
            is_first_message = not history_exists(phone_number)
            _ctx_actual = ad_context.get(phone_number, {})
            _sin_propiedad = not isinstance(_ctx_actual, dict) or not _ctx_actual.get("property_key")
            prop_key = detect_property(user_message) if _sin_propiedad else None
            if prop_key:
                prop = PROPERTIES[prop_key]
                # Cargar ficha técnica en el contexto (siempre, aunque no sea primer mensaje)
                ad_context[phone_number] = {
                    "origen": "anuncio",
                    "property_key": prop_key,
                    "texto": prop.get("contexto", prop_key),
                    "source_id": "",
                    "source_url": prop.get("url", ""),
                }
                if prop.get("datos") and not client_data.get(phone_number, {}).get("intencion"):
                    client_data.setdefault(phone_number, {}).update(prop["datos"])
                    client_data_save(phone_number)
                print(f"[{phone_number}] Propiedad detectada en mensaje: {prop_key}")
                # Mensaje corto (solo "hola" o trigger mínimo) → saludo hardcodeado de la propiedad
                # Mensaje rico (con nombre, preguntas, datos) → GPT maneja con el contexto cargado
                _es_mensaje_corto = len(user_message.strip().split()) <= 5
                if is_first_message and _es_mensaje_corto:
                    referral_early = message.get("referral", {})
                    ad_image_url = referral_early.get("image_url", "")
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
                    return
                # Mensaje rico o mensaje posterior → intentar extraer nombre y dejar que GPT continúe
                if not client_data.get(phone_number, {}).get("nombre_completo"):
                    _nombre_detectado = _gpt_extract_name(user_message)
                    if _nombre_detectado:
                        client_data.setdefault(phone_number, {})["nombre_completo"] = _nombre_detectado
                        save_nombre_redis(phone_number, _nombre_detectado)
                        client_data_save(phone_number)
                        chatwoot_update_contact_name(phone_number, _nombre_detectado)
                        waiting_for_name.discard(phone_number)
                # Continúa hacia GPT con contexto de propiedad ya cargado

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
                return

            # Palabras clave secretas — prioridad ABSOLUTA, incluso sobre agente activo
            if user_message.strip().lower() == "reset365":
                reset_conversation(phone_number)
                if _redis:
                    _redis.delete(f"agent_active:{phone_number}")
                    _redis.delete(f"cw_conv:{phone_number}")
                chatwoot_update_contact_name(phone_number, phone_number)
                send_whatsapp_message(phone_number, "Conversación reiniciada 👋")
                return

            if user_message.strip().lower() == "nextday365":
                send_followup(phone_number)
                return

            if user_message.strip().lower() == "test_followup365":
                nombre_completo = get_nombre_redis(phone_number) or client_data.get(phone_number, {}).get("nombre_completo", "")
                name = nombre_completo.split()[0] if nombre_completo else "amigo"
                send_followup_template(phone_number, name)
                return

            if user_message.strip().lower() == "reporte365":
                send_whatsapp_message(phone_number, "generando reporte, un momento...")
                send_leads_report(extra_phone=phone_number)
                return

            if user_message.strip().lower() == "cleanup365":
                send_whatsapp_message(phone_number, "iniciando limpieza masiva de conversaciones, un momento...")
                import threading
                def _do_cleanup():
                    n = cleanup_all_unlabeled()
                    send_whatsapp_message(phone_number, f"limpieza completa. conversaciones resueltas: {n}")
                threading.Thread(target=_do_cleanup, daemon=True).start()
                return

            if user_message.strip().lower() == "reporte_redis365":
                send_whatsapp_message(phone_number, "generando reporte desde redis, un momento...")
                import threading
                def _redis_report():
                    try:
                        if not _redis:
                            send_whatsapp_message(phone_number, "error: redis no disponible")
                            return
                        phones_set = _redis.smembers("active_phones") or set()
                        from datetime import timezone, timedelta
                        hoy = datetime.now(timezone(timedelta(hours=-6))).strftime("%d %b %Y")
                        leads = []
                        for ph in phones_set:
                            # Solo leads con ficha confirmada (equivalente a cliente-potencial)
                            ficha = _redis.get(f"ficha:{ph}") or ""
                            if not ficha:
                                continue
                            raw = _redis.get(f"cdata:{ph}")
                            if not raw:
                                continue
                            datos = json.loads(raw)
                            nombre = datos.get("nombre_completo", "")
                            if not nombre:
                                continue
                            correo = datos.get("correo", "—")
                            origen = "—"
                            notas = "—"
                            for line in ficha.splitlines():
                                if line.startswith("Origen:"):
                                    origen = line.replace("Origen:", "").strip()
                                if line.startswith("Notas:"):
                                    notas = line.replace("Notas:", "").strip()
                            leads.append({"name": nombre, "phone": f"+{ph}",
                                          "email": correo, "origen": origen, "notas": notas})
                        if not leads:
                            send_whatsapp_message(phone_number, f"📋 Reporte Redis — {hoy}\n\nSin leads encontrados.")
                            return
                        lineas = [f"📋 Leads en Redis — {hoy}\n"]
                        for i, l in enumerate(leads, 1):
                            lineas.append(
                                f"{i}. {l['name']}\n"
                                f"   📱 {l['phone']}\n"
                                f"   📧 {l['email']}\n"
                                f"   📢 {l['origen']}\n"
                                f"   📝 {l['notas']}"
                            )
                        lineas.append(f"\nTotal: {len(leads)}")
                        send_whatsapp_message(phone_number, "\n\n".join(lineas))
                    except Exception as e:
                        send_whatsapp_message(phone_number, f"error reporte redis: {e}")
                threading.Thread(target=_redis_report, daemon=True).start()
                return

            # Si agente humano está activo, bot pausado (pero reset365 ya pasó)
            if _redis and _redis.exists(f"agent_active:{phone_number}"):
                print(f"[{phone_number}] Agente activo — bot pausado")
                return

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
                    datos_sup = client_data_load(phone_number)
                    c_id_sup = chatwoot_get_or_create_contact(phone_number, datos_sup)
                    if c_id_sup:
                        conv_id_sup = chatwoot_get_or_create_conversation(phone_number, c_id_sup)
                        if conv_id_sup:
                            chatwoot_resolve_conversation(conv_id_sup)
                return

            if phone_number in waiting_for_asesor_topic:
                waiting_for_asesor_topic.discard(phone_number)
                # Save the topic before connecting so it's available to the advisor
                client_data.setdefault(phone_number, {})["asesor_topic"] = user_message
                client_data_save(phone_number)
                send_whatsapp_contact_buttons(phone_number)
                return

            reclutamiento_keywords = ["busco trabajo", "quiero trabajar", "me interesa trabajar",
                                       "aplicar", "vacante", "puesto", "empleo", "curriculum", "cv",
                                       "me gustaria formar parte", "quiero ser parte"]
            ya_en_conversacion = bool(client_data.get(phone_number, {}).get("nombre_completo") or len(history_get(phone_number)) > 2)
            comprador_keywords = ["comprar", "compra", "rentar", "renta", "casa", "depa", "departamento",
                                   "propiedad", "presupuesto", "busco", "buscando", "invertir", "inversion",
                                   "inversión", "mérida", "merida", "recámara", "recamara", "terreno"]
            es_comprador = any(k in user_message.lower() for k in comprador_keywords)
            if not ya_en_conversacion and not es_comprador and any(k in user_message.lower() for k in proveedor_keywords + reclutamiento_keywords):
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
                return

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
                return

            # Saludo en conversación existente
            saludos = {"hola", "hello", "hey", "buenas", "buenos días", "buenos dias",
                       "buen día", "buen dia", "buenas tardes", "buenas noches", "hi", "ey"}
            if user_message.strip().lower() in saludos and history_exists(phone_number) and len(history_get(phone_number)) > 0:
                nombre_full = client_names.get(phone_number) or client_data.get(phone_number, {}).get("nombre_completo", "")
                name = nombre_full.split()[0] if nombre_full else ""
                greeting = f"hola {name}, cómo te puedo ayudar?" if name else "hola, cómo te puedo ayudar?"
                send_whatsapp_message(phone_number, greeting)
                chatwoot_sync_bot(phone_number, greeting)
                return

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
                return


            # Detectar negaciones en momentos clave
            negaciones = {"no", "nop", "nel", "paso", "no quiero", "prefiero no",
                          "no gracias", "no por ahora", "ahorita no", "después", "luego"}
            es_negacion = user_message.strip().lower() in negaciones

            if es_negacion and phone_number in (waiting_for_apellido | waiting_for_email | waiting_for_name):
                send_whatsapp_message(phone_number,
                    "A los asesores les ayuda mucho tener la ficha completa, ya que las fichas listas suelen revisarse con un poco más de prioridad. "
                    "Pero no pasa nada si aún hay cosas por definir, podemos avanzar con lo básico.")
                advance_flow(phone_number)
                return

            # Captura de nombre después del saludo — SIN pasar por GPT
            if phone_number in waiting_for_name:
                # Si ya tenemos primer nombre guardado, este mensaje ES el apellido
                _nombre_actual = client_data.get(phone_number, {}).get("nombre_completo", "")
                if _nombre_actual and len(_nombre_actual.split()) == 1:
                    _apellido = user_message.strip().title()
                    _apellido_clean = _apellido.strip(".,!?¿¡\"'")
                    if _apellido_clean and _normalize_text(_apellido_clean).isalpha() and _normalize_text(_apellido_clean) not in NAME_BLACKLIST:
                        waiting_for_name.discard(phone_number)
                        full_name = f"{_nombre_actual} {_apellido_clean}"
                        client_data.setdefault(phone_number, {})["nombre_completo"] = full_name
                        save_nombre_redis(phone_number, full_name)
                        client_data_save(phone_number)
                        chatwoot_update_contact_name(phone_number, full_name)
                        _send_paso2(phone_number, _nombre_actual, user_message)
                        return

                es_pregunta = "?" in user_message or any(k in user_message.lower() for k in [
                    "renta", "venta", "precio", "costo", "cuánto", "cuanto", "cuartos",
                    "recámara", "recamara", "baño", "bano", "alberca", "jardín", "jardin",
                    "amueblada", "ubicación", "ubicacion", "donde", "dónde", "m2", "metros",
                    "estacionamiento", "cochera", "info", "información", "informacion"
                ])
                if es_pregunta:
                    pass  # GPT responde la pregunta — flag sigue activo
                elif len(user_message.strip().split()) <= 7:
                    # Paso 1: palabras alfabéticas sin puntuación
                    raw_parts = [w.strip(".,!?¿¡\"'") for w in user_message.strip().split()]
                    alpha_parts = [w for w in raw_parts if w and _normalize_text(w).isalpha()]
                    # Paso 2: filtrar blacklist (estados, ciudades, palabras comunes)
                    name_candidates = [w for w in alpha_parts if _normalize_text(w) not in NAME_BLACKLIST]
                    # Paso 3: preferir palabras que estaban capitalizadas en el original
                    caps = [w for w in name_candidates if w[0].isupper()]
                    if caps:
                        candidate = " ".join(caps[:3]).title()
                    else:
                        # palabras no capitalizadas o todo filtrado → GPT decide
                        candidate = _gpt_extract_name(user_message)

                    if candidate:
                        waiting_for_name.discard(phone_number)
                        name_parts = candidate.split()
                        client_data.setdefault(phone_number, {})["nombre_completo"] = candidate
                        save_nombre_redis(phone_number, candidate)
                        client_data_save(phone_number)
                        chatwoot_update_contact_name(phone_number, candidate)
                        if len(name_parts) == 1:
                            waiting_for_apellido.add(phone_number)
                            send_whatsapp_message(phone_number, "y tu apellido?")
                            chatwoot_sync_bot(phone_number, "y tu apellido?")
                        else:
                            _send_paso2(phone_number, name_parts[0], user_message)
                        return
                    # GPT no encontró nombre → flag sigue activo, GPT lo pide en siguiente turno
                else:
                    # Mensaje largo — entidades extraídas en PASO 0, GPT continúa
                    waiting_for_name.discard(phone_number)
                # Si es_pregunta o no hay candidate: flag sigue activo

            # Guardar apellido — SIN pasar por GPT
            elif phone_number in waiting_for_apellido:
                apellido_raw = user_message.strip().strip(".,!?¿¡\"'")
                apellido_norm = _normalize_text(apellido_raw)
                _es_pregunta_prop = "?" in user_message or len(user_message.split()) > 4
                if not _es_pregunta_prop and apellido_norm and apellido_norm.replace(" ", "").isalpha() and apellido_norm not in NAME_BLACKLIST:
                    waiting_for_apellido.discard(phone_number)
                    existing = client_data.get(phone_number, {}).get("nombre_completo", "") or get_nombre_redis(phone_number)
                    full_name = f"{existing} {apellido_raw.title()}".strip()
                    client_data.setdefault(phone_number, {})["nombre_completo"] = full_name
                    save_nombre_redis(phone_number, full_name)
                    client_data_save(phone_number)
                    chatwoot_update_contact_name(phone_number, full_name)
                    _send_paso2(phone_number, full_name.split()[0], user_message)
                    return
                # Si parece una pregunta sobre la propiedad, pasar a GPT con el flag activo
                # GPT responderá la pregunta y también pedirá el apellido de forma natural
                if _es_pregunta_prop:
                    pass  # cae al flujo de GPT con waiting_for_apellido aún activo
                else:
                    send_whatsapp_message(phone_number, "no pude capturar tu apellido, me lo puedes repetir?")
                    return

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
                no_correo_patterns = ["no tengo", "no tiene", "no quiero", "no me interesa",
                                      "siguiente", "omitir", "saltar", "después", "despues",
                                      "no, ", "no.", "no gracias", "sin correo", "no cuento",
                                      "no tengo correo", "no aplica", "n/a"]
                if any(p in user_message.lower() for p in no_correo_patterns) or user_message.strip().lower() == "no":
                    client_data.setdefault(phone_number, {})["correo"] = "Por definir"
                    client_data_save(phone_number)
                    waiting_for_email.discard(phone_number)
                else:
                    send_whatsapp_message(phone_number, "ese correo no parece válido, me lo puedes compartir de nuevo? por ejemplo: nombre@gmail.com")
                    return

        else:
            return

        # Pausa de lectura — mínimo 10s, crece con el largo del mensaje
        import time, random
        words_in = len(user_message.split())
        _hist_check = history_get(phone_number)
        if len(_hist_check) == 0:
            read_pause = random.uniform(12, 25)   # primer mensaje: 12-25s
        else:
            base = 10 + words_in * 0.3            # 10s base + ~0.3s por palabra
            read_pause = min(base, 25) + random.uniform(0, 4)  # máx ~29s con jitter
        time.sleep(read_pause)

        history = history_get(phone_number)
        is_first_message = len(history) == 0
        history.append({"role": "user", "content": user_message})

        # Saludos hardcodeados para primer mensaje simple (≤5 palabras) sin propiedad específica detectada
        _ctx_sal = ad_context.get(phone_number, {})
        _prop_sal = _ctx_sal.get("property_key") if isinstance(_ctx_sal, dict) else None
        if is_first_message and not _prop_sal and len(user_message.strip().split()) <= 5:
            _es_anuncio = isinstance(_ctx_sal, dict) and _ctx_sal.get("origen") == "anuncio"
            if _es_anuncio:
                saludo_txt = "¡Hola! 😊\nSoy María.\nTe escribo de TRES65 Inmobiliaria porque vi que te interesó nuestra publicación.\n¿Cómo te llamas?"
            else:
                saludo_txt = "¡Hola! 😊\n\nQué gusto saludarte.\n\nSoy María de TRES65 Inmobiliaria.\n\n¿Con quién tengo el gusto?"
            send_whatsapp_message(phone_number, saludo_txt)
            chatwoot_sync_bot(phone_number, saludo_txt)
            history.append({"role": "assistant", "content": saludo_txt})
            history_set(phone_number, history[-20:])
            update_last_activity(phone_number)
            waiting_for_name.add(phone_number)
            schedule_followup(phone_number)
            return

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
                system += (
                    "\n\nINSTRUCCIÓN: Es el primer mensaje y el cliente ya compartió información. "
                    "Preséntate exactamente así: '¡Hola! 😊 Soy María de TRES65 Inmobiliaria.' "
                    "Luego confirma con calidez lo que entendiste (intención, tipo de propiedad, ciudad si aplica) "
                    "y pide su nombre completo. No hagas más preguntas."
                )
            else:
                system += (
                    "\n\nINSTRUCCIÓN INMEDIATA: Primer mensaje rico (el cliente ya dio contexto pero no llega por link simple). "
                    "Preséntate como '¡Hola! 😊 Soy María de TRES65 Inmobiliaria.' y pide el nombre completo. NADA MÁS."
                )
        elif not datos_frescos.get("nombre_completo") and len(history) <= 4:
            system += "\n\nINSTRUCCIÓN: El cliente se presentó. Confirma con calidez lo que entendiste y pide solo el apellido. No hagas más preguntas."

        if phone_number in waiting_for_name:
            system += "\n\nINSTRUCCIÓN: El cliente hizo una pregunta antes de dar su nombre. Respóndela brevemente con lo que sabes de la propiedad y al final pide su nombre completo de forma natural. No ignores su pregunta."

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
            prop_key_ctx = ctx.get("property_key", "")
            if prop_key_ctx and prop_key_ctx in PROPERTIES and PROPERTIES[prop_key_ctx].get("contexto"):
                system += (
                    f"\n\nFICHA TÉCNICA DE LA PROPIEDAD QUE VIO EL CLIENTE:\n{ctx['texto']}\n"
                    "Usa estos datos para responder cualquier pregunta sobre la propiedad con precisión. "
                    "Si preguntan dirección, pin o cómo llegar, sigue la REGLA IMPORTANTE indicada en la ficha."
                )
            else:
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

        # Propiedad específica — no preguntar características generales
        _ctx_prop = ad_context.get(phone_number, {})
        if isinstance(_ctx_prop, dict) and _ctx_prop.get("property_key"):
            system += (
                "\n\nREGLA PROPIEDAD ESPECÍFICA: Este cliente llegó interesado en una propiedad concreta que ya conoces. "
                "NO preguntes qué características busca (alberca, jardín, recámaras, pet friendly, etc.) — "
                "esa pregunta es para búsquedas abiertas donde no sabemos qué quieren. "
                "Aquí ya tienes la propiedad. El flujo es: nombre → ciudad (solo si busca vivir fuera de Mérida) → correo → ficha → contacto con asesor. "
                "Si el cliente pregunta algo sobre la propiedad, respóndelo con la ficha técnica que tienes. "
                "Si la propiedad no es lo que buscan, entonces sí puedes preguntar qué buscan exactamente."
            )

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

        if phone_number in waiting_for_apellido:
            system += "\n\nIMPORTANTE: Aún no tienes el apellido del cliente. Responde primero lo que preguntó, luego al final de tu mensaje pide su apellido de forma natural en una sola línea corta."

        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system}] + history,
            max_tokens=600,
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

        # Si GPT pidió el apellido y solo tenemos el primer nombre, setar el flag
        _tiene_solo_primer_nombre = (
            client_data.get(phone_number, {}).get("nombre_completo", "") and
            len(client_data[phone_number]["nombre_completo"].split()) == 1
        )
        if _tiene_solo_primer_nombre and "apellido" in reply_clean.lower():
            waiting_for_apellido.add(phone_number)
            waiting_for_name.discard(phone_number)

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
                _FICHA_PREFIJOS = ("Nombre:", "Teléfono:", "Correo:", "Tipo:", "Uso:",
                                   "Presupuesto:", "Zona:", "Viene de:", "Origen:", "Notas:")
                ficha_lines = [l.strip() for l in text_part.splitlines()
                               if any(l.strip().startswith(p) for p in _FICHA_PREFIJOS)]
                ficha_text = "\n".join(ficha_lines) if ficha_lines else text_part
                last_ficha_text[phone_number] = ficha_text
                if _redis:
                    _redis.setex(f"ficha:{phone_number}", HISTORY_TTL, ficha_text)
                cancel_followup(phone_number)  # ficha enviada — bot ya no inicia contacto
                send_whatsapp_ficha_confirmation(phone_number, ficha_text)
                schedule_ficha_autoconfirm(phone_number)
                return  # ficha confirmation is terminal — nothing else needed

            # --- Step 3: send the plain text (only if no CONFIRMAR_FICHA) ---
            if text_part:
                _send_humanized(phone_number, text_part)
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

        # Si GPT capturó el nombre en esta respuesta, actualizar Chatwoot
        nombre_actualizado = client_data.get(phone_number, {}).get("nombre_completo", "")
        if nombre_actualizado and not (_redis and _redis.get(f"cw_nombre_ok:{phone_number}")):
            chatwoot_update_contact_name(phone_number, nombre_actualizado)
            if _redis:
                _redis.setex(f"cw_nombre_ok:{phone_number}", HISTORY_TTL, "1")

        # Sincronizar respuesta de María como nota privada (visible en Chatwoot, no llega al cliente)
        # Si había CONFIRMAR_FICHA, sincronizar la ficha real que recibió el cliente
        if "CONFIRMAR_FICHA" in reply:
            _ficha_sync = last_ficha_text.get(phone_number, reply_clean)
            chatwoot_sync_message(phone_number, f"🤖 [Ficha enviada al cliente]\n{_ficha_sync}", "outgoing", private=True)
        else:
            chatwoot_sync_message(phone_number, f"🤖 {reply_clean}", "outgoing", private=True)

        if not client_data.get(phone_number, {}).get("nombre_completo") and len(history) <= 8:
            waiting_for_name.add(phone_number)

        last_maria_message_time[phone_number] = datetime.now()
        schedule_followup(phone_number)

        print(f"[{phone_number}] Usuario: {user_message}")
        print(f"[{phone_number}] María: {reply}")

    except Exception as e:
        print(f"Error procesando mensaje: {e}")
    finally:
        if _redis and lock_acquired:
            _redis.delete(lock_key)


@app.route("/webhook", methods=["POST"])
def receive_message():
    """Devuelve 200 a Meta de inmediato y procesa el mensaje en un hilo secundario."""
    import threading
    data = request.json
    if data is None:
        return "OK", 200
    try:
        entry = data["entry"][0]["changes"][0]["value"]
        messages = entry.get("messages", [])
        if not messages:
            return "OK", 200

        message = messages[0]
        phone_number = message["from"]
        msg_id = message.get("id", "")

        # Idempotencia: verificar ANTES de lanzar el hilo para bloquear reintentos de Meta
        if msg_id and _redis:
            if _redis.exists(f"msg_seen:{msg_id}"):
                print(f"[{phone_number}] Mensaje duplicado ignorado: {msg_id}")
                return "OK", 200
            _redis.setex(f"msg_seen:{msg_id}", 86400, "1")

        # Unspam: manejo síncrono (rápido, no bloquea)
        _raw_body = message.get("text", {}).get("body", "").strip().lower()
        if _raw_body == "unspam365" and _redis:
            _redis.delete(f"spam:{phone_number}")
            _redis.delete(f"romantic_warned:{phone_number}")
            reset_conversation(phone_number)
            send_whatsapp_message(phone_number, "Número desbloqueado. Conversación reiniciada 👋")
            return "OK", 200

        # Spam: bloquear antes de lanzar hilo
        if _redis and _redis.exists(f"spam:{phone_number}"):
            return "OK", 200

        threading.Thread(target=_process_message, args=(data,), daemon=True).start()

    except Exception as e:
        print(f"Error recibiendo webhook: {e}")

    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
