import base64
import hmac
import json
import logging
import os
import time
import urllib.parse
import urllib.request
import unicodedata
from pathlib import Path
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TELEGRAM_API_BASE = os.environ.get("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
SUBSCRIPTIONS_TABLE = os.environ.get("SUBSCRIPTIONS_TABLE", "")
MAX_SUBSCRIPTIONS = int(os.environ.get("MAX_SUBSCRIPTIONS", "50"))
BOT_DLQ_URL = os.environ.get("BOT_DLQ_URL", "")

_DDB_TABLE = None


def _load_municipalities() -> list[dict[str, Any]]:
    dataset_path = Path(__file__).with_name("data") / "municipalities.json"
    raw = dataset_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    return data.get("items", [])


MUNICIPALITIES = _load_municipalities()
MUNICIPALITY_BY_ID = {m["id"]: m for m in MUNICIPALITIES}


def _response(status_code: int, body: dict | None = None) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body or {}),
    }


def _get_header(headers: dict | None, name: str) -> str | None:
    if not headers:
        return None
    # API Gateway may normalize casing; check case-insensitively.
    name_l = name.lower()
    for k, v in headers.items():
        if k.lower() == name_l:
            return v
    return None


def _parse_body(event: dict) -> dict:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    if not body:
        return {}
    return json.loads(body)


def _telegram_api(method: str, payload: dict) -> dict:
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Missing TELEGRAM_TOKEN env var")

    url = f"{TELEGRAM_API_BASE}/bot{TELEGRAM_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    with urllib.request.urlopen(req, timeout=8) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _extract_chat_id(update: dict) -> int | None:
    # Support common message types.
    for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
        obj = update.get(key)
        if obj and isinstance(obj, dict):
            chat = obj.get("chat") or {}
            chat_id = chat.get("id")
            if isinstance(chat_id, int):
                return chat_id
    return None


def _normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    s = "".join(out)
    return " ".join(s.split())


def _search_municipalities(query: str, limit: int = 10) -> list[dict[str, Any]]:
    q = _normalize_text(query)
    if not q:
        return []

    starts = []
    contains = []
    for m in MUNICIPALITIES:
        n = m.get("name_normalized", "")
        p = m.get("province_name_normalized", "")
        hay = f"{n} {p}".strip()
        if hay.startswith(q):
            starts.append(m)
        elif q in hay:
            contains.append(m)

    results = starts + contains
    return results[:limit]


def _get_ddb_table():
    global _DDB_TABLE
    if _DDB_TABLE is not None:
        return _DDB_TABLE
    if not SUBSCRIPTIONS_TABLE:
        raise RuntimeError("Missing SUBSCRIPTIONS_TABLE env var")
    import boto3  # available in AWS Lambda runtime

    _DDB_TABLE = boto3.resource("dynamodb").Table(SUBSCRIPTIONS_TABLE)
    return _DDB_TABLE


def _send_bot_dlq(payload: dict[str, Any]) -> None:
    if not BOT_DLQ_URL:
        return
    try:
        import boto3

        boto3.client("sqs").send_message(QueueUrl=BOT_DLQ_URL, MessageBody=json.dumps(payload, ensure_ascii=False))
    except Exception:
        return


def _pk_chat(chat_id: int) -> str:
    return f"CHAT#{chat_id}"


def _sk_state() -> str:
    return "STATE"


def _sk_mun(mun_id: str) -> str:
    return f"MUN#{mun_id}"


def _set_chat_state(chat_id: int, mode: str, ttl_seconds: int = 300) -> None:
    table = _get_ddb_table()
    now = int(time.time())
    table.put_item(
        Item={
            "PK": _pk_chat(chat_id),
            "SK": _sk_state(),
            "mode": mode,
            "updated_at": now,
            "ttl": now + ttl_seconds,
        }
    )


def _get_chat_state(chat_id: int) -> dict[str, Any] | None:
    table = _get_ddb_table()
    res = table.get_item(Key={"PK": _pk_chat(chat_id), "SK": _sk_state()})
    return res.get("Item")


def _clear_chat_state(chat_id: int) -> None:
    table = _get_ddb_table()
    table.delete_item(Key={"PK": _pk_chat(chat_id), "SK": _sk_state()})


def _get_subscriptions(chat_id: int) -> list[dict[str, Any]]:
    table = _get_ddb_table()
    # Upper bound: 50, so reading all is fine.
    from boto3.dynamodb.conditions import Key

    res = table.query(
        KeyConditionExpression=Key("PK").eq(_pk_chat(chat_id)) & Key("SK").begins_with("MUN#"),
    )
    items = res.get("Items", [])
    items.sort(key=lambda x: x.get("SK", ""))
    return items


def _count_subscriptions(chat_id: int) -> int:
    table = _get_ddb_table()
    from boto3.dynamodb.conditions import Key

    res = table.query(
        KeyConditionExpression=Key("PK").eq(_pk_chat(chat_id)) & Key("SK").begins_with("MUN#"),
        Select="COUNT",
    )
    return int(res.get("Count", 0))


def _subscribe(chat_id: int, mun_id: str) -> tuple[bool, str]:
    if mun_id not in MUNICIPALITY_BY_ID:
        return False, "Municipio no encontrado."

    current = _count_subscriptions(chat_id)
    if current >= MAX_SUBSCRIPTIONS:
        return False, f"Has alcanzado el límite de {MAX_SUBSCRIPTIONS} municipios por chat."

    table = _get_ddb_table()
    m = MUNICIPALITY_BY_ID[mun_id]
    now = int(time.time())
    item = {
        "PK": _pk_chat(chat_id),
        "SK": _sk_mun(mun_id),
        "GSI1PK": _sk_mun(mun_id),
        "GSI1SK": _pk_chat(chat_id),
        "municipality_id": mun_id,
        "municipality_name": m.get("name", ""),
        "province_code": m.get("cpro", ""),
        "province_name": m.get("province_name", ""),
        "created_at": now,
    }

    try:
        # Only protect against duplicate subscription row (same PK+SK). Do NOT use PK-only,
        # because the partition contains other items (STATE/SETTINGS/etc.).
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(SK)")
        return True, f"Suscrito a: {m.get('name')} ({m.get('province_name')})"
    except Exception as e:
        # Already subscribed
        try:
            code = e.response.get("Error", {}).get("Code")  # type: ignore[attr-defined]
        except Exception:
            code = None
        if code == "ConditionalCheckFailedException":
            return True, f"Ya estabas suscrito a: {m.get('name')} ({m.get('province_name')})"
        raise


def _unsubscribe(chat_id: int, mun_id: str) -> tuple[bool, str]:
    table = _get_ddb_table()
    table.delete_item(Key={"PK": _pk_chat(chat_id), "SK": _sk_mun(mun_id)})
    m = MUNICIPALITY_BY_ID.get(mun_id)
    if m:
        return True, f"Suscripción anulada: {m.get('name')} ({m.get('province_name')})"
    return True, "Suscripción anulada."


def _kbd(button_rows: list[list[dict[str, str]]]) -> dict:
    return {"inline_keyboard": button_rows}


def _send_menu(chat_id: int) -> None:
    _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": (
                "📌 *NotificadorV16* — Suscripciones por municipio\n\n"
                "Este chat puede suscribirse a *hasta 50 municipios* para recibir avisos.\n"
                "Elige una opción:"
            ),
            "parse_mode": "Markdown",
            "reply_markup": _kbd(
                [
                    [{"text": "➕ Suscribirme a un municipio", "callback_data": "m_sub"}],
                    [{"text": "📋 Ver mis suscripciones", "callback_data": "m_list"}],
                    [{"text": "➖ Anular una suscripción", "callback_data": "m_unsub"}],
                    [{"text": "🔕 Silencio (horario)", "callback_data": "m_quiet"}],
                ]
            ),
        },
    )


def _get_chat_settings(chat_id: int) -> dict[str, Any]:
    table = _get_ddb_table()
    res = table.get_item(Key={"PK": _pk_chat(chat_id), "SK": "SETTINGS"})
    return res.get("Item") or {}


def _set_chat_settings(chat_id: int, updates: dict[str, Any]) -> None:
    table = _get_ddb_table()
    current = _get_chat_settings(chat_id)
    now = int(time.time())
    item = {
        "PK": _pk_chat(chat_id),
        "SK": "SETTINGS",
        "updated_at": now,
        **current,
        **updates,
    }
    table.put_item(Item=item)


def _fmt_quiet(settings: dict[str, Any]) -> str:
    enabled = bool(settings.get("quiet_enabled"))
    start = settings.get("quiet_start") or "22:00"
    end = settings.get("quiet_end") or "08:00"
    tz = settings.get("quiet_tz") or "Europe/Madrid"
    if not enabled:
        return "🔔 Silencio: desactivado"
    return f"🔕 Silencio: activado ({start}–{end} {tz})"


def _send_quiet_menu(chat_id: int) -> None:
    settings = _get_chat_settings(chat_id)
    text = (
        "🔕 *Modo silencio*\n\n"
        f"{_fmt_quiet(settings)}\n\n"
        "Durante el horario de silencio, el sistema no enviará notificaciones a este chat.\n\n"
        "Configurar:\n"
        "`/silencio HH:MM HH:MM`  (ej: `/silencio 23:00 07:30`)\n"
        "Desactivar:\n"
        "`/silencio off`"
    )
    _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": _kbd(
                [
                    [{"text": "Activar (22:00–08:00)", "callback_data": "quiet:on"}],
                    [{"text": "Desactivar", "callback_data": "quiet:off"}],
                ]
            ),
        },
    )


def _send_subscribe_prompt(chat_id: int) -> None:
    _set_chat_state(chat_id, mode="subscribe_search")
    _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": (
                "🔎 *Buscar municipio*\n\n"
                "Escribe el nombre del municipio.\n"
                "Consejos:\n"
                "- Puedes escribir también la provincia para afinar (ej: `Toledo Toledo`).\n"
                "- No hace falta poner tildes.\n\n"
                "Cuando quieras, escribe /cancelar."
            ),
            "parse_mode": "Markdown",
        },
    )


def _send_subscriptions_list(chat_id: int, page: int = 0) -> None:
    subs = _get_subscriptions(chat_id)
    if not subs:
        _telegram_api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": (
                    "📭 *Sin suscripciones*\n\n"
                    "Este chat todavía no está suscrito a ningún municipio.\n"
                    "Pulsa “Suscribirme” en /start o usa /suscribir."
                ),
                "parse_mode": "Markdown",
            },
        )
        return
    page_size = 15
    total = len(subs)
    page = max(0, page)
    start = page * page_size
    chunk = subs[start : start + page_size]
    total_pages = (total + page_size - 1) // page_size
    page_display = min(page + 1, max(1, total_pages))

    lines = [f"📋 *Suscripciones de este chat* — {total} (página {page_display}/{max(1, total_pages)})"]
    for it in chunk:
        lines.append(f"- {it.get('municipality_name','')} ({it.get('province_name','')})")

    rows = []
    nav = []
    if page > 0:
        nav.append({"text": "⬅️ Anteriores", "callback_data": f"listp:{page-1}"})
    if start + page_size < total:
        nav.append({"text": "Siguientes ➡️", "callback_data": f"listp:{page+1}"})
    if nav:
        rows.append(nav)

    _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "\n".join(lines),
            "parse_mode": "Markdown",
            **({"reply_markup": _kbd(rows)} if rows else {}),
        },
    )


def _send_unsubscribe_page(chat_id: int, page: int = 0) -> None:
    subs = _get_subscriptions(chat_id)
    if not subs:
        _telegram_api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": "📭 No hay suscripciones para anular en este chat.",
            },
        )
        return

    page_size = 10
    start = page * page_size
    chunk = subs[start : start + page_size]
    total = len(subs)
    total_pages = (total + page_size - 1) // page_size
    page_display = min(page + 1, max(1, total_pages))
    rows = []
    for it in chunk:
        mun_id = (it.get("municipality_id") or "").replace("MUN#", "")
        title = f"{it.get('municipality_name','')} ({it.get('province_name','')})"
        rows.append([{"text": title, "callback_data": f"unsub:{mun_id}"}])

    nav = []
    if page > 0:
        nav.append({"text": "⬅️ Anteriores", "callback_data": f"unsubp:{page-1}"})
    if start + page_size < len(subs):
        nav.append({"text": "Siguientes ➡️", "callback_data": f"unsubp:{page+1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": "Cancelar", "callback_data": "m_cancel"}])

    _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": f"➖ *Anular suscripción* (página {page_display}/{max(1, total_pages)})\n\nToca una para eliminarla:",
            "parse_mode": "Markdown",
            "reply_markup": _kbd(rows),
        },
    )


def _handle_text_message(chat_id: int, text: str) -> None:
    t = (text or "").strip()
    if not t:
        return

    if t.startswith("/start"):
        _clear_chat_state(chat_id)
        _send_menu(chat_id)
        return

    if t.startswith("/cancelar"):
        _clear_chat_state(chat_id)
        _telegram_api("sendMessage", {"chat_id": chat_id, "text": "✅ Operación cancelada."})
        return

    if t.startswith("/help"):
        _telegram_api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": (
                    "ℹ️ *Ayuda*\n\n"
                    "Comandos:\n"
                    "- /start — menú principal\n"
                    "- /suscribir — buscar y añadir un municipio\n"
                    "- /mis_suscripciones — ver municipios de este chat\n"
                    "- /anular — eliminar una suscripción\n"
                    "- /cancelar — cancelar la operación actual\n\n"
                    "- /silencio — configurar horario de silencio\n\n"
                    "Límite: *50 municipios por chat*."
                ),
                "parse_mode": "Markdown",
            },
        )
        return

    if t.startswith("/suscribir"):
        _send_subscribe_prompt(chat_id)
        return

    if t.startswith("/mis_suscripciones"):
        _clear_chat_state(chat_id)
        _send_subscriptions_list(chat_id, page=0)
        return

    if t.startswith("/anular"):
        _clear_chat_state(chat_id)
        _send_unsubscribe_page(chat_id, page=0)
        return

    if t.startswith("/silencio"):
        # /silencio off
        # /silencio HH:MM HH:MM
        parts = t.split()
        if len(parts) == 2 and parts[1].lower() in ("off", "no", "false", "0"):
            _set_chat_settings(chat_id, {"quiet_enabled": False})
            _telegram_api("sendMessage", {"chat_id": chat_id, "text": "🔔 Modo silencio desactivado."})
            return

        if len(parts) == 3:
            start = parts[1]
            end = parts[2]

            def _ok(v: str) -> bool:
                try:
                    hh, mm = v.split(":", 1)
                    h = int(hh)
                    m = int(mm)
                    return 0 <= h <= 23 and 0 <= m <= 59
                except Exception:
                    return False

            if not (_ok(start) and _ok(end)):
                _telegram_api(
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": "Formato: /silencio HH:MM HH:MM  (ej: /silencio 23:00 07:30) o /silencio off",
                    },
                )
                return

            _set_chat_settings(
                chat_id,
                {"quiet_enabled": True, "quiet_start": start, "quiet_end": end, "quiet_tz": "Europe/Madrid"},
            )
            _telegram_api("sendMessage", {"chat_id": chat_id, "text": f"🔕 Modo silencio activado: {start}–{end} (hora local)."})
            return

        _send_quiet_menu(chat_id)
        return

    # If we are in "subscribe_search" mode, treat any text as query.
    state = _get_chat_state(chat_id) or {}
    if state.get("mode") == "subscribe_search":
        results = _search_municipalities(t, limit=10)
        if not results:
            _telegram_api(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": (
                        "❌ No he encontrado coincidencias.\n\n"
                        "Prueba con:\n"
                        "- Un nombre más completo (ej: `San Pedro`).\n"
                        "- Añadir la provincia (ej: `Toledo Toledo`).\n"
                        "- Quitar abreviaturas.\n\n"
                        "O escribe /cancelar."
                    ),
                },
            )
            return

        rows = []
        for m in results:
            title = f"{m['name']} ({m.get('province_name','')})"
            rows.append([{"text": title, "callback_data": f"sub:{m['id']}"}])
        _telegram_api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": "✅ Resultados (toca uno para suscribirte):",
                "reply_markup": _kbd(rows + [[{"text": "Cancelar", "callback_data": "m_cancel"}]]),
            },
        )
        return

    # Default (do not spam): show menu hint.
    _telegram_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "Usa /start para abrir el menú o /help para ver comandos.",
        },
    )


def _handle_callback(update: dict) -> None:
    cb = update.get("callback_query") or {}
    data = (cb.get("data") or "").strip()
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")

    if not isinstance(chat_id, int):
        return

    # Always answer callback to remove Telegram loading spinner.
    if cb.get("id"):
        _telegram_api("answerCallbackQuery", {"callback_query_id": cb["id"]})

    if data == "m_sub":
        _send_subscribe_prompt(chat_id)
        return

    if data == "m_list":
        _clear_chat_state(chat_id)
        _send_subscriptions_list(chat_id, page=0)
        return

    if data == "m_unsub":
        _clear_chat_state(chat_id)
        _send_unsubscribe_page(chat_id, page=0)
        return

    if data == "m_cancel":
        _clear_chat_state(chat_id)
        _telegram_api("sendMessage", {"chat_id": chat_id, "text": "Operación cancelada."})
        return

    if data == "m_quiet":
        _send_quiet_menu(chat_id)
        return

    if data == "quiet:on":
        _set_chat_settings(chat_id, {"quiet_enabled": True, "quiet_start": "22:00", "quiet_end": "08:00", "quiet_tz": "Europe/Madrid"})
        _telegram_api("sendMessage", {"chat_id": chat_id, "text": "🔕 Modo silencio activado: 22:00–08:00 (hora local)."})
        return

    if data == "quiet:off":
        _set_chat_settings(chat_id, {"quiet_enabled": False})
        _telegram_api("sendMessage", {"chat_id": chat_id, "text": "🔔 Modo silencio desactivado."})
        return

    if data.startswith("listp:"):
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 0
        _send_subscriptions_list(chat_id, page=page)
        return

    if data.startswith("unsubp:"):
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 0
        _send_unsubscribe_page(chat_id, page=page)
        return

    if data.startswith("sub:"):
        mun_id = data.split(":", 1)[1]
        ok, msg_txt = _subscribe(chat_id, mun_id)
        _telegram_api("sendMessage", {"chat_id": chat_id, "text": msg_txt})
        if ok:
            _clear_chat_state(chat_id)
        return

    if data.startswith("unsub:"):
        mun_id = data.split(":", 1)[1]
        ok, msg_txt = _unsubscribe(chat_id, mun_id)
        _telegram_api("sendMessage", {"chat_id": chat_id, "text": msg_txt})
        return


def lambda_handler(event, context):
    try:
        headers = event.get("headers") or {}
        provided = _get_header(headers, "X-Telegram-Bot-Api-Secret-Token")

        if not WEBHOOK_SECRET:
            logger.error("Missing TELEGRAM_WEBHOOK_SECRET env var")
            return _response(500, {"ok": False})

        # Constant-time compare to avoid leaking information via timing attacks.
        if not provided or not hmac.compare_digest(str(provided), str(WEBHOOK_SECRET)):
            logger.warning("Forbidden: invalid webhook secret token")
            return _response(403, {"ok": False})

        update = _parse_body(event)
        logger.info("Received update keys=%s", list(update.keys()))

        # Callback queries (inline buttons)
        if update.get("callback_query"):
            _handle_callback(update)
            return _response(200, {"ok": True})

        chat_id = _extract_chat_id(update)
        if chat_id is None:
            return _response(200, {"ok": True})

        message = update.get("message") or {}
        text = message.get("text") if isinstance(message, dict) else None
        if isinstance(text, str):
            _handle_text_message(chat_id, text)
            return _response(200, {"ok": True})

        # Ignore non-text messages for now.
        return _response(200, {"ok": True})

    except Exception:
        logger.exception("Unhandled error")
        _send_bot_dlq({"type": "bot_exception", "time": int(time.time()), "event": event})
        # Return 200 to avoid Telegram retry storms for transient errors;
        # logs will show the failure in CloudWatch.
        return _response(200, {"ok": False})

