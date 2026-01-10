import base64
import hmac
import json
import logging
import os
import urllib.parse
import urllib.request

logger = logging.getLogger()
logger.setLevel(logging.INFO)


TELEGRAM_API_BASE = os.environ.get("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")


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
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

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

        chat_id = _extract_chat_id(update)
        if chat_id is None:
            # Acknowledge but do nothing for unsupported update types.
            return _response(200, {"ok": True})

        _telegram_api("sendMessage", {"chat_id": chat_id, "text": "En Desarrollo"})
        return _response(200, {"ok": True})

    except Exception:
        logger.exception("Unhandled error")
        # Return 200 to avoid Telegram retry storms for transient errors;
        # logs will show the failure in CloudWatch.
        return _response(200, {"ok": False})

