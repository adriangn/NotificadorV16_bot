import json
import logging
import os
import time
import urllib.request
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable

import hmac
from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib
import random
import urllib.error

logger = logging.getLogger()
logger.setLevel(logging.INFO)


TELEGRAM_API_BASE = os.environ.get("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
SUBSCRIPTIONS_TABLE = os.environ.get("SUBSCRIPTIONS_TABLE", "")
OPS_TABLE = os.environ.get("OPS_TABLE", "")
POLLER_DLQ_URL = os.environ.get("POLLER_DLQ_URL", "")

DGT_XML_URL = os.environ.get(
    "DGT_XML_URL",
    "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml",
)

# Dedup window: how long we keep "sent" markers (seconds).
NOTIFY_TTL_SECONDS = int(os.environ.get("NOTIFY_TTL_SECONDS", str(60 * 60 * 24)))
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "NotificadorV16Bot")
POLLER_LOCK_TTL_SECONDS = int(os.environ.get("POLLER_LOCK_TTL_SECONDS", "55"))


NS = {
    "sit": "http://levelC/schema/3/situation",
    "com": "http://levelC/schema/3/common",
    "loc": "http://levelC/schema/3/locationReferencing",
    "lse": "http://levelC/schema/3/locationReferencingSpanishExtension",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}


_DDB_TABLE = None
_OPS_TABLE = None


def _get_ddb_table():
    global _DDB_TABLE
    if _DDB_TABLE is not None:
        return _DDB_TABLE
    if not SUBSCRIPTIONS_TABLE:
        raise RuntimeError("Missing SUBSCRIPTIONS_TABLE env var")
    import boto3  # available in AWS Lambda runtime

    _DDB_TABLE = boto3.resource("dynamodb").Table(SUBSCRIPTIONS_TABLE)
    return _DDB_TABLE


def _get_ops_table():
    global _OPS_TABLE
    if _OPS_TABLE is not None:
        return _OPS_TABLE
    if not OPS_TABLE:
        raise RuntimeError("Missing OPS_TABLE env var")
    import boto3

    _OPS_TABLE = boto3.resource("dynamodb").Table(OPS_TABLE)
    return _OPS_TABLE

def _send_dlq(payload: dict[str, Any]) -> None:
    if not POLLER_DLQ_URL:
        return
    try:
        import boto3

        boto3.client("sqs").send_message(QueueUrl=POLLER_DLQ_URL, MessageBody=json.dumps(payload, ensure_ascii=False))
    except Exception:
        # Best-effort only.
        return


def _log(level: str, message: str, **fields: Any) -> None:
    line = {"msg": message, **fields}
    if level == "error":
        logger.error(json.dumps(line, ensure_ascii=False))
    elif level == "warning":
        logger.warning(json.dumps(line, ensure_ascii=False))
    else:
        logger.info(json.dumps(line, ensure_ascii=False))


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
    return " ".join("".join(out).split())


def _load_municipalities() -> list[dict[str, Any]]:
    dataset_path = Path(__file__).with_name("data") / "municipalities.json"
    raw = dataset_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    return data.get("items", [])


MUNICIPALITIES = _load_municipalities()

# Some INE names carry trailing articles: "Cañiza, A" vs DGT: "A Cañiza".
_ARTICLES = {"a", "o", "os", "as", "la", "las", "el", "los", "l"}


def _article_variants(norm: str) -> set[str]:
    """
    Returns a set containing `norm` plus a swapped-article variant when applicable.
    This helps match DGT naming (leading article) with INE naming (trailing article).
    """
    norm = (norm or "").strip()
    if not norm:
        return set()
    toks = norm.split()
    out = {norm}
    if len(toks) >= 2 and toks[-1] in _ARTICLES:
        out.add(" ".join([toks[-1]] + toks[:-1]))
    if len(toks) >= 2 and toks[0] in _ARTICLES:
        out.add(" ".join(toks[1:] + [toks[0]]))
    return out


# Maps (municipality_normalized, province_normalized) -> municipality_id
MUNPROV_TO_ID: dict[tuple[str, str], str] = {}
# Maps municipality_normalized -> [municipality_id, ...] (fallback if province mismatch)
MUN_TO_IDS: dict[str, list[str]] = {}

for m in MUNICIPALITIES:
    mid = m["id"]
    mn = m.get("name_normalized", "") or _normalize_text(m.get("name", ""))
    pn = m.get("province_name_normalized", "") or _normalize_text(m.get("province_name", ""))
    for mnv in _article_variants(mn):
        for pnv in _article_variants(pn):
            if mnv and pnv:
                MUNPROV_TO_ID[(mnv, pnv)] = mid
        if mnv:
            MUN_TO_IDS.setdefault(mnv, []).append(mid)


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

def _emit_metrics(**values: int) -> None:
    """
    Emit CloudWatch metrics using Embedded Metric Format (EMF) via logs.
    """
    ts = int(time.time() * 1000)
    metric_defs = [{"Name": k, "Unit": "Count"} for k in values.keys()]
    doc = {
        "_aws": {
            "Timestamp": ts,
            "CloudWatchMetrics": [
                {
                    "Namespace": METRICS_NAMESPACE,
                    "Dimensions": [["Service"]],
                    "Metrics": metric_defs,
                }
            ],
        },
        "Service": "PollerFunction",
        **values,
    }
    # CloudWatch automatically extracts EMF metrics from logs.
    print(json.dumps(doc))

def _metrics_update_subscribed_chats(delta: int) -> None:
    """
    Maintain a global counter of chats with >=1 subscription.
    Stored in OpsTable:
      PK = METRIC#subscriptions, SK = COUNTERS, subscribed_chats (N)
    Best-effort (never breaks poller).
    """
    if delta == 0:
        return
    table = _get_ops_table()
    now = int(time.time())
    try:
        if delta < 0:
            table.update_item(
                Key={"PK": "METRIC#subscriptions", "SK": "COUNTERS"},
                UpdateExpression="ADD subscribed_chats :d SET updated_at = :now",
                # Only decrement if the counter exists and is > 0 (avoid creating negative values).
                ConditionExpression="subscribed_chats > :z",
                ExpressionAttributeValues={":d": delta, ":now": now, ":z": 0},
            )
        else:
            table.update_item(
                Key={"PK": "METRIC#subscriptions", "SK": "COUNTERS"},
                UpdateExpression="ADD subscribed_chats :d SET updated_at = :now",
                ExpressionAttributeValues={":d": delta, ":now": now},
            )
    except Exception:
        return


def _put_poller_state(run_id: str, **state: Any) -> None:
    """
    Store poller last-run state in OpsTable for /estado and debugging.
    """
    table = _get_ops_table()
    now = int(time.time())
    item = {
        "PK": "STATE#PollerFunction",
        "SK": "CURRENT",
        "run_id": run_id,
        "updated_at": now,
        **state,
    }
    table.put_item(Item=item)


def _get_subscribed_chats_metric() -> int | None:
    """
    Read global subscribed chat count from OpsTable.
    """
    try:
        res = _get_ops_table().get_item(Key={"PK": "METRIC#subscriptions", "SK": "COUNTERS"})
        item = res.get("Item") or {}
        val = item.get("subscribed_chats")
        return int(val) if val is not None else None
    except Exception:
        return None


def _retry(fn, *, tries: int, base_delay: float, max_delay: float, jitter: float = 0.2):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i == tries - 1:
                break
            delay = min(max_delay, base_delay * (2**i))
            delay = delay * (1.0 + random.uniform(-jitter, jitter))
            time.sleep(max(0.0, delay))
    raise last  # type: ignore[misc]


def _telegram_send_message(chat_id: int, text: str) -> None:
    """
    Send message with basic retry/backoff for common transient failures.
    """

    def _do():
        return _telegram_api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
        )

    def _wrapped():
        try:
            return _do()
        except urllib.error.HTTPError as e:
            code = getattr(e, "code", None)
            # Handle rate limit (429) with Retry-After header when available.
            if code == 429:
                ra = e.headers.get("Retry-After") if getattr(e, "headers", None) else None
                try:
                    wait = float(ra) if ra else 1.0
                except Exception:
                    wait = 1.0
                time.sleep(min(5.0, max(0.5, wait)))
                raise
            # Retry 5xx
            if code and 500 <= int(code) < 600:
                raise
            # Non-retryable
            raise

    _retry(_wrapped, tries=3, base_delay=0.5, max_delay=3.0)


def _telegram_error_details(exc: Exception) -> dict[str, Any]:
    """
    Return sanitized details useful for debugging Telegram API failures.
    Do NOT include message text or full payloads.
    """
    details: dict[str, Any] = {"exc_type": type(exc).__name__}
    try:
        if isinstance(exc, urllib.error.HTTPError):
            details["http_status"] = int(getattr(exc, "code", 0) or 0)
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            # Body typically contains {"ok":false,"error_code":403,"description":"..."}
            if body:
                details["body"] = body[:400]
            ra = exc.headers.get("Retry-After") if getattr(exc, "headers", None) else None
            if ra:
                details["retry_after"] = ra
    except Exception:
        pass
    return details

def _is_permanent_telegram_chat_error(details: dict[str, Any]) -> bool:
    """
    Return True for failures that indicate the chat will never be deliverable
    unless the user re-starts/unblocks the bot.
    """
    status = int(details.get("http_status") or 0)
    body = (details.get("body") or "").lower()
    if status == 403 and ("bot was blocked by the user" in body or "bot was kicked" in body):
        return True
    if status == 400 and ("chat not found" in body or "group chat was upgraded" in body):
        return True
    return False


def _purge_chat_subscriptions(chat_id: int) -> int:
    """
    Delete all subscription rows for a chat (PK=CHAT#<id>, SK begins_with MUN#).
    Returns number of deleted rows (best-effort).
    """
    table = _get_ddb_table()
    from boto3.dynamodb.conditions import Key

    deleted = 0
    res = table.query(
        KeyConditionExpression=Key("PK").eq(f"CHAT#{chat_id}") & Key("SK").begins_with("MUN#"),
    )
    items = res.get("Items") or []
    if not items:
        return 0
    with table.batch_writer() as batch:
        for it in items:
            pk = it.get("PK")
            sk = it.get("SK")
            if isinstance(pk, str) and isinstance(sk, str):
                batch.delete_item(Key={"PK": pk, "SK": sk})
                deleted += 1
    return deleted


def _purge_chat_ops(chat_id: int) -> None:
    """
    Best-effort cleanup of OpsTable items for a chat (settings/state) to reduce noise.
    """
    try:
        ops = _get_ops_table()
        ops.delete_item(Key={"PK": f"CHAT#{chat_id}", "SK": "SETTINGS"})
        ops.delete_item(Key={"PK": f"CHAT#{chat_id}", "SK": "STATE"})
    except Exception:
        return


def _fetch_dgt_xml() -> bytes:
    def _do():
        req = urllib.request.Request(DGT_XML_URL, method="GET")
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read()

    return _retry(_do, tries=3, base_delay=0.8, max_delay=5.0)


def _iter_v16_events(xml_bytes: bytes) -> Iterable[dict[str, Any]]:
    """
    Yield events that match the V16-like format shown by the user.
    """
    root = ET.fromstring(xml_bytes)
    for sit in root.findall(".//sit:situation", NS):
        situation_id = sit.attrib.get("id")
        rec = sit.find("./sit:situationRecord", NS)
        if rec is None:
            continue
        xsi_type = rec.attrib.get(f"{{{NS['xsi']}}}type", "")
        if xsi_type != "sit:GenericSituationRecord":
            continue

        validity_status = rec.findtext("./sit:validity/com:validityStatus", default="", namespaces=NS)
        if validity_status != "active":
            continue

        cause_type = rec.findtext("./sit:cause/sit:causeType", default="", namespaces=NS)
        vehicle_type = rec.findtext(
            "./sit:cause/sit:detailedCauseType/sit:vehicleObstructionType",
            default="",
            namespaces=NS,
        )
        if cause_type != "vehicleObstruction" or vehicle_type != "vehicleStuck":
            continue

        road = rec.findtext(
            "./sit:locationReference/loc:supplementaryPositionalDescription/loc:roadInformation/loc:roadName",
            default="",
            namespaces=NS,
        )

        lat = rec.findtext(".//loc:pointCoordinates/loc:latitude", default="", namespaces=NS)
        lon = rec.findtext(".//loc:pointCoordinates/loc:longitude", default="", namespaces=NS)

        # Spanish extension provides municipality/province inside loc:extendedTpegNonJunctionPoint
        municipality = rec.findtext(
            ".//loc:extendedTpegNonJunctionPoint/lse:municipality", default="", namespaces=NS
        )
        province = rec.findtext(
            ".//loc:extendedTpegNonJunctionPoint/lse:province", default="", namespaces=NS
        )
        km = rec.findtext(
            ".//loc:extendedTpegNonJunctionPoint/lse:kilometerPoint", default="", namespaces=NS
        )
        start_time = rec.findtext("./sit:validity/com:validityTimeSpecification/com:overallStartTime", default="", namespaces=NS)
        creation_ref = rec.findtext("./sit:situationRecordCreationReference", default="", namespaces=NS)

        record_id = rec.attrib.get("id") or situation_id or ""
        if not (record_id and municipality and province):
            continue

        yield {
            "record_id": record_id,
            "situation_id": situation_id or "",
            "creation_ref": creation_ref,
            "municipality": municipality,
            "province": province,
            "road": road,
            "km": km,
            "start_time": start_time,
            "lat": lat,
            "lon": lon,
        }


def _municipality_id_from_names(municipality: str, province: str) -> str | None:
    mn = _normalize_text(municipality)
    pn = _normalize_text(province)

    for mnv in _article_variants(mn):
        for pnv in _article_variants(pn):
            mid = MUNPROV_TO_ID.get((mnv, pnv))
            if mid:
                return mid

    # Fallback: if province naming differs, use unique municipality match.
    for mnv in _article_variants(mn):
        candidates = MUN_TO_IDS.get(mnv, [])
        if len(candidates) == 1:
            return candidates[0]
    return None


def _query_subscribed_chats(municipality_id: str) -> list[int]:
    table = _get_ddb_table()
    from boto3.dynamodb.conditions import Key

    res = table.query(
        IndexName="GSI1",
        KeyConditionExpression=Key("GSI1PK").eq(f"MUN#{municipality_id}"),
    )
    items = res.get("Items", [])
    chat_ids: list[int] = []
    for it in items:
        pk = it.get("PK", "")
        if isinstance(pk, str) and pk.startswith("CHAT#"):
            try:
                chat_ids.append(int(pk.split("#", 1)[1]))
            except Exception:
                pass
    return chat_ids


def _get_chat_settings(chat_id: int) -> dict[str, Any]:
    """
    Settings are stored in OpsTable:
      PK = CHAT#<chat_id>, SK = SETTINGS
    """
    key = {"PK": f"CHAT#{chat_id}", "SK": "SETTINGS"}
    res = _get_ops_table().get_item(Key=key)
    return res.get("Item") or {}


def _parse_hhmm(value: str) -> tuple[int, int] | None:
    try:
        hh, mm = value.split(":", 1)
        h = int(hh)
        m = int(mm)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h, m
    except Exception:
        return None
    return None


def _is_quiet_now(settings: dict[str, Any]) -> bool:
    if not settings.get("quiet_enabled"):
        return False
    mode = settings.get("quiet_mode") or "window"
    if mode == "always":
        return True
    start = settings.get("quiet_start")  # "HH:MM"
    end = settings.get("quiet_end")      # "HH:MM"
    tz = settings.get("quiet_tz") or "Europe/Madrid"
    if not (isinstance(start, str) and isinstance(end, str)):
        return False
    s = _parse_hhmm(start)
    e = _parse_hhmm(end)
    if not s or not e:
        return False
    now = datetime.now(ZoneInfo(tz))
    now_m = now.hour * 60 + now.minute
    s_m = s[0] * 60 + s[1]
    e_m = e[0] * 60 + e[1]
    if s_m == e_m:
        return True  # full-day silence
    if s_m < e_m:
        return s_m <= now_m < e_m
    # spans midnight
    return now_m >= s_m or now_m < e_m


def _dedupe_mark_sent(record_id: str, chat_id: int) -> bool:
    """
    Return True if we should send (first time), False if already sent recently.

    We store one item per (record_id, chat_id) with TTL so duplicates across
    consecutive XML fetches do not re-notify.
    """
    # Dedupe markers belong to ops/state table.
    now = int(time.time())
    pk = f"EVENT#{record_id}"
    sk = f"CHAT#{chat_id}"

    # Use constant-time compare for paranoia in case future refactors touch secrets.
    _ = hmac.compare_digest("a", "a")

    table = _get_ops_table()
    try:
        table.put_item(
            Item={"PK": pk, "SK": sk, "ttl": now + NOTIFY_TTL_SECONDS, "created_at": now},
            # PutItem conditions are evaluated against the *existing item with the same (PK, SK)*.
            # Using SK here makes it explicit that the uniqueness is per (event, chat).
            # Be explicit for composite keys: the marker is unique per (PK, SK).
            ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
        )
        return True
    except Exception as e:
        # Conditional check failed or other DDB error; be conservative and do not spam.
        try:
            # botocore ClientError
            code = e.response.get("Error", {}).get("Code")  # type: ignore[attr-defined]
        except Exception:
            code = None
        if code == "ConditionalCheckFailedException":
            return False
        logger.warning("Dedupe put failed for %s/%s: %s", pk, sk, e)
        return False


def _format_road_km(road: str, km: str) -> str:
    road = (road or "").strip()
    km = (km or "").strip()
    if road and km:
        return f"{road} km {km}"
    if road:
        return road
    if km:
        return f"km {km}"
    return ""


def _acquire_poller_lock(owner: str) -> bool:
    """
    Prevent overlapping poller runs without using ReservedConcurrentExecutions.
    Uses a DynamoDB conditional put with TTL.
    """
    table = _get_ops_table()
    now = int(time.time())
    pk = "LOCK#PollerFunction"
    sk = "RUN"
    try:
        # IMPORTANT: DynamoDB TTL expiry is asynchronous, so we must not rely on the
        # item disappearing to consider the lock released. We allow acquiring the
        # lock if it doesn't exist OR it exists but its ttl has expired.
        table.put_item(
            Item={
                "PK": pk,
                "SK": sk,
                "ttl": now + POLLER_LOCK_TTL_SECONDS,
                "created_at": now,
                "owner": owner,
            },
            # NOTE: "ttl" is a reserved keyword in DynamoDB expressions.
            ConditionExpression="attribute_not_exists(PK) OR #ttl < :now",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":now": now},
        )
        return True
    except Exception as e:
        try:
            code = e.response.get("Error", {}).get("Code")  # type: ignore[attr-defined]
        except Exception:
            code = None
        if code == "ConditionalCheckFailedException":
            return False
        logger.warning("Lock put failed: %s", e)
        return False


def _release_poller_lock(owner: str) -> None:
    """
    Best-effort lock release. Not strictly required thanks to ttl-based acquisition,
    but helps reduce skipped runs when TTL deletion is delayed.
    """
    try:
        # Only release if we still own the lock to avoid deleting a newer run's lock.
        _get_ops_table().delete_item(
            Key={"PK": "LOCK#PollerFunction", "SK": "RUN"},
            ConditionExpression="#o = :owner",
            ExpressionAttributeNames={"#o": "owner"},
            ExpressionAttributeValues={":owner": owner},
        )
    except Exception:
        return


def _format_message(ev: dict[str, Any]) -> str:
    parts = ["🚨 Baliza V16 activa"]
    mun = ev.get("municipality", "")
    prov = ev.get("province", "")
    road = ev.get("road", "")
    km = ev.get("km", "")
    start_time = ev.get("start_time", "")
    lat = (ev.get("lat", "") or "").strip()
    lon = (ev.get("lon", "") or "").strip()
    # Compact location line: municipality + road/km when available
    loc = ""
    if mun or prov:
        loc = f"{mun} ({prov})".strip()
    suffix = _format_road_km(road, km)
    if loc and suffix:
        parts.append(f"📍 {loc} — {suffix}")
    elif loc:
        parts.append(f"📍 {loc}")
    elif suffix:
        parts.append(f"🛣️ {suffix}")

    if start_time:
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(ZoneInfo("Europe/Madrid"))
            parts.append(f"🕒 Activa desde: {dt.strftime('%d/%m/%Y %H:%M')} (hora local)")
        except Exception:
            parts.append(f"🕒 Activa desde: {start_time}")

    if lat and lon:
        parts.append(f"🗺️ [Ver en el mapa](https://www.google.com/maps?q={lat},{lon})")

    # Traceability: helps understand whether repeated notifications are the same or different DGT records.
    sid = (ev.get("situation_id") or "").strip()
    rid = (ev.get("record_id") or "").strip()
    if sid or rid:
        parts.append(f"Ref. DGT: {sid or '-'} / {rid or '-'}")
    return "\n".join(parts)


def _event_dedupe_key(ev: dict[str, Any]) -> str:
    """
    Build a dedupe key for notifications.

    Prefer DGT identifiers to avoid re-notifying the same ongoing incident across
    consecutive snapshots. Order:
    1) sit:situation id
    2) situationRecordCreationReference
    3) situationRecord id
    """
    situation_id = (ev.get("situation_id") or "").strip()
    if situation_id:
        return f"situation:{situation_id}"

    creation_ref = (ev.get("creation_ref") or "").strip()
    if creation_ref:
        return f"creation_ref:{creation_ref}"

    record_id = (ev.get("record_id") or "").strip()
    if record_id:
        return f"record:{record_id}"

    # Fallback: hash the whole event
    return hashlib.sha1(json.dumps(ev, sort_keys=True).encode("utf-8")).hexdigest()[:20]


def lambda_handler(event, context):
    lock_acquired = False
    try:
        run_id = f"{int(time.time())}-{random.randint(1000, 9999)}"
        if not _acquire_poller_lock(run_id):
            _log("info", "poller_skip_lock_held", run_id=run_id)
            _emit_metrics(poller_lock_skipped=1)
            return {"ok": True, "skipped": True}
        lock_acquired = True

        xml_bytes = _fetch_dgt_xml()
        events = list(_iter_v16_events(xml_bytes))
        _log("info", "poller_fetched", run_id=run_id, events=len(events))

        parsed = len(events)
        mapped = 0
        unmapped = 0
        sent = 0
        telegram_errors = 0
        ddb_errors = 0
        quiet_skipped = 0
        unique_candidate_chats: set[int] = set()
        to_notify: dict[int, list[dict[str, Any]]] = {}

        for ev in events:
            mid = _municipality_id_from_names(ev["municipality"], ev["province"])
            if not mid:
                unmapped += 1
                continue
            mapped += 1
            chat_ids = _query_subscribed_chats(mid)
            if not chat_ids:
                continue
            for cid in chat_ids:
                unique_candidate_chats.add(cid)
            record_id = _event_dedupe_key(ev)
            for chat_id in chat_ids:
                try:
                    settings = _get_chat_settings(chat_id)
                    if _is_quiet_now(settings):
                        quiet_skipped += 1
                        continue
                except Exception:
                    # If settings read fails, default to sending (do not silently drop).
                    pass
                try:
                    if not _dedupe_mark_sent(record_id, chat_id):
                        continue
                except Exception:
                    ddb_errors += 1
                    _log(
                        "warning",
                        "dedupe_mark_failed",
                        run_id=run_id,
                        dedupe_key=record_id,
                        chat_id=chat_id,
                        situation_id=ev.get("situation_id"),
                    )
                    continue
                # Collect for batching per chat (grouping by situation within a single run).
                to_notify.setdefault(chat_id, []).append(ev)

        candidate_chats = len(unique_candidate_chats)
        notify_targets = len(to_notify)  # chats with at least one pending event after quiet+dedupe
        blocked_targets: set[int] = set()

        # Send notifications batched per chat
        for chat_id, evs in to_notify.items():
            if not evs:
                continue
            if len(evs) == 1:
                msg = _format_message(evs[0])
            else:
                lines = [f"🚨 Baliza V16 activa ({len(evs)} nuevas)"]
                for ev in evs[:10]:
                    mun = ev.get("municipality", "")
                    prov = ev.get("province", "")
                    road = ev.get("road", "")
                    km = ev.get("km", "")
                    lat = (ev.get("lat", "") or "").strip()
                    lon = (ev.get("lon", "") or "").strip()
                    sid = (ev.get("situation_id") or "").strip()
                    rid = (ev.get("record_id") or "").strip()
                    where = f"{mun} ({prov})".strip()
                    suffix = _format_road_km(road, km)
                    if where and suffix:
                        lines.append(f"- 📍 {where} — {suffix}")
                    elif where:
                        lines.append(f"- 📍 {where}")
                    elif suffix:
                        lines.append(f"- 🛣️ {suffix}")
                    if lat and lon:
                        lines.append(f"  🗺️ https://www.google.com/maps?q={lat},{lon}")
                    lines.append(f"  Ref. DGT: {sid or '-'} / {rid or '-'}")
                if len(evs) > 10:
                    lines.append(f"(+{len(evs)-10} más)")
                msg = "\n".join(lines)

            try:
                _telegram_send_message(chat_id, msg)
                sent += 1
            except Exception as e:
                telegram_errors += 1
                details = _telegram_error_details(e)
                _log("warning", "telegram_send_failed", run_id=run_id, chat_id=chat_id, **details)
                # If user blocked the bot (or chat is permanently invalid), stop retrying forever:
                # remove subscriptions and (optionally) chat settings/state.
                if _is_permanent_telegram_chat_error(details):
                    blocked_targets.add(chat_id)
                    deleted = _purge_chat_subscriptions(chat_id)
                    if deleted > 0:
                        _metrics_update_subscribed_chats(delta=-1)
                    _purge_chat_ops(chat_id)
                    _log(
                        "info",
                        "chat_purged_after_telegram_error",
                        run_id=run_id,
                        chat_id=chat_id,
                        deleted_subscriptions=deleted,
                        http_status=details.get("http_status"),
                    )
                continue

        # Targets excluding permanent Telegram delivery failures (blocked/chat not found).
        # This lets alarms focus on "deliverable but not delivered" rather than user blocks.
        notify_deliverable_targets = max(0, notify_targets - len(blocked_targets))

        subscribed_chats = _get_subscribed_chats_metric()
        _emit_metrics(
            events_parsed=parsed,
            events_mapped=mapped,
            events_unmapped=unmapped,
            notifications_sent=sent,
            candidate_chats=candidate_chats,
            notify_targets=notify_targets,
            notify_deliverable_targets=notify_deliverable_targets,
            telegram_errors=telegram_errors,
            ddb_errors=ddb_errors,
            quiet_skipped=quiet_skipped,
            **({"subscribed_chats": int(subscribed_chats)} if subscribed_chats is not None else {}),
        )
        _put_poller_state(
            run_id,
            parsed=parsed,
            mapped=mapped,
            unmapped=unmapped,
            candidate_chats=candidate_chats,
            notify_targets=notify_targets,
            notify_deliverable_targets=notify_deliverable_targets,
            notifications_sent=sent,
            telegram_errors=telegram_errors,
            ddb_errors=ddb_errors,
            quiet_skipped=quiet_skipped,
            subscribed_chats=subscribed_chats,
        )

        _log(
            "info",
            "poller_summary",
            run_id=run_id,
            parsed=parsed,
            mapped=mapped,
            unmapped=unmapped,
            candidate_chats=candidate_chats,
            notify_targets=notify_targets,
            notify_deliverable_targets=notify_deliverable_targets,
            sent=sent,
            telegram_errors=telegram_errors,
            ddb_errors=ddb_errors,
            quiet_skipped=quiet_skipped,
            subscribed_chats=subscribed_chats,
        )

        return {"ok": True, "sent": sent, "events": len(events)}
    except Exception:
        logger.exception("Poller error")
        # Best-effort DLQ capture: never include full XML or large payloads.
        _send_dlq({"type": "poller_exception", "time": int(time.time())})
        _emit_metrics(poller_errors=1)
        return {"ok": False}
    finally:
        if lock_acquired:
            _release_poller_lock(run_id)

