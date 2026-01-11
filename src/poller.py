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


def _get_ddb_table():
    global _DDB_TABLE
    if _DDB_TABLE is not None:
        return _DDB_TABLE
    if not SUBSCRIPTIONS_TABLE:
        raise RuntimeError("Missing SUBSCRIPTIONS_TABLE env var")
    import boto3  # available in AWS Lambda runtime

    _DDB_TABLE = boto3.resource("dynamodb").Table(SUBSCRIPTIONS_TABLE)
    return _DDB_TABLE


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
        return _telegram_api("sendMessage", {"chat_id": chat_id, "text": text})

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


def _dedupe_mark_sent(record_id: str, chat_id: int) -> bool:
    """
    Return True if we should send (first time), False if already sent recently.

    We store one item per (record_id, chat_id) with TTL so duplicates across
    consecutive XML fetches do not re-notify.
    """
    table = _get_ddb_table()
    now = int(time.time())
    pk = f"EVENT#{record_id}"
    sk = f"CHAT#{chat_id}"

    # Use constant-time compare for paranoia in case future refactors touch secrets.
    _ = hmac.compare_digest("a", "a")

    try:
        table.put_item(
            Item={"PK": pk, "SK": sk, "ttl": now + NOTIFY_TTL_SECONDS, "created_at": now},
            ConditionExpression="attribute_not_exists(PK)",
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


def _acquire_poller_lock() -> bool:
    """
    Prevent overlapping poller runs without using ReservedConcurrentExecutions.
    Uses a DynamoDB conditional put with TTL.
    """
    table = _get_ddb_table()
    now = int(time.time())
    pk = "LOCK#PollerFunction"
    sk = "RUN"
    try:
        table.put_item(
            Item={"PK": pk, "SK": sk, "ttl": now + POLLER_LOCK_TTL_SECONDS, "created_at": now},
            ConditionExpression="attribute_not_exists(PK)",
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


def _format_message(ev: dict[str, Any]) -> str:
    parts = ["🚨 Baliza V16 activa"]
    mun = ev.get("municipality", "")
    prov = ev.get("province", "")
    road = ev.get("road", "")
    km = ev.get("km", "")
    start_time = ev.get("start_time", "")
    lat = (ev.get("lat", "") or "").strip()
    lon = (ev.get("lon", "") or "").strip()
    if mun or prov:
        parts.append(f"📍 {mun} ({prov})".strip())
    if road and km:
        parts.append(f"🛣️ {road} — km {km}")
    elif road:
        parts.append(f"🛣️ {road}")
    elif km:
        parts.append(f"📌 km {km}")

    if start_time:
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(ZoneInfo("Europe/Madrid"))
            parts.append(f"🕒 Desde: {dt.strftime('%d/%m/%Y %H:%M')} (hora local)")
        except Exception:
            parts.append(f"🕒 Desde: {start_time}")

    if lat and lon:
        parts.append(f"🗺️ Mapa: https://www.google.com/maps?q={lat},{lon}")
    return "\n".join(parts)


def _event_dedupe_key(ev: dict[str, Any]) -> str:
    """
    Build a stable dedupe key. Prefer creation_ref when available and hash stable fields.
    """
    creation_ref = (ev.get("creation_ref") or "").strip()
    base = creation_ref or (ev.get("situation_id") or "") or (ev.get("record_id") or "")
    stable = "|".join(
        [
            base,
            (ev.get("province") or ""),
            (ev.get("municipality") or ""),
            (ev.get("road") or ""),
            (ev.get("km") or ""),
            (ev.get("start_time") or ""),
            (ev.get("lat") or ""),
            (ev.get("lon") or ""),
        ]
    )
    return hashlib.sha1(stable.encode("utf-8")).hexdigest()[:20]


def lambda_handler(event, context):
    try:
        if not _acquire_poller_lock():
            logger.info("Skipping run: lock held")
            _emit_metrics(poller_lock_skipped=1)
            return {"ok": True, "skipped": True}

        xml_bytes = _fetch_dgt_xml()
        events = list(_iter_v16_events(xml_bytes))
        logger.info("Fetched %d V16-like events", len(events))

        parsed = len(events)
        mapped = 0
        unmapped = 0
        sent = 0
        telegram_errors = 0
        ddb_errors = 0

        for ev in events:
            mid = _municipality_id_from_names(ev["municipality"], ev["province"])
            if not mid:
                unmapped += 1
                continue
            mapped += 1
            chat_ids = _query_subscribed_chats(mid)
            if not chat_ids:
                continue
            text = _format_message(ev)
            record_id = _event_dedupe_key(ev)
            for chat_id in chat_ids:
                try:
                    if not _dedupe_mark_sent(record_id, chat_id):
                        continue
                except Exception:
                    ddb_errors += 1
                    continue
                try:
                    _telegram_send_message(chat_id, text)
                    sent += 1
                except Exception:
                    telegram_errors += 1
                    # Don't abort the batch.
                    continue

        _emit_metrics(
            events_parsed=parsed,
            events_mapped=mapped,
            events_unmapped=unmapped,
            notifications_sent=sent,
            telegram_errors=telegram_errors,
            ddb_errors=ddb_errors,
        )

        return {"ok": True, "sent": sent, "events": len(events)}
    except Exception:
        logger.exception("Poller error")
        _emit_metrics(poller_errors=1)
        return {"ok": False}

