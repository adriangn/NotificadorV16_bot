# Telegram Bot on AWS Lambda

This repository contains a **Telegram bot** designed to run on **AWS Lambda**.

## Overview

- The bot logic is intended to be deployed as an AWS Lambda function.
- It uses a Telegram Bot token to authenticate against the Telegram Bot API.
- Deployment is expected to be automated using GitHub repository secrets (see below).
- It manages Telegram **chat** subscriptions to municipalities (future: notify about roadside assistance beacons).

## GitHub Secrets

This repository expects the following GitHub Actions secrets to be configured:

- **AWS_ACCESS_KEY_ID**: AWS access key for the deployment identity.
- **AWS_SECRET_ACCESS_KEY**: AWS secret key for the deployment identity.
- **AWS_REGION**: AWS region where the Lambda function is deployed (e.g. `us-east-1`).
- **TELEGRAM_TOKEN**: Telegram Bot token (from BotFather).
- **TELEGRAM_WEBHOOK_SECRET**: Secret token used to validate incoming webhook requests via the `X-Telegram-Bot-Api-Secret-Token` header.

## Bot commands

- `/start`: Show the main menu (subscribe / list / unsubscribe).
- `/suscribir`: Start municipality search flow.
- `/mis_suscripciones`: List current chat subscriptions.
- `/anular`: Unsubscribe flow (inline button selection).
- `/estado`: Show poller status (last run summary).

## Data (municipalities)

The searchable municipality dataset is shipped as `src/data/municipalities.json` (UTF-8).

To rebuild it from the official CSV sources under `.context/`:

```bash
python3 scripts/build_municipalities.py
```

## Notes

- Do not commit credentials or tokens to the repository.
- Make sure the AWS identity used by the secrets has the required permissions to deploy/update Lambda resources.

## Deployment (AWS SAM)

This project is deployed using **AWS SAM** via GitHub Actions (`.github/workflows/deploy.yml`).

- On push to `main`, the workflow builds and deploys the stack `notificadorv16-bot`.
- After deploy, it configures the Telegram webhook to point to:
  - `${HttpApiUrl}/webhook`
  - and sets `secret_token` to `TELEGRAM_WEBHOOK_SECRET`.

## DGT feed polling (V16)

A scheduled Lambda (`PollerFunction`) runs **every minute** and fetches the DATEX2 SituationPublication feed from DGT:

- `https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml`

It extracts V16-like records (GenericSituationRecord with `vehicleObstruction/vehicleStuck` and Spanish location extension fields: municipality + province) and notifies all chats subscribed to the matching municipality.

### Deduplication

The same V16 beacon can appear in multiple consecutive XML snapshots. To avoid spamming, the poller stores a "sent" marker in DynamoDB per `(record_id, chat_id)` with a TTL (`NOTIFY_TTL_SECONDS`, default 24h).

### History + real-time charts (Aurora DSQL)

The poller can persist a **historical record** of V16-like incidents and also write **minute-level buckets** ready for real-time charts (filters by municipality + road + road type + date ranges).

- **Storage**: Aurora DSQL (Postgres-compatible SQL).
- **Schema**: `src/sql/schema.sql`
  - `incidents`: 1 row per incident (`incident_id`) with fixed metadata as columns (municipality, province, road, road_type, lat/lon, start/end, first/last seen, etc.).
  - `minute_buckets_*`: pre-aggregated per-minute metrics for fast chart queries:
    - `minute_buckets_road` (municipality + road)
    - `minute_buckets_type` (municipality + road_type)
    - `minute_buckets_mun` (municipality only)
- **Connection**: poller reads a Secrets Manager secret (`DSQL_SECRET_ARN`) containing JSON connection fields: `host`, `port`, `dbname`, `username`, `password`.
- **Closing logic**:
  - If the feed reports `validityStatus != active`, the incident is closed immediately (`end_reason=validity_inactive`).
  - If an incident disappears from the feed, it is closed after `HISTORY_CLOSE_MISSING_AFTER_SECONDS` (default 180s) to avoid transient feed gaps (`end_reason=missing_from_feed`).
- **Road type**: derived from the road name prefix (e.g. `AP`, `A`, `N`, `M`), stored as a dedicated column (`road_type`) so it can be indexed and filtered efficiently.

## Alerts and DLQ

### Alerts (SNS + CloudWatch Alarms)

The stack creates an SNS topic and a few CloudWatch alarms.

- Output: `AlertsTopicArn`
- You can subscribe your email/SMS/Slack integration to this topic.

### Dead-letter queues (SQS)

The stack creates two SQS queues:

- `BotDlqQueueUrl`: webhook bot errors (best-effort capture)
- `PollerDlqQueueUrl`: poller errors (best-effort capture)

These queues retain messages for 14 days.

## Quiet hours (silence)

You can silence notifications for a chat during a time window (Europe/Madrid):

- `/silencio 23:00 07:30`
- `/silencio off`

Or use `/start` → "Silencio (horario)".

You can also pause notifications indefinitely:

- `/silencio on`
