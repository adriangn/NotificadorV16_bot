import json
import os
import urllib.request


def _send_cfn_response(event, context, status: str, reason: str = "", data: dict | None = None):
    response_url = event["ResponseURL"]
    body = {
        "Status": status,
        "Reason": reason or f"See CloudWatch Logs: {context.log_stream_name}",
        "PhysicalResourceId": event.get("PhysicalResourceId") or context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": False,
        "Data": data or {},
    }
    req = urllib.request.Request(
        response_url,
        data=json.dumps(body).encode("utf-8"),
        method="PUT",
        headers={"content-type": ""},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def lambda_handler(event, context):
    try:
        req_type = event.get("RequestType", "Create")
        props = event.get("ResourceProperties", {})
        log_group_names = props.get("LogGroupNames") or []
        retention = int(props.get("RetentionInDays") or os.environ.get("LOG_RETENTION_DAYS") or "90")

        import boto3

        logs = boto3.client("logs")

        if req_type in ("Create", "Update"):
            for name in log_group_names:
                # Ensure log group exists (CreateLogGroup is idempotent-ish)
                try:
                    logs.create_log_group(logGroupName=name)
                except Exception:
                    pass
                logs.put_retention_policy(logGroupName=name, retentionInDays=retention)

        # On Delete: do nothing (leave retention as-is)
        _send_cfn_response(event, context, "SUCCESS", data={"RetentionInDays": retention})
    except Exception as e:
        _send_cfn_response(event, context, "FAILED", reason=str(e))

