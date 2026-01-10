# Telegram Bot on AWS Lambda

This repository contains a **Telegram bot** designed to run on **AWS Lambda**.

## Overview

- The bot logic is intended to be deployed as an AWS Lambda function.
- It uses a Telegram Bot token to authenticate against the Telegram Bot API.
- Deployment is expected to be automated using GitHub repository secrets (see below).

## GitHub Secrets

This repository expects the following GitHub Actions secrets to be configured:

- **AWS_ACCESS_KEY_ID**: AWS access key for the deployment identity.
- **AWS_SECRET_ACCESS_KEY**: AWS secret key for the deployment identity.
- **AWS_REGION**: AWS region where the Lambda function is deployed (e.g. `us-east-1`).
- **TELEGRAM_TOKEN**: Telegram Bot token (from BotFather).
- **TELEGRAM_WEBHOOK_SECRET**: Secret token used to validate incoming webhook requests via the `X-Telegram-Bot-Api-Secret-Token` header.

## Notes

- Do not commit credentials or tokens to the repository.
- Make sure the AWS identity used by the secrets has the required permissions to deploy/update Lambda resources.

## Deployment (AWS SAM)

This project is deployed using **AWS SAM** via GitHub Actions (`.github/workflows/deploy.yml`).

- On push to `main`, the workflow builds and deploys the stack `notificadorv16-bot`.
- After deploy, it configures the Telegram webhook to point to:
  - `${HttpApiUrl}/webhook`
  - and sets `secret_token` to `TELEGRAM_WEBHOOK_SECRET`.
