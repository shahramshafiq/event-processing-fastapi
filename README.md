<div align="center">

<h1>Event Processing API</h1>

<p>A FastAPI backend that extracts text from uploaded PDFs in the background and streams the progress live.</p>

<p>
  <img alt="Python 3.10+" src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white" />
  <img alt="FastAPI 0.141" src="https://img.shields.io/badge/FastAPI-0.141-009688?style=flat-square&logo=fastapi&logoColor=white" />
  <img alt="Pydantic 2" src="https://img.shields.io/badge/Pydantic-2.13-E92063?style=flat-square&logo=pydantic&logoColor=white" />
  <img alt="Valkey" src="https://img.shields.io/badge/Valkey-Cache%20and%20Locks-5A4FCF?style=flat-square" />
</p>

</div>

## Overview

Processing documents attached to a record is slow work that should not block an API request. This service lets a client create events, attach PDF files to them, and start a background job that extracts the text from every PDF and delivers it to a configurable webhook. Progress is pushed to the client over Server-Sent Events (SSE), and Valkey handles caching, rate limiting, idempotency keys and a lock that keeps one event from being processed twice at the same time.

## Key Features

- **Event CRUD** under `/events`, with records stored in a local JSON file.
- **PDF upload** per event with a content-type check (HTTP 415) and a configurable size limit (HTTP 413); files are saved under generated UUID names.
- **Background processing** with FastAPI `BackgroundTasks`: the text of each PDF is extracted with PyPDF2 and POSTed to the webhook, with up to 3 attempts per file. The endpoint answers `202` immediately.
- **Live progress over SSE** at `/events/{event_id}/stream`: the stream reads state from Valkey once a second, sends a message only when something changed, and closes when the event is `completed` or `failed`.
- **Valkey-backed protections**: 5-minute cache for single-event lookups, per-IP rate limit on the process endpoint (5 requests per minute), `Idempotency-Key` header handling, and an `NX` processing lock with a one-hour expiry.
- **Restart recovery**: on startup, any event left in `processing` is marked `failed` and its lock is released.
- **Consistent error responses**: application errors return `{"error": "CODE", "message": "..."}`, and unexpected exceptions return a generic 500 without a stack trace.
- **Health probes**: `/health` for liveness and `/health/ready`, which returns 503 when Valkey is unreachable.

## Tech Stack

| Area | Technology |
|---|---|
| Language | Python 3.10+ |
| Web framework | FastAPI, served with Uvicorn |
| Validation and config | Pydantic, pydantic-settings (`.env` file) |
| Cache, locks, rate limiting | Valkey (`valkey` Python client) |
| PDF text extraction | PyPDF2 |
| Webhook delivery | Requests |
| Live updates | Server-Sent Events via `StreamingResponse` |
| Storage | Local JSON files and an `uploaded_files/` directory |

## Project Structure

```text
.
|-- main.py             # Whole application: settings, routes, background job, SSE
|-- requirements.txt    # Pinned dependencies
|-- .env.example        # Template for the required environment variables
|-- .gitignore
|-- events.json         # Created at setup, event records (git-ignored)
|-- files.json          # Created at setup, uploaded file metadata (git-ignored)
`-- uploaded_files/     # Created on first upload, stored PDFs (git-ignored)
```

## Getting Started

### Prerequisites

- Python 3.10 or newer
- A running Valkey server (for example via Docker)
- A URL that accepts POST requests for the extracted text (a free endpoint from webhook.site works for testing)

### Installation

```bash
git clone https://github.com/shahramshafiq/event-processing-fastapi.git
cd event-processing-fastapi
python -m venv .venv
# macOS/Linux: source .venv/bin/activate
# Windows:     .venv\Scripts\activate
pip install -r requirements.txt python-multipart
```

`python-multipart` is installed alongside the pinned requirements because FastAPI needs it to accept file uploads.

Create your environment file and the two local data files (the app reads them on every request and does not create them itself):

```bash
cp .env.example .env
python -c "open('events.json','w').write('[]'); open('files.json','w').write('[]')"
```

Set the variables below in `.env`. All of them are required; `WEBHOOK_URL` in `.env.example` is a placeholder that you must replace.

| Variable | Purpose |
|---|---|
| `APP_NAME` | Application name |
| `DEBUG` | Debug flag (`true` or `false`) |
| `VALKEY_HOST` | Valkey host |
| `VALKEY_PORT` | Valkey port |
| `WEBHOOK_URL` | URL that receives the extracted text |
| `EXTERNAL_API_TIMEOUT` | Webhook request timeout in seconds |
| `MAX_FILE_SIZE` | Maximum upload size in bytes |
| `ALLOWED_ORIGINS` | Comma-separated list of allowed CORS origins |

### Run

Start Valkey, then the API from the project root (the JSON files and upload folder are resolved relative to it):

```bash
docker run -p 6379:6379 valkey/valkey
uvicorn main:app --reload
```

The API is served at `http://localhost:8000` and the interactive docs at `http://localhost:8000/docs`.

## Usage

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Create an event |
| `GET` | `/events` | List all events |
| `GET` | `/events/{event_id}` | Get one event (cached in Valkey) |
| `PUT` | `/events/{event_id}` | Replace an event |
| `DELETE` | `/events/{event_id}` | Delete an event |
| `POST` | `/events/{event_id}/files` | Upload a PDF for an event |
| `GET` | `/events/{event_id}/files` | List the files attached to an event |
| `POST` | `/events/{event_id}/process` | Start background processing (requires an `Idempotency-Key` header) |
| `GET` | `/events/{event_id}/stream` | Stream progress as SSE |
| `GET` | `/health`, `/health/ready` | Liveness and Valkey readiness |

An end-to-end run with `curl`:

```bash
# 1. Create an event (the client chooses the integer id)
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "name": "Quarterly reports", "description": "Documents for review"}'

# 2. Attach a PDF
curl -X POST http://localhost:8000/events/1/files \
  -F "file=@report.pdf;type=application/pdf"

# 3. Start processing (returns 202)
curl -X POST http://localhost:8000/events/1/process \
  -H "Idempotency-Key: demo-key-1"

# 4. Watch progress (-N turns off curl buffering so each update shows as it arrives)
curl -N http://localhost:8000/events/1/stream
```

Each stream message has this shape, and with 4 PDFs the progress moves through 25, 50, 75 and 100:

```text
data: {"event_id": 1, "status": "processing", "progress": 50}
```

If a PDF cannot be opened, has no extractable text, or its webhook delivery fails after 3 attempts, the whole event is marked `failed`. Repeating the process request with the same `Idempotency-Key`, or while the event is already running, returns `409`.

## Limitations and Roadmap

- Events and file metadata live in local JSON files that are rewritten on each change, with no locking, so the service is meant for a single process. Moving to a database is the natural next step.
- There is no authentication, and rate limiting applies only to the process endpoint.
- Webhook retries are immediate (no backoff), and one failed file fails the whole event.
- There are no automated tests yet.

## Author

**Shahram Shafiq** - [GitHub](https://github.com/shahramshafiq) | [LinkedIn](https://www.linkedin.com/in/shahramshafiq/)
