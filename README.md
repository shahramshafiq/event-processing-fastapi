# Event Processing System (FastAPI)

A FastAPI backend for creating **events**, attaching **PDF files** to them, and running a background job that extracts text from each PDF and delivers it to a configurable webhook. Live progress can be watched over Server-Sent Events (SSE). [Valkey](https://valkey.io/) (a Redis-compatible store) is used for caching, rate limiting, idempotency, and processing locks.

## Features

- **Event CRUD** - create, read, update, and delete events (`/events`)
- **PDF upload** - attach one or more PDF files to an event, with content-type and size validation
- **Background processing** - extracts text from every PDF attached to an event and POSTs it to a webhook, with automatic retries
- **Live progress via SSE** - `/events/{event_id}/stream` pushes status/progress updates as processing runs
- **Valkey-backed reliability**:
  - Response caching for single-event lookups
  - Per-IP rate limiting on the process endpoint
  - Idempotency keys to safely retry process requests
  - Distributed lock so an event is never processed twice concurrently
- **Health checks** - `/health` (liveness) and `/health/ready` (readiness, checks Valkey connectivity)

## Tech stack

- [FastAPI](https://fastapi.tiangolo.com/)
- [Pydantic](https://docs.pydantic.dev/) / `pydantic-settings` for config
- [Valkey](https://valkey.io/) client (`valkey` Python package)
- [PyPDF2](https://pypi.org/project/PyPDF2/) for PDF text extraction
- `requests` for outbound webhook calls

## Project structure

```
.
├── main.py              # Application: routes, background processing, SSE
├── requirements.txt      # Python dependencies
├── .env.example          # Sample environment configuration
└── .gitignore
```

Data files (`events.json`, `files.json`) and uploaded PDFs (`uploaded_files/`) are created automatically at runtime and are not tracked in version control.

## Getting started

### Prerequisites

- Python 3.10+
- A running Valkey (or Redis) instance

### Setup

1. Clone the repo and install dependencies:

   ```bash
   git clone <repo-url>
   cd "Event Processing System - FastAPI"
   pip install -r requirements.txt
   ```

2. Copy the example environment file and fill in your own values:

   ```bash
   cp .env.example .env
   ```

3. Create the local data stores (only needed once, on first run):

   ```bash
   echo "[]" > events.json
   echo "[]" > files.json
   ```

4. Start Valkey (or Redis) locally, e.g. with Docker:

   ```bash
   docker run -p 6379:6379 valkey/valkey
   ```

5. Run the API:

   ```bash
   uvicorn main:app --reload
   ```

   The API is now available at `http://localhost:8000`, with interactive docs at `http://localhost:8000/docs`.

## Configuration

All configuration is read from environment variables (see `.env.example`):

| Variable | Description |
|---|---|
| `APP_NAME` | Application name |
| `DEBUG` | Debug mode toggle |
| `VALKEY_HOST` | Valkey/Redis host |
| `VALKEY_PORT` | Valkey/Redis port |
| `WEBHOOK_URL` | Endpoint that receives extracted PDF text after processing |
| `EXTERNAL_API_TIMEOUT` | Timeout (seconds) for webhook requests |
| `MAX_FILE_SIZE` | Maximum allowed upload size in bytes |
| `ALLOWED_ORIGINS` | Comma-separated list of allowed CORS origins |

## API overview

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Create a new event |
| `GET` | `/events` | List all events |
| `GET` | `/events/{event_id}` | Get a single event (cached) |
| `PUT` | `/events/{event_id}` | Update an event |
| `DELETE` | `/events/{event_id}` | Delete an event |
| `POST` | `/events/{event_id}/files` | Upload a PDF file for an event |
| `GET` | `/events/{event_id}/files` | List files attached to an event |
| `POST` | `/events/{event_id}/process` | Start background processing (requires `Idempotency-Key` header) |
| `GET` | `/events/{event_id}/stream` | SSE stream of processing status/progress |
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check (verifies Valkey connectivity) |

### Processing flow

1. Create an event via `POST /events`.
2. Upload one or more PDFs via `POST /events/{event_id}/files`.
3. Kick off processing via `POST /events/{event_id}/process` with an `Idempotency-Key` header. This endpoint is rate-limited and guarded by a Valkey lock so an event can only be processed by one request at a time.
4. Watch progress live via `GET /events/{event_id}/stream`, or poll `GET /events/{event_id}`.
5. Each PDF's extracted text is POSTed to `WEBHOOK_URL`, with up to 3 retry attempts on failure. If any file fails to open, fails extraction, contains no extractable text, or the webhook can't be delivered, the event is marked `failed`. Once every file succeeds, the event is marked `completed`.

## License

This project currently has no license file; all rights reserved by the author unless stated otherwise.
