# Event Processing System (FastAPI)

A small FastAPI backend for creating events, attaching PDF files to them, and processing those files in the background: extracting the text from each PDF and sending it to a webhook. You can watch an event's progress live over SSE instead of polling for it. Valkey (a Redis-compatible store) handles caching, rate limiting, idempotency, and making sure the same event never gets processed twice at once.

## What it does

- Full CRUD on events (`/events`)
- PDF upload per event, with content-type and size checks
- Background processing: extract text from every PDF attached to an event, POST it to a webhook, with retries if the webhook fails
- Live progress over SSE at `/events/{event_id}/stream`
- Valkey used for caching single-event lookups, per-IP rate limiting on the process endpoint, idempotency keys, and a processing lock so one event can't be processed by two requests at once
- If the app crashes or gets restarted while an event is mid-processing, it gets picked back up on the next startup and marked failed instead of staying stuck showing "processing" forever
- Any error, expected or not, comes back in the same `{"error": ..., "message": ...}` shape, no raw stack traces leaking to the client
- Health checks at `/health` and `/health/ready`

## Tech stack

- FastAPI
- Pydantic and pydantic-settings for config
- Valkey (the `valkey` Python package)
- PyPDF2 for extracting text from PDFs
- `requests` for the outbound webhook calls

## Project structure

Everything lives in one file on purpose:

```
.
├── main.py             # the whole application
├── requirements.txt
├── .env.example
└── .gitignore
```

`events.json` and `files.json` are created automatically the first time you run it, and `uploaded_files/` gets created the first time a PDF is uploaded. None of these are tracked in git.

## Getting started

### You'll need

- Python 3.10+
- A running Valkey (or Redis) instance

### Setup

1. Clone it and install dependencies:

   ```bash
   git clone <repo-url>
   cd "Event Processing System - FastAPI"
   pip install -r requirements.txt
   ```

2. Copy the example env file and fill in your own values:

   ```bash
   cp .env.example .env
   ```

3. Create the local data files, only needed once:

   ```bash
   echo "[]" > events.json
   echo "[]" > files.json
   ```

4. Start Valkey locally, for example with Docker:

   ```bash
   docker run -p 6379:6379 valkey/valkey
   ```

5. Run it:

   ```bash
   uvicorn main:app --reload
   ```

   The API is at `http://localhost:8000`, and the interactive docs are at `http://localhost:8000/docs`.

## Configuration

Everything's read from environment variables, see `.env.example`:

| Variable | Description |
|---|---|
| `APP_NAME` | Application name |
| `DEBUG` | Debug mode toggle |
| `VALKEY_HOST` | Valkey/Redis host |
| `VALKEY_PORT` | Valkey/Redis port |
| `WEBHOOK_URL` | Where extracted PDF text gets POSTed after processing (use a free URL from webhook.site) |
| `EXTERNAL_API_TIMEOUT` | Timeout in seconds for webhook requests |
| `MAX_FILE_SIZE` | Max upload size in bytes, files bigger than this get rejected |
| `ALLOWED_ORIGINS` | Comma-separated list of allowed CORS origins |

## API overview

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Create an event |
| `GET` | `/events` | List every event |
| `GET` | `/events/{event_id}` | Get one event (cached in Valkey) |
| `PUT` | `/events/{event_id}` | Update an event |
| `DELETE` | `/events/{event_id}` | Delete an event |
| `POST` | `/events/{event_id}/files` | Upload a PDF for an event |
| `GET` | `/events/{event_id}/files` | List the files attached to an event |
| `POST` | `/events/{event_id}/process` | Kick off background processing (needs an `Idempotency-Key` header) |
| `GET` | `/events/{event_id}/stream` | Watch processing progress live over SSE |
| `GET` | `/health` | Is the app running |
| `GET` | `/health/ready` | Is Valkey reachable too |

## How PDF processing actually works

1. Create an event with `POST /events`.
2. Upload one or more PDFs to it with `POST /events/{event_id}/files`. Only PDFs are accepted, and anything over `MAX_FILE_SIZE` gets rejected.
3. Call `POST /events/{event_id}/process` with an `Idempotency-Key` header. This is rate-limited per IP (5 requests a minute) and locked in Valkey so the same event can't start processing twice at once, calling it again while it's already running just gets you an error instead of a second job.
4. The event's status flips to `processing`, and each uploaded PDF gets opened, has its text pulled out, and that text gets POSTed to `WEBHOOK_URL`. If the webhook call fails, it retries up to 3 times before giving up.
5. Progress updates after every file, so with 4 PDFs you'd see it move 25%, 50%, 75%, 100%. If any single file fails to open, has no extractable text, or its webhook delivery never succeeds after retries, the whole event gets marked `failed` right there. Once every file goes through, the event is marked `completed`.
6. You can watch this happen live with the stream endpoint below, or just poll `GET /events/{event_id}` if you'd rather not keep a connection open.

## How to test the SSE stream

Because it's a live stream, a normal browser tab or Postman's plain "Send" button won't show it properly, they wait for the whole response before displaying anything. Use `curl -N` instead (`-N` turns off curl's own buffering so you see each update as it arrives):

```bash
curl -N http://localhost:8000/events/1/stream
```

With an event that has files attached and processing already kicked off, you should see a new `data: {...}` line appear roughly once a second as progress updates, ending once the event hits `completed` or `failed`, at which point the connection just closes on its own.

If you're testing in Postman instead, recent versions (10.10+) handle SSE natively, just send a normal GET request to that URL and Postman will stream the events into the response panel live instead of waiting for the connection to end.

## License

No license file yet, all rights reserved by the author unless stated otherwise.
