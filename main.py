from fastapi import FastAPI, File, UploadFile, BackgroundTasks, Request, Header
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import valkey as vk
from fastapi.middleware.cors import CORSMiddleware
import json
from fastapi.responses import JSONResponse, StreamingResponse
import asyncio
import uuid
import os
from datetime import datetime
from PyPDF2 import PdfReader
import logging
import requests as rq
from contextlib import asynccontextmanager
from typing import Literal


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

class Settings(BaseSettings):

    app_name: str
    debug: bool
    valkey_host: str
    valkey_port: int
    webhook_url: str
    external_api_timeout: int
    max_file_size: int
    allowed_origins: str

    class Config:
        env_file = ".env"


settings = Settings()


# ---------------------------------------------------------
# Global resources
# ---------------------------------------------------------

valkey_client = None
http_session = None


# ---------------------------------------------------------
# Lifespan
# ---------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):

    global valkey_client, http_session

    logging.info("Application starting up")

    # Create Valkey connection when application starts
    valkey_client = vk.Valkey(
        host=settings.valkey_host,
        port=settings.valkey_port,
        decode_responses=True
    )

    # Create HTTP session when application starts
    http_session = rq.Session()

    try:
        valkey_client.ping()
        logging.info("Valkey connection is available")

    except Exception as e:
        logging.warning(f"Valkey is unavailable: {e}")

    try:
        yield

    finally:

        logging.info("Application shutting down")

        # Close HTTP session
        if http_session is not None:
            http_session.close()

        # Close Valkey connection
        if valkey_client is not None:
            valkey_client.close()


# ---------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------

app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------
# CORS
# ---------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------
# Models
# ---------------------------------------------------------

class Event(BaseModel):

    id: int
    name: str
    description: str

    status: Literal[
        "created",
        "processing",
        "completed",
        "failed"
    ] = "created"

    created_date: str = ""
    updated_date: str = ""

    progress: int = 0


class FileMetadata(BaseModel):

    file_id: str
    event_id: int
    original_filename: str
    stored_filename: str
    file_size: int
    upload_time: str


# ---------------------------------------------------------
# Valkey test
# ---------------------------------------------------------

@app.get("/test-valkey")
def test_valkey():

    valkey_client.set(
        "test",
        "Hello Valkey"
    )

    value = valkey_client.get("test")

    return {
        "value": value
    }


# ---------------------------------------------------------
# JSON file helpers
# ---------------------------------------------------------

def read_events():

    with open("events.json", "r") as file:
        events_list = json.load(file)

    return events_list


def write_events(events_list):

    with open("events.json", "w") as file:
        json.dump(
            events_list,
            file,
            indent=4
        )


def read_files():

    with open("files.json", "r") as file:
        files_list = json.load(file)

    return files_list


def write_files(files_list):

    with open("files.json", "w") as file:
        json.dump(
            files_list,
            file,
            indent=4
        )


# ---------------------------------------------------------
# Processing state helper
# ---------------------------------------------------------

def set_processing_state(
    event_id,
    status,
    progress
):

    # Store processing status in Valkey
    valkey_client.set(
        f"event:{event_id}:status",
        status,
        ex=3600
    )

    # Store processing progress in Valkey
    valkey_client.set(
        f"event:{event_id}:progress",
        progress,
        ex=3600
    )

    # Delete cached event so GET /events/{event_id}
    # does not return old processing information
    valkey_client.delete(
        f"event:{event_id}"
    )


# ---------------------------------------------------------
# Create event
# ---------------------------------------------------------

@app.post("/events")
def create_event(event: Event):

    events_list = read_events()

    # Check whether ID already exists
    for i in range(len(events_list)):

        if events_list[i]["id"] == event.id:

            return JSONResponse(
                content={
                    "error": "EVENT_ALREADY_EXISTS",
                    "message": "This id already exists"
                },
                status_code=409
            )

    current_time = datetime.now().isoformat()

    event.status = "created"
    event.progress = 0
    event.created_date = current_time
    event.updated_date = current_time

    events_list.append(
        event.model_dump()
    )

    write_events(events_list)

    logging.info(
        f"Event {event.id} is created"
    )

    return event


# ---------------------------------------------------------
# Get all events
# ---------------------------------------------------------

@app.get("/events")
def get_event():

    events_list = read_events()

    return events_list


# ---------------------------------------------------------
# Get one event
# ---------------------------------------------------------

@app.get("/events/{event_id}")
def get_event(event_id: int):

    # Check Valkey cache first
    cached_event = valkey_client.get(
        f"event:{event_id}"
    )

    if cached_event:

        return json.loads(cached_event)

    events_list = read_events()

    for event in events_list:

        if event["id"] == event_id:

            # Save event in cache
            valkey_client.set(
                f"event:{event_id}",
                json.dumps(event),
                ex=300
            )

            return event

    return JSONResponse(
        content={
            "error": "EVENT_NOT_FOUND",
            "message": "An event with this ID does not exist"
        },
        status_code=404
    )


# ---------------------------------------------------------
# Update event
# ---------------------------------------------------------

@app.put("/events/{event_id}")
def put_event(
    event_id: int,
    updated_event: Event
):

    events_list = read_events()

    for i in range(len(events_list)):

        if events_list[i]["id"] == event_id:

            events_list[i] = updated_event.model_dump()

            write_events(events_list)

            # Delete old cache
            valkey_client.delete(
                f"event:{event_id}"
            )

            return events_list[i]

    return JSONResponse(
        content={
            "error": "EVENT_NOT_FOUND",
            "message": "An event with this ID does not exist"
        },
        status_code=404
    )


# ---------------------------------------------------------
# Delete event
# ---------------------------------------------------------

@app.delete("/events/{event_id}")
def delete_event(event_id: int):

    events_list = read_events()

    for i in range(len(events_list)):

        if events_list[i]["id"] == event_id:

            events_list.remove(
                events_list[i]
            )

            write_events(events_list)

            # Delete cached event
            valkey_client.delete(
                f"event:{event_id}"
            )

            return events_list

    return JSONResponse(
        content={
            "error": "EVENT_NOT_FOUND",
            "message": "An event with this ID does not exist"
        },
        status_code=404
    )


# ---------------------------------------------------------
# Upload PDF
# ---------------------------------------------------------

@app.post("/events/{event_id}/files")
def upload_file(
    event_id: int,
    file: UploadFile = File(...)
):

    events_list = read_events()

    for i in range(len(events_list)):

        if events_list[i]["id"] == event_id:

            # Only PDF files are allowed
            if file.content_type != "application/pdf":

                return JSONResponse(
                    content={
                        "error": "INVALID_FILE_TYPE",
                        "message": "File type is not PDF"
                    },
                    status_code=415
                )

            # Check file size
            file.file.seek(0, 2)

            file_size = file.file.tell()

            file.file.seek(0)

            if file_size > settings.max_file_size:

                return JSONResponse(
                    content={
                        "error": "FILE_TOO_LARGE",
                        "message": "File size is larger than the configured maximum"
                    },
                    status_code=413
                )

            # Generate unique file ID
            file_id = str(uuid.uuid4())

            # Create upload directory
            os.makedirs(
                "uploaded_files",
                exist_ok=True
            )

            # Generate stored filename
            stored_filename = file_id + ".pdf"

            file_path = os.path.join(
                "uploaded_files",
                stored_filename
            )

            # Save file locally
            with open(
                file_path,
                "wb"
            ) as output_file:

                output_file.write(
                    file.file.read()
                )

            # Create metadata
            metadata = FileMetadata(
                file_id=file_id,
                event_id=event_id,
                original_filename=file.filename,
                stored_filename=stored_filename,
                file_size=file_size,
                upload_time=datetime.now().isoformat()
            )

            # Save metadata
            files_list = read_files()

            files_list.append(
                metadata.model_dump()
            )

            write_files(files_list)

            logging.info(
                f"File {file_id} uploaded for event {event_id}"
            )

            return metadata

    return JSONResponse(
        content={
            "error": "EVENT_NOT_FOUND",
            "message": "An event with this ID does not exist"
        },
        status_code=404
    )


# ---------------------------------------------------------
# Get files belonging to event
# ---------------------------------------------------------

@app.get("/events/{event_id}/files")
def get_file(event_id: int):

    events_list = read_events()

    event_exists = False

    for event in events_list:

        if event["id"] == event_id:

            event_exists = True

            break

    if not event_exists:

        return JSONResponse(
            content={
                "error": "EVENT_NOT_FOUND",
                "message": "An event with this ID does not exist"
            },
            status_code=404
        )

    files_list = read_files()

    event_files = []

    for file in files_list:

        if file["event_id"] == event_id:

            event_files.append(file)

    return event_files


# ---------------------------------------------------------
# Mark event as failed
# ---------------------------------------------------------

def mark_event_failed(
    event_id,
    event_index,
    progress,
    message
):

    events_list = read_events()

    if event_index >= 0 and event_index < len(events_list):

        events_list[event_index]["status"] = "failed"

        events_list[event_index]["progress"] = progress

        events_list[event_index]["updated_date"] = (
            datetime.now().isoformat()
        )

        write_events(events_list)

    set_processing_state(
        event_id,
        "failed",
        progress
    )

    # Release processing lock
    valkey_client.delete(
        f"processing:{event_id}"
    )

    logging.error(message)


# ---------------------------------------------------------
# Process event
# ---------------------------------------------------------

def process_event(event_id: int):

    try:

        events_list = read_events()

        files_list = read_files()

        event_files = []

        # -------------------------------------------------
        # Find all files belonging to event
        # -------------------------------------------------

        for file in files_list:

            if file["event_id"] == event_id:

                event_files.append(file)

        # -------------------------------------------------
        # Find event
        # -------------------------------------------------

        event_index = -1

        for index in range(len(events_list)):

            if events_list[index]["id"] == event_id:

                event_index = index

                break

        if event_index == -1:

            logging.error(
                f"Event {event_id} does not exist"
            )

            return

        # -------------------------------------------------
        # Check that event has files
        # -------------------------------------------------

        if len(event_files) == 0:

            mark_event_failed(
                event_id,
                event_index,
                0,
                f"No files found for event {event_id}"
            )

            return

        logging.info(
            f"Started processing event {event_id}"
        )

        # -------------------------------------------------
        # Total number of files
        # -------------------------------------------------

        total_files = len(event_files)

        # -------------------------------------------------
        # Process every PDF
        # -------------------------------------------------

        for index, file in enumerate(event_files):

            logging.info(
                f"Processing file {index + 1}/{total_files}: "
                f"{file['original_filename']}"
            )

            file_path = os.path.join(
                "uploaded_files",
                file["stored_filename"]
            )

            # -------------------------------------------------
            # 1. Read PDF
            # -------------------------------------------------

            try:

                reader = PdfReader(
                    file_path
                )

            except Exception as e:

                mark_event_failed(
                    event_id,
                    event_index,
                    int((index / total_files) * 100),
                    f"Failed to open PDF "
                    f"{file['file_id']}: {e}"
                )

                return

            # -------------------------------------------------
            # 2. Extract text
            # -------------------------------------------------

            text = ""

            try:

                for page in reader.pages:

                    page_text = page.extract_text()

                    if page_text:

                        text += page_text

            except Exception as e:

                mark_event_failed(
                    event_id,
                    event_index,
                    int((index / total_files) * 100),
                    f"Text extraction failed for file "
                    f"{file['file_id']}: {e}"
                )

                return

            # -------------------------------------------------
            # PDF has no extractable text
            # -------------------------------------------------

            if not text.strip():

                mark_event_failed(
                    event_id,
                    event_index,
                    int((index / total_files) * 100),
                    f"PDF {file['file_id']} "
                    f"contains no extractable text"
                )

                return

            logging.info(
                f"Text extracted successfully from "
                f"{file['original_filename']}"
            )

            # -------------------------------------------------
            # 3. Send extracted text to webhook
            # -------------------------------------------------

            data = {
                "event_id": event_id,
                "file_id": file["file_id"],
                "filename": file["original_filename"],
                "status": "completed",
                "text": text
            }

            success = False

            # Try webhook up to 3 times
            for attempt in range(1, 4):

                try:

                    logging.info(
                        f"Sending webhook for "
                        f"{file['original_filename']} "
                        f"(attempt {attempt}/3)"
                    )

                    response = http_session.post(
                        settings.webhook_url,
                        json=data,
                        timeout=settings.external_api_timeout
                    )

                    response.raise_for_status()

                    success = True

                    logging.info(
                        f"Webhook succeeded for "
                        f"{file['original_filename']}"
                    )

                    break

                except rq.exceptions.Timeout:

                    logging.error(
                        f"Webhook timeout "
                        f"(attempt {attempt}/3)"
                    )

                except rq.exceptions.RequestException as e:

                    logging.error(
                        f"Webhook failed "
                        f"(attempt {attempt}/3): {e}"
                    )

            # -------------------------------------------------
            # All webhook attempts failed
            # -------------------------------------------------

            if not success:

                mark_event_failed(
                    event_id,
                    event_index,
                    int((index / total_files) * 100),
                    f"Webhook failed after 3 attempts "
                    f"for {file['original_filename']}"
                )

                return

            # -------------------------------------------------
            # 4. Update progress
            # -------------------------------------------------

            # Calculate progress based on actual
            # number of files.
            #
            # Example:
            #
            # 1 file:
            # 100%
            #
            # 4 files:
            # 25%, 50%, 75%, 100%
            #
            # 6 files:
            # 16%, 33%, 50%, 66%, 83%, 100%

            progress = int(
                ((index + 1) / total_files) * 100
            )

            events_list = read_events()

            events_list[event_index]["progress"] = progress

            events_list[event_index]["status"] = "processing"

            events_list[event_index]["updated_date"] = (
                datetime.now().isoformat()
            )

            write_events(events_list)

            set_processing_state(
                event_id,
                "processing",
                progress
            )

            logging.info(
                f"Event {event_id} progress: "
                f"{progress}%"
            )

        # -----------------------------------------------------
        # 5. All files successfully processed
        # -----------------------------------------------------

        events_list = read_events()

        events_list[event_index]["progress"] = 100

        events_list[event_index]["status"] = "completed"

        events_list[event_index]["updated_date"] = (
            datetime.now().isoformat()
        )

        write_events(events_list)

        set_processing_state(
            event_id,
            "completed",
            100
        )

        # Release processing lock
        valkey_client.delete(
            f"processing:{event_id}"
        )

        logging.info(
            f"Event {event_id} processing "
            f"completed successfully"
        )

    except Exception as e:

        # Catch unexpected errors so the event does not
        # remain stuck in processing.

        logging.exception(
            f"Unexpected error while processing "
            f"event {event_id}: {e}"
        )

        try:

            events_list = read_events()

            event_index = -1

            for index in range(len(events_list)):

                if events_list[index]["id"] == event_id:

                    event_index = index

                    break

            if event_index != -1:

                current_progress = events_list[
                    event_index
                ].get(
                    "progress",
                    0
                )

                events_list[event_index]["status"] = "failed"

                events_list[event_index]["updated_date"] = (
                    datetime.now().isoformat()
                )

                write_events(events_list)

                set_processing_state(
                    event_id,
                    "failed",
                    current_progress
                )

        except Exception as cleanup_error:

            logging.error(
                f"Failed to update event after "
                f"unexpected error: {cleanup_error}"
            )

        # Always try to release processing lock
        try:

            valkey_client.delete(
                f"processing:{event_id}"
            )

        except Exception:

            pass


# ---------------------------------------------------------
# Start event processing
# ---------------------------------------------------------

@app.post("/events/{event_id}/process")
def post_process(
    event_id: int,
    bg_tasks: BackgroundTasks,
    request: Request,
    idempotency_key: str = Header(...)
):

    # -----------------------------------------------------
    # Rate limiting
    # -----------------------------------------------------

    client_ip = request.client.host

    rate_limit_key = f"rate_limit:{client_ip}"

    request_count = valkey_client.incr(
        rate_limit_key
    )

    if request_count == 1:

        valkey_client.expire(
            rate_limit_key,
            60
        )

    if request_count > 5:

        return JSONResponse(
            content={
                "error": "RATE_LIMIT_EXCEEDED",
                "message": "Too many requests"
            },
            status_code=429
        )

    # -----------------------------------------------------
    # Check event
    # -----------------------------------------------------

    events_list = read_events()

    event_index = -1

    for index in range(len(events_list)):

        if events_list[index]["id"] == event_id:

            event_index = index

            break

    if event_index == -1:

        return JSONResponse(
            content={
                "error": "EVENT_NOT_FOUND",
                "message": "An event with this ID does not exist"
            },
            status_code=404
        )

    # -----------------------------------------------------
    # Check files
    # -----------------------------------------------------

    files_list = read_files()

    event_files = []

    for file in files_list:

        if file["event_id"] == event_id:

            event_files.append(file)

    if len(event_files) == 0:

        return JSONResponse(
            content={
                "error": "NO_FILES",
                "message": "There are no uploaded files for this event"
            },
            status_code=404
        )

    # -----------------------------------------------------
    # Idempotency
    # -----------------------------------------------------

    idempotency_valkey_key = (
        f"idempotency:{idempotency_key}"
    )

    if valkey_client.get(
        idempotency_valkey_key
    ):

        return JSONResponse(
            content={
                "error": "DUPLICATE_REQUEST",
                "message": "This request has already been processed"
            },
            status_code=409
        )

    # -----------------------------------------------------
    # Processing lock
    # -----------------------------------------------------

    processing_key = (
        f"processing:{event_id}"
    )

    # NX means:
    # Only create this key if it does not already exist.
    #
    # Therefore, two requests cannot start the same
    # event processing at the same time.

    if not valkey_client.set(
        processing_key,
        "processing",
        nx=True,
        ex=3600
    ):

        return JSONResponse(
            content={
                "error": "EVENT_ALREADY_PROCESSING",
                "message": "This event is already being processed"
            },
            status_code=409
        )

    # -----------------------------------------------------
    # Save idempotency key
    # -----------------------------------------------------

    valkey_client.set(
        idempotency_valkey_key,
        "processed",
        ex=300
    )

    # -----------------------------------------------------
    # Set initial processing state
    # -----------------------------------------------------

    events_list[event_index]["status"] = "processing"

    events_list[event_index]["progress"] = 0

    events_list[event_index]["updated_date"] = (
        datetime.now().isoformat()
    )

    write_events(events_list)

    set_processing_state(
        event_id,
        "processing",
        0
    )

    logging.info(
        f"Processing event {event_id}"
    )

    # -----------------------------------------------------
    # Start background processing
    # -----------------------------------------------------

    bg_tasks.add_task(
        process_event,
        event_id
    )

    # -----------------------------------------------------
    # Return immediately
    # -----------------------------------------------------

    return JSONResponse(
        content={
            "message": "Event processing started"
        },
        status_code=202
    )


# ---------------------------------------------------------
# SSE stream
# ---------------------------------------------------------

@app.get("/events/{event_id}/stream")
async def event_stream(event_id: int):

    # -----------------------------------------------------
    # Check event exists
    # -----------------------------------------------------

    events_list = read_events()

    event_exists = False

    for event in events_list:

        if event["id"] == event_id:

            event_exists = True

            break

    if not event_exists:

        return JSONResponse(
            content={
                "error": "EVENT_NOT_FOUND",
                "message": "An event with this ID does not exist"
            },
            status_code=404
        )

    # -----------------------------------------------------
    # SSE generator
    # -----------------------------------------------------

    async def event_generator():

        last_data = None

        try:

            while True:

                # Read processing state from Valkey
                status = valkey_client.get(
                    f"event:{event_id}:status"
                )

                progress = valkey_client.get(
                    f"event:{event_id}:progress"
                )

                # If Valkey does not have processing state,
                # read the event from JSON.
                if status is None:

                    latest_events = read_events()

                    current_event = None

                    for event in latest_events:

                        if event["id"] == event_id:

                            current_event = event

                            break

                    if current_event is None:

                        return

                    status = current_event["status"]

                    progress = current_event["progress"]

                else:

                    progress = int(progress)

                data = {
                    "event_id": event_id,
                    "status": status,
                    "progress": progress
                }

                # Only send an SSE message when something
                # actually changed.
                if data != last_data:

                    yield (
                        f"data: {json.dumps(data)}\n\n"
                    )

                    last_data = data

                # Stop stream when processing finishes
                if status == "completed":

                    return

                if status == "failed":

                    return

                await asyncio.sleep(1)

        except asyncio.CancelledError:

            logging.info(
                f"SSE client disconnected from "
                f"event {event_id}"
            )

            raise

        finally:

            logging.info(
                f"SSE stream closed for event {event_id}"
            )

    # -----------------------------------------------------
    # Return SSE response
    # -----------------------------------------------------

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


# ---------------------------------------------------------
# Health check
# ---------------------------------------------------------

@app.get("/health")
def health():

    return {
        "status": "healthy"
    }


# ---------------------------------------------------------
# Readiness check
# ---------------------------------------------------------

@app.get("/health/ready")
def health_ready():

    try:

        valkey_client.ping()

        return {
            "status": "ready",
            "valkey": "available"
        }

    except Exception:

        return JSONResponse(
            content={
                "status": "not ready",
                "valkey": "unavailable"
            },
            status_code=503
        )