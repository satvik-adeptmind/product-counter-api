# app.py - FINAL VERSION for Render Free Tier
# This version uses a background thread within a single service.

import aiohttp
import asyncio
import json
import os
import uuid
from quart import Quart, request, jsonify
import threading
import queue
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type

# =================================================================================
# 1. SETUP: In-Memory Queue and Shared Data Store (Thread-Safe)
# =================================================================================

app = Quart(__name__)

# A thread-safe queue to hold incoming jobs. The web server adds to this.
job_queue = queue.Queue()

# A thread-safe dictionary to store job statuses and results.
# The lock ensures that the web server and worker thread don't corrupt the data.
job_database = {}
db_lock = threading.Lock()

headers = {'Content-Type': 'application/json'}

# =================================================================================
# 2. THE HEAVY LIFTING LOGIC (The Worker's Job)
# =================================================================================

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_advanced(session, base_url, keyword, remove_unnecessary_fields=True):
    if not keyword or not keyword.strip(): return 0
    query = keyword.strip()
    data = {"query": query, "size": 300}
    if remove_unnecessary_fields: data["include_fields"] = ["product_id"]

    async with session.post(base_url, headers=headers, data=json.dumps(data), timeout=30) as response:
        response.raise_for_status()
        response_json = await response.json()
        products = response_json.get("products", [])
        prod_count = len(products)
        if prod_count > 0: return prod_count
        if "timed_out_services" in response_json: raise asyncio.TimeoutError("API service timed out.")
        if remove_unnecessary_fields: return await fetch_single_keyword_advanced(session, base_url, keyword, remove_unnecessary_fields=False)
        return 0

async def process_job_async(job_id, job_data):
    """The core asynchronous data processing logic."""
    shop_id = job_data['shop_id']
    keywords = job_data['keywords']
    env = job_data.get('environment', 'prod')

    print(f"Worker processing job {job_id} with {len(keywords)} keywords.")
    
    with db_lock:
        job_database[job_id] = {"status": "processing"}

    try:
        base_url = f"https://search-{env}-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        all_results = []
        
        sem = asyncio.Semaphore(8) # Safely control concurrency
        
        async with aiohttp.ClientSession() as session:
            async def wrapper(kw):
                async with sem:
                    try: return await fetch_single_keyword_advanced(session, base_url, kw)
                    except Exception: return -1

            tasks = [wrapper(kw) for kw in keywords]
            all_results = await asyncio.gather(*tasks)

        with db_lock:
            job_database[job_id] = {"status": "complete", "results": all_results}
        print(f"Worker finished job {job_id} successfully.")

    except Exception as e:
        print(f"Worker FAILED job {job_id}: {e}")
        with db_lock:
            job_database[job_id] = {"status": "failed", "results": str(e)}

# =================================================================================
# 3. THE WORKER THREAD (The "Factory Worker" Mind)
# =================================================================================

def worker_loop():
    """This function runs in a separate thread, processing jobs from the queue."""
    print("Worker thread started. Waiting for jobs.")
    while True:
        job_id, job_data = job_queue.get() # This will block until a job is available
        asyncio.run(process_job_async(job_id, job_data))
        job_queue.task_done()

# =================================================================================
# 4. THE WEB SERVER (The "Receptionist" Mind)
# =================================================================================

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    """Receives a job, puts it in the queue, and returns immediately."""
    request_data = await request.get_json()
    if not request_data or 'keywords' not in request_data:
        return jsonify({"error": "Invalid request"}), 400

    job_id = str(uuid.uuid4())
    
    # This is extremely fast and uses very little memory.
    job_queue.put((job_id, request_data))
    
    with db_lock:
        job_database[job_id] = {"status": "queued"}
    
    print(f"Web server queued job {job_id}.")
    return jsonify({"status": "success", "job_id": job_id})

@app.route('/get_results/<job_id>', methods=['GET'])
async def get_results_endpoint(job_id):
    """Checks the shared database for the job status/results."""
    with db_lock:
        job = job_database.get(job_id, {"status": "not_found"})
    return jsonify(job)

@app.route('/health', methods=['GET'])
async def health_check():
    return jsonify({"status": "ok"}), 200

# =================================================================================
# 5. STARTUP
# =================================================================================

# Create and start the worker thread. It's a daemon, so it will exit when the main app exits.
worker_thread = threading.Thread(target=worker_loop, daemon=True)
worker_thread.start()

# The main process continues on to run the web server.
# The 'if __name__' block is not strictly necessary on Render but is good practice.
# Render will use your Gunicorn start command.
