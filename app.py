# app.py - FINAL Version 2, using Lifespan Protocol

import aiohttp
import asyncio
import json
import uuid
from quart import Quart, request, jsonify
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type

# =================================================================================
# 1. SETUP: Use asyncio-native tools
# =================================================================================

app = Quart(__name__)

# An asyncio-native queue. More natural for a Quart app.
job_queue = asyncio.Queue()

# Simple dictionary for results. Since asyncio is single-threaded,
# we don't need locks for this.
job_database = {}

headers = {'Content-Type': 'application/json'}

# =================================================================================
# 2. THE WORKER LOGIC (Runs in the background)
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
    """The core data processing logic."""
    shop_id, keywords = job_data['shop_id'], job_data['keywords']
    env = job_data.get('environment', 'prod')

    print(f"Worker is now PROCESSING job {job_id} with {len(keywords)} keywords.")
    job_database[job_id] = {"status": "processing"}

    try:
        base_url = f"https://search-{env}-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        sem = asyncio.Semaphore(8)
        
        async with aiohttp.ClientSession() as session:
            async def wrapper(kw):
                async with sem:
                    try: return await fetch_single_keyword_advanced(session, base_url, kw)
                    except Exception: return -1
            tasks = [wrapper(kw) for kw in keywords]
            results = await asyncio.gather(*tasks)
        
        job_database[job_id] = {"status": "complete", "results": results}
        print(f"Worker has FINISHED job {job_id}.")

    except Exception as e:
        print(f"Worker FAILED job {job_id}: {e}")
        job_database[job_id] = {"status": "failed", "results": str(e)}

async def worker_loop():
    """The main background task loop."""
    print("Background worker task has started. Waiting for jobs.")
    while True:
        job_id, job_data = await job_queue.get()
        await process_job_async(job_id, job_data)
        job_queue.task_done()

# =================================================================================
# 3. LIFESPAN MANAGEMENT (The key to making this work)
# =================================================================================

@app.before_serving
async def startup():
    """This function runs ONCE when the application starts up."""
    print("Application starting up. Launching the background worker task.")
    # This creates the worker task inside the correct, running event loop.
    asyncio.create_task(worker_loop())

# =================================================================================
# 4. THE WEB SERVER ENDPOINTS
# =================================================================================

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    """Receives a job, puts it in the async queue."""
    request_data = await request.get_json()
    if not request_data or 'keywords' not in request_data:
        return jsonify({"error": "Invalid request"}), 400

    job_id = str(uuid.uuid4())
    await job_queue.put((job_id, request_data))
    job_database[job_id] = {"status": "queued"}
    
    print(f"Web server queued job {job_id}. The background worker will pick it up.")
    return jsonify({"status": "success", "job_id": job_id})

@app.route('/get_results/<job_id>', methods=['GET'])
async def get_results_endpoint(job_id):
    """Checks the shared database for the job status/results."""
    job = job_database.get(job_id, {"status": "not_found"})
    return jsonify(job)

@app.route('/health', methods=['GET'])
async def health_check():
    return jsonify({"status": "ok"}), 200
