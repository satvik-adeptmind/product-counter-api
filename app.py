from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import uuid
import random
import time

app = Quart(__name__)

# This dictionary will now store a timestamp with each job.
job_database = {}
headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=True):
    # This core logic is perfect and remains unchanged.
    if not keyword or not keyword.strip(): return 0
    query = keyword.strip()
    data = {"query": query, "size": 300}
    if remove_unnecessary_fields: data["include_fields"] = ["product_id"]
    async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
        response.raise_for_status()
        response_json = await response.json()
        products = response_json.get("products", [])
        prod_count = len(products)
        if prod_count > 0: return prod_count
        if "timed_out_services" in response_json: raise asyncio.TimeoutError("API service timed out internally.")
        if remove_unnecessary_fields: return await fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=False)
        return 0

async def background_task(job_id, shop_id, keywords, env):
    """The main worker task. It processes keywords and stores the final result."""
    print(f"Starting background task for job_id: {job_id}")
    job_database[job_id] = {"status": "processing", "results": None, "timestamp": time.time()}
    
    try:
        if env == "prod": base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        else: base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
        
        all_results = []
        chunk_size = 50 # Keep chunk size low to be kind to memory
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(keywords), chunk_size):
                chunk = keywords[i:i + chunk_size]
                print(f"Job {job_id}: Processing chunk {i//chunk_size + 1}...")
                tasks = []
                async def wrapper(kw):
                    try: return await fetch_single_keyword_with_fallback(session, base_url, kw)
                    except Exception: return -1
                for kw in chunk: tasks.append(wrapper(kw))
                
                chunk_results = await asyncio.gather(*tasks)
                all_results.extend(chunk_results)

                if i + chunk_size < len(keywords):
                    sleep_time = random.randint(3, 8)
                    await asyncio.sleep(sleep_time)

        job_database[job_id] = {"status": "complete", "results": all_results, "timestamp": time.time()}
        print(f"Job {job_id} completed successfully.")
    except Exception as e:
        print(f"FATAL ERROR in job {job_id}: {e}")
        job_database[job_id] = {"status": "failed", "results": str(e), "timestamp": time.time()}

async def cleanup_old_jobs():
    """
    This background "janitor" task runs forever, cleaning up old jobs to prevent memory leaks.
    """
    while True:
        await asyncio.sleep(600)  # Run every 10 minutes
        current_time = time.time()
        # It's important to iterate over a copy of the keys, as we are modifying the dictionary
        jobs_to_delete = []
        for job_id, job_data in job_database.items():
            # Delete jobs that are older than 1 hour (3600 seconds)
            if current_time - job_data.get("timestamp", 0) > 3600:
                jobs_to_delete.append(job_id)
        
        if jobs_to_delete:
            print(f"Cleaning up old jobs: {jobs_to_delete}")
            for job_id in jobs_to_delete:
                del job_database[job_id]

@app.before_serving
async def startup():
    """This function runs once when the application starts up."""
    # Start the background janitor task.
    asyncio.create_task(cleanup_old_jobs())

# --- API Endpoints ---

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    request_data = await request.get_json()
    if not request_data: return jsonify({"error": "Invalid request"}), 400
    job_id = str(uuid.uuid4())
    asyncio.create_task(background_task(job_id, request_data['shop_id'], request_data['keywords'], request_data.get('environment', 'prod')))
    return jsonify({"status": "success", "job_id": job_id})

@app.route('/health', methods=['GET'])
async def health_check():
    """
    A simple health check endpoint that UptimeRobot can hit.
    It returns a 200 OK status to show that the service is live.
    """
    return jsonify({"status": "ok"}), 200

@app.route('/get_results/<job_id>', methods=['GET'])
async def get_results_endpoint(job_id):
    job = job_database.get(job_id)
    if not job: return jsonify({"status": "not_found"}), 404
    
    # --- IMMEDIATE CLEANUP ---
    # If the job is finished, return the results and then immediately delete it from memory.
    if job["status"] in ["complete", "failed"]:
        job_data_to_return = job.copy()
        del job_database[job_id]
        print(f"Job {job_id} has been fetched and cleared from memory.")
        return jsonify(job_data_to_return)
    else:
        # If still processing, just return the status.
        return jsonify(job)

if __name__ == '__main__':
    app.run()
