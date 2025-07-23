# app.py - Version 3 (Advanced Fetching Logic + Memory Safe Concurrency)

from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import uuid
from datetime import datetime, timedelta

# Use Quart, the async-native framework
app = Quart(__name__)

# A simple in-memory database to store job status and results.
job_database = {}

headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_advanced(session, base_url, keyword, remove_unnecessary_fields=True):
    """
    This is the new, robust fetching function inspired by your script.
    It includes the smart fallback mechanism.
    """
    if not keyword or not keyword.strip():
        return 0  # Return 0 for empty or whitespace-only keywords

    query = keyword.strip()
    data = {"query": query, "size": 300}
    
    if remove_unnecessary_fields:
        data["include_fields"] = ["product_id"]

    async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
        response.raise_for_status()  # Raise an error for non-2xx responses
        response_json = await response.json()
        
        products = response_json.get("products", [])
        prod_count = len(products)

        # Success Case: We got products, return the count.
        if prod_count > 0:
            return prod_count

        # Failure Case 1: The API itself timed out internally. Raise to trigger a retry.
        if "timed_out_services" in response_json:
            raise asyncio.TimeoutError("API service timed out internally.")

        # Failure Case 2: No products found, but no timeout. Try the fallback.
        if remove_unnecessary_fields:
            # This is the recursive fallback from your ideal script.
            return await fetch_single_keyword_advanced(session, base_url, keyword, remove_unnecessary_fields=False)
        
        # Final Case: Fallback also returned no products. The count is genuinely 0.
        return 0

# =================================================================================
# BACKGROUND WORKER & SERVER-SIDE LOGIC
# =================================================================================

async def background_task(job_id, shop_id, keywords, env):
    """
    This is the main worker task that runs in the background on the server.
    It now uses a Semaphore for memory-safe concurrency.
    """
    print(f"Starting ADVANCED background task for job_id: {job_id} with {len(keywords)} keywords.")
    job_database[job_id]["status"] = "processing"
    
    # *** KEY TO STABILITY: The Semaphore ***
    # This limits concurrent requests to a safe number (e.g., 15) to prevent memory crashes.
    sem = asyncio.Semaphore(15)

    try:
        base_url = f"https://search-{env}-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        
        all_results = []
        
        async with aiohttp.ClientSession() as session:
            
            # This wrapper safely calls the fetching logic and is controlled by the semaphore.
            async def wrapper(kw):
                async with sem: # The semaphore acts as a gatekeeper here
                    try:
                        return await fetch_single_keyword_advanced(session, base_url, kw)
                    except Exception as e:
                        print(f"Error fetching keyword '{kw}': {e}")
                        return -1 # Return -1 to indicate a failure for this specific keyword

            # Create all tasks, but the semaphore will ensure only 15 run at a time.
            tasks = [wrapper(kw) for kw in keywords]
            chunk_results = await asyncio.gather(*tasks)
            all_results.extend(chunk_results)
        
        # When the entire job is done, store the complete results.
        job_database[job_id]["status"] = "complete"
        job_database[job_id]["results"] = all_results
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"FATAL ERROR in job {job_id}: {e}")
        job_database[job_id]["status"] = "failed"
        job_database[job_id]["results"] = str(e)


# =================================================================================
# API ENDPOINTS (No changes needed here)
# =================================================================================

def cleanup_jobs():
    """Removes jobs older than 2 hours to prevent memory leaks."""
    now = datetime.utcnow()
    jobs_to_delete = [
        job_id for job_id, data in job_database.items()
        if data.get("timestamp") and now - data["timestamp"] > timedelta(hours=2)
    ]
    for job_id in jobs_to_delete:
        try:
            del job_database[job_id]
            print(f"Cleaned up expired job: {job_id}")
        except KeyError:
            pass # Job might have been deleted in another request, which is fine.

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    """Receives the keyword list, creates a unique job_id, starts the background task, and returns the job_id."""
    request_data = await request.get_json()
    if not request_data:
        return jsonify({"error": "Invalid request"}), 400

    # Run cleanup of old jobs before starting a new one.
    cleanup_jobs()

    job_id = str(uuid.uuid4())
    shop_id = request_data['shop_id']
    keywords = request_data['keywords']
    env = request_data.get('environment', 'prod') # Default to 'prod' if not specified

    # Store initial job info
    job_database[job_id] = {
        "status": "queued", 
        "results": None, 
        "timestamp": datetime.utcnow()
    }
    
    asyncio.create_task(background_task(job_id, shop_id, keywords, env))
    
    return jsonify({"status": "success", "job_id": job_id})

@app.route('/health', methods=['GET'])
async def health_check():
    """A simple health check endpoint."""
    return jsonify({"status": "ok"}), 200

    
@app.route('/get_results/<job_id>', methods=['GET'])
async def get_results_endpoint(job_id):
    """Allows the Google Sheet to poll for the results of a specific job."""
    job = job_database.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    
    return jsonify(job)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)




