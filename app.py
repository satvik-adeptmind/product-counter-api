from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import uuid

# Use Quart, the async-native framework
app = Quart(__name__)

# A simple in-memory database to store job status and results.
# This will be cleared if the server restarts, which is an acceptable trade-off.
job_database = {}

headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=True):
    """
    This function contains the core, trusted logic to fetch a single product count,
    including the fallback mechanism.
    """
    if not keyword or not keyword.strip():
        return 0
    query = keyword.strip()
    data = {"query": query, "size": 300}
    if remove_unnecessary_fields:
        data["include_fields"] = ["product_id"]
    async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
        response.raise_for_status()
        response_json = await response.json()
        products = response_json.get("products", [])
        prod_count = len(products)
        if prod_count > 0:
            return prod_count
        if "timed_out_services" in response_json:
            raise asyncio.TimeoutError("API service timed out internally.")
        if remove_unnecessary_fields:
            return await fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=False)
        return 0

async def background_task(job_id, shop_id, keywords, env):
    """
    This is the main worker task that runs in the background on the server.
    It processes all keywords and stores the final result in the database.
    """
    print(f"Starting background task for job_id: {job_id} with {len(keywords)} keywords.")
    job_database[job_id] = {"status": "processing", "results": None}
    
    try:
        if env == "prod":
            base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        else:
            base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
        
        all_results = []
        # Chunk size is set to a memory-safe value for Render's free tier.
        chunk_size = 50
        
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(keywords), chunk_size):
                chunk = keywords[i:i + chunk_size]
                print(f"Job {job_id}: Processing chunk {i//chunk_size + 1}...")
                tasks = []
                async def wrapper(kw):
                    try:
                        return await fetch_single_keyword_with_fallback(session, base_url, kw)
                    except Exception:
                        return -1
                for kw in chunk:
                    tasks.append(wrapper(kw))
                
                chunk_results = await asyncio.gather(*tasks)
                all_results.extend(chunk_results)
        
        # When the entire job is done, store the complete results in our database.
        job_database[job_id] = {"status": "complete", "results": all_results}
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"FATAL ERROR in job {job_id}: {e}")
        job_database[job_id] = {"status": "failed", "results": str(e)}

# --- API Endpoints ---

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    """Receives the keyword list, creates a unique job_id, starts the background task, and returns the job_id."""
    request_data = await request.get_json()
    if not request_data:
        return jsonify({"error": "Invalid request"}), 400

    job_id = str(uuid.uuid4())
    
    # Start the long-running process in the background without waiting for it.
    asyncio.create_task(background_task(
        job_id,
        request_data['shop_id'],
        request_data['keywords'],
        request_data.get('environment', 'prod')
    ))
    
    # Immediately return the job_id so the Google Sheet can save it.
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
    """Allows the Google Sheet to poll for the results of a specific job."""
    job = job_database.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    
    # Return the current status and the results (if complete).
    return jsonify(job)

# This part is used by Render to start the server.
if __name__ == '__main__':
    app.run()
