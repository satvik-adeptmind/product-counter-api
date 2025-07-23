from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import uuid
import random

# Use Quart, the async-native framework
app = Quart(__name__)

# A simple in-memory dictionary to store job status and results.
# This is safe for the free tier as long as we manage job size.
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
    This is the main worker task. It processes keywords in small, memory-safe chunks
    and includes a cool-down period to avoid CPU throttling.
    """
    print(f"Starting background task for job_id: {job_id} with {len(keywords)} keywords.")
    job_database[job_id] = {"status": "processing", "results": None}
    
    try:
        if env == "prod":
            base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        else:
            base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
        
        all_results = []
        # A smaller chunk size to keep peak memory usage low on Render's free tier.
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

                # Pause between chunks to prevent CPU throttling.
                if i + chunk_size < len(keywords):
                    sleep_time = random.randint(3, 8) # A slightly shorter sleep is fine for smaller chunks
                    print(f"Job {job_id}: Chunk complete. Sleeping for {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)

        job_database[job_id] = {"status": "complete", "results": all_results}
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"FATAL ERROR in job {job_id}: {e}")
        job_database[job_id] = {"status": "failed", "results": str(e)}

# --- API Endpoints ---

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    request_data = await request.get_json()
    if not request_data:
        return jsonify({"error": "Invalid request"}), 400
    job_id = str(uuid.uuid4())
    asyncio.create_task(background_task(job_id, request_data['shop_id'], request_data['keywords'], request_data.get('environment', 'prod')))
    return jsonify({"status": "success", "job_id": job_id})

@app.route('/get_results/<job_id>', methods=['GET'])
async def get_results_endpoint(job_id):
    job = job_database.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)

if __name__ == '__main__':
    app.run()
