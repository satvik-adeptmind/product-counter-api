from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import uuid
import random # Import the random library for sleeping

app = Quart(__name__)

job_database = {}
headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=True):
    if not keyword or not keyword.strip(): return 0
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
    print(f"Starting background task for job_id: {job_id} with {len(keywords)} keywords.")
    job_database[job_id] = {"status": "processing", "results": None}
    
    try:
        if env == "prod":
            base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        else:
            base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
        
        all_results = []
        # Your original script used a larger chunk size with a sleep. Let's replicate that.
        chunk_size = 1000 
        
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

                # --- CRITICAL PERFORMANCE FIX ---
                # If this is not the last chunk, pause to cool down.
                if i + chunk_size < len(keywords):
                    sleep_time = random.randint(5, 15)
                    print(f"Job {job_id}: Chunk complete. Sleeping for {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time) # Use asyncio.sleep in an async function

        job_database[job_id] = {"status": "complete", "results": all_results}
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"FATAL ERROR in job {job_id}: {e}")
        job_database[job_id] = {"status": "failed", "results": str(e)}

# --- API Endpoints (Unchanged) ---

@app.route('/health', methods=['GET'])
async def health_check():
    """
    A simple health check endpoint that UptimeRobot can hit.
    It returns a 200 OK status to show that the service is live.
    """
    return jsonify({"status": "ok"}), 200

@app.route('/start_job', methods=['POST'])
async def start_job_endpoint():
    request_data = await request.get_json()
    if not request_data: return jsonify({"error": "Invalid request"}), 400
    job_id = str(uuid.uuid4())
    asyncio.create_task(background_task(job_id, request_data['shop_id'], request_data['keywords'], request_data.get('environment', 'prod')))
    return jsonify({"status": "success", "job_id": job_id})

@app.route('/get_results/<job_id>', methods=['GET'])
async def get_results_endpoint(job_id):
    job = job_database.get(job_id)
    if not job: return jsonify({"status": "not_found"}), 404
    return jsonify(job)

# This part is used by Render to start the server.
if __name__ == '__main__':
    app.run()
