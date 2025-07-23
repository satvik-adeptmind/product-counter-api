from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type
import uuid
import random
import os

app = Quart(__name__)

# --- NEW: Use the server's disk for storage ---
# Render provides a temporary disk space at '/var/data'. We'll use this.
JOB_STORAGE_PATH = "/var/data/jobs"
# Ensure the directory exists when the app starts.
os.makedirs(JOB_STORAGE_PATH, exist_ok=True)


headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=True):
    # This function is already perfect and needs no changes.
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
    """
    This background worker now writes results to a file instead of storing them in memory.
    """
    job_file_path = os.path.join(JOB_STORAGE_PATH, f"{job_id}.json")
    print(f"Starting background task for job_id: {job_id}. Output file: {job_file_path}")
    
    # Immediately write a "processing" status to the file.
    with open(job_file_path, 'w') as f:
        json.dump({"status": "processing", "results": None}, f)
    
    try:
        if env == "prod":
            base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        else:
            base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
        
        all_results = []
        chunk_size = 1000 # Keep the high chunk size, it's safe now.
        
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
                    sleep_time = random.randint(5, 15)
                    print(f"Job {job_id}: Chunk complete. Sleeping for {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)

        # When done, write the complete results to the file.
        with open(job_file_path, 'w') as f:
            json.dump({"status": "complete", "results": all_results}, f)
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"FATAL ERROR in job {job_id}: {e}")
        with open(job_file_path, 'w') as f:
            json.dump({"status": "failed", "results": str(e)}, f)

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
    """
    This endpoint now reads the result from the job file on disk.
    """
    job_file_path = os.path.join(JOB_STORAGE_PATH, f"{job_id}.json")
    
    try:
        with open(job_file_path, 'r') as f:
            job_data = json.load(f)
        return jsonify(job_data)
    except FileNotFoundError:
        return jsonify({"status": "not_found"}), 404
    except Exception as e:
        return jsonify({"status": "failed", "results": f"Error reading job file: {e}"}), 500

if __name__ == '__main__':
    app.run()
