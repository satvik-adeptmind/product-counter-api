from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type

# Use Quart, the async-native framework
app = Quart(__name__)

headers = {'Content-Type': 'application/json'}


# This is the background task that does all the work
async def background_task(shop_id, keywords, env, callback_url, sheet_url):
    try:
        if env == "prod":
            base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
        else:
            base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
        
        chunk_size = 50

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(keywords), chunk_size):
                chunk = keywords[i:i + chunk_size]
                print(f"Processing chunk for sheet {sheet_url}, starting row {i + 2}")
                
                tasks = []
                # Define the worker function inside the loop
                async def wrapper(kw):
                    try:
                        return await fetch_single_keyword_with_fallback(session, base_url, kw)
                    except Exception as e:
                        print(f"Error on keyword '{kw}': {e}")
                        return -1
                
                for kw in chunk:
                    tasks.append(wrapper(kw))
                
                chunk_results = await asyncio.gather(*tasks)

                # Send the results of this chunk back to the Google Sheet
                callback_payload = {
                    "sheetUrl": sheet_url,
                    "results": chunk_results,
                    "startingRow": i + 2
                }
                print(f"Sending {len(chunk_results)} results back to {callback_url}")
                try:
                    await session.post(callback_url, headers=headers, data=json.dumps(callback_payload))
                except Exception as e:
                    print(f"ERROR sending callback: {e}")

    except Exception as e:
        print(f"FATAL ERROR in background task: {e}")


# --- THIS IS THE FULLY DEFINED FUNCTION ---
@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=True):
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

@app.route('/health', methods=['GET'])
async def health_check():
    """
    A simple health check endpoint that UptimeRobot can hit.
    It returns a 200 OK status to show that the service is live.
    """
    return jsonify({"status": "ok"}), 200

# This is the API endpoint that receives the initial request
@app.route('/fetch_counts', methods=['POST'])
async def handle_fetch_request():
    request_data = await request.get_json()
    
    if not request_data or 'callback_url' not in request_data:
        return jsonify({"error": "Request must include a 'callback_url'"}), 400
    
    # Run the processing as a background task
    asyncio.create_task(background_task(
        shop_id=request_data['shop_id'],
        keywords=request_data['keywords'],
        env=request_data.get('environment', 'prod'),
        callback_url=request_data['callback_url'],
        sheet_url=request_data['sheet_url']
    ))
    
    # Immediately return a success message
    return jsonify({"status": "success", "message": "Job accepted and is running in the background."}), 200

# This part is for local testing and not used by Render's Gunicorn
if __name__ == '__main__':
    app.run()
