from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type

app = Quart(__name__)

headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword(session, base_url, keyword):
    if not keyword or not keyword.strip():
        return 0
    query = keyword.strip()
    data = {"query": query, "size": 300, "include_fields": ["product_id"]}
    async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
        response.raise_for_status()
        response_json = await response.json()
        if response_json.get("timed_out_services"):
            raise asyncio.TimeoutError("Adeptmind API service timed out.")
        return len(response_json.get("products", []))

async def process_keywords_in_chunks(shop_id, keywords):
    """
    Processes the main list of keywords in smaller, more manageable chunks
    to keep memory usage low and avoid timeouts.
    """
    base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
    all_results = []
    chunk_size = 200  # Process 200 keywords at a time

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(keywords), chunk_size):
            chunk = keywords[i:i + chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}...") # Log progress on the server
            
            tasks = []
            async def wrapper(kw):
                try:
                    return await fetch_single_keyword(session, base_url, kw)
                except Exception:
                    return -1
            
            for kw in chunk:
                tasks.append(wrapper(kw))
            
            chunk_results = await asyncio.gather(*tasks)
            all_results.extend(chunk_results)

    return all_results

@app.route('/fetch_counts', methods=['POST'])
def handle_fetch_request():
    request_data = request.get_json()
    if not request_data or 'shop_id' not in request_data or 'keywords' not in request_data:
        return jsonify({"error": "Request must include 'shop_id' and 'keywords'"}), 400
    
    shop_id = request_data['shop_id']
    keywords = request_data['keywords']
    
    # Use the new chunking processor
    product_counts = asyncio.run(process_keywords_in_chunks(shop_id, keywords))
    
    return jsonify({"product_counts": product_counts})

# Gunicorn is used for production, this part is for local testing if needed.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
