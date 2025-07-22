from quart import Quart, request, jsonify
import aiohttp
import asyncio
import json
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_exception_type

# Use Quart, the async-native framework
app = Quart(__name__)

headers = {'Content-Type': 'application/json'}

@retry(
    wait=wait_random_exponential(min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
)
async def fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=True):
    """
    This function now perfectly mirrors the logic of your original Streamlit script.
    """
    if not keyword or not keyword.strip():
        return 0

    query = keyword.strip()
    data = {"query": query, "size": 300}

    # First, try the optimized call if specified
    if remove_unnecessary_fields:
        data["include_fields"] = ["product_id"]

    async with session.post(base_url, headers=headers, data=json.dumps(data)) as response:
        response.raise_for_status()  # Raise an error for non-2xx responses
        response_json = await response.json()

        products = response_json.get("products", [])
        prod_count = len(products)

        if prod_count > 0:
            return prod_count

        # If the API timed out internally, raise an exception to trigger a Tenacity retry
        if "timed_out_services" in response_json:
            raise asyncio.TimeoutError("API service timed out internally.")

        # If we got 0 products and were only asking for IDs, try again asking for all fields
        if remove_unnecessary_fields:
            return await fetch_single_keyword_with_fallback(session, base_url, keyword, remove_unnecessary_fields=False)
        
        # If we still have 0 products after the full fallback, the count is truly 0
        return 0

async def process_keywords_in_chunks(shop_id, keywords, env):
    """Main orchestrator for processing a list of keywords in chunks."""
    if env == "prod":
        base_url = f"https://search-prod-dlp-adept-search.search-prod.adeptmind.app/search?shop_id={shop_id}"
    else:
        # Note: Added staging URL from your script for completeness
        base_url = f"https://dlp-staging-search-api.retail.adeptmind.ai/search?shop_id={shop_id}"
    
    all_results = []
    chunk_size = 200

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(keywords), chunk_size):
            chunk = keywords[i:i + chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}...")
            
            tasks = []
            # Wrapper to catch any final exception after retries fail
            async def wrapper(kw):
                try:
                    # Start the process with the optimized call
                    return await fetch_single_keyword_with_fallback(session, base_url, kw)
                except Exception as e:
                    print(f"Error on keyword '{kw}': {e}")
                    return -1
            
            for kw in chunk:
                tasks.append(wrapper(kw))
            
            chunk_results = await asyncio.gather(*tasks)
            all_results.extend(chunk_results)

    return all_results

@app.route('/fetch_counts', methods=['POST'])
async def handle_fetch_request():
    request_data = await request.get_json()
    
    if not request_data or 'shop_id' not in request_data or 'keywords' not in request_data:
        return jsonify({"error": "Request must include 'shop_id' and 'keywords'"}), 400
    
    shop_id = request_data['shop_id']
    keywords = request_data['keywords']
    environment = request_data.get('environment', 'prod') 
    
    product_counts = await process_keywords_in_chunks(shop_id, keywords, environment)
    
    return jsonify({"product_counts": product_counts})

# This part is for local testing and not used by Render's Gunicorn
if __name__ == '__main__':
    app.run()
