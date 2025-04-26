import asyncio
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f"dispatcher.log", mode='w', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s', '%m/%d/%Y %I:%M:%S %p'))
logger.addHandler(file_handler)

async def request_gdacs_collector(host, port, coordinates, radius):
    """
    Send request to gdacs_collecor node. 

    Parameters: 
        host: gdacs_collector node address
        port: gdacs_collector node port
        coordinates: Location of interest 
        radius: 

    Returns: 
        
    """
    try:
        reader, writer = await asyncio.open_connection(host, port)
        logger.info(f"Connected to gdacs_collector at {host}:{port}")
        
        request_data = {
            "coordinates": coordinates,
            "radius": radius
        }
        
        request_json = json.dumps(request_data).encode('utf-8')
        writer.write(request_json)
        await writer.drain()
        logger.info(f"Sent request: {request_data}")
        
        buff_sz = 2**14
        response_data = await reader.read(buff_sz)  
        response = json.loads(response_data.decode('utf-8'))
        
        writer.close()
        
        return response 
    except Exception as e:
        logger.error(f"Error communicating with gdacs_collector: {str(e)}")
        return {"error": str(e)}

async def main():
    response = await request_gdacs_collector(
        "localhost", 
        8080, 
        coordinates=[41.9028, 12.4964],  # Rome, Italy
        radius=5000  
    )
    
    if isinstance(response, list):
        print(f"\n=== Found {len(response)} disasters in the specified area ===")
        for i, disaster in enumerate(response, 1):
            print(f"\n--- Disaster {i} ---")
            print(f"Title: {disaster.get('title', 'N/A')}")
            print(f"Description: {disaster.get('description', 'N/A')}")
    else:
        print(f"Error: invalid response from gdacs_collector. ")

if __name__ == "__main__":
    asyncio.run(main())
