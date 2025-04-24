import asyncio
import json
import logging
from typing import Dict, Any, List, Tuple, Optional

from aiohttp import ClientSession # for making HTTP requests to GDACS
from aio_georss_gdacs import GdacsFeed

logger = logging.getLogger(__name__)
logging.basicConfig(
        filename=__name__ + ".log",
        filemode='w', 
        encoding='utf-8',
        format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', 
        level=logging.DEBUG
        )

class GDACS_Collector:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        logger.info(f"GDACS_Collector initialized to listen on {host}:{port}")
    
    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        dispatcher_addr = writer.get_extra_info('peername')
        logger.info(f"New request from dispatcher at {dispatcher_addr}")
        
        data = await reader.read(4096)
        if not data:
            logger.warning(f"No data received from {dispatcher_addr}")
            writer.close()
            return
        
        try:
            request = json.loads(data.decode('utf-8')) # from json to dict
            logger.info(f"Received request: {request}")
            result = await self.collect(request)
            writer.write(json.dumps(result).encode('utf-8'))
            await writer.drain() # send
        except Exception as e:
            logger.error(f"Error handling request: {str(e)}")
            response = {"error": str(e)}
            writer.write(json.dumps(response).encode('utf-8'))
            await writer.drain()
        finally:
            writer.close()
    
    async def collect(self, request: Dict[str, Any]):
        try:
            coordinates = request.get('coordinates')
            radius = request.get('radius')

            if (coordinates is None or radius is None):
                logger.error("Invalid coordinates or radius") 
                return "Invalid request"
                                  
            async with ClientSession() as session:
                feed = GdacsFeed(
                    session, 
                    tuple(coordinates), 
                    filter_radius=radius
                )
                
                logger.info(f"Fetching GDACS data with parameters: coordinates={coordinates}, radius={radius}")
                status, entries = await feed.update()

                return entries               
                
        except Exception as e:
            logger.error(f"Error collecting disaster data: {str(e)}")
            return {"error": f"Failed to collect disaster data: {str(e)}"}
    
    async def start(self):
        server = await asyncio.start_server(
            self.handle_request, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        logger.info(f'Global Disasters Collector serving on {addr}')
        
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    collector = GDACS_Collector("localhost", 8080)
    asyncio.run(collector.start())
