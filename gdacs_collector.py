import asyncio
import json
import logging
from typing import Dict, Any
from aiohttp import ClientSession
from aio_georss_gdacs import GdacsFeed

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f"gdacs_collector.log", mode='w', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s', '%m/%d/%Y %I:%M:%S %p'))
logger.addHandler(file_handler)


class GdacsCollector:
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Receive request from dispatcher and sends a response in return. 
        
        Paremeters: 
            reader: Contains request. 
            writer: Contains response. 
        Returns:
            
        """
        dispatcher_addr = writer.get_extra_info('peername')
        request = await reader.read(4096)
        if not request:
            logger.warning(f"No request received from {dispatcher_addr}")
            writer.close()
            return
        
        try:
            request = json.loads(request.decode('utf-8')) # from json to dict
            response = await self.collect(request) # list of GdacsFeedEntry objects
            
            """ 
            `json.dump` cannot serialize objects. 
            `GdacsFeedEntry` objects in `response` must be 
            converted to a serializable type, e.g., `Dict`. 
            """
            serializable_reponse = self.serialize_entries(response)
                       
            writer.write(json.dumps(serializable_reponse).encode('utf-8')) # write response in json
            await writer.drain() # send 
        except Exception as e:
            logger.error(f"Error with handle_request: {str(e)}")
            response = {"error": str(e)}
            writer.write(json.dumps(response).encode('utf-8'))
            await writer.drain()
        finally:
            writer.close()
    
    def serialize_entries(self, entries):
        """
        Convert list of GdacsFeedEntry objects to
        a list of serializable dictionaries.
        """
        if not isinstance(entries, list):
            logger.warning("gdacs_collector response is not a list. ")
        
        serializable_entries = []
        for entry in entries:
            entry_dict = {
                "title": entry.title,
                "description": entry.description,
                # "external_id": entry.external_id,         # uncomment as needed
                # "coordinates": entry.coordinates,
                # "distance_to_home": entry.distance_to_home,
                # "category": entry.category,
                # "event_type": entry.event_type,
                # "event_type_short": entry.event_type_short,
                # "alert_level": entry.alert_level,
                # "country": entry.country,
                # "event_id": entry.event_id,
                # "event_name": entry.event_name,
                # "from_date": str(entry.from_date) if entry.from_date else None,
                # "to_date": str(entry.to_date) if entry.to_date else None,
                # "icon_url": entry.icon_url,
                # "is_current": entry.is_current,
                # "population": entry.population,
                # "severity": entry.severity,
                # "temporary": entry.temporary,
                # "version": entry.version,
                # "vulnerability": entry.vulnerability
            }
            serializable_entries.append(entry_dict)
        return serializable_entries
    
    async def collect(self, request: Dict[str, Any]):
        """
        Send HTTP request to GDACS to get relevant data. 

        Parameters: 
            request: Contains coordinates and radius 

        Returns: 

        """
        try:
            coordinates = request.get('coordinates')
            radius = request.get('radius')

            if (coordinates is None or radius is None):
                logger.error("Invalid request: missing coordinates or radius") 
                return "Invalid request: missing coordinates or radius"
                                  
            async with ClientSession() as session:
                feed = GdacsFeed(
                    session, 
                    tuple(coordinates), 
                    filter_radius=radius
                )
                
                status, entries = await feed.update()
                return entries               
        except Exception as e:
            logger.error(f"Failed to collect information from GDACS: {str(e)}")
            return {"error": f"Failed to collect information from GDACS: {str(e)}"}
    
    async def start(self):
        server = await asyncio.start_server(
            self.handle_request, self.host, self.port)
        
        logger.info(f"Server listening on {self.host}:{self.port}")
        addr = server.sockets[0].getsockname()
        logger.info(f'Global Disasters Collector serving on {addr}')
        
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    collector = GdacsCollector("localhost", 8080)
    asyncio.run(collector.start())
