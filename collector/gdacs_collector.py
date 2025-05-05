"""
GDACS Collector: asyncio‐based TCP server that accepts JSON requests
and returns disaster entries via aio_georss_gdacs.
"""

import asyncio, json, logging, Dict
from aiohttp import ClientSession
from aio_georss_gdacs import GdacsFeed
from collector.config import COLLECTOR_CONFIG

# Logging setup
logger = logging.getLogger("GDACSCollector")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(COLLECTOR_CONFIG["log_file"].replace("collector.log", "gdacs.log"))
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)


class GdacsCollector:
    """
    Asyncio server: handle JSON {coordinates, radius} requests
    and return list of event dicts.
    """

    def __init__(self, host="0.0.0.0", port=8080):
        self.host = host
        self.port = port

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Read JSON request, call collect(), serialize entries,
        write JSON response, and close.
        """
        peer = writer.get_extra_info("peername")
        raw = await reader.read(4096)
        if not raw:
            logger.warning(f"No data from {peer}")
            writer.close()
            return

        try:
            req = json.loads(raw.decode())
            entries = await self.collect(req)
            serial = self.serialize_entries(entries)
            writer.write(json.dumps(serial).encode())
            await writer.drain()
        except Exception as e:
            logger.error(f"Request handling error: {e}")
            writer.write(json.dumps({"error": str(e)}).encode())
            await writer.drain()
        finally:
            writer.close()

    def serialize_entries(self, entries):
        """
        Convert GdacsFeedEntry list into JSON-serializable dicts.
        """
        out = []
        if not isinstance(entries, list):
            logger.warning("Expected list of entries")
            return out
        for e in entries:
            out.append({
                "title": e.title,
                "description": e.description,
                # add other fields as needed...
            })
        return out

    async def collect(self, request):
        """
        Use aio_georss_gdacs to fetch events near coordinates with radius.
        """
        coords = request.get("coordinates")
        radius = request.get("radius")
        if coords is None or radius is None:
            raise ValueError("Missing coordinates or radius")
        async with ClientSession() as session:
            feed = GdacsFeed(session, tuple(coords), filter_radius=radius)
            status, entries = await feed.update()
            return entries

    async def start(self):
        """
        Start the asyncio TCP server on configured port.
        """
        server = await asyncio.start_server(self.handle_request, self.host, self.port)
        logger.info(f"GDACS collector listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(GdacsCollector().start())
