import httpx
import time
import io
from typing import Optional, List


class ImageCompressor:
    API_URL = "https://api.resmush.it/ws.php"
    USER_AGENT = "scraper-trendtvision/1.0"
    REFERER = "https://www.trendtvision.com"
    QUALITY = 90
    DELAY = 0.3
    
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def close(self):
        await self.client.aclose()
    
    async def _download_image(self, url: str) -> Optional[bytes]:
        try:
            response = await self.client.get(url)
            if response.status_code == 200:
                return response.content
        except Exception as e:
            print(f"  Download failed: {e}")
        return None
    
    async def compress_image(self, image_url: str) -> Optional[str]:
        if not image_url:
            return None
        
        try:
            image_data = await self._download_image(image_url)
            if not image_data:
                return None
            
            files = {
                "files": (image_url.split("/")[-1], image_data, "image/png")
            }
            
            data = {"qlty": str(self.QUALITY)}
            
            headers = {
                "User-Agent": self.USER_AGENT,
                "Referer": self.REFERER
            }
            
            response = await self.client.post(self.API_URL, files=files, data=data, headers=headers)
            
            if response.status_code == 403:
                print(f"  403 forbidden (Shopify blocks reSmush) - skipping")
                return None
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            
            if data.get("error"):
                print(f"  Compression error: {data.get('error')}")
                return None
            
            compressed_url = data.get("dest")
            if compressed_url:
                time.sleep(self.DELAY)
                return compressed_url
                
        except Exception as e:
            print(f"  Compression failed: {e}")
        
        return None
    
    async def compress_images(self, image_urls: List[str]) -> List[str]:
        compressed = []
        for url in image_urls:
            result = await self.compress_image(url)
            if result:
                compressed.append(result)
        return compressed


async def compress_single(url: str) -> Optional[str]:
    compressor = ImageCompressor()
    try:
        return await compressor.compress_image(url)
    finally:
        await compressor.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python image_compressor.py <image_url>")
        sys.exit(1)
    
    import asyncio
    
    async def main():
        url = sys.argv[1]
        result = await compress_single(url)
        print(f"Compressed: {result}")
    
    asyncio.run(main())