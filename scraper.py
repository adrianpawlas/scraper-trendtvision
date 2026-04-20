import asyncio
import json
import re
import httpx
import time
from playwright.async_api import async_playwright, Page, Browser
from typing import List, Dict, Any, Optional, Set
from datetime import datetime


class TrendtVisionScraper:
    def __init__(self):
        self.base_url = "https://www.trendtvision.com"
        self.collection_url = "https://www.trendtvision.com/collections/shop-all-new"
        self.api_base = "https://www.trendtvision.com/products"
        self.browser: Browser = None
        self.page: Page = None

    async def __aenter__(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if self.browser:
            await self.browser.close()

    async def scroll_to_load_all_products(self, max_scrolls: int = 100, scroll_pause: float = 2.0) -> List[str]:
        product_urls = set()
        
        await self.page.goto(self.collection_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)

        last_count = 0
        no_new_count = 0

        for scroll_num in range(max_scrolls):
            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(scroll_pause)

            product_links = await self.page.query_selector_all("a[href*='/products/']")
            
            for link in product_links:
                href = await link.get_attribute("href")
                if href and "/products/" in href:
                    clean_url = href.split("?variant=")[0] if "?variant=" in href else href
                    full_url = clean_url if clean_url.startswith("http") else f"{self.base_url}{clean_url}"
                    product_urls.add(full_url)

            current_count = len(product_urls)
            print(f"Scroll {scroll_num + 1}: Found {current_count} unique products")

            if current_count == last_count:
                no_new_count += 1
                if no_new_count >= 3:
                    print("No new products loaded in 3 scrolls. Stopping.")
                    break
            else:
                no_new_count = 0

            last_count = current_count

        return list(product_urls)

    async def fetch_product_json(self, product_handle: str) -> Optional[Dict]:
        url = f"{self.api_base}/{product_handle}.json"
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.json()
            except Exception as e:
                print(f"Error fetching {url}: {e}")
        return None

    async def extract_product_data(self, url: str) -> Dict[str, Any]:
        product_data = {
            "product_url": url,
            "title": None,
            "description": None,
            "price": [],
            "sale": [],
            "image_url": None,
            "additional_images": [],
            "category": None,
            "gender": None,
            "metadata": {},
            "sizes": [],
            "colors": [],
        }

        handle = url.split("/products/")[-1].split("?")[0]
        product_json = await self.fetch_product_json(handle)

        if not product_json:
            return product_data

        product = product_json.get("product", {})
        if not product:
            return product_data

        product_data["title"] = product.get("title")
        
        description = product.get("body_html", "")
        if description:
            clean_desc = re.sub(r'<[^>]+>', ' ', description)
            clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
            product_data["description"] = clean_desc

        product_data["vendor"] = product.get("vendor")
        product_data["product_type"] = product.get("product_type")

        variants = product.get("variants", [])
        prices = set()
        for variant in variants:
            price = variant.get("price", "")
            if price:
                prices.add(f"{price}EUR")
            compare_at = variant.get("compare_at_price")
            if compare_at and str(compare_at) != "":
                product_data["sale"].append(f"{compare_at}EUR")
        product_data["price"] = list(prices)

        if product_data["sale"] and not product_data["price"]:
            product_data["price"] = product_data["sale"]

        images = product.get("images", [])
        
        front_images = []
        back_images = []
        other_valid_images = []
        
        for img in images:
            src = img.get("src", "")
            if not src:
                continue
            src_lower = src.lower()
            if "detail" in src_lower or "onbody" in src_lower:
                continue
            elif "front" in src_lower:
                front_images.append(src)
            elif "back" in src_lower:
                back_images.append(src)
            else:
                other_valid_images.append(src)
        
        if front_images:
            product_data["image_url"] = front_images[0]
            product_data["additional_images"] = back_images + front_images[1:] + other_valid_images[:7]
        elif back_images:
            product_data["image_url"] = back_images[0]
            product_data["additional_images"] = back_images[1:] + other_valid_images[:8]
        elif other_valid_images:
            product_data["image_url"] = other_valid_images[0]
            product_data["additional_images"] = other_valid_images[1:]
        else:
            product_data["image_url"] = None
            product_data["additional_images"] = []

        options = product.get("options", [])
        for option in options:
            if option.get("name", "").lower() == "size":
                product_data["sizes"] = option.get("values", [])
            elif option.get("name", "").lower() == "color":
                product_data["colors"] = option.get("values", [])

        tags = product.get("tags", "")
        if tags:
            product_data["metadata"]["tags"] = tags

        return product_data


async def get_all_product_urls() -> List[str]:
    async with TrendtVisionScraper() as scraper:
        urls = await scraper.scroll_to_load_all_products()
        return urls


async def scrape_product(url: str) -> Dict[str, Any]:
    async with TrendtVisionScraper() as scraper:
        return await scraper.extract_product_data(url)