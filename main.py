import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple
from dataclasses import dataclass, field

from scraper import TrendtVisionScraper
from embedding import EmbeddingGenerator, create_product_info_string
from supabase_client import SupabaseUploader, init_supabase_uploader


@dataclass
class RunStats:
    new_products: int = 0
    products_updated: int = 0
    products_unchanged: int = 0
    stale_products_deleted: int = 0
    embeddings_generated: int = 0
    errors: List[str] = field(default_factory=list)


class TrendtVisionImporter:
    BATCH_SIZE = 50
    MAX_RETRIES = 3
    EMBEDDING_DELAY = 0.5
    
    def __init__(self):
        self.scraper = TrendtVisionScraper()
        self.embedding_generator = None
        self.uploader = init_supabase_uploader()
        self.stats = RunStats()
        
        self.source = "scraper-trendtvision"
        
        self.stale_tracking_file = "stale_products.json"
        self.error_log_file = "error_log.txt"
        
        self._load_stale_tracking()

    def _load_stale_tracking(self):
        if os.path.exists(self.stale_tracking_file):
            with open(self.stale_tracking_file, 'r') as f:
                self.previous_run_products = set(json.load(f))
        else:
            self.previous_run_products = set()

    def _save_stale_tracking(self, current_products: Set[str]):
        with open(self.stale_tracking_file, 'w') as f:
            json.dump(list(current_products), f)

    def _log_error(self, error_msg: str):
        self.stats.errors.append(error_msg)
        timestamp = datetime.now().isoformat()
        with open(self.error_log_file, 'a') as f:
            f.write(f"[{timestamp}] {error_msg}\n")

    def _get_existing_products(self) -> Dict[str, Dict[str, Any]]:
        try:
            response = self.uploader.supabase.table("products").select(
                "id, product_url, title, price, image_url, additional_images, size, sale, created_at"
            ).eq("source", self.source).execute()
            
            existing = {}
            for row in response.data:
                key = row.get("product_url", "")
                existing[key] = row
            return existing
        except Exception as e:
            print(f"Error fetching existing products: {e}")
            return {}

    def _compare_products(self, scraped: Dict[str, Any], existing: Dict[str, Any]) -> bool:
        if not existing:
            return True
        
        if scraped.get("title") != existing.get("title"):
            return True
        if ", ".join(scraped.get("price", [])) != existing.get("price"):
            return True
        if scraped.get("image_url") != existing.get("image_url"):
            return True
        if ", ".join(scraped.get("additional_images", [])) != existing.get("additional_images"):
            return True
        if ", ".join(scraped.get("sizes", [])) != existing.get("size"):
            return True
        if ", ".join(scraped.get("sale", [])) != existing.get("sale"):
            return True
            
        return False

    def _transform_product(
        self, 
        raw_data: Dict[str, Any], 
        image_embedding: List = None, 
        info_embedding: List = None,
        force_new: bool = False
    ) -> Dict[str, Any]:
        return self.uploader.transform_product_data(
            raw_data, 
            image_embedding if force_new else None, 
            info_embedding if force_new else None
        )

    async def _generate_embeddings_safe(
        self, 
        url: str, 
        raw_data: Dict[str, Any], 
        force: bool = False
    ) -> Tuple[List, List]:
        image_emb = None
        info_emb = None
        
        if not force and not url:
            return image_emb, info_emb
        
        if not self.embedding_generator:
            self.embedding_generator = EmbeddingGenerator()
        
        image_url = raw_data.get("image_url")
        if image_url:
            try:
                image_emb = self.embedding_generator.get_image_embedding(image_url)
                if image_emb:
                    self.stats.embeddings_generated += 1
                    time.sleep(self.EMBEDDING_DELAY)
            except Exception as e:
                print(f"  Image embedding error: {e}")
        
        info_text = create_product_info_string(
            title=raw_data.get("title", ""),
            price=", ".join(raw_data.get("price", [])),
            description=raw_data.get("description", ""),
            category=raw_data.get("product_type", ""),
            gender=raw_data.get("gender"),
            sizes=raw_data.get("sizes"),
            colors=raw_data.get("colors"),
            tags=raw_data.get("metadata", {}).get("tags")
        )
        
        try:
            info_emb = self.embedding_generator.get_text_embedding(info_text)
            if info_emb:
                time.sleep(self.EMBEDDING_DELAY)
        except Exception as e:
            print(f"  Info embedding error: {e}")
        
        return image_emb, info_emb

    async def _batch_insert(
        self, 
        products: List[Dict[str, Any]], 
        is_new_batch: bool = False
    ) -> Tuple[int, int]:
        if not products:
            return 0, 0
        
        success_count = 0
        fail_count = 0
        
        for retry in range(self.MAX_RETRIES):
            try:
                data = [p for p in products if p.get("title") and p.get("image_url")]
                
                if not data:
                    return 0, 0
                
                response = self.uploader.supabase.table("products").upsert(
                    data,
                    on_conflict="source,product_url"
                ).execute()
                
                success_count = len(data)
                break
                
            except Exception as e:
                if retry < self.MAX_RETRIES - 1:
                    print(f"  Batch insert retry {retry + 1}: {e}")
                    time.sleep(1)
                else:
                    fail_count = len(products)
                    self._log_error(f"Batch insert failed after {self.MAX_RETRIES} retries: {e}")
                    for p in products:
                        self._log_error(f"  Failed product: {p.get('product_url')}")
        
        return success_count, fail_count

    def _find_stale_products(self, seen_products: Set[str]) -> List[str]:
        stale = []
        
        for prev_url in self.previous_run_products:
            if prev_url not in seen_products:
                stale.append(prev_url)
        
        return stale

    async def _delete_stale_products(self, stale_urls: List[str]) -> int:
        if not stale_urls:
            return 0
        
        deleted = 0
        for url in stale_urls:
            try:
                self.uploader.supabase.table("products").delete().match({
                    "source": self.source,
                    "product_url": url
                }).execute()
                deleted += 1
            except Exception as e:
                self._log_error(f"Failed to delete stale product {url}: {e}")
        
        return deleted

    async def import_all_products(self, max_scrolls: int = 100):
        print(f"Starting import at {datetime.now()}")
        print("=" * 50)
        
        print("\n=== Step 1: Scraping product URLs ===")
        async with TrendtVisionScraper() as scraper:
            all_product_urls = await scraper.scroll_to_load_all_products(max_scrolls=max_scrolls)
        
        current_seen = set(all_product_urls)
        print(f"Found {len(all_product_urls)} unique product URLs")

        if not all_product_urls:
            print("No products found!")
            return

        print("\n=== Step 2: Fetching existing products from DB ===")
        existing_products = self._get_existing_products()
        print(f"Already in database: {len(existing_products)} products")

        existing_urls = set(existing_products.keys())
        new_urls = [url for url in all_product_urls if url not in existing_urls]
        existing_to_check = [url for url in all_product_urls if url in existing_urls]
        
        print(f"New products: {len(new_urls)}")
        print(f"Existing to check: {len(existing_to_check)}")

        print("\n=== Step 3: Processing products ===")
        
        products_to_insert = []
        current_scraped = {}
        
        async with TrendtVisionScraper() as scraper:
            all_urls_to_process = new_urls + existing_to_check
            
            for i, url in enumerate(all_urls_to_process):
                try:
                    if (i + 1) % 10 == 0:
                        print(f"  Progress: {i+1}/{len(all_urls_to_process)}")
                    
                    raw_data = await scraper.extract_product_data(url)
                    
                    if not raw_data.get("title") or not raw_data.get("image_url"):
                        print(f"  Skipping {url} - no data")
                        continue
                    
                    current_scraped[url] = raw_data
                    
                    existing = existing_products.get(url)
                    is_new = url in new_urls
                    
                    has_changes = self._compare_products(raw_data, existing) if existing else True
                    
                    if is_new:
                        image_emb, info_emb = await self._generate_embeddings_safe(url, raw_data, force=True)
                        
                        transformed = self._transform_product(raw_data, image_emb, info_emb, force_new=True)
                        products_to_insert.append(transformed)
                        
                        self.stats.new_products += 1
                        
                    elif has_changes:
                        image_emb, info_emb = await self._generate_embeddings_safe(url, raw_data, force=True)
                        
                        transformed = self._transform_product(raw_data, image_emb, info_emb, force_new=True)
                        products_to_insert.append(transformed)
                        
                        self.stats.products_updated += 1
                        
                    else:
                        self.stats.products_unchanged += 1
                    
                    if len(products_to_insert) >= self.BATCH_SIZE:
                        success, fail = await self._batch_insert(products_to_insert)
                        products_to_insert = []
                        
                except Exception as e:
                    print(f"  Error processing {url}: {e}")
                    self._log_error(f"Processing error {url}: {e}")
        
        if products_to_insert:
            await self._batch_insert(products_to_insert)

        print("\n=== Step 4: Detecting stale products ===")
        stale_urls = self._find_stale_products(current_seen)
        stale_count = len(stale_urls)
        print(f"Stale products from previous run: {stale_count}")
        
        if stale_count > 0:
            deleted = await self._delete_stale_products(stale_urls)
            self.stats.stale_products_deleted = deleted
            print(f"Deleted stale products: {deleted}")

        self._save_stale_tracking(current_seen)

        print("\n" + "=" * 50)
        print("RUN SUMMARY")
        print("=" * 50)
        print(f"New products added:       {self.stats.new_products}")
        print(f"Products updated:         {self.stats.products_updated}")
        print(f"Products unchanged:      {self.stats.products_unchanged}")
        print(f"Stale products deleted: {self.stats.stale_products_deleted}")
        print(f"Embeddings generated:    {self.stats.embeddings_generated}")
        
        if self.stats.errors:
            print(f"\nErrors: {len(self.stats.errors)}")
            for err in self.stats.errors[:5]:
                print(f"  - {err}")


async def main():
    importer = TrendtVisionImporter()
    await importer.import_all_products(max_scrolls=50)


if __name__ == "__main__":
    asyncio.run(main())