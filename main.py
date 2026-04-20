import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Any

from scraper import TrendtVisionScraper
from embedding import EmbeddingGenerator, create_product_info_string
from supabase_client import SupabaseUploader, init_supabase_uploader


class TrendtVisionImporter:
    def __init__(self):
        self.scraper = TrendtVisionScraper()
        self.embedding_generator = EmbeddingGenerator()
        self.uploader = init_supabase_uploader()
        self.products_processed = 0
        self.products_inserted = 0
        self.products_failed = 0
        self.errors = []

    async def import_all_products(self, max_scrolls: int = 100):
        print(f"Starting import at {datetime.now()}")
        
        print("\n=== Step 1: Scraping product URLs ===")
        async with TrendtVisionScraper() as scraper:
            all_product_urls = await scraper.scroll_to_load_all_products(max_scrolls=max_scrolls)
            print(f"Found {len(all_product_urls)} unique product URLs")

        if not all_product_urls:
            print("No products found!")
            return

        existing_urls = set(self.uploader.check_existing_products())
        print(f"Already in database: {len(existing_urls)} products")

        new_urls = [url for url in all_product_urls if url not in existing_urls]
        print(f"New products to process: {len(new_urls)}")

        if not new_urls:
            print("No new products to import!")
            return

        print("\n=== Step 2: Scraping and embedding products ===")
        
        async with TrendtVisionScraper() as scraper:
            for i, url in enumerate(new_urls):
                try:
                    print(f"\n[{i+1}/{len(new_urls)}] Processing: {url}")
                    
                    raw_data = await scraper.extract_product_data(url)
                    if not raw_data.get("title"):
                        print(f"  Skipping - no title found")
                        continue
                    
                    print(f"  Title: {raw_data.get('title')}")
                    
                    image_url = raw_data.get("image_url")
                    image_embedding = None
                    if image_url:
                        image_embedding = self.embedding_generator.get_image_embedding(image_url)
                        if image_embedding:
                            print(f"  Image embedding: {len(image_embedding)} dims")
                    
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
                    
                    info_embedding = self.embedding_generator.get_text_embedding(info_text)
                    if info_embedding:
                        print(f"  Info embedding: {len(info_embedding)} dims")

                    product_data = self.uploader.transform_product_data(
                        raw_data,
                        image_embedding=image_embedding,
                        info_embedding=info_embedding
                    )
                    
                    result = self.uploader.insert_products([product_data])
                    if result.get("success"):
                        self.products_inserted += 1
                        print(f"  Inserted successfully!")
                    else:
                        self.products_failed += 1
                        self.errors.append(f"{url}: {result.get('error')}")
                        print(f"  Failed: {result.get('error')}")

                    self.products_processed += 1

                except Exception as e:
                    print(f"  Error: {e}")
                    self.products_failed += 1
                    self.errors.append(f"{url}: {str(e)}")

        print("\n" + "="*50)
        print(f"IMPORT COMPLETE")
        print(f"="*50)
        print(f"Total products processed: {self.products_processed}")
        print(f"Successfully inserted: {self.products_inserted}")
        print(f"Failed: {self.products_failed}")
        
        if self.errors:
            print(f"\nErrors ({len(self.errors)}):")
            for err in self.errors[:10]:
                print(f"  - {err}")


async def main():
    importer = TrendtVisionImporter()
    await importer.import_all_products(max_scrolls=50)


if __name__ == "__main__":
    asyncio.run(main())