from supabase import create_client, Client
from typing import List, Dict, Any, Optional
from datetime import datetime


class SupabaseUploader:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.source = "scraper-trendtvision"
        self.brand = "Trendt Vision"
        self.country = None

    def generate_product_id(self, url: str) -> str:
        handle = url.split("/products/")[-1].split("?")[0] if "/products/" in url else url
        return f"{self.source}_{handle}"

    def transform_product_data(self, raw_data: Dict[str, Any], image_embedding: List = None, info_embedding: List = None) -> Dict[str, Any]:
        product_url = raw_data.get("product_url", "")
        
        product_id = self.generate_product_id(product_url)
        
        prices = raw_data.get("price", [])
        price_str = ", ".join(prices) if prices else None
        
        sale_prices = raw_data.get("sale", [])
        sale_str = ", ".join(sale_prices) if sale_prices else None
        
        category = raw_data.get("product_type")
        if not category:
            tags = raw_data.get("metadata", {}).get("tags", "")
            if tags:
                categories = [t.strip() for t in tags.split(",") if t.strip() in ["Bottoms", "Tops", "Hoodies", "Jackets", "Accessories", "Headwear", "Footwear"]]
                category = ", ".join(categories) if categories else None
        
        additional_images = raw_data.get("additional_images", [])
        additional_images_str = " , ".join(additional_images) if additional_images else None
        
        sizes = raw_data.get("sizes", [])
        sizes_str = ", ".join(sizes) if sizes else None
        
        colors = raw_data.get("colors", [])
        colors_str = ", ".join(colors) if colors else None
        
        metadata = {
            "title": raw_data.get("title"),
            "description": raw_data.get("description"),
            "sizes": sizes_str,
            "colors": colors_str,
            "vendor": raw_data.get("vendor"),
            "tags": raw_data.get("metadata", {}).get("tags"),
        }
        
        transformed = {
            "id": product_id,
            "source": self.source,
            "product_url": product_url,
            "image_url": raw_data.get("image_url"),
            "brand": self.brand,
            "title": raw_data.get("title"),
            "description": raw_data.get("description"),
            "category": category,
            "gender": raw_data.get("gender"),
            "metadata": str(metadata),
            "size": sizes_str,
            "second_hand": False,
            "image_embedding": image_embedding,
            "country": None,
            "compressed_image_url": None,
            "tags": None,
            "price": price_str,
            "sale": sale_str if sale_str != price_str else None,
            "additional_images": additional_images_str,
            "info_embedding": info_embedding,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        return transformed

    def insert_products(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            data = [p for p in products if p.get("title") and p.get("image_url")]
            
            if not data:
                return {"success": False, "error": "No valid products to insert"}
            
            response = self.supabase.table("products").upsert(
                data,
                on_conflict="source,product_url"
            ).execute()
            
            return {
                "success": True,
                "inserted": len(data),
                "response": response
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }

    def check_existing_products(self) -> List[str]:
        try:
            response = self.supabase.table("products").select("product_url").eq("source", self.source).execute()
            return [r["product_url"] for r in response.data]
        except Exception as e:
            print(f"Error checking existing products: {e}")
            return []


def init_supabase_uploader() -> SupabaseUploader:
    import os
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://yqawmzggcgpeyaaynrjk.supabase.co")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4")
    
    return SupabaseUploader(SUPABASE_URL, SUPABASE_KEY)


if __name__ == "__main__":
    uploader = init_supabase_uploader()
    print(f"Connected to Supabase - Source: {uploader.source}, Brand: {uploader.brand}")