import torch
import numpy as np
from typing import List, Optional
from PIL import Image
import requests
from io import BytesIO


class EmbeddingGenerator:
    def __init__(self, model_name: str = "google/siglip-base-patch16-384"):
        self.model_name = model_name
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.processor = None
        self.model = None
        self._load_model()

    def _load_model(self):
        from transformers import AutoProcessor, AutoModel

        print(f"Loading model {self.model_name}...")
        self.processor = AutoProcessor.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()
        print(f"Model loaded on {self.device}")

    def get_image_embedding(self, image_url: str) -> Optional[List]:
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            }
            response = requests.get(image_url, timeout=30, headers=headers)
            response.raise_for_status()
            image = Image.open(BytesIO(response.content)).convert("RGB")

            inputs = self.processor(images=image, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.no_grad():
                outputs = self.model.get_image_features(**inputs)
                embedding = outputs.last_hidden_state
                if embedding.ndim == 3:
                    embedding = embedding.mean(dim=1)
                embedding = embedding.squeeze().cpu().numpy()

            return embedding.tolist()

        except Exception as e:
            print(f"Error generating image embedding for {image_url}: {e}")
            return None

    def get_text_embedding(self, text: str) -> Optional[List]:
        try:
            text = text[:500]

            inputs = self.processor(text=text, return_tensors="pt", padding=True, truncation=True, max_length=64)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.no_grad():
                outputs = self.model.get_text_features(**inputs)
                embedding = outputs.last_hidden_state
                if embedding.ndim == 3:
                    embedding = embedding.mean(dim=1)
                embedding = embedding.squeeze().cpu().numpy()

            return embedding.tolist()

        except Exception as e:
            print(f"Error generating text embedding: {e}")
            return None


def create_product_info_string(
    title: str,
    price: str,
    description: str,
    category: str,
    gender: str,
    sizes: List[str] = None,
    colors: List[str] = None,
    tags: str = None
) -> str:
    parts = []

    if title:
        parts.append(f"Title: {title}")
    if price:
        parts.append(f"Price: {price}")
    if category:
        parts.append(f"Category: {category}")
    if gender:
        parts.append(f"Gender: {gender}")
    if description:
        parts.append(f"Description: {description}")
    if sizes:
        parts.append(f"Sizes: {', '.join(sizes)}")
    if colors:
        parts.append(f"Colors: {', '.join(colors)}")
    if tags:
        parts.append(f"Tags: {tags}")

    return " | ".join(parts)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python embedding.py <image_url>")
        sys.exit(1)

    generator = EmbeddingGenerator()
    url = sys.argv[1]

    print(f"Generating embedding for: {url}")
    embedding = generator.get_image_embedding(url)

    if embedding:
        print(f"Embedding length: {len(embedding)}")
        print(f"First 10 values: {embedding[:10]}")
    else:
        print("Failed to generate embedding")