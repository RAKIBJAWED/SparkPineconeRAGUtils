#!/usr/bin/env python3
"""
Production-Ready Embedding Examples for RAG Applications
Shows different approaches to generate meaningful text embeddings.
"""

import numpy as np
from typing import List, Dict, Any
import hashlib
import requests
import json

class EmbeddingGenerator:
    """Class demonstrating different embedding approaches."""
    
    def __init__(self):
        self.openai_api_key = None  # Set your OpenAI API key
        
    # 1. DEMO APPROACH (Current - Not Recommended for Production)
    def hash_based_embedding(self, text: str, dimension: int = 1024) -> List[float]:
        """
        Simple hash-based embedding (current approach).
        NOT recommended for production RAG applications.
        """
        hash_obj = hashlib.md5(text.encode())
        hash_hex = hash_obj.hexdigest()
        
        embedding = []
        for i in range(0, len(hash_hex), 2):
            hex_pair = hash_hex[i:i+2]
            value = int(hex_pair, 16) / 255.0
            embedding.append(value)
        
        while len(embedding) < dimension:
            embedding.extend(embedding[:min(len(embedding), dimension - len(embedding))])
        
        return embedding[:dimension]
    
    # 2. OPENAI EMBEDDINGS (Recommended for Production)
    def openai_embedding(self, text: str, model: str = "text-embedding-3-small") -> List[float]:
        """
        Generate embeddings using OpenAI's embedding API.
        Best for production RAG applications.
        
        Models:
        - text-embedding-3-small: 1536 dimensions, $0.02/1M tokens
        - text-embedding-3-large: 3072 dimensions, $0.13/1M tokens
        - text-embedding-ada-002: 1536 dimensions (legacy)
        """
        if not self.openai_api_key:
            raise ValueError("OpenAI API key not set")
            
        headers = {
            "Authorization": f"Bearer {self.openai_api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "input": text,
            "model": model
        }
        
        response = requests.post(
            "https://api.openai.com/v1/embeddings",
            headers=headers,
            json=data
        )
        
        if response.status_code == 200:
            result = response.json()
            return result["data"][0]["embedding"]
        else:
            raise Exception(f"OpenAI API error: {response.text}")
    
    # 3. SENTENCE TRANSFORMERS (Free, Local)
    def sentence_transformer_embedding(self, text: str, model_name: str = "all-MiniLM-L6-v2") -> List[float]:
        """
        Generate embeddings using Sentence Transformers (runs locally).
        Free alternative to OpenAI, good for production.
        
        Popular models:
        - all-MiniLM-L6-v2: 384 dimensions, fast, good quality
        - all-mpnet-base-v2: 768 dimensions, better quality, slower
        - multi-qa-MiniLM-L6-cos-v1: Optimized for Q&A tasks
        """
        try:
            from sentence_transformers import SentenceTransformer
            
            # Load model (downloads on first use)
            model = SentenceTransformer(model_name)
            
            # Generate embedding
            embedding = model.encode(text)
            
            return embedding.tolist()
            
        except ImportError:
            raise ImportError("Install sentence-transformers: pip install sentence-transformers")
    
    # 4. HUGGING FACE TRANSFORMERS
    def huggingface_embedding(self, text: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> List[float]:
        """
        Generate embeddings using Hugging Face Transformers.
        More control over the model and tokenization.
        """
        try:
            from transformers import AutoTokenizer, AutoModel
            import torch
            
            # Load tokenizer and model
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            model = AutoModel.from_pretrained(model_name)
            
            # Tokenize and encode
            inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
            
            with torch.no_grad():
                outputs = model(**inputs)
                # Use mean pooling
                embeddings = outputs.last_hidden_state.mean(dim=1)
            
            return embeddings.squeeze().tolist()
            
        except ImportError:
            raise ImportError("Install transformers: pip install transformers torch")
    
    # 5. COHERE EMBEDDINGS (Alternative API)
    def cohere_embedding(self, text: str, model: str = "embed-english-v3.0") -> List[float]:
        """
        Generate embeddings using Cohere's API.
        Alternative to OpenAI with competitive pricing.
        """
        try:
            import cohere
            
            if not hasattr(self, 'cohere_api_key') or not self.cohere_api_key:
                raise ValueError("Cohere API key not set")
            
            co = cohere.Client(self.cohere_api_key)
            response = co.embed(
                texts=[text],
                model=model,
                input_type="search_document"
            )
            
            return response.embeddings[0]
            
        except ImportError:
            raise ImportError("Install cohere: pip install cohere")


def demonstrate_embeddings():
    """Demonstrate different embedding approaches."""
    
    sample_texts = [
        "Spark performance optimization with adaptive query execution",
        "Enable AQE for better query performance in Apache Spark",
        "Database connection timeout error in production",
        "Python pandas dataframe operations"
    ]
    
    generator = EmbeddingGenerator()
    
    print("=== Embedding Comparison ===\n")
    
    for i, text in enumerate(sample_texts):
        print(f"Text {i+1}: {text}")
        
        # 1. Hash-based (current approach)
        hash_emb = generator.hash_based_embedding(text, dimension=384)
        print(f"  Hash-based: [{hash_emb[0]:.4f}, {hash_emb[1]:.4f}, ...] (dim: {len(hash_emb)})")
        
        # 2. Sentence Transformers (if available)
        try:
            st_emb = generator.sentence_transformer_embedding(text)
            print(f"  SentenceTransformers: [{st_emb[0]:.4f}, {st_emb[1]:.4f}, ...] (dim: {len(st_emb)})")
        except ImportError:
            print("  SentenceTransformers: Not installed")
        except Exception as e:
            print(f"  SentenceTransformers: Error - {e}")
        
        print()
    
    # Demonstrate similarity
    print("=== Similarity Comparison ===")
    text1 = "Spark performance optimization"
    text2 = "Apache Spark query performance"
    text3 = "Database connection error"
    
    # Hash-based similarity (poor)
    emb1_hash = generator.hash_based_embedding(text1, 384)
    emb2_hash = generator.hash_based_embedding(text2, 384)
    emb3_hash = generator.hash_based_embedding(text3, 384)
    
    def cosine_similarity(a, b):
        a, b = np.array(a), np.array(b)
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
    
    print(f"Hash-based similarities:")
    print(f"  '{text1}' vs '{text2}': {cosine_similarity(emb1_hash, emb2_hash):.4f}")
    print(f"  '{text1}' vs '{text3}': {cosine_similarity(emb1_hash, emb3_hash):.4f}")
    
    # Sentence Transformers similarity (good)
    try:
        emb1_st = generator.sentence_transformer_embedding(text1)
        emb2_st = generator.sentence_transformer_embedding(text2)
        emb3_st = generator.sentence_transformer_embedding(text3)
        
        print(f"\nSentenceTransformers similarities:")
        print(f"  '{text1}' vs '{text2}': {cosine_similarity(emb1_st, emb2_st):.4f}")
        print(f"  '{text1}' vs '{text3}': {cosine_similarity(emb1_st, emb3_st):.4f}")
        
    except ImportError:
        print("\nSentenceTransformers not available for similarity test")


# PRODUCTION RECOMMENDATIONS
def production_recommendations():
    """Print recommendations for production RAG systems."""
    
    print("\n" + "="*60)
    print("PRODUCTION EMBEDDING RECOMMENDATIONS")
    print("="*60)
    
    print("""
1. OPENAI EMBEDDINGS (Easiest, Best Quality)
   - Model: text-embedding-3-small (1536 dim)
   - Cost: $0.02 per 1M tokens
   - Pros: Excellent quality, easy to use, maintained
   - Cons: API dependency, cost for large volumes

2. SENTENCE TRANSFORMERS (Free, Local)
   - Model: all-MiniLM-L6-v2 (384 dim) or all-mpnet-base-v2 (768 dim)
   - Cost: Free (runs locally)
   - Pros: No API calls, privacy, customizable
   - Cons: Requires GPU for speed, model management

3. HYBRID APPROACH
   - Use Sentence Transformers for development/testing
   - Switch to OpenAI for production
   - Same vector search logic, just swap embedding function

INSTALLATION:
pip install sentence-transformers  # For local embeddings
pip install openai                 # For OpenAI embeddings

PINECONE INDEX SETUP:
- Create index with dimension matching your embedding model
- text-embedding-3-small: 1536 dimensions
- all-MiniLM-L6-v2: 384 dimensions
- all-mpnet-base-v2: 768 dimensions
""")


if __name__ == "__main__":
    demonstrate_embeddings()
    production_recommendations()