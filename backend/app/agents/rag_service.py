"""RAG (Retrieval Augmented Generation) service for context management."""
import hashlib
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import chromadb
from sentence_transformers import SentenceTransformer

from app.core.config import get_settings
from app.agents.llm_service import get_llm_service

logger = logging.getLogger(__name__)


class RAGService:
    """Service for managing context using RAG."""
    
    def __init__(self):
        self.settings = get_settings()
        self._chroma_client: Optional[chromadb.Client] = None
        self._collection: Optional[chromadb.Collection] = None
        self._embedding_model: Optional[SentenceTransformer] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize RAG service."""
        if self._initialized:
            return
        
        try:
            # Initialize ChromaDB
            self._chroma_client = chromadb.PersistentClient(
                path=self.settings.VECTOR_DB_PATH,
            )
            
            # Get or create collection
            self._collection = self._chroma_client.get_or_create_collection(
                name="data_quality_context",
                metadata={"hnsw:space": "cosine"}
            )
            
            # Initialize embedding model
            if self.settings.EMBEDDING_PROVIDER == "ollama":
                # Use local sentence transformer for embeddings
                self._embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            else:
                self._embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            
            self._initialized = True
            logger.info("RAG service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize RAG service: {str(e)}")
            raise
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text."""
        if self._embedding_model is None:
            raise RuntimeError("RAG service not initialized")
        
        embedding = self._embedding_model.encode(text)
        return embedding.tolist()
    
    def _generate_content_hash(self, content: str) -> str:
        """Generate hash for content."""
        return hashlib.sha256(content.encode()).hexdigest()
    
    async def add_document(
        self,
        document_type: str,
        source_id: str,
        title: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add a document to the context store."""
        await self.initialize()
        
        doc_id = f"{document_type}:{source_id}:{self._generate_content_hash(content)}"
        
        # Check if document already exists with same hash
        existing = self._collection.get(ids=[doc_id])
        if existing and existing['ids']:
            logger.debug(f"Document {doc_id} already exists, skipping")
            return doc_id
        
        # Generate embedding
        embedding = self._generate_embedding(content)
        
        # Prepare metadata
        doc_metadata = {
            "document_type": document_type,
            "source_id": source_id,
            "title": title,
            "content_hash": self._generate_content_hash(content),
            "created_at": datetime.utcnow().isoformat(),
            **(metadata or {})
        }
        
        # Add to collection
        self._collection.add(
            ids=[doc_id],
            embeddings=[embedding],
            documents=[content],
            metadatas=[doc_metadata]
        )
        
        logger.info(f"Added document {doc_id} to context store")
        return doc_id
    
    async def retrieve_context(
        self,
        query: str,
        document_types: Optional[List[str]] = None,
        source_ids: Optional[List[str]] = None,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Retrieve relevant context for a query."""
        await self.initialize()
        
        # Generate query embedding
        query_embedding = self._generate_embedding(query)
        
        # Build filter — ChromaDB requires $and when combining multiple conditions
        conditions = []
        if document_types:
            conditions.append({"document_type": {"$in": document_types}})
        if source_ids:
            conditions.append({"source_id": {"$in": source_ids}})
        
        if len(conditions) > 1:
            where_filter = {"$and": conditions}
        elif len(conditions) == 1:
            where_filter = conditions[0]
        else:
            where_filter = {}
        
        # Query collection
        results = self._collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=where_filter if where_filter else None,
            include=["documents", "metadatas", "distances"]
        )
        
        # Format results
        context_items = []
        if results['ids'] and results['ids'][0]:
            for i, doc_id in enumerate(results['ids'][0]):
                context_items.append({
                    "id": doc_id,
                    "content": results['documents'][0][i],
                    "metadata": results['metadatas'][0][i],
                    "similarity_score": 1 - results['distances'][0][i],  # Convert distance to similarity
                })
        
        return context_items
    
    async def add_schema_context(
        self,
        source_id: str,
        schema: Dict[str, Any],
        sample_data: Optional[List[Dict]] = None,
    ) -> str:
        """Add schema information as context."""
        # Format schema content
        schema_content = f"""Data Schema for {source_id}:

Columns:
"""
        for col_name, col_info in schema.get("columns", {}).items():
            schema_content += f"- {col_name}: {col_info.get('type', 'unknown')}"
            if col_info.get('nullable') is not None:
                schema_content += f" (nullable: {col_info['nullable']})"
            if col_info.get('description'):
                schema_content += f" - {col_info['description']}"
            schema_content += "\n"
        
        if sample_data:
            schema_content += f"\nSample Data ({len(sample_data)} rows):\n"
            schema_content += json.dumps(sample_data[:5], indent=2, default=str)
        
        return await self.add_document(
            document_type="schema",
            source_id=source_id,
            title=f"Schema: {source_id}",
            content=schema_content,
            metadata={"column_count": len(schema.get("columns", {}))}
        )
    
    async def add_validation_history(
        self,
        source_id: str,
        validation_results: Dict[str, Any],
    ) -> str:
        """Add validation history as context."""
        content = f"""Validation History for {source_id}:

Quality Score: {validation_results.get('quality_score', 'N/A')}
Total Rules: {validation_results.get('total_rules', 0)}
Passed: {validation_results.get('passed_rules', 0)}
Failed: {validation_results.get('failed_rules', 0)}

Failed Rules:
"""
        for result in validation_results.get('results', []):
            if result.get('status') == 'failed':
                content += f"- {result.get('rule_name')}: {result.get('failure_percentage', 0):.2f}% failed\n"
                if result.get('ai_insights'):
                    content += f"  AI Insight: {result['ai_insights']}\n"
        
        return await self.add_document(
            document_type="validation_history",
            source_id=source_id,
            title=f"Validation History: {source_id}",
            content=content,
            metadata={
                "quality_score": validation_results.get('quality_score'),
                "validation_date": datetime.utcnow().isoformat(),
            }
        )
    
    async def add_business_rules(
        self,
        source_id: str,
        rules: List[Dict[str, Any]],
    ) -> List[str]:
        """Add business rules as context."""
        doc_ids = []
        for rule in rules:
            content = f"""Business Rule: {rule.get('name')}

Description: {rule.get('description', 'N/A')}
Type: {rule.get('rule_type')}
Severity: {rule.get('severity')}
Target Columns: {', '.join(rule.get('target_columns', []))}
"""
            if rule.get('expression'):
                content += f"Expression: {rule['expression']}\n"
            
            doc_id = await self.add_document(
                document_type="business_rule",
                source_id=source_id,
                title=f"Rule: {rule.get('name')}",
                content=content,
                metadata={
                    "rule_type": rule.get('rule_type'),
                    "severity": rule.get('severity'),
                }
            )
            doc_ids.append(doc_id)
        
        return doc_ids
    
    async def get_relevant_context_for_validation(
        self,
        source_id: str,
        schema: Dict[str, Any],
        query: Optional[str] = None,
    ) -> str:
        """Get formatted context string for validation."""
        # Build query from schema if not provided
        if query is None:
            query = f"Data quality rules for {source_id} with columns: {', '.join(schema.get('columns', {}).keys())}"
        
        # Retrieve relevant context
        context_items = await self.retrieve_context(
            query=query,
            document_types=["schema", "business_rule", "validation_history"],
            source_ids=[source_id],
            top_k=10,
        )
        
        # Format context for LLM prompt
        formatted_context = "## Relevant Context\n\n"
        
        for item in context_items:
            doc_type = item['metadata'].get('document_type', 'unknown')
            formatted_context += f"### {item['metadata'].get('title', 'Context')}\n"
            formatted_context += f"{item['content']}\n\n"
        
        return formatted_context
    
    async def delete_source_context(self, source_id: str):
        """Delete all context for a source."""
        await self.initialize()
        
        # Find all documents for this source
        results = self._collection.get(
            where={"source_id": source_id}
        )
        
        if results['ids']:
            self._collection.delete(ids=results['ids'])
            logger.info(f"Deleted {len(results['ids'])} context documents for source {source_id}")


# Singleton instance
_rag_service: Optional[RAGService] = None


async def get_rag_service() -> RAGService:
    """Get RAG service singleton."""
    global _rag_service
    if _rag_service is None:
        _rag_service = RAGService()
        await _rag_service.initialize()
    return _rag_service
