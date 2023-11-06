import re
from typing import Collection

import pymongo

from llama_index import Document
from llama_index import ServiceContext
from llama_index.embeddings import OpenAIEmbedding
from llama_index.node_parser import SimpleNodeParser
from llama_index.storage.storage_context import StorageContext
from llama_index.indices.vector_store.base import VectorStoreIndex
from llama_index.vector_stores.mongodb import MongoDBAtlasVectorSearch


def clean_documents_text(docs: Collection[Document]) -> Collection[Document]:
    """
    Takes a list of Documents, processes their text attribute and 
    returns the Documents. The following changes are made:
    * leading / trailing whitespace removed
    * newline characters replaced with a single space
    * tab characters replaced with a single space
    * multiple spaces replaced with a single space

    Returns a list of docs
    """

    for d in docs:
        d.text = d.text.strip()
        d.text = d.text.replace('\n', ' ')
        d.text = d.text.replace('\t', ' ')
        d.text = re.sub('[ ]+', ' ', d.text)

    return docs

def embed_documents(docs: Collection[Document],
                    mongo_URI: str,
                    mongo_db_name: str,
                    mongo_collection_name: str,
                    openAI_tok: str,
                    chunk_size: int,
                    chunk_overlap: int,
                    ) -> bool:
    
    embed_model = OpenAIEmbedding(api_key = openAI_tok)
    node_parser = SimpleNodeParser.from_defaults(chunk_size=512,
                                                 chunk_overlap=50)
    
    mongo_client = pymongo.MongoClient(mongo_URI)

    vector_store = MongoDBAtlasVectorSearch(db_name=mongo_db_name,
                                            collection_name=mongo_collection_name)
    
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    index = VectorStoreIndex.from_documents(
        docs,
        sto
    )
    

