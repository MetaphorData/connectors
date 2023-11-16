import re
from typing import Collection

from llama_index import Document, ServiceContext
from llama_index.embeddings import OpenAIEmbedding
from llama_index.indices.vector_store.base import VectorStoreIndex
from llama_index.node_parser import SimpleNodeParser
from llama_index.storage.storage_context import StorageContext
from llama_index.vector_stores import SimpleVectorStore


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
        d.text = d.text.replace("\n", " ")
        d.text = d.text.replace("\t", " ")
        d.text = re.sub("[ ]+", " ", d.text)

    return docs


def embed_documents(
    docs: Collection[Document],
    openAI_tok: str,
    logger,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
) -> VectorStoreIndex:
    """
    Generates embeddings for Documents and upserts them to a provided
    MongoDB instance. Has configurable chunk and overlap sizes for node
    generation from Documents.

    Returns a VectorStoreIndex (in-memory VectorStore)
    """
    logger.info("Initializing embedding contexts")

    embed_model = OpenAIEmbedding(api_key=openAI_tok)

    node_parser = SimpleNodeParser.from_defaults(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )

    service_context = ServiceContext.from_defaults(
        embed_model=embed_model, node_parser=node_parser
    )

    vector_store = SimpleVectorStore()

    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    VSI = VectorStoreIndex.from_documents(
        docs,
        service_context=service_context,
        storage_context=storage_context,
        show_progress=True,
    )

    logger.info(f"Successfully embedded {len(docs)} documents")

    return VSI


def map_metadata(embedding_dict: dict, metadata_dict: dict) -> Collection[dict]:
    """
    Takes the embedding_dict from VSI.storage_context.to_dict() and
    maps the correct metadata to each entry in the dictionary, then
    flattens each entry into its own dictionary.

    Returns a list of nodes.
    """
    out = []

    for nodeid in embedding_dict:
        embedding_dict[nodeid] = {
            "nodeId": nodeid,
            "embedding": embedding_dict[nodeid],
            "lastRefreshed": metadata_dict[nodeid]["lastRefreshed"],
            "metadata": metadata_dict[nodeid],
        }

        out.append(embedding_dict[nodeid])

    return out
