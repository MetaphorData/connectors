import re
from typing import Collection, Sequence

from llama_index import Document, ServiceContext
from llama_index.embeddings import AzureOpenAIEmbedding
from llama_index.indices.vector_store.base import VectorStoreIndex
from llama_index.node_parser import SimpleNodeParser
from llama_index.storage.storage_context import StorageContext
from llama_index.vector_stores import SimpleVectorStore


def clean_text(input_string: str) -> str:
    """
    Takes an input string and applies the following manipulations:
    * leading / trailing whitespace removed
    * newline characters replaced with a single space
    * tab characters replaced with a single space
    * multiple spaces replaced with a single space

    Returns a string.
    """

    input_string = input_string.strip().replace("\n", " ").replace("\t", " ")

    output = re.sub("[ ]+", " ", input_string)

    return output


def embed_documents(
    docs: Sequence[Document],
    azure_openAI_key: str,
    azure_openAI_ver: str,
    azure_openAI_endpoint: str,
    azure_openAI_model: str,
    azure_openAI_model_name: str,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
) -> VectorStoreIndex:
    """
    Generates embeddings for Documents and returns them as stored in a
    VectorStoreIndex object. Has configurable chunk
    and overlap sizes for node generation from Documents.

    Returns a VectorStoreIndex (in-memory VectorStore)
    """

    embed_model = AzureOpenAIEmbedding(
        model=azure_openAI_model,
        deployment_name=azure_openAI_model_name,
        api_key=azure_openAI_key,
        azure_endpoint=azure_openAI_endpoint,
        api_version=azure_openAI_ver,
    )

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

    return VSI


def map_metadata(
    VSI: VectorStoreIndex,
    include_text: bool,
) -> Collection[dict]:
    """
    Takes the embedding_dict, metadata_dict, and doc_store from
    a VectorStoreIndex's to_dict() method. Outputs a list of
    externalSearchDocuments that matches the document ingestion schema.

    Returns a list of nodes, represented as dictionaries.
    """
    # retrieve appropriate dictionaries

    vector_store = VSI.storage_context.to_dict()["vector_store"]["default"]
    doc_store = VSI.storage_context.to_dict()["doc_store"]

    embedding_dict = vector_store["embedding_dict"]
    metadata_dict = vector_store["metadata_dict"]
    doc_store = doc_store["docstore/data"]

    out = []

    if include_text:
        for nodeid in embedding_dict:
            # alter nodeid to match our input schema
            # this should already be 32 characters
            nodeid_format = f"EXTERNAL_DOCUMENT~{nodeid.replace('-', '').upper()}"

            embedding_dict[nodeid] = {
                "entityId": nodeid_format,
                "documentId": nodeid_format,
                "embedding_1": embedding_dict[nodeid],
                "pageId": metadata_dict[nodeid]["pageId"],
                "embeddedString_1": clean_text(doc_store[nodeid]["__data__"]["text"]),
                "lastRefreshed": metadata_dict[nodeid]["lastRefreshed"],
                "metadata": metadata_dict[nodeid],
            }

            out.append({"externalSearchDocument": embedding_dict[nodeid]})

    else:
        for nodeid in embedding_dict:
            # alter nodeid to match our input schema
            # this should already be 32 characters
            nodeid_format = f"EXTERNAL_DOCUMENT~{nodeid.replace('-', '')}"

            embedding_dict[nodeid] = {
                "entityId": nodeid_format,
                "documentId": nodeid_format,
                "embedding_1": embedding_dict[nodeid],
                "pageId": metadata_dict[nodeid]["pageId"],
                "lastRefreshed": metadata_dict[nodeid]["lastRefreshed"],
                "metadata": metadata_dict[nodeid],
            }

            out.append({"externalSearchDocument": embedding_dict[nodeid]})

    return out
