import re
from typing import Collection, Sequence

from llama_index import Document, ServiceContext
from llama_index.embeddings import AzureOpenAIEmbedding
from llama_index.indices.vector_store.base import VectorStoreIndex
from llama_index.node_parser import SimpleNodeParser
from llama_index.storage.storage_context import StorageContext
from llama_index.vector_stores import SimpleVectorStore


def sanitize_text(input_string: str) -> str:
    """
    Takes an input string and applies the following manipulations:
    * newline characters replaced with a single space
    * tab characters replaced with a single space
    * carriage return characters replaced with a single space
    * multiple spaces replaced with a single space
    * leading / trailing whitespace removed

    Returns a string.
    """

    input_string = input_string.replace("\n", " ").replace("\t", " ").replace("\r", " ")

    output = re.sub("[ ]+", " ", input_string)

    output = output.strip()

    return output


def embed_documents(
    docs: Sequence[Document],
    azure_openAI_key: str,
    azure_openAI_ver: str,
    azure_openAI_endpoint: str,
    azure_openAI_model: str,
    azure_openAI_model_name: str,
    chunk_size: int,
    chunk_overlap: int,
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
        embed_model=embed_model, node_parser=node_parser, llm=None
    )

    vector_store = SimpleVectorStore()

    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    vsi = VectorStoreIndex.from_documents(
        docs,
        service_context=service_context,
        storage_context=storage_context,
        show_progress=True,
    )

    return vsi


def map_metadata(
    vsi: VectorStoreIndex,
    include_text: bool,
) -> Collection[dict]:
    """
    Takes the embedding_dict, metadata_dict, and doc_store from
    a VectorStoreIndex's to_dict() method. Outputs a list of
    externalSearchDocuments that matches the document ingestion schema.

    Returns a list of nodes, represented as dictionaries.
    """
    # retrieve appropriate dictionaries

    vector_store = vsi.storage_context.to_dict()["vector_store"]["default"]
    doc_store = vsi.storage_context.to_dict()["doc_store"]

    embedding_dict = vector_store["embedding_dict"]
    metadata_dict = vector_store["metadata_dict"]
    doc_store = doc_store["docstore/data"]

    out = []

    for nodeid in embedding_dict:
        # alter nodeid to match our input schema
        # this should already be 32 characters
        nodeid_format = f"EXTERNAL_DOCUMENT~{nodeid.replace('-', '').upper()}"

        embedding_dict[nodeid] = {
            "entityId": nodeid_format,
            "documentId": nodeid_format,
            "embedding_1": embedding_dict[nodeid],
            "pageId": metadata_dict[nodeid]["pageId"],
            "lastRefreshed": metadata_dict[nodeid]["lastRefreshed"],
            "metadata": metadata_dict[nodeid],
        }

        if include_text:
            chunk_text = doc_store[nodeid]["__data__"]["text"]
            title = metadata_dict[nodeid]["title"]
            embedding_dict[nodeid]["embeddedString_1"] = f"Title: {title}\n{chunk_text}"

        out.append({"externalSearchDocument": embedding_dict[nodeid]})

    return out
