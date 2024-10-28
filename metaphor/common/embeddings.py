import re
from typing import Collection, Sequence

from llama_index.core import Document, Settings, StorageContext, VectorStoreIndex
from llama_index.core.llms.mock import MockLLM
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.vector_stores import SimpleVectorStore
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding
from llama_index.embeddings.openai import OpenAIEmbedding

from metaphor.common.embeddings_config import (
    AzureOpenAIConfig,
    EmbeddingModelConfig,
    OpenAIConfig,
)
from metaphor.models.metadata_change_event import ExternalSearchDocument


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
    embedding_model: EmbeddingModelConfig,
) -> VectorStoreIndex:
    """
    Generates embeddings for Documents and returns them as stored in a
    VectorStoreIndex object. Has configurable chunk
    and overlap sizes for node generation from Documents.

    Returns a VectorStoreIndex (in-memory VectorStore)
    """

    # Determine the source from the embedding_model configuration
    active_config = embedding_model.active_config

    if isinstance(active_config, AzureOpenAIConfig):
        embed_model = AzureOpenAIEmbedding(
            model=active_config.model,
            deployment_name=active_config.deployment_name,
            api_key=active_config.key,
            azure_endpoint=active_config.endpoint,
            api_version=active_config.version,
        )
    elif isinstance(active_config, OpenAIConfig):
        embed_model = OpenAIEmbedding(
            model=active_config.model, api_key=active_config.key
        )
    else:
        raise Exception(f"Embedding source {type(active_config)} not supported")

    node_parser = SentenceSplitter.from_defaults(
        chunk_size=embedding_model.chunk_size,
        chunk_overlap=embedding_model.chunk_overlap,
    )

    Settings.llm = MockLLM()
    Settings.embed_model = embed_model
    Settings.node_parser = node_parser

    vector_store = SimpleVectorStore()

    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    vector_store_index = VectorStoreIndex.from_documents(
        docs,
        storage_context=storage_context,
        show_progress=True,
    )

    return vector_store_index


def map_metadata(
    vsi: VectorStoreIndex,
    include_text: bool,
) -> Collection[ExternalSearchDocument]:
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
            "embedding_1": embedding_dict[nodeid],
            "pageId": metadata_dict[nodeid]["pageId"],
            "lastRefreshed": metadata_dict[nodeid]["lastRefreshed"],
            "metadata": metadata_dict[nodeid],
        }

        if include_text:
            chunk_text = doc_store[nodeid]["__data__"]["text"]
            title = metadata_dict[nodeid]["title"]
            embedding_dict[nodeid]["embeddedString_1"] = f"Title: {title}\n{chunk_text}"

        out.append(ExternalSearchDocument.from_dict(embedding_dict[nodeid]))
    return out
