from flask import Flask, request, jsonify
import os
# from langchain.embeddings import HuggingFaceBgeEmbeddings
from langchain_community.embeddings import HuggingFaceBgeEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain.docstore.document import Document
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import CohereRerank
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
# from langchain.retrievers import BM25Retriever, EnsembleRetriever
from langchain.retrievers import EnsembleRetriever
from langchain_community.retrievers import BM25Retriever
from langchain_google_genai import ChatGoogleGenerativeAI
import google.generativeai as genai
from dotenv import load_dotenv
import pymongo
import numpy as np
import faiss
import logging
from threading import Lock
import json

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
os.environ["COHERE_API_KEY"] = os.getenv("COHERE_API_KEY")

# Initialize Flask app
app = Flask(__name__)

# MongoDB setup
client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB_NAME")]
collection = db["transformers_new_embedded_scripts"]

# Lock for FAISS index
index_lock = Lock()
vector_store = None

def load_embeddings_from_mongo():
    projection = {"text_chunk": 1, "embedding": 1}
    documents = collection.find({}, projection)
    text_chunks = []
    embeddings = []
    for doc in documents:
        text_chunks.append(doc["text_chunk"])
        embeddings.append(np.array(doc["embedding"], dtype=np.float32))  # Ensure float32
    return text_chunks, embeddings

def create_faiss_index(embeddings, text_chunks):

    print("*****<2>*****")
    # Ensure embeddings are not empty
    if len(embeddings) == 0:
        raise ValueError("No embeddings found. FAISS index cannot be created.")

    embeddings = np.array(embeddings).astype('float32')  # Ensure it's a 2D float32 array

    # Ensure embeddings have correct shape (2D array)
    if len(embeddings.shape) != 2:
        raise ValueError(f"Embeddings array has incorrect shape: {embeddings.shape}")

    dimension = embeddings.shape[1]  # Ensure we can access the embedding dimensions
    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings)

    docstore = InMemoryDocstore({i: Document(page_content=chunk) for i, chunk in enumerate(text_chunks)})
    index_to_docstore_id = {i: i for i in range(len(text_chunks))}

    embedding_function = HuggingFaceBgeEmbeddings(model_name="BAAI/bge-large-en-v1.5")
    return FAISS(index=index, docstore=docstore, index_to_docstore_id=index_to_docstore_id, embedding_function=embedding_function)

def initialize_faiss_index():
    print("***<1>***")
    # import pdb;pdb.set_trace()
    global vector_store
    with index_lock:
        if vector_store is None:
            text_chunks, embeddings = load_embeddings_from_mongo()

            # Ensure valid text_chunks and embeddings are loaded
            if len(text_chunks) == 0 or len(embeddings) == 0:
                raise ValueError("No text chunks or embeddings were loaded from MongoDB.")

            # Initialize the FAISS index
            vector_store = create_faiss_index(embeddings, text_chunks)


# Initialize FAISS index once at startup
initialize_faiss_index()

def get_conversational_chain(retriever):
    prompt_template = """
    Answer the question based on the provided context. Be as detailed as possible. If the answer is not in the context, say "answer is not available in the context".
    Context: {context}
    Question: {question}
    Answer:
    """
    llm = ChatGoogleGenerativeAI(model="gemini-pro", temperature=0.2)
    prompt = ChatPromptTemplate.from_template(prompt_template)
    output_parser = StrOutputParser()
    chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | output_parser
    )
    return chain

def format_code(code_str):
    # Replace the escaped characters with proper formatting
    formatted_code = code_str.replace("\\'", "'").replace("\\n", "\n").replace('\\"', '"')
    return formatted_code

@app.route('/new_ask', methods=['POST'])
def ask_question():
    user_question = request.json.get('question')
    
    if not user_question:
        return jsonify({"error": "No question provided"}), 400
    
    global vector_store
    with index_lock:
        retriever_vectordb = vector_store.as_retriever()

    # import pdb;pdb.set_trace()
    # Ensure that all documents have page_content and metadata
    documents = []
    for doc_id in vector_store.index_to_docstore_id.values():
        # Retrieve the document content from the docstore using the document ID
        doc = vector_store.docstore.search(doc_id)
        
        # Check if the document exists and has valid content
        if doc and isinstance(doc.page_content, str) and doc.page_content.strip():
            documents.append(Document(page_content=doc.page_content, metadata={}))

    # Check if we have valid documents before proceeding
    if not documents:
        return jsonify({"error": "No valid documents available"}), 500
    
    # Initialize BM25Retriever with valid documents
    keyword_retriever = BM25Retriever.from_documents(documents)

    ensemble_retriever = EnsembleRetriever(
        retrievers=[retriever_vectordb, keyword_retriever],
        weights=[0.8, 0.2]
    )

    compressor = CohereRerank(model="rerank", top_n=5)
    compression_retriever = ContextualCompressionRetriever(
        base_compressor=compressor, base_retriever=ensemble_retriever
    )

    compressed_docs = compression_retriever.get_relevant_documents(user_question)
    print(compressed_docs)
    print(len(compressed_docs))

    chain = get_conversational_chain(retriever=compression_retriever)
    response = chain.invoke(user_question)

    print(response)
    
    # return jsonify({"answer": response})
    formatted_response = format_code(response)
    print(formatted_response)
    # To return in the API response
    return jsonify({"answer": formatted_response})


if __name__ == "__main__":
    app.run(debug=True)
