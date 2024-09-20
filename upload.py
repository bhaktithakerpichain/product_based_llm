import os
import re
import html
import asyncio
import PyPDF2
import pdfplumber
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import pymongo
import logging

# Set up logging with more granular levels
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Load a more accurate model for generating embeddings
model_name = "BAAI/bge-large-en-v1.5"
embedding_model = SentenceTransformer(model_name)

# Connect to MongoDB
client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB_NAME")]
collection = db["transformers_new_embedded_resumes"]

# Clean and structure the text from the PDF
def clean_text(text):
    # Remove HTML tags and decode HTML entities
    text = re.sub(r'<.*?>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[\*\•\xA0]', ' ', text)
    # Remove unwanted characters and normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove unwanted sections like 'Page 1 of 1'
    text = re.sub(r'Page \d+ of \d+', '', text)
    return text

# Extract text from PDF
async def extract_text_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            content = []
            for page in pdf.pages:
                content.append(page.extract_text())
            content = " ".join(content)
            return clean_text(content)
    except Exception as e:
        logger.error(f"Error extracting text from PDF {file_path}: {e}")
        return ""

# Process all .pdf files in the folder asynchronously
async def get_text_from_folder_async(folder_path):
    tasks = []
    for filename in filter(lambda f: f.endswith(".pdf"), os.listdir(folder_path)):
        file_path = os.path.join(folder_path, filename)
        logger.info(f"Processing file: {filename}")
        tasks.append(extract_text_from_pdf(file_path))
    texts = await asyncio.gather(*tasks)
    resumes = [text for text in texts if text]
    return resumes

# Store embeddings in MongoDB with optional caching
def store_embeddings_in_mongo(resumes, batch_size=100):
    documents = []
    embedding_cache = {}

    for i, resume in enumerate(resumes):
        try:
            if resume in embedding_cache:
                embedding = embedding_cache[resume]
            else:
                embedding = embedding_model.encode(resume).tolist()
                embedding_cache[resume] = embedding

            documents.append({"text_chunk": resume, "embedding": embedding})
            
            if len(documents) >= batch_size:
                collection.insert_many(documents)
                logger.info(f"Stored {len(documents)} documents in MongoDB")
                documents = []
        except Exception as e:
            logger.error(f"Failed to generate/store embedding for resume {i}: {e}")
    
    if documents:
        try:
            collection.insert_many(documents)
            logger.info(f"Stored {len(documents)} remaining documents in MongoDB")
        except Exception as e:
            logger.error(f"Failed to store documents in MongoDB: {e}")

# Main function
def main():
    logger.info("Uploading resume content from .pdf files to MongoDB using Sentence Transformers")

    folder_path = input("Enter the path to the folder containing .pdf files: ").strip()
    
    try:
        resumes = asyncio.run(get_text_from_folder_async(folder_path))
        if resumes:
            store_embeddings_in_mongo(resumes)
            logger.info("Processing and uploading complete.")
        else:
            logger.info("No valid text found in the .pdf files.")
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")

if __name__ == "__main__":
    main()
