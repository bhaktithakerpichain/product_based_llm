import os
import re
import html
import asyncio
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import pymongo
import logging
import nest_asyncio

# Set up logging with more granular levels
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Load a more accurate model for generating embeddings
model_name = "BAAI/bge-large-en-v1.5"
logger.info(f"Loading embedding model: {model_name}")
embedding_model = SentenceTransformer(model_name)
logger.info("Embedding model loaded successfully.")

# Connect to MongoDB
logger.info("Connecting to MongoDB...")
client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB_NAME")]
collection = db["transformers_new_embedded_scripts"]
logger.info("Successfully connected to MongoDB.")

# Clean and structure the text from the Python file
def clean_text(text):
    logger.debug("Cleaning text content.")
    text = re.sub(r'<.*?>', '', text)
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Extract text from a Python (.py) file
async def extract_text_from_py(file_path):
    logger.info(f"Extracting text from Python file: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            return clean_text(content)
    except Exception as e:
        logger.error(f"Error extracting text from Python file {file_path}: {e}")
        return ""

# Process all .py files in the folder asynchronously
async def get_text_from_folder_async(folder_path):
    logger.info(f"Processing all .py files in the folder: {folder_path}")
    tasks = []
    for root, dirs, files in os.walk(folder_path):
        for filename in filter(lambda f: f.endswith(".py"), files):
            file_path = os.path.join(root, filename)
            tasks.append(extract_text_from_py(file_path))
    
    if not tasks:
        logger.warning("No .py files found in the provided folder.")
        return []
    
    texts = await asyncio.gather(*tasks)
    scripts = [text for text in texts if text]
    logger.info(f"Total Python scripts processed: {len(scripts)}")
    return scripts

# Store embeddings in MongoDB with optional caching
def store_embeddings_in_mongo(scripts, batch_size=100):
    logger.info("Storing embeddings in MongoDB...")
    documents = []
    embedding_cache = {}

    for i, script in enumerate(scripts):
        try:
            if script in embedding_cache:
                embedding = embedding_cache[script]
            else:
                embedding = embedding_model.encode(script).tolist()
                embedding_cache[script] = embedding

            documents.append({"text_chunk": script, "embedding": embedding})
            
            if len(documents) >= batch_size:
                collection.insert_many(documents)
                logger.info(f"Stored {len(documents)} documents in MongoDB.")
                documents = []
        except Exception as e:
            logger.error(f"Failed to generate/store embedding for script {i}: {e}")
    
    if documents:
        try:
            collection.insert_many(documents)
            logger.info(f"Stored remaining {len(documents)} documents in MongoDB.")
        except Exception as e:
            logger.error(f"Failed to store remaining documents in MongoDB: {e}")

# Main function to coordinate the process
async def main():
    logger.info("Starting the script processing pipeline...")

    folder_path = "./Pichains/"
    logger.info(f"Target folder for processing: {folder_path}")

    try:
        scripts = await get_text_from_folder_async(folder_path)
        if scripts:
            store_embeddings_in_mongo(scripts)
            logger.info("Processing and uploading complete.")
        else:
            logger.info("No valid text found in the .py files.")
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
    finally:
        logger.info("Script processing pipeline completed.")

# Apply nest_asyncio to allow asyncio in environments with a running loop
nest_asyncio.apply()

if __name__ == "__main__":
    # In Jupyter or similar environments with a running event loop
    logger.info("Script execution started.")
    try:
        asyncio.get_running_loop().run_until_complete(main())
    except RuntimeError:  # In case there's no running loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    logger.info("Script execution finished.")
    print("Script execution completed successfully.")
