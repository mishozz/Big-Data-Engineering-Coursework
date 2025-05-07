import pymongo
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


MONGO_CONNECTION = {
    'host': 'mongodb',
    'port': 27017,
    'username': 'mongodb_user',
    'password': 'mongodb_password',
    'database': 'mldata',
    'collection': 'processed_data'
}


def get_mongo_connection():
    """
    Establish a connection to MongoDB with retry logic.
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            client = pymongo.MongoClient(
                host=MONGO_CONNECTION['host'],
                port=MONGO_CONNECTION['port'],
                username=MONGO_CONNECTION['username'],
                password=MONGO_CONNECTION['password'],
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )
            
            # Check connection
            client.server_info()
            logger.info("Successfully connected to MongoDB")
            return client
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            raise


def fetch_mongodb_data(conn, db_name, collection_name, query=None, limit=None):
    if query is None:
        query = {}
    
    try:
        db = conn[db_name]
        collection = db[collection_name]

        cursor = collection.find(query, {'_id': 0})

        if limit:
            cursor = cursor.limit(limit)

        results = list(cursor)

        logger.info(f"Retrieved {len(results)} documents from MongoDB")
        return results
    
    except Exception as e:
        logger.error(f"Error fetching data from MongoDB: {str(e)}")
        raise
    finally:
        conn.close()
