from gqlalchemy import Memgraph
from pymongo import MongoClient
from config import config

def init_memgraph():
    """Initialize Memgraph graph database"""
    print("Initializing Memgraph...")
    mg = Memgraph(host=config.MEMGRAPH_HOST, port=config.MEMGRAPH_PORT)
    
    # Clear existing data
    mg.execute("MATCH (n) DETACH DELETE n")
    print("  ✓ Cleared existing data")
    
    # Create indexes for faster queries
    try:
        mg.execute("CREATE INDEX ON :Repository(name)")
        mg.execute("CREATE INDEX ON :Language(name)")
        mg.execute("CREATE INDEX ON :Location(name)")
        mg.execute("CREATE INDEX ON :Owner(login)")
        print("  ✓ Created indexes")
    except:
        print("  ✓ Indexes already exist")
    
    print(" Memgraph initialized!\n")

def init_mongodb():
    """Initialize MongoDB document database"""
    print("Initializing MongoDB...")
    client = MongoClient(f"mongodb://{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
    db = client[config.MONGODB_DB]
    
    # Collections are created automatically in MongoDB, but we can create indexes
    collections = [
        'repositories',    # Main repo data
        'owners',          # Users and organizations
        'contributors',    # Contribution records
        'languages',       # Language statistics
        'locations',       # Country statistics
        'topics',          # Topic statistics
        'licenses',        # License information
        'frameworks',      # Framework usage
        'organizations',   # Organization details
        'cities'           # City-level data
    ]
    
    for collection in collections:
        if collection not in db.list_collection_names():
            db.create_collection(collection)
            print(f"  ✓ Created collection: {collection}")
        else:
            print(f"  ✓ Collection exists: {collection}")
    
    # Create indexes for faster queries
    print("\n  Creating indexes...")
    db.repositories.create_index('full_name')
    db.repositories.create_index('language')
    db.repositories.create_index('location')
    db.repositories.create_index([('stars', -1)])  # Descending for top repos
    db.owners.create_index('login')
    db.locations.create_index('name')
    print("  ✓ Created indexes")
    
    client.close()
    print("✅ MongoDB initialized!\n")

if __name__ == '__main__':
    init_memgraph()
    init_mongodb()
    print("🎉 All databases initialized and ready!")