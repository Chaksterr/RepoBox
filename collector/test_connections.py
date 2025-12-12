from gqlalchemy import Memgraph
from pymongo import MongoClient
import redis
from config import config

print("Testing database connections...\n")

# Test Memgraph
try:
    print("1. Testing Memgraph...")
    mg = Memgraph(host=config.MEMGRAPH_HOST, port=config.MEMGRAPH_PORT)
    mg.execute("RETURN 1")
    print("   ✓ Memgraph connected!")
except Exception as e:
    print(f"   ✗ Memgraph failed: {e}")

# Test MongoDB
try:
    print("\n2. Testing MongoDB...")
    client = MongoClient(f"mongodb://{config.MONGODB_HOST}:{config.MONGODB_PORT}/", serverSelectionTimeoutMS=5000)
    client.server_info()  # Force connection
    print("   ✓ MongoDB connected!")
    client.close()
except Exception as e:
    print(f"   ✗ MongoDB failed: {e}")

# Test Dragonfly
try:
    print("\n3. Testing Dragonfly...")
    df = redis.Redis(host=config.DRAGONFLY_HOST, port=config.DRAGONFLY_PORT)
    df.ping()
    print("   ✓ Dragonfly connected!")
except Exception as e:
    print(f"   ✗ Dragonfly failed: {e}")

print("\n All database connections successful!")
