from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from gqlalchemy import Memgraph
from pymongo import MongoClient
import redis
import os
import json
from functools import wraps
from typing import Callable

app = FastAPI(title="GitHub Insights API", version="1.0.0")

# Enable CORS for Grafana
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connections
mg = Memgraph(host=os.getenv('MEMGRAPH_HOST', 'memgraph'), port=7687)
mongo_client = MongoClient(f"mongodb://{os.getenv('MONGODB_HOST', 'mongodb')}:{os.getenv('MONGODB_PORT', '27017')}/")
mongodb = mongo_client[os.getenv('MONGODB_DB', 'repobox')]
dragonfly = redis.Redis(host=os.getenv('DRAGONFLY_HOST', 'dragonfly'), port=6379, decode_responses=True)

# Cache decorator
def cache_response(ttl: int = 300):
    """Cache API responses in Dragonfly for TTL seconds (default 5 minutes)"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"api:{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Try to get from cache
            try:
                cached = dragonfly.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception as e:
                print(f"Cache read error: {e}")
            
            # If not in cache, execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            try:
                dragonfly.setex(cache_key, ttl, json.dumps(result))
            except Exception as e:
                print(f"Cache write error: {e}")
            
            return result
        return wrapper
    return decorator

@app.get("/")
def root():
    # Get cache stats
    try:
        cache_info = dragonfly.info('stats')
        cache_keys = dragonfly.dbsize()
    except:
        cache_info = {}
        cache_keys = 0
    
    return {
        "status": "ok",
        "message": "GitHub Insights API with Dragonfly Cache",
        "version": "1.0.0",
        "cache": {
            "enabled": True,
            "keys": cache_keys,
            "ttl": "5 minutes"
        },
        "endpoints": [
            "/metrics/locations/map",
            "/locations/{location}/repos",
            "/metrics/languages",
            "/metrics/locations/compare",
            "/cache/stats",
            "/cache/clear"
        ]
    }

@app.get("/metrics/locations/map")
@cache_response(ttl=600)  # Cache for 10 minutes
def get_location_map():
    """Get location data with coordinates for world map (cached)"""
    result = mg.execute_and_fetch("""
        MATCH (loc:Location)
        MATCH (r:Repository)-[:LOCATED_IN]->(loc)
        RETURN loc.name as location,
               count(r) as repos,
               avg(r.stars) as avg_stars,
               sum(r.stars) as total_stars
        ORDER BY repos DESC
    """)
    
    # Coordinates for countries
    coordinates = {
        'Tunisia': {'lat': 33.8869, 'lon': 9.5375},
        'France': {'lat': 46.2276, 'lon': 2.2137},
        'USA': {'lat': 37.0902, 'lon': -95.7129},
        'Germany': {'lat': 51.1657, 'lon': 10.4515},
        'Japan': {'lat': 36.2048, 'lon': 138.2529},
        'UK': {'lat': 55.3781, 'lon': -3.4360},
        'Canada': {'lat': 56.1304, 'lon': -106.3468},
        'India': {'lat': 20.5937, 'lon': 78.9629},
        'Brazil': {'lat': -14.2350, 'lon': -51.9253},
        'Australia': {'lat': -25.2744, 'lon': 133.7751},
    }
    
    return [
        {
            'location': row['location'],
            'latitude': coordinates.get(row['location'], {}).get('lat', 0),
            'longitude': coordinates.get(row['location'], {}).get('lon', 0),
            'repos': row['repos'],
            'avg_stars': round(row['avg_stars'], 2),
            'total_stars': row['total_stars']
        }
        for row in result if row['location'] in coordinates
    ]

@app.get("/locations/{location}/repos")
@cache_response(ttl=300)  # Cache for 5 minutes
def get_repos_by_location(location: str, limit: int = 10):
    """Get top repositories by location (cached)"""
    repos = mongodb.repositories.find(
        {'location': location}
    ).sort('stars', -1).limit(limit)
    
    result = []
    for repo in repos:
        repo['_id'] = str(repo['_id'])  # Convert ObjectId to string
        result.append(repo)
    return result

@app.get("/metrics/languages")
@cache_response(ttl=600)  # Cache for 10 minutes
def get_languages():
    """Get language statistics (cached)"""
    result = mg.execute_and_fetch("""
        MATCH (r:Repository)-[:USES]->(l:Language)
        RETURN l.name as language,
               count(r) as repos,
               avg(r.stars) as avg_stars
        ORDER BY repos DESC
    """)
    return list(result)

@app.get("/metrics/locations/compare")
@cache_response(ttl=600)  # Cache for 10 minutes
def compare_locations():
    """Compare statistics across locations (cached)"""
    result = mg.execute_and_fetch("""
        MATCH (loc:Location)
        MATCH (r:Repository)-[:LOCATED_IN]->(loc)
        RETURN loc.name as location,
               count(r) as repos,
               avg(r.stars) as avg_stars,
               sum(r.stars) as total_stars
        ORDER BY repos DESC
    """)
    return list(result)

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/cache/stats")
def cache_stats():
    """Get Dragonfly cache statistics"""
    try:
        info = dragonfly.info()
        keys = dragonfly.dbsize()
        
        return {
            "status": "connected",
            "total_keys": keys,
            "memory_used": info.get('used_memory_human', 'N/A'),
            "uptime_seconds": info.get('uptime_in_seconds', 0),
            "connected_clients": info.get('connected_clients', 0),
            "total_commands": info.get('total_commands_processed', 0),
            "cache_hit_rate": "N/A"  # Dragonfly doesn't expose this directly
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/cache/clear")
def clear_cache():
    """Clear all cached data"""
    try:
        # Get all API cache keys
        keys = dragonfly.keys("api:*")
        if keys:
            dragonfly.delete(*keys)
            return {"status": "success", "cleared_keys": len(keys)}
        return {"status": "success", "cleared_keys": 0}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/cache/keys")
def list_cache_keys():
    """List all cached keys"""
    try:
        keys = dragonfly.keys("api:*")
        key_info = []
        for key in keys[:50]:  # Limit to 50 keys
            ttl = dragonfly.ttl(key)
            key_info.append({
                "key": key,
                "ttl_seconds": ttl
            })
        return {
            "total_keys": len(keys),
            "showing": len(key_info),
            "keys": key_info
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Grafana JSON API plugin endpoints
@app.get("/search")
def grafana_search():
    """Return available metrics for Grafana"""
    return [
        "languages",
        "locations_map",
        "locations_compare"
    ]

@app.post("/query")
def grafana_query(request: dict):
    """Handle Grafana query requests"""
    targets = request.get('targets', [])
    results = []
    
    for target in targets:
        target_name = target.get('target', '')
        
        if target_name == 'languages' or target_name == '/metrics/languages':
            data = get_languages()
            # Convert to Grafana table format
            results.append({
                "target": "languages",
                "type": "table",
                "columns": [
                    {"text": "language", "type": "string"},
                    {"text": "repos", "type": "number"},
                    {"text": "avg_stars", "type": "number"}
                ],
                "rows": [[row['language'], row['repos'], row['avg_stars']] for row in data]
            })
    
    return results