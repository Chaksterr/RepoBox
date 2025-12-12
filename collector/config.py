import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # GitHub API
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    GITHUB_API_BASE = 'https://api.github.com'
    
    # Memgraph
    MEMGRAPH_HOST = os.getenv('MEMGRAPH_HOST', 'localhost')
    MEMGRAPH_PORT = int(os.getenv('MEMGRAPH_PORT', 7687))
    
    # MongoDB
    MONGODB_HOST = os.getenv('MONGODB_HOST', 'localhost')
    MONGODB_PORT = int(os.getenv('MONGODB_PORT', 27018))
    MONGODB_DB = 'repobox'
    
    # Dragonfly
    DRAGONFLY_HOST = os.getenv('DRAGONFLY_HOST', 'localhost')
    DRAGONFLY_PORT = int(os.getenv('DRAGONFLY_PORT', 6379))
    
    # Collection settings
    FILTER_BY_COUNTRY = os.getenv('FILTER_BY_COUNTRY', '')
    LANGUAGES = os.getenv('LANGUAGES', 'python').split(',')
    REPOS_PER_LANGUAGE = int(os.getenv('REPOS_PER_LANGUAGE', 500))

config = Config()
