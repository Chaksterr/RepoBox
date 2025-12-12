#!/usr/bin/env python3
"""
Sync MongoDB data to PostgreSQL for Superset visualization
"""

import os
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime

# Database connections
MONGODB_HOST = os.getenv('MONGODB_HOST', 'mongodb')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', '27017'))
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))

print("=" * 60)
print("🔄 Syncing MongoDB → PostgreSQL for Superset")
print("=" * 60)

# Connect to MongoDB
print("\n1. Connecting to MongoDB...")
mongo_client = MongoClient(f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/')
mongodb = mongo_client['repobox']
print("   ✓ MongoDB connected")

# Connect to PostgreSQL
print("\n2. Connecting to PostgreSQL...")
pg_conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database='repobox',
    user='repobox',
    password='repobox123'
)
pg_cursor = pg_conn.cursor()
print("   ✓ PostgreSQL connected")

# Create tables
print("\n3. Creating PostgreSQL tables...")

# Repositories table
pg_cursor.execute("""
    DROP TABLE IF EXISTS repositories CASCADE;
    CREATE TABLE repositories (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        full_name VARCHAR(255),
        owner_login VARCHAR(255),
        owner_type VARCHAR(50),
        description TEXT,
        language VARCHAR(100),
        stars INTEGER,
        forks INTEGER,
        watchers INTEGER,
        open_issues INTEGER,
        size_kb INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        pushed_at TIMESTAMP,
        url TEXT,
        homepage TEXT,
        is_fork BOOLEAN,
        topics TEXT,
        frameworks TEXT,
        license VARCHAR(100)
    );
""")
print("   ✓ Created repositories table")

# Owners table
pg_cursor.execute("""
    DROP TABLE IF EXISTS owners CASCADE;
    CREATE TABLE owners (
        id VARCHAR(255) PRIMARY KEY,
        login VARCHAR(255),
        type VARCHAR(50),
        total_repos INTEGER,
        total_stars INTEGER,
        total_forks INTEGER,
        avg_stars FLOAT,
        repos TEXT,
        languages TEXT
    );
""")
print("   ✓ Created owners table")

# Languages table
pg_cursor.execute("""
    DROP TABLE IF EXISTS languages CASCADE;
    CREATE TABLE languages (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(100),
        total_repos INTEGER,
        total_stars INTEGER,
        avg_stars FLOAT,
        total_forks INTEGER
    );
""")
print("   ✓ Created languages table")

# Topics table
pg_cursor.execute("""
    DROP TABLE IF EXISTS topics CASCADE;
    CREATE TABLE topics (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        repo_count INTEGER,
        total_stars INTEGER,
        avg_stars FLOAT
    );
""")
print("   ✓ Created topics table")

# Frameworks table
pg_cursor.execute("""
    DROP TABLE IF EXISTS frameworks CASCADE;
    CREATE TABLE frameworks (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(100),
        language VARCHAR(100),
        repo_count INTEGER,
        total_stars INTEGER
    );
""")
print("   ✓ Created frameworks table")

pg_conn.commit()

# Sync repositories
print("\n4. Syncing repositories...")
repos = list(mongodb.repositories.find())
if repos:
    repo_data = []
    for repo in repos:
        # Use full_name as ID if id is missing
        repo_id = repo.get('id') or repo.get('full_name', '').replace('/', '_')
        if not repo_id:
            continue
            
        repo_data.append((
            repo_id,
            repo.get('name', ''),
            repo.get('full_name', ''),
            repo.get('owner_login', ''),
            repo.get('owner_type', ''),
            repo.get('description', ''),
            repo.get('language', ''),
            repo.get('stars', 0),
            repo.get('forks', 0),
            repo.get('watchers', 0),
            repo.get('open_issues', 0),
            repo.get('size', 0),
            repo.get('created_at'),
            repo.get('updated_at'),
            repo.get('pushed_at'),
            repo.get('url', ''),
            repo.get('homepage', ''),
            repo.get('is_fork', False),
            ','.join(repo.get('topics', [])),
            ','.join(repo.get('frameworks', [])),
            repo.get('license', '')
        ))
    
    execute_values(pg_cursor, """
        INSERT INTO repositories VALUES %s
    """, repo_data)
    print(f"   ✓ Synced {len(repos)} repositories")
else:
    print("   ⚠ No repositories found")

# Sync owners
print("\n5. Syncing owners...")
owners = list(mongodb.owners.find())
if owners:
    owner_data = []
    for owner in owners:
        owner_id = owner.get('id') or owner.get('login', '')
        if not owner_id:
            continue
            
        owner_data.append((
            owner_id,
            owner.get('login', ''),
            owner.get('type', ''),
            owner.get('total_repos', 0),
            owner.get('total_stars', 0),
            owner.get('total_forks', 0),
            owner.get('avg_stars', 0.0),
            ','.join(owner.get('repos', [])),
            ','.join(owner.get('languages', []))
        ))
    
    execute_values(pg_cursor, """
        INSERT INTO owners VALUES %s
    """, owner_data)
    print(f"   ✓ Synced {len(owners)} owners")
else:
    print("   ⚠ No owners found")

# Sync languages
print("\n6. Syncing languages...")
languages = list(mongodb.languages.find())
if languages:
    lang_data = []
    for lang in languages:
        lang_id = lang.get('id') or lang.get('name', '')
        if not lang_id:
            continue
            
        lang_data.append((
            lang_id,
            lang.get('name', ''),
            lang.get('total_repos', 0),
            lang.get('total_stars', 0),
            lang.get('avg_stars', 0.0),
            lang.get('total_forks', 0)
        ))
    
    execute_values(pg_cursor, """
        INSERT INTO languages VALUES %s
    """, lang_data)
    print(f"   ✓ Synced {len(languages)} languages")
else:
    print("   ⚠ No languages found")

# Sync topics
print("\n7. Syncing topics...")
topics = list(mongodb.topics.find())
if topics:
    topic_data = []
    for topic in topics:
        topic_id = topic.get('id') or topic.get('name', '')
        if not topic_id:
            continue
            
        topic_data.append((
            topic_id,
            topic.get('name', ''),
            topic.get('repo_count', 0),
            topic.get('total_stars', 0),
            topic.get('avg_stars', 0.0)
        ))
    
    execute_values(pg_cursor, """
        INSERT INTO topics VALUES %s
    """, topic_data)
    print(f"   ✓ Synced {len(topics)} topics")
else:
    print("   ⚠ No topics found")

# Sync frameworks
print("\n8. Syncing frameworks...")
frameworks = list(mongodb.frameworks.find())
if frameworks:
    fw_data = []
    for fw in frameworks:
        fw_id = fw.get('id') or fw.get('name', '')
        if not fw_id:
            continue
            
        fw_data.append((
            fw_id,
            fw.get('name', ''),
            fw.get('language', ''),
            fw.get('repo_count', 0),
            fw.get('total_stars', 0)
        ))
    
    execute_values(pg_cursor, """
        INSERT INTO frameworks VALUES %s
    """, fw_data)
    print(f"   ✓ Synced {len(frameworks)} frameworks")
else:
    print("   ⚠ No frameworks found")

pg_conn.commit()

# Create indexes for better performance
print("\n9. Creating indexes...")
pg_cursor.execute("CREATE INDEX idx_repos_language ON repositories(language);")
pg_cursor.execute("CREATE INDEX idx_repos_stars ON repositories(stars DESC);")
pg_cursor.execute("CREATE INDEX idx_repos_owner ON repositories(owner_login);")
pg_cursor.execute("CREATE INDEX idx_owners_stars ON owners(total_stars DESC);")
pg_cursor.execute("CREATE INDEX idx_topics_count ON topics(repo_count DESC);")
pg_conn.commit()
print("   ✓ Created indexes")

# Close connections
pg_cursor.close()
pg_conn.close()
mongo_client.close()

print("\n" + "=" * 60)
print("✅ Sync complete! PostgreSQL ready for Superset")
print("=" * 60)
print("\nConnection details for Superset:")
print("  Host: postgres")
print("  Port: 5432")
print("  Database: repobox")
print("  User: repobox")
print("  Password: repobox123")
print("\nSQLAlchemy URI:")
print("  postgresql://repobox:repobox123@postgres:5432/repobox")
print("=" * 60)
