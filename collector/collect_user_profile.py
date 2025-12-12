#!/usr/bin/env python3
"""
Collect specific GitHub user profile and their repositories
"""

import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
import requests
import time
from gqlalchemy import Memgraph

load_dotenv()

class SimpleGitHubAPI:
    def __init__(self, token):
        self.base_url = 'https://api.github.com'
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
    
    def get(self, endpoint, params=None):
        """Make GET request to GitHub API"""
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            print(f"API Error: {response.status_code}")
            return None

# Famous developers to track
FAMOUS_DEVELOPERS = [
    'torvalds',      # Linus Torvalds - Linux creator
    'gvanrossum',    # Guido van Rossum - Python creator
    'tj',            # TJ Holowaychuk - Node.js legend
    'sindresorhus',  # Sindre Sorhus - Open source hero
    'addyosmani',    # Addy Osmani - Google Chrome
    'paulirish',     # Paul Irish - Google Chrome
    'defunkt',       # Chris Wanstrath - GitHub co-founder
    'mojombo',       # Tom Preston-Werner - GitHub co-founder
    'dhh',           # David Heinemeier Hansson - Rails creator
    'matz',          # Yukihiro Matsumoto - Ruby creator
]

def collect_user_profile(username: str):
    """Collect user profile and their top repositories"""
    
    github = SimpleGitHubAPI(os.getenv('GITHUB_TOKEN'))
    
    print(f"\n{'='*60}")
    print(f"Collecting profile for: {username}")
    print('='*60)
    
    # Get user info
    try:
        user_data = github.get(f'/users/{username}')
        if not user_data:
            print(f"  ❌ User not found: {username}")
            return None
    except Exception as e:
        print(f"  ❌ Error fetching user: {e}")
        return None
    
    print(f"  ✓ Found: {user_data.get('name', username)}")
    print(f"    Followers: {user_data.get('followers', 0):,}")
    print(f"    Public repos: {user_data.get('public_repos', 0)}")
    
    # Get user's repositories
    print(f"\n  Fetching repositories...")
    repos = github.get(f'/users/{username}/repos', params={
        'sort': 'stars',
        'per_page': 100,
        'type': 'owner'
    })
    
    if not repos:
        print(f"  ⚠️  No repositories found")
        return None
    
    print(f"  ✓ Found {len(repos)} repositories")
    
    # Calculate stats
    total_stars = sum(r.get('stargazers_count', 0) for r in repos)
    total_forks = sum(r.get('forks_count', 0) for r in repos)
    languages = set(r.get('language') for r in repos if r.get('language'))
    
    user_profile = {
        'login': username,
        'name': user_data.get('name', username),
        'type': 'User',
        'avatar_url': user_data.get('avatar_url'),
        'bio': user_data.get('bio'),
        'company': user_data.get('company'),
        'location': user_data.get('location'),
        'blog': user_data.get('blog'),
        'twitter': user_data.get('twitter_username'),
        'followers': user_data.get('followers', 0),
        'following': user_data.get('following', 0),
        'public_repos': user_data.get('public_repos', 0),
        'total_repos': len(repos),
        'total_stars': total_stars,
        'total_forks': total_forks,
        'languages': list(languages),
        'created_at': user_data.get('created_at'),
        'updated_at': user_data.get('updated_at'),
    }
    
    print(f"\n  Profile Stats:")
    print(f"    Total stars: {total_stars:,}")
    print(f"    Total forks: {total_forks:,}")
    print(f"    Languages: {', '.join(languages)}")
    
    return user_profile, repos

def store_user_data(user_profile, repos):
    """Store user profile and repos in databases"""
    
    # Connect to databases
    mongo_client = MongoClient(f"mongodb://{os.getenv('MONGODB_HOST', 'localhost')}:{os.getenv('MONGODB_PORT', '27018')}/")
    mongodb = mongo_client['repobox']
    
    mg = Memgraph(host=os.getenv('MEMGRAPH_HOST', 'localhost'), port=int(os.getenv('MEMGRAPH_PORT', '7687')))
    
    # Store user profile
    mongodb.user_profiles.update_one(
        {'login': user_profile['login']},
        {'$set': user_profile},
        upsert=True
    )
    
    # Store repositories
    for repo in repos:
        repo_data = {
            'id': f"{repo['owner']['login']}_{repo['name']}",
            'name': repo['name'],
            'full_name': repo['full_name'],
            'owner_login': repo['owner']['login'],
            'owner_type': 'User',
            'description': repo.get('description', ''),
            'language': repo.get('language', ''),
            'stars': repo.get('stargazers_count', 0),
            'forks': repo.get('forks_count', 0),
            'watchers': repo.get('watchers_count', 0),
            'open_issues': repo.get('open_issues_count', 0),
            'size': repo.get('size', 0),
            'created_at': repo.get('created_at'),
            'updated_at': repo.get('updated_at'),
            'pushed_at': repo.get('pushed_at'),
            'url': repo.get('html_url'),
            'homepage': repo.get('homepage'),
            'is_fork': repo.get('fork', False),
            'topics': repo.get('topics', []),
            'license': repo.get('license', {}).get('name') if repo.get('license') else None,
        }
        
        mongodb.repositories.update_one(
            {'id': repo_data['id']},
            {'$set': repo_data},
            upsert=True
        )
    
    # Update owners collection
    mongodb.owners.update_one(
        {'login': user_profile['login']},
        {'$set': {
            'login': user_profile['login'],
            'type': 'User',
            'total_repos': user_profile['total_repos'],
            'total_stars': user_profile['total_stars'],
            'total_forks': user_profile['total_forks'],
            'avg_stars': user_profile['total_stars'] / max(user_profile['total_repos'], 1),
            'repos': [r['name'] for r in repos[:20]],
            'languages': user_profile['languages'],
        }},
        upsert=True
    )
    
    print(f"  ✓ Stored in MongoDB")
    
    # Memgraph (Graph Database)
    try:
        # Create User node
        mg.execute(f"""
            MERGE (u:User {{login: '{user_profile['login']}'}})
            SET u.name = '{user_profile.get('name', '').replace("'", "\\'")}',
                u.bio = '{user_profile.get('bio', '').replace("'", "\\'") if user_profile.get('bio') else ''}',
                u.company = '{user_profile.get('company', '').replace("'", "\\'") if user_profile.get('company') else ''}',
                u.location = '{user_profile.get('location', '').replace("'", "\\'") if user_profile.get('location') else ''}',
                u.followers = {user_profile.get('followers', 0)},
                u.following = {user_profile.get('following', 0)},
                u.total_repos = {user_profile['total_repos']},
                u.total_stars = {user_profile['total_stars']},
                u.total_forks = {user_profile['total_forks']},
                u.type = 'User'
        """)
        
        # Create Repository nodes and relationships
        for repo in repos:
            repo_name = repo['name'].replace("'", "\\'")
            repo_desc = repo.get('description', '').replace("'", "\\'") if repo.get('description') else ''
            language = repo.get('language', 'Unknown')
            
            # Create Repository node
            mg.execute(f"""
                MERGE (r:Repository {{full_name: '{repo['full_name']}'}})
                SET r.name = '{repo_name}',
                    r.description = '{repo_desc}',
                    r.stars = {repo.get('stargazers_count', 0)},
                    r.forks = {repo.get('forks_count', 0)},
                    r.watchers = {repo.get('watchers_count', 0)},
                    r.language = '{language}',
                    r.is_fork = {str(repo.get('fork', False)).lower()},
                    r.created_at = '{repo.get('created_at', '')}',
                    r.url = '{repo.get('html_url', '')}'
            """)
            
            # Create OWNS relationship
            mg.execute(f"""
                MATCH (u:User {{login: '{user_profile['login']}'}})
                MATCH (r:Repository {{full_name: '{repo['full_name']}'}})
                MERGE (u)-[:OWNS]->(r)
            """)
            
            # Create Language node and relationship
            if language and language != 'Unknown':
                mg.execute(f"""
                    MERGE (l:Language {{name: '{language}'}})
                """)
                
                mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo['full_name']}'}})
                    MATCH (l:Language {{name: '{language}'}})
                    MERGE (r)-[:USES]->(l)
                """)
            
            # Create Topic nodes and relationships
            for topic in repo.get('topics', [])[:10]:  # Limit to 10 topics
                topic_clean = topic.replace("'", "\\'")
                mg.execute(f"""
                    MERGE (t:Topic {{name: '{topic_clean}'}})
                """)
                
                mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo['full_name']}'}})
                    MATCH (t:Topic {{name: '{topic_clean}'}})
                    MERGE (r)-[:HAS_TOPIC]->(t)
                """)
        
        print(f"  ✓ Stored in Memgraph")
        
    except Exception as e:
        print(f"   Memgraph error: {e}")
    
    # PostgreSQL
    try:
        pg = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5433')),
            database='repobox',
            user='repobox',
            password='repobox123'
        )
        cur = pg.cursor()
        
        # Store user profile
        cur.execute("""
            INSERT INTO owners (id, login, type, total_repos, total_stars, total_forks, avg_stars, repos, languages)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                total_repos = EXCLUDED.total_repos,
                total_stars = EXCLUDED.total_stars,
                total_forks = EXCLUDED.total_forks,
                avg_stars = EXCLUDED.avg_stars,
                repos = EXCLUDED.repos,
                languages = EXCLUDED.languages
        """, (
            user_profile['login'],
            user_profile['login'],
            'User',
            user_profile['total_repos'],
            user_profile['total_stars'],
            user_profile['total_forks'],
            user_profile['total_stars'] / max(user_profile['total_repos'], 1),
            ','.join([r['name'] for r in repos[:20]]),
            ','.join(user_profile['languages'])
        ))
        
        # Store repositories
        for repo in repos:
            cur.execute("""
                INSERT INTO repositories (id, name, full_name, owner_login, owner_type, description, language, 
                                        stars, forks, watchers, open_issues, size_kb, created_at, updated_at, 
                                        pushed_at, url, homepage, is_fork, topics, license)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    stars = EXCLUDED.stars,
                    forks = EXCLUDED.forks,
                    updated_at = EXCLUDED.updated_at
            """, (
                f"{repo['owner']['login']}_{repo['name']}",
                repo['name'],
                repo['full_name'],
                repo['owner']['login'],
                'User',
                repo.get('description', ''),
                repo.get('language', ''),
                repo.get('stargazers_count', 0),
                repo.get('forks_count', 0),
                repo.get('watchers_count', 0),
                repo.get('open_issues_count', 0),
                repo.get('size', 0),
                repo.get('created_at'),
                repo.get('updated_at'),
                repo.get('pushed_at'),
                repo.get('html_url'),
                repo.get('homepage'),
                repo.get('fork', False),
                ','.join(repo.get('topics', [])),
                repo.get('license', {}).get('name') if repo.get('license') else None
            ))
        
        pg.commit()
        cur.close()
        pg.close()
        
        print(f"  ✓ Stored in PostgreSQL")
        
    except Exception as e:
        print(f"    PostgreSQL error: {e}")
    
    # Close connections
    mongo_client.close()
    # Memgraph connection is automatically managed

def main():
    """Main function"""
    
    if len(sys.argv) > 1:
        # Collect specific user
        username = sys.argv[1]
        users = [username]
    else:
        # Collect famous developers
        users = FAMOUS_DEVELOPERS
    
    print("="*60)
    print("🎯 GitHub User Profile Collector")
    print("="*60)
    print(f"Collecting {len(users)} user profiles...")
    
    collected = 0
    for username in users:
        result = collect_user_profile(username)
        if result:
            user_profile, repos = result
            store_user_data(user_profile, repos)
            collected += 1
    
    print(f"\n{'='*60}")
    print(f"Collection Complete!")
    print(f"{'='*60}")
    print(f"Collected: {collected}/{len(users)} users")
    print("="*60)

if __name__ == '__main__':
    main()
