from gqlalchemy import Memgraph
from pymongo import MongoClient
import redis
from utils import GitHubAPI
from config import config
from datetime import datetime
import json
import time

class RepositoryCollector:
    def __init__(self):
        self.github = GitHubAPI()
        self.mg = Memgraph(host=config.MEMGRAPH_HOST, port=config.MEMGRAPH_PORT)
        mongo_client = MongoClient(f"mongodb://{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        self.mongodb = mongo_client[config.MONGODB_DB]
        self.dragonfly = redis.Redis(host=config.DRAGONFLY_HOST, port=config.DRAGONFLY_PORT)
    
    def _detect_frameworks(self, repo):
        """Detect frameworks from repo topics and description"""
        frameworks = []
        text = f"{' '.join(repo.get('topics', []))} {repo.get('description', '')}".lower()
        
        framework_map = {
            'django': 'Python', 'flask': 'Python', 'fastapi': 'Python',
            'react': 'JavaScript', 'vue': 'JavaScript', 'angular': 'JavaScript',
            'express': 'JavaScript', 'nextjs': 'JavaScript', 'nest': 'TypeScript',
            'spring': 'Java', 'laravel': 'PHP', 'rails': 'Ruby',
            'gin': 'Go', 'fiber': 'Go', 'actix': 'Rust', 'rocket': 'Rust',
            'asp.net': 'C#', 'blazor': 'C#'
        }
        
        for fw, lang in framework_map.items():
            if fw in text:
                frameworks.append((fw.title(), lang))
        
        return frameworks[:3]  # Limit to 3 frameworks
    
    def _extract_city(self, location_str):
        """Extract city name from location string"""
        if not location_str:
            return None
        
        # Common city patterns
        cities = {
            'San Francisco': ['san francisco', 'sf, ca'],
            'New York': ['new york', 'nyc', 'ny'],
            'London': ['london'],
            'Paris': ['paris'],
            'Berlin': ['berlin'],
            'Tokyo': ['tokyo'],
            'Beijing': ['beijing'],
            'Shanghai': ['shanghai'],
            'Seattle': ['seattle'],
            'Austin': ['austin'],
            'Boston': ['boston'],
            'Chicago': ['chicago'],
            'Los Angeles': ['los angeles', 'la, ca'],
            'Toronto': ['toronto'],
            'Vancouver': ['vancouver'],
            'Sydney': ['sydney'],
            'Melbourne': ['melbourne'],
            'Singapore': ['singapore'],
            'Bangalore': ['bangalore', 'bengaluru'],
            'Mumbai': ['mumbai'],
            'Tel Aviv': ['tel aviv'],
            'Amsterdam': ['amsterdam'],
            'Stockholm': ['stockholm'],
            'Copenhagen': ['copenhagen']
        }
        
        location_lower = location_str.lower()
        for city, patterns in cities.items():
            for pattern in patterns:
                if pattern in location_lower:
                    return city
        
        return None
    
    def _detect_dependencies(self, repo):
        """Detect dependencies from repo topics and description"""
        dependencies = []
        text = f"{' '.join(repo.get('topics', []))} {repo.get('description', '')}".lower()
        
        # Common dependencies/libraries
        dep_map = {
            'numpy': 'Python', 'pandas': 'Python', 'tensorflow': 'Python',
            'pytorch': 'Python', 'scikit-learn': 'Python', 'matplotlib': 'Python',
            'axios': 'JavaScript', 'lodash': 'JavaScript', 'moment': 'JavaScript',
            'redux': 'JavaScript', 'webpack': 'JavaScript', 'babel': 'JavaScript',
            'junit': 'Java', 'mockito': 'Java', 'hibernate': 'Java',
            'tokio': 'Rust', 'serde': 'Rust', 'clap': 'Rust',
            'gin': 'Go', 'gorm': 'Go', 'cobra': 'Go'
        }
        
        for dep, lang in dep_map.items():
            if dep in text:
                dependencies.append((dep, lang))
        
        return dependencies[:3]  # Limit to 3 dependencies
    
    def collect_all(self):
        """Collect repositories for all configured languages"""
        total_repos = 0
        country = config.FILTER_BY_COUNTRY
        start_time = time.time()
        
        print("="*60)
        print(" Repobox - Data Collection")
        print("="*60)
        print(f" Location Filter: {country or 'Global (all countries)'}")
        print(f" Repos per language: {config.REPOS_PER_LANGUAGE}")
        print(f" Languages: {', '.join(config.LANGUAGES)}")
        print(f" Total expected: {config.REPOS_PER_LANGUAGE * len(config.LANGUAGES)} repos")
        print("="*60 + "\n")
        
        for idx, language in enumerate(config.LANGUAGES, 1):
            print(f"[{idx}/{len(config.LANGUAGES)}] Collecting {language} repositories...")
            
            try:
                repos = self.github.search_repositories(
                    language=language,
                    total_repos=config.REPOS_PER_LANGUAGE,
                    country=country if country else None
                )
                
                print(f"  ✓ Fetched {len(repos)} repos")
                print(f"   Storing in databases...")
            
                for repo_idx, repo in enumerate(repos, 1):
                    self.store_repo(repo, country)
                    total_repos += 1
                    
                    if repo_idx % 50 == 0:
                        print(f"    Progress: {repo_idx}/{len(repos)} repos stored")
                
                print(f"   Completed {language}: {len(repos)} repos\n")
                
            except Exception as e:
                print(f"  ✗ Error collecting {language}: {e}\n")
                continue
        
        elapsed = time.time() - start_time
        
        print("="*60)
        print(" Collection Complete!")
        print("="*60)
        print(f" Total repositories: {total_repos}")
        print(f" Total time: {elapsed/60:.1f} minutes")
        print(f" Average: {elapsed/total_repos:.2f} seconds per repo")
        print("="*60)
    
    def store_repo(self, repo, country):
        """Store repository in all 3 databases"""
        try:
            # 1. Store in Memgraph (graph) - Enhanced structure
            repo_name = repo['name'].replace("'", "\\'")
            repo_full_name = repo['full_name'].replace("'", "\\'")
            repo_desc = repo.get('description', '').replace("'", "\\'")[:200] if repo.get('description') else ''
            
            # Create Repository node with full properties
            self.mg.execute(f"""
                MERGE (r:Repository {{full_name: '{repo_full_name}'}})
                SET r.name = '{repo_name}',
                    r.stars = {repo['stargazers_count']},
                    r.forks = {repo['forks_count']},
                    r.open_issues = {repo.get('open_issues_count', 0)},
                    r.watchers = {repo.get('watchers_count', 0)},
                    r.size = {repo.get('size', 0)},
                    r.description = '{repo_desc}',
                    r.url = '{repo['html_url']}',
                    r.created_at = '{repo.get('created_at', '')}',
                    r.updated_at = '{repo.get('updated_at', '')}',
                    r.is_fork = {str(repo.get('fork', False)).lower()},
                    r.default_branch = '{repo.get('default_branch', 'main')}',
                    r.has_wiki = {str(repo.get('has_wiki', False)).lower()},
                    r.has_issues = {str(repo.get('has_issues', True)).lower()}
            """)
            
            # Create User or Organization node directly (no generic Owner)
            owner_login = repo['owner']['login'].replace("'", "\\'")
            owner_type = repo['owner']['type']
            owner_avatar = repo['owner'].get('avatar_url', '').replace("'", "\\'")
            owner_url = repo['owner'].get('html_url', '').replace("'", "\\'")
            
            if owner_type == 'User':
                # Create User node
                self.mg.execute(f"""
                    MERGE (u:User {{login: '{owner_login}'}})
                    SET u.avatar_url = '{owner_avatar}',
                        u.url = '{owner_url}',
                        u.type = 'User'
                """)
                
                # Repository OWNED_BY User
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (u:User {{login: '{owner_login}'}})
                    MERGE (r)-[:OWNED_BY]->(u)
                """)
            else:
                # Create Organization node
                self.mg.execute(f"""
                    MERGE (org:Organization {{login: '{owner_login}'}})
                    SET org.avatar_url = '{owner_avatar}',
                        org.url = '{owner_url}',
                        org.type = 'Organization'
                """)
                
                # Repository OWNED_BY Organization
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (org:Organization {{login: '{owner_login}'}})
                    MERGE (r)-[:OWNED_BY]->(org)
                """)
            
            # Create Location (Country) and City nodes
            if country:
                country_clean = country.replace("'", "\\'")
                
                # Create Country node
                self.mg.execute(f"""
                    MERGE (loc:Location {{name: '{country_clean}'}})
                    SET loc.code = '{country_clean[:2].upper()}',
                        loc.type = 'country'
                """)
                
                # Repository LOCATED_IN Country
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (loc:Location {{name: '{country_clean}'}})
                    MERGE (r)-[:LOCATED_IN]->(loc)
                """)
                
                # Create City node (extract from owner location if available)
                city_name = self._extract_city(repo['owner'].get('location', ''))
                if city_name:
                    city_clean = city_name.replace("'", "\\'")
                    self.mg.execute(f"""
                        MERGE (city:City {{name: '{city_clean}'}})
                        SET city.country = '{country_clean}'
                    """)
                    
                    # City PART_OF Country
                    self.mg.execute(f"""
                        MATCH (city:City {{name: '{city_clean}'}})
                        MATCH (loc:Location {{name: '{country_clean}'}})
                        MERGE (city)-[:PART_OF]->(loc)
                    """)
                    
                    # Owner LOCATED_IN City
                    if owner_type == 'User':
                        self.mg.execute(f"""
                            MATCH (u:User {{login: '{owner_login}'}})
                            MATCH (city:City {{name: '{city_clean}'}})
                            MERGE (u)-[:LOCATED_IN]->(city)
                        """)
                    else:
                        self.mg.execute(f"""
                            MATCH (org:Organization {{login: '{owner_login}'}})
                            MATCH (city:City {{name: '{city_clean}'}})
                            MERGE (org)-[:LOCATED_IN]->(city)
                        """)
            
            # Create Language node with enhanced properties
            if repo.get('language'):
                lang = repo['language'].replace("'", "\\'")
                lang_colors = {
                    'Python': '#3572A5', 'JavaScript': '#f1e05a', 'TypeScript': '#2b7489',
                    'Go': '#00ADD8', 'Rust': '#dea584', 'Java': '#b07219',
                    'C++': '#f34b7d', 'C#': '#178600', 'C': '#555555'
                }
                lang_color = lang_colors.get(lang, '#cccccc')
                
                self.mg.execute(f"""
                    MERGE (l:Language {{name: '{lang}'}})
                    SET l.color = '{lang_color}',
                        l.type = 'programming'
                """)
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (l:Language {{name: '{lang}'}})
                    MERGE (r)-[:USES]->(l)
                """)
            
            # Create Topic nodes and HAS_TOPIC relationships
            for topic in repo.get('topics', [])[:5]:
                topic_clean = topic.replace("'", "\\'")
                self.mg.execute(f"""
                    MERGE (t:Topic {{name: '{topic_clean}'}})
                    SET t.display_name = '{topic_clean.replace('-', ' ').title()}'
                """)
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (t:Topic {{name: '{topic_clean}'}})
                    MERGE (r)-[:HAS_TOPIC]->(t)
                """)
            
            # Detect frameworks from topics and description
            frameworks = self._detect_frameworks(repo)
            for framework_name, framework_lang in frameworks:
                fw_name = framework_name.replace("'", "\\'")
                fw_lang = framework_lang.replace("'", "\\'")
                self.mg.execute(f"""
                    MERGE (fw:Framework {{name: '{fw_name}'}})
                    SET fw.language = '{fw_lang}',
                        fw.category = 'web'
                """)
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (fw:Framework {{name: '{fw_name}'}})
                    MERGE (r)-[:USES_FRAMEWORK]->(fw)
                """)
            
            # Detect dependencies/libraries
            dependencies = self._detect_dependencies(repo)
            for dep_name, dep_lang in dependencies:
                dep_clean = dep_name.replace("'", "\\'")
                dep_lang_clean = dep_lang.replace("'", "\\'")
                self.mg.execute(f"""
                    MERGE (dep:Dependency {{name: '{dep_clean}'}})
                    SET dep.language = '{dep_lang_clean}',
                        dep.type = 'library'
                """)
                self.mg.execute(f"""
                    MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                    MATCH (dep:Dependency {{name: '{dep_clean}'}})
                    MERGE (r)-[:DEPENDS_ON]->(dep)
                """)
            
            # Create Contributor node for the owner (they're the main contributor)
            contributor_id = f"{owner_login}_{repo_full_name}".replace("'", "\\'")
            self.mg.execute(f"""
                MERGE (c:Contributor {{id: '{contributor_id}'}})
                SET c.user_login = '{owner_login}',
                    c.repo_full_name = '{repo_full_name}',
                    c.role = 'owner',
                    c.contributions = {repo['stargazers_count']}
            """)
            
            # Contributor CONTRIBUTES_TO Repository
            self.mg.execute(f"""
                MATCH (c:Contributor {{id: '{contributor_id}'}})
                MATCH (r:Repository {{full_name: '{repo_full_name}'}})
                MERGE (c)-[:CONTRIBUTES_TO]->(r)
            """)
            
            # User/Org HAS_CONTRIBUTOR Contributor
            if owner_type == 'User':
                self.mg.execute(f"""
                    MATCH (u:User {{login: '{owner_login}'}})
                    MATCH (c:Contributor {{id: '{contributor_id}'}})
                    MERGE (u)-[:HAS_CONTRIBUTOR]->(c)
                """)
            else:
                self.mg.execute(f"""
                    MATCH (org:Organization {{login: '{owner_login}'}})
                    MATCH (c:Contributor {{id: '{contributor_id}'}})
                    MERGE (org)-[:HAS_CONTRIBUTOR]->(c)
                """)
            
            # 2. Store in MongoDB (document)
            doc = {
                '_id': repo['full_name'].replace('/', '_'),
                'name': repo['name'],
                'full_name': repo['full_name'],
                'stars': repo['stargazers_count'],
                'forks': repo['forks_count'],
                'watchers': repo.get('watchers_count', 0),
                'open_issues': repo.get('open_issues_count', 0),
                'size': repo.get('size', 0),
                'language': repo.get('language', 'Unknown'),
                'topics': repo.get('topics', []),
                'frameworks': [fw[0] for fw in self._detect_frameworks(repo)],
                'location': country if country else 'Global',
                'owner_login': repo['owner']['login'],
                'owner_type': repo['owner']['type'],
                'description': repo.get('description', ''),
                'url': repo['html_url'],
                'created_at': repo.get('created_at'),
                'updated_at': repo.get('updated_at'),
                'is_fork': repo.get('fork', False),
                'default_branch': repo.get('default_branch', 'main'),
                'collected_at': datetime.now().isoformat()
            }
            self.mongodb.repositories.replace_one({'_id': doc['_id']}, doc, upsert=True)
            
            # 3. Store in Dragonfly (ultra-fast cache)
            
            # Global leaderboard (sorted by stars)
            self.dragonfly.zadd('leaderboard:global:stars', 
                               {repo['full_name']: repo['stargazers_count']})
            
            # Forks leaderboard
            self.dragonfly.zadd('leaderboard:global:forks', 
                               {repo['full_name']: repo['forks_count']})
            
            # Language-specific leaderboards
            if repo.get('language'):
                lang = repo['language']
                self.dragonfly.zadd(f'leaderboard:language:{lang}', 
                                   {repo['full_name']: repo['stargazers_count']})
            
            # Owner-specific stats
            self.dragonfly.hincrby(f'owner:{owner_login}:stats', 'total_repos', 1)
            self.dragonfly.hincrby(f'owner:{owner_login}:stats', 'total_stars', repo['stargazers_count'])
            self.dragonfly.hincrby(f'owner:{owner_login}:stats', 'total_forks', repo['forks_count'])
            
            # Language statistics
            if repo.get('language'):
                self.dragonfly.hincrby('stats:languages', repo['language'], 1)
                self.dragonfly.zincrby('trending:languages', 1, repo['language'])
            
            # Topic statistics
            for topic in repo.get('topics', [])[:5]:
                self.dragonfly.zincrby('trending:topics', 1, topic)
            
            # Framework statistics
            for fw_name, _ in self._detect_frameworks(repo):
                self.dragonfly.zincrby('trending:frameworks', 1, fw_name)
            
            # Cache full repo data (for fast lookups)
            repo_cache_key = f'repo:{repo["full_name"]}'
            self.dragonfly.hset(repo_cache_key, mapping={
                'name': repo['name'],
                'stars': repo['stargazers_count'],
                'forks': repo['forks_count'],
                'language': repo.get('language', 'Unknown'),
                'owner': owner_login,
                'url': repo['html_url']
            })
            self.dragonfly.expire(repo_cache_key, 3600)  # Cache for 1 hour
            
            # Location-based stats
            if country:
                self.dragonfly.zadd(f'leaderboard:location:{country}', 
                                   {repo['full_name']: repo['stargazers_count']})
                self.dragonfly.hincrby(f'stats:location:{country}', 'total_repos', 1)
                self.dragonfly.hincrby(f'stats:location:{country}', 'total_stars', repo['stargazers_count'])
                
                if repo.get('language'):
                    self.dragonfly.zincrby(f'location:{country}:languages', 1, repo['language'])
            
            # Recent repos (time-based)
            self.dragonfly.zadd('recent:repos', 
                               {repo['full_name']: time.time()})
            
            # Keep only last 100 recent repos
            self.dragonfly.zremrangebyrank('recent:repos', 0, -101)
            
        except Exception as e:
            print(f"    ⚠️  Error storing {repo['full_name']}: {e}")

if __name__ == '__main__':
    collector = RepositoryCollector()
    collector.collect_all()