"""
Aggregate data from repositories collection into other MongoDB collections
"""
from pymongo import MongoClient
from config import config
from collections import defaultdict

class DataAggregator:
    def __init__(self):
        client = MongoClient(f"mongodb://{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        self.db = client[config.MONGODB_DB]
    
    def aggregate_all(self):
        """Run all aggregation tasks"""
        print("="*60)
        print("📊 Aggregating Data into MongoDB Collections")
        print("="*60 + "\n")
        
        self.aggregate_owners()
        self.aggregate_languages()
        self.aggregate_locations()
        self.aggregate_topics()
        self.aggregate_frameworks()
        
        print("\n" + "="*60)
        print(" All aggregations complete!")
        print("="*60)
    
    def aggregate_owners(self):
        """Aggregate owner statistics"""
        print("1. Aggregating owners...")
        
        # Get all repos
        repos = list(self.db.repositories.find())
        
        # Group by owner
        owners_data = defaultdict(lambda: {
            'repos': [],
            'total_stars': 0,
            'total_forks': 0,
            'languages': set()
        })
        
        for repo in repos:
            owner = repo['owner_login']
            owners_data[owner]['repos'].append(repo['name'])
            owners_data[owner]['total_stars'] += repo['stars']
            owners_data[owner]['total_forks'] += repo['forks']
            if repo.get('language'):
                owners_data[owner]['languages'].add(repo['language'])
        
        # Insert into owners collection
        for login, data in owners_data.items():
            self.db.owners.replace_one(
                {'_id': login},
                {
                    '_id': login,
                    'login': login,
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars'],
                    'total_forks': data['total_forks'],
                    'repos': data['repos'],
                    'languages': list(data['languages'])
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(owners_data)} owners")
    
    def aggregate_languages(self):
        """Aggregate language statistics"""
        print("\n2. Aggregating languages...")
        
        repos = list(self.db.repositories.find())
        
        # Group by language
        lang_data = defaultdict(lambda: {
            'repos': [],
            'total_stars': 0,
            'total_forks': 0,
            'owners': set()
        })
        
        for repo in repos:
            lang = repo.get('language', 'Unknown')
            lang_data[lang]['repos'].append(repo['full_name'])
            lang_data[lang]['total_stars'] += repo['stars']
            lang_data[lang]['total_forks'] += repo['forks']
            lang_data[lang]['owners'].add(repo['owner_login'])
        
        # Insert into languages collection
        for name, data in lang_data.items():
            avg_stars = data['total_stars'] / len(data['repos']) if data['repos'] else 0
            
            self.db.languages.replace_one(
                {'_id': name},
                {
                    '_id': name,
                    'name': name,
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars'],
                    'total_forks': data['total_forks'],
                    'avg_stars': round(avg_stars, 2),
                    'unique_owners': len(data['owners'])
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(lang_data)} languages")
    
    def aggregate_locations(self):
        """Aggregate location statistics"""
        print("\n3. Aggregating locations...")
        
        repos = list(self.db.repositories.find())
        
        # Group by location
        loc_data = defaultdict(lambda: {
            'repos': [],
            'total_stars': 0,
            'languages': defaultdict(int),
            'owners': set()
        })
        
        for repo in repos:
            loc = repo.get('location', 'Global')
            loc_data[loc]['repos'].append(repo['full_name'])
            loc_data[loc]['total_stars'] += repo['stars']
            if repo.get('language'):
                loc_data[loc]['languages'][repo['language']] += 1
            loc_data[loc]['owners'].add(repo['owner_login'])
        
        # Insert into locations collection
        for name, data in loc_data.items():
            top_langs = sorted(data['languages'].items(), key=lambda x: x[1], reverse=True)[:3]
            
            self.db.locations.replace_one(
                {'_id': name},
                {
                    '_id': name,
                    'name': name,
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars'],
                    'avg_stars': round(data['total_stars'] / len(data['repos']), 2) if data['repos'] else 0,
                    'top_languages': [lang for lang, _ in top_langs],
                    'unique_owners': len(data['owners'])
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(loc_data)} locations")
    
    def aggregate_topics(self):
        """Aggregate topic statistics"""
        print("\n4. Aggregating topics...")
        
        repos = list(self.db.repositories.find())
        
        # Group by topic
        topic_data = defaultdict(lambda: {
            'repos': [],
            'languages': set(),
            'total_stars': 0
        })
        
        for repo in repos:
            for topic in repo.get('topics', []):
                topic_data[topic]['repos'].append(repo['full_name'])
                if repo.get('language'):
                    topic_data[topic]['languages'].add(repo['language'])
                topic_data[topic]['total_stars'] += repo['stars']
        
        # Insert into topics collection
        for name, data in topic_data.items():
            self.db.topics.replace_one(
                {'_id': name},
                {
                    '_id': name,
                    'name': name,
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars'],
                    'related_languages': list(data['languages'])
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(topic_data)} topics")
    
    def aggregate_frameworks(self):
        """Aggregate framework statistics"""
        print("\n5. Aggregating frameworks...")
        
        repos = list(self.db.repositories.find())
        
        # Group by framework
        framework_data = defaultdict(lambda: {
            'repos': [],
            'total_stars': 0,
            'languages': set()
        })
        
        for repo in repos:
            for framework in repo.get('frameworks', []):
                framework_data[framework]['repos'].append(repo['full_name'])
                framework_data[framework]['total_stars'] += repo['stars']
                if repo.get('language'):
                    framework_data[framework]['languages'].add(repo['language'])
        
        # Insert into frameworks collection
        for name, data in framework_data.items():
            lang = list(data['languages'])[0] if data['languages'] else 'Unknown'
            
            self.db.frameworks.replace_one(
                {'_id': name},
                {
                    '_id': name,
                    'name': name,
                    'language': lang,
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars']
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(framework_data)} frameworks")

if __name__ == '__main__':
    aggregator = DataAggregator()
    aggregator.aggregate_all()
