"""
Enrich MongoDB with additional data for better dashboards
Populates: contributors, licenses, organizations, cities
"""
from pymongo import MongoClient
from config import config
from collections import defaultdict, Counter
import random

class DataEnricher:
    def __init__(self):
        client = MongoClient(f"mongodb://{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        self.db = client[config.MONGODB_DB]
    
    def enrich_all(self):
        """Run all enrichment tasks"""
        print("="*60)
        print(" Enriching Data for Better Dashboards")
        print("="*60 + "\n")
        
        self.enrich_licenses()
        self.enrich_organizations()
        self.enrich_cities()
        self.enrich_contributors()
        
        print("\n" + "="*60)
        print("All enrichments complete!")
        print("="*60)
    
    def enrich_licenses(self):
        """Extract and aggregate license information"""
        print("1. Enriching licenses...")
        
        repos = list(self.db.repositories.find())
        
        # Common licenses mapping
        license_map = {
            'mit': 'MIT License',
            'apache': 'Apache License 2.0',
            'gpl': 'GNU General Public License',
            'bsd': 'BSD License',
            'mpl': 'Mozilla Public License',
            'lgpl': 'GNU Lesser General Public License',
            'agpl': 'GNU Affero General Public License',
            'unlicense': 'The Unlicense',
            'isc': 'ISC License',
            'cc0': 'Creative Commons Zero'
        }
        
        license_data = defaultdict(lambda: {
            'repos': [],
            'languages': set(),
            'total_stars': 0
        })
        
        # Simulate license data (in real scenario, this comes from GitHub API)
        for repo in repos:
            # Assign random license for demo
            license_key = random.choice(list(license_map.keys()))
            license_name = license_map[license_key]
            
            license_data[license_name]['repos'].append(repo['full_name'])
            license_data[license_name]['total_stars'] += repo['stars']
            if repo.get('language'):
                license_data[license_name]['languages'].add(repo['language'])
        
        # Insert into licenses collection
        for name, data in license_data.items():
            self.db.licenses.replace_one(
                {'_id': name},
                {
                    '_id': name,
                    'name': name,
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars'],
                    'languages': list(data['languages']),
                    'popularity_rank': len(data['repos'])
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(license_data)} licenses")
    
    def enrich_organizations(self):
        """Extract organization-specific data"""
        print("\n2. Enriching organizations...")
        
        repos = list(self.db.repositories.find({'owner_type': 'Organization'}))
        
        org_data = defaultdict(lambda: {
            'repos': [],
            'total_stars': 0,
            'total_forks': 0,
            'languages': set(),
            'topics': set()
        })
        
        for repo in repos:
            org = repo['owner_login']
            org_data[org]['repos'].append(repo['name'])
            org_data[org]['total_stars'] += repo['stars']
            org_data[org]['total_forks'] += repo['forks']
            if repo.get('language'):
                org_data[org]['languages'].add(repo['language'])
            org_data[org]['topics'].update(repo.get('topics', []))
        
        # Insert into organizations collection
        for name, data in org_data.items():
            self.db.organizations.replace_one(
                {'_id': name},
                {
                    '_id': name,
                    'name': name,
                    'type': 'Organization',
                    'total_repos': len(data['repos']),
                    'total_stars': data['total_stars'],
                    'total_forks': data['total_forks'],
                    'avg_stars': round(data['total_stars'] / len(data['repos']), 2) if data['repos'] else 0,
                    'languages': list(data['languages']),
                    'top_topics': list(data['topics'])[:10],
                    'repos': data['repos'][:20]  # Top 20 repos
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(org_data)} organizations")
    
    def enrich_cities(self):
        """Create city-level data from location information"""
        print("\n3. Enriching cities...")
        
        # Major tech cities with coordinates
        cities = {
            'San Francisco': {'country': 'USA', 'lat': 37.7749, 'lon': -122.4194},
            'New York': {'country': 'USA', 'lat': 40.7128, 'lon': -74.0060},
            'London': {'country': 'UK', 'lat': 51.5074, 'lon': -0.1278},
            'Berlin': {'country': 'Germany', 'lat': 52.5200, 'lon': 13.4050},
            'Paris': {'country': 'France', 'lat': 48.8566, 'lon': 2.3522},
            'Tokyo': {'country': 'Japan', 'lat': 35.6762, 'lon': 139.6503},
            'Beijing': {'country': 'China', 'lat': 39.9042, 'lon': 116.4074},
            'Bangalore': {'country': 'India', 'lat': 12.9716, 'lon': 77.5946},
            'Toronto': {'country': 'Canada', 'lat': 43.6532, 'lon': -79.3832},
            'Sydney': {'country': 'Australia', 'lat': -33.8688, 'lon': 151.2093},
            'Seattle': {'country': 'USA', 'lat': 47.6062, 'lon': -122.3321},
            'Amsterdam': {'country': 'Netherlands', 'lat': 52.3676, 'lon': 4.9041},
            'Singapore': {'country': 'Singapore', 'lat': 1.3521, 'lon': 103.8198},
            'Tel Aviv': {'country': 'Israel', 'lat': 32.0853, 'lon': 34.7818},
            'Stockholm': {'country': 'Sweden', 'lat': 59.3293, 'lon': 18.0686}
        }
        
        repos = list(self.db.repositories.find())
        
        # Distribute repos across cities (simulated)
        for city_name, city_info in cities.items():
            # Random sample of repos for this city
            city_repos = random.sample(repos, min(random.randint(5, 30), len(repos)))
            
            total_stars = sum(r['stars'] for r in city_repos)
            languages = Counter(r.get('language', 'Unknown') for r in city_repos)
            
            self.db.cities.replace_one(
                {'_id': city_name},
                {
                    '_id': city_name,
                    'name': city_name,
                    'country': city_info['country'],
                    'latitude': city_info['lat'],
                    'longitude': city_info['lon'],
                    'total_repos': len(city_repos),
                    'total_stars': total_stars,
                    'avg_stars': round(total_stars / len(city_repos), 2) if city_repos else 0,
                    'top_language': languages.most_common(1)[0][0] if languages else 'Unknown',
                    'languages': dict(languages.most_common(5))
                },
                upsert=True
            )
        
        print(f"   ✓ Inserted {len(cities)} cities")
    
    def enrich_contributors(self):
        """Create contributor statistics"""
        print("\n4. Enriching contributors...")
        
        repos = list(self.db.repositories.find())
        owners = list(self.db.owners.find())
        
        # Create contributor records from owners
        contributor_count = 0
        for owner in owners:
            # Simulate contribution data
            contributions = random.randint(1, 100)
            repos_contributed = random.sample(
                [r['full_name'] for r in repos],
                min(random.randint(1, 10), len(repos))
            )
            
            self.db.contributors.replace_one(
                {'_id': owner['_id']},
                {
                    '_id': owner['_id'],
                    'login': owner['login'],
                    'total_contributions': contributions,
                    'repos_contributed': len(repos_contributed),
                    'repos': repos_contributed,
                    'languages': owner.get('languages', []),
                    'contribution_score': contributions * len(repos_contributed)
                },
                upsert=True
            )
            contributor_count += 1
        
        print(f"   ✓ Inserted {contributor_count} contributors")

if __name__ == '__main__':
    enricher = DataEnricher()
    enricher.enrich_all()
