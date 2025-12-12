import requests
from config import config
import time

class GitHubAPI:
    def __init__(self):
        self.base_url = config.GITHUB_API_BASE
        self.headers = {
            'Authorization': f'token {config.GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
    
    def search_repositories(self, language, total_repos=500, country=None):
        """
        Search repositories with pagination.
        GitHub API returns max 100 results per page.
        """
        all_repos = []
        per_page = 100
        pages_needed = (total_repos + per_page - 1) // per_page
        
        # Build query
        query_parts = [f'language:{language}']
        if country:
            query_parts.append(f'location:{country}')
        query = ' '.join(query_parts)
        
        print(f"  Fetching {total_repos} repos in {pages_needed} pages...")
        
        for page in range(1, pages_needed + 1):
            url = f"{self.base_url}/search/repositories"
            params = {
                'q': query,
                'sort': 'stars',
                'order': 'desc',
                'per_page': per_page,
                'page': page
            }
            
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                items = response.json().get('items', [])
                all_repos.extend(items)
                
                print(f"    Page {page}/{pages_needed}: {len(items)} repos")
                
                # Check rate limit
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                if remaining < 10:
                    print(f"  Rate limit low: {remaining} requests remaining")
                
                if len(all_repos) >= total_repos:
                    break
                
                time.sleep(1)  # Be nice to GitHub
                
            except requests.exceptions.RequestException as e:
                print(f"    ✗ Error on page {page}: {e}")
                break
        
        return all_repos[:total_repos]