# RepoBox - GitHub Repository Analytics Platform

Collect, analyze, and visualize GitHub repository data using 3 specialized databases and interactive dashboards.

---

## What is RepoBox?

RepoBox collects GitHub repositories and stores them in **3 different databases**:
- **Memgraph** (Graph DB) - For exploring connections between repos, users, and languages
- **MongoDB** (Document DB) - For storing complete repository data
- **Dragonfly** (Cache) - For ultra-fast data access

Then it provides:
- **FastAPI** - REST API to access the data
- **Apache Superset** - Beautiful dashboards and charts

---

## What You'll Get

- Analyze repositories across multiple programming languages
- Explore developer networks and connections
- Visualize trends with interactive dashboards
- Lightning-fast queries with smart caching
- Graph relationships between repos, users, topics, and frameworks

---

## Quick Start (5 Minutes)

### Prerequisites

You only need 2 things:
- **Docker** (with Docker Compose)
- **GitHub Token** (free, takes 1 minute to create)

---

### Step 1: Get a GitHub Token

1. Go to https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Give it a name like "RepoBox"
4. Select scopes: `public_repo` and `read:user`
5. Click "Generate token"
6. **Copy the token** (starts with `ghp_...`)

---

### Step 2: Clone and Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/repobox.git
cd repobox

# Edit .env and paste your GitHub token
nano .env  # or use any text editor
```

**Edit the `.env` file and update your token:**
```bash
GITHUB_TOKEN=ghp_YOUR_TOKEN_HERE
REPOS_PER_LANGUAGE=10
LANGUAGES=python,javascript,typescript,go,rust,java
```

---

### Step 3: Start Everything

```bash
# Start all services (databases, API, dashboards)
docker compose up -d

# Wait 30 seconds for everything to start
# Check if everything is running
docker compose ps
```

You should see 6 services running:
- repo-memgraph
- repo-mongodb
- repo-dragonfly
- github-backend
- repobox-superset
- repo-postgres

---

### Step 4: Collect Data

```bash
# Install Python dependencies
pip install -r collector/requirements.txt

# Initialize databases
cd collector
python init_databases.py

# Collect GitHub repositories (takes ~2 minutes)
python collect_repos.py

# Aggregate the data
python aggregate_data.py
```

**That's it!**

---

## Access Your Dashboards

| Service | URL | Login |
|---------|-----|-------|
| **Superset Dashboards** | http://localhost:8088 | admin / admin |
| **FastAPI Docs** | http://localhost:5000/docs | No login |
| **Memgraph Lab** | http://localhost:3002 | No login |
| **MongoDB UI** | http://localhost:8083 | No login |

---

## What Data Gets Collected?

For each programming language (Python, JavaScript, etc.), the system collects:
- Top repositories by stars
- Repository details (name, description, stars, forks)
- Owner information (users and organizations)
- Programming languages used
- Topics and frameworks
- Dependencies

**Example:** If you set `REPOS_PER_LANGUAGE=10` and `LANGUAGES=python,javascript`, you'll get 20 repositories total.

---

## How to Use

### View Data in Superset

1. Open http://localhost:8088
2. Login: `admin` / `admin`
3. Create charts and dashboards
4. Connect to PostgreSQL or MongoDB

### Query the API

```bash
# Get language statistics
curl http://localhost:5000/metrics/languages

# Get location data
curl http://localhost:5000/metrics/locations/map

# Check cache stats
curl http://localhost:5000/cache/stats
```

### Explore the Graph

1. Open http://localhost:3002 (Memgraph Lab)
2. Run Cypher queries:

```cypher
// Find all Python repositories
MATCH (r:Repository)-[:USES]->(l:Language {name: 'Python'})
RETURN r.name, r.stars
ORDER BY r.stars DESC
LIMIT 10

// Find repos using Django framework
MATCH (r:Repository)-[:USES_FRAMEWORK]->(f:Framework {name: 'Django'})
RETURN r.name, r.stars

// Find most popular topics
MATCH (r:Repository)-[:HAS_TOPIC]->(t:Topic)
RETURN t.name, count(r) as repos
ORDER BY repos DESC
LIMIT 10
```

---

## Configuration

Edit `.env` to customize:

```bash
# How many repos per language?
REPOS_PER_LANGUAGE=10

# Which languages to collect?
LANGUAGES=python,javascript,typescript,go,rust,java,cpp,csharp

# Filter by country (optional)
FILTER_BY_COUNTRY=Tunisia
# Leave empty for global collection
```

---

## Stop Everything

```bash
# Stop all services
docker compose down

# Stop and delete all data (fresh start)
docker compose down -v
```

---

## Project Structure

```
repobox/
├── docker-compose.yaml       # All services configuration
├── .env                      # Your settings (GitHub token, etc.)
│
├── collector/                # Data collection scripts
│   ├── collect_repos.py     # Main collector
│   ├── aggregate_data.py    # Data aggregation
│   ├── init_databases.py    # Database setup
│   └── requirements.txt     # Python dependencies
│
├── backend/                  # FastAPI REST API
│   ├── api.py               # API endpoints
│   └── requirements.txt     # API dependencies
│
└── superset/                 # Superset configuration
    ├── Dockerfile
    └── superset_config.py
```

---

## Troubleshooting

### Services won't start?
```bash
# Check Docker is running
docker --version

# View logs
docker compose logs -f
```

### Can't collect data?
```bash
# Check your GitHub token is set
cat .env | grep GITHUB_TOKEN

# Test database connections
cd collector
python test_connections.py
```

### Port already in use?
Edit `docker-compose.yaml` and change the port numbers:
```yaml
ports:
  - "5001:5000"  # Change 5000 to 5001
```

---

## What's Inside?

### Databases
- **Memgraph** - Graph database with nodes and relationships
- **MongoDB** - Document database with flexible JSON storage
- **Dragonfly** - Ultra-fast cache (25x faster than Redis)
- **PostgreSQL** - SQL database for Superset

### API Endpoints
- `/` - API status
- `/metrics/languages` - Language statistics
- `/metrics/locations/map` - World map data
- `/locations/{location}/repos` - Repos by location
- `/cache/stats` - Cache performance
- `/cache/clear` - Clear cache

### Graph Model
- **9 node types:** Repository, User, Organization, Language, Framework, Topic, Dependency, Contributor, City
- **7 relationship types:** OWNED_BY, USES, HAS_TOPIC, DEPENDS_ON, USES_FRAMEWORK, CONTRIBUTES_TO, HAS_CONTRIBUTOR

---

## Next Steps

- Create custom Superset dashboards
- Add more programming languages
- Collect user profiles with `collect_user_profile.py`
- Explore graph relationships in Memgraph Lab
- Build your own API endpoints

---


---

## License

MIT License - Feel free to use and modify!

---

## Contributing

Found a bug? Have an idea? Open an issue or submit a pull request!

---

**Built using Docker, FastAPI, Memgraph, MongoDB, Dragonfly, and Apache Superset**
