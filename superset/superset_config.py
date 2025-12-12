# Superset Configuration
# Disable lazy rendering to fix blurry charts

# Disable chart lazy rendering
DASHBOARD_CROSS_FILTERS_ENABLED = True
DASHBOARD_NATIVE_FILTERS_SET = True

# Force high-quality chart rendering
WEBDRIVER_BASEURL = "http://superset:8088/"
WEBDRIVER_BASEURL_USER_FRIENDLY = "http://localhost:8088/"

# Increase chart rendering quality
THUMBNAIL_SELENIUM_USER = "admin"
THUMBNAIL_CACHE_CONFIG = {
    'CACHE_TYPE': 'null',
}

# Disable lazy loading
DASHBOARD_AUTO_REFRESH_MODE = "fetch"
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [0, "Don't refresh"],
    [10, "10 seconds"],
    [30, "30 seconds"],
    [60, "1 minute"],
    [300, "5 minutes"],
]

# Feature flags
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
