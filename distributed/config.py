"""
Configuration for WDT Distributed File Sharing.
"""
import os
import secrets

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Discovery server settings
DISCOVERY_HOST = os.environ.get("WDT_DISCOVERY_HOST", "0.0.0.0")
DISCOVERY_PORT = int(os.environ.get("WDT_DISCOVERY_PORT", "8400"))

# Peer settings
PEER_HOST = os.environ.get("WDT_PEER_HOST", "0.0.0.0")
PEER_PORT = int(os.environ.get("WDT_PEER_PORT", "8401"))

# GUI settings
GUI_HOST = os.environ.get("WDT_GUI_HOST", "127.0.0.1")
GUI_PORT = int(os.environ.get("WDT_GUI_PORT", "8402"))

# Storage settings
STORAGE_DIR = os.environ.get("WDT_STORAGE_DIR", os.path.join(BASE_DIR, "storage"))
SHARD_DIR = os.environ.get("WDT_SHARD_DIR", os.path.join(STORAGE_DIR, "shards"))
META_DIR = os.environ.get("WDT_META_DIR", os.path.join(STORAGE_DIR, "meta"))
DOWNLOAD_DIR = os.environ.get("WDT_DOWNLOAD_DIR", os.path.join(STORAGE_DIR, "downloads"))
CERT_DIR = os.environ.get("WDT_CERT_DIR", os.path.join(BASE_DIR, "certs"))

# Sharding settings
SHARD_SIZE = int(os.environ.get("WDT_SHARD_SIZE", str(4 * 1024 * 1024)))  # 4 MB default
REPLICATION_FACTOR = int(os.environ.get("WDT_REPLICATION_FACTOR", "2"))

# Security settings
SECRET_KEY = os.environ.get("WDT_SECRET_KEY", secrets.token_hex(32))
TOKEN_EXPIRY_SECONDS = int(os.environ.get("WDT_TOKEN_EXPIRY", "3600"))

# Heartbeat settings
HEARTBEAT_INTERVAL = int(os.environ.get("WDT_HEARTBEAT_INTERVAL", "10"))
PEER_TIMEOUT = int(os.environ.get("WDT_PEER_TIMEOUT", "30"))

# Transfer settings
MAX_CONCURRENT_TRANSFERS = int(os.environ.get("WDT_MAX_TRANSFERS", "5"))
TRANSFER_BUFFER_SIZE = int(os.environ.get("WDT_TRANSFER_BUFFER", str(64 * 1024)))  # 64KB
