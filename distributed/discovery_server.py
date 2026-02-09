"""
Discovery Server for WDT Distributed File Sharing.
Central registry where peers register, discover each other, and share file metadata.
Runs as a Flask application with token-based authentication.
"""
import logging
import threading
import time
from typing import Dict, List, Optional

from flask import Flask, jsonify, request

from config import (
    DISCOVERY_HOST,
    DISCOVERY_PORT,
    HEARTBEAT_INTERVAL,
    PEER_TIMEOUT,
    SECRET_KEY,
)
from security import CertificateManager, TokenManager

logger = logging.getLogger("wdt.discovery")


class PeerRecord:
    """Tracks a registered peer."""

    def __init__(self, peer_id: str, host: str, port: int, capacity_bytes: int = 0):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.capacity_bytes = capacity_bytes
        self.shared_files: Dict[str, dict] = {}  # file_id -> file_metadata_dict
        self.shard_ids: List[str] = []
        self.last_heartbeat = time.time()
        self.registered_at = time.time()

    @property
    def is_alive(self) -> bool:
        return (time.time() - self.last_heartbeat) < PEER_TIMEOUT

    def to_dict(self) -> dict:
        return {
            "peer_id": self.peer_id,
            "host": self.host,
            "port": self.port,
            "capacity_bytes": self.capacity_bytes,
            "file_count": len(self.shared_files),
            "shard_count": len(self.shard_ids),
            "last_heartbeat": self.last_heartbeat,
            "is_alive": self.is_alive,
        }


class DiscoveryServer:
    """Central discovery and coordination server."""

    def __init__(self, host: str = DISCOVERY_HOST, port: int = DISCOVERY_PORT,
                 secret_key: str = SECRET_KEY):
        self.host = host
        self.port = port
        self.token_manager = TokenManager(secret_key)
        self.cert_manager = CertificateManager()
        self.peers: Dict[str, PeerRecord] = {}
        self.file_registry: Dict[str, dict] = {}  # file_id -> metadata dict
        self._lock = threading.Lock()
        self._running = False

        self.app = Flask(__name__)
        self._register_routes()

    def _authenticate(self) -> Optional[dict]:
        """Validate the Authorization header token."""
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        token = auth[7:]
        return self.token_manager.validate_token(token)

    def _require_auth(self):
        """Return error response if authentication fails."""
        payload = self._authenticate()
        if payload is None:
            return None, (jsonify({"error": "Unauthorized"}), 401)
        return payload, None

    def _register_routes(self):
        app = self.app

        @app.route("/api/register", methods=["POST"])
        def register_peer():
            """Register a new peer. Returns auth token."""
            data = request.get_json()
            if not data or "peer_id" not in data:
                return jsonify({"error": "peer_id required"}), 400

            peer_id = data["peer_id"]
            host = data.get("host", request.remote_addr)
            port = data.get("port", 8401)
            capacity = data.get("capacity_bytes", 0)

            token = self.token_manager.generate_token(peer_id)

            with self._lock:
                self.peers[peer_id] = PeerRecord(
                    peer_id=peer_id, host=host, port=port,
                    capacity_bytes=capacity,
                )

            logger.info(f"Peer registered: {peer_id} at {host}:{port}")
            return jsonify({
                "status": "registered",
                "token": token,
                "peer_id": peer_id,
            })

        @app.route("/api/heartbeat", methods=["POST"])
        def heartbeat():
            """Peer heartbeat to stay alive."""
            payload, err = self._require_auth()
            if err:
                return err

            peer_id = payload["peer_id"]
            with self._lock:
                peer = self.peers.get(peer_id)
                if peer:
                    peer.last_heartbeat = time.time()
                    # Update shard list if provided
                    data = request.get_json() or {}
                    if "shard_ids" in data:
                        peer.shard_ids = data["shard_ids"]
                    return jsonify({"status": "ok"})

            return jsonify({"error": "Peer not found"}), 404

        @app.route("/api/peers", methods=["GET"])
        def list_peers():
            """List all alive peers."""
            payload, err = self._require_auth()
            if err:
                return err

            with self._lock:
                alive_peers = [
                    p.to_dict() for p in self.peers.values() if p.is_alive
                ]
            return jsonify({"peers": alive_peers})

        @app.route("/api/files/publish", methods=["POST"])
        def publish_file():
            """Publish file metadata to the network."""
            payload, err = self._require_auth()
            if err:
                return err

            data = request.get_json()
            if not data or "file_id" not in data:
                return jsonify({"error": "file metadata required"}), 400

            file_id = data["file_id"]
            peer_id = payload["peer_id"]

            with self._lock:
                self.file_registry[file_id] = data
                peer = self.peers.get(peer_id)
                if peer:
                    peer.shared_files[file_id] = data

            logger.info(f"File published: {data.get('filename', file_id)} by {peer_id}")
            return jsonify({"status": "published", "file_id": file_id})

        @app.route("/api/files", methods=["GET"])
        def list_files():
            """List all available files on the network."""
            payload, err = self._require_auth()
            if err:
                return err

            with self._lock:
                files = []
                for fid, meta in self.file_registry.items():
                    # Find all peers that have shards for this file
                    holders = set()
                    for shard in meta.get("shards", []):
                        holders.update(shard.get("holders", []))
                    files.append({
                        "file_id": fid,
                        "filename": meta.get("filename", "unknown"),
                        "total_size": meta.get("total_size", 0),
                        "shard_count": meta.get("shard_count", 0),
                        "owner": meta.get("owner_peer_id", ""),
                        "holders": list(holders),
                    })

            return jsonify({"files": files})

        @app.route("/api/files/<file_id>", methods=["GET"])
        def get_file_metadata(file_id):
            """Get full metadata for a specific file."""
            payload, err = self._require_auth()
            if err:
                return err

            with self._lock:
                meta = self.file_registry.get(file_id)
                if meta:
                    return jsonify(meta)

            return jsonify({"error": "File not found"}), 404

        @app.route("/api/files/<file_id>", methods=["DELETE"])
        def delete_file(file_id):
            """Remove a file from the registry."""
            payload, err = self._require_auth()
            if err:
                return err

            peer_id = payload["peer_id"]

            with self._lock:
                meta = self.file_registry.get(file_id)
                if not meta:
                    return jsonify({"error": "File not found"}), 404
                if meta.get("owner_peer_id") != peer_id:
                    return jsonify({"error": "Not the file owner"}), 403
                del self.file_registry[file_id]
                for peer in self.peers.values():
                    peer.shared_files.pop(file_id, None)

            return jsonify({"status": "deleted"})

        @app.route("/api/shards/locate/<shard_id>", methods=["GET"])
        def locate_shard(shard_id):
            """Find which peers hold a specific shard."""
            payload, err = self._require_auth()
            if err:
                return err

            with self._lock:
                holders = []
                for peer in self.peers.values():
                    if peer.is_alive and shard_id in peer.shard_ids:
                        holders.append(peer.to_dict())

            return jsonify({"shard_id": shard_id, "holders": holders})

        @app.route("/api/unregister", methods=["POST"])
        def unregister_peer():
            """Unregister a peer from the network."""
            payload, err = self._require_auth()
            if err:
                return err

            peer_id = payload["peer_id"]
            with self._lock:
                self.peers.pop(peer_id, None)

            logger.info(f"Peer unregistered: {peer_id}")
            return jsonify({"status": "unregistered"})

        @app.route("/api/status", methods=["GET"])
        def status():
            """Public endpoint returning server status."""
            with self._lock:
                alive = sum(1 for p in self.peers.values() if p.is_alive)
                return jsonify({
                    "status": "running",
                    "total_peers": len(self.peers),
                    "alive_peers": alive,
                    "total_files": len(self.file_registry),
                })

    def _cleanup_loop(self):
        """Periodically remove dead peers."""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                dead = [
                    pid for pid, p in self.peers.items()
                    if not p.is_alive
                ]
                for pid in dead:
                    logger.info(f"Removing dead peer: {pid}")
                    del self.peers[pid]

    def run(self, use_tls: bool = True):
        """Start the discovery server."""
        self._running = True

        cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        cleanup_thread.start()

        ssl_ctx = None
        if use_tls:
            self.cert_manager.generate_peer_cert("discovery-server")
            ssl_ctx = self.cert_manager.create_ssl_context("discovery-server", server_side=True)

        logger.info(f"Discovery server starting on {self.host}:{self.port} (TLS={use_tls})")
        self.app.run(
            host=self.host,
            port=self.port,
            ssl_context=ssl_ctx,
            threaded=True,
            use_reloader=False,
        )

    def stop(self):
        self._running = False


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    server = DiscoveryServer()
    server.run(use_tls=True)


if __name__ == "__main__":
    main()
