"""
Peer Node for WDT Distributed File Sharing.
Each peer hosts file shards, serves them to other peers, and can download
files from the network by fetching shards from multiple peers in parallel.
"""
import hashlib
import json
import logging
import os
import socket
import ssl
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

from config import (
    DOWNLOAD_DIR,
    HEARTBEAT_INTERVAL,
    MAX_CONCURRENT_TRANSFERS,
    PEER_HOST,
    PEER_PORT,
    TRANSFER_BUFFER_SIZE,
)
from protocol import (
    MessageType,
    make_error,
    make_file_meta_response,
    make_handshake,
    make_shard_request,
    make_shard_response,
    parse_handshake,
    parse_shard_request,
    parse_shard_response,
    recv_message,
    send_message,
)
from security import CertificateManager, FileEncryptor, TokenManager
from sharding import FileMetadata, ShardManager

logger = logging.getLogger("wdt.peer")

# Suppress SSL warnings for self-signed certs
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class TransferProgress:
    """Tracks progress of a file transfer."""
    file_id: str
    filename: str
    total_shards: int
    completed_shards: int = 0
    total_bytes: int = 0
    transferred_bytes: int = 0
    status: str = "pending"  # pending, downloading, complete, failed
    error: str = ""
    started_at: float = 0.0
    completed_at: float = 0.0

    @property
    def progress_pct(self) -> float:
        if self.total_shards == 0:
            return 0.0
        return (self.completed_shards / self.total_shards) * 100

    def to_dict(self) -> dict:
        return {
            "file_id": self.file_id,
            "filename": self.filename,
            "total_shards": self.total_shards,
            "completed_shards": self.completed_shards,
            "total_bytes": self.total_bytes,
            "transferred_bytes": self.transferred_bytes,
            "progress_pct": round(self.progress_pct, 1),
            "status": self.status,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


class PeerNode:
    """A peer in the distributed file sharing network."""

    def __init__(self, peer_id: str = None, host: str = PEER_HOST, port: int = PEER_PORT,
                 discovery_url: str = None, shared_dir: str = None):
        self.peer_id = peer_id or f"peer-{uuid.uuid4().hex[:8]}"
        self.host = host
        self.port = port
        self.discovery_url = discovery_url
        self.shared_dir = shared_dir or os.path.join(os.path.expanduser("~"), "wdt_shared")
        os.makedirs(self.shared_dir, exist_ok=True)

        self.shard_manager = ShardManager()
        self.cert_manager = CertificateManager()
        self.token_manager = TokenManager()

        self._auth_token: Optional[str] = None
        self._peers: Dict[str, dict] = {}
        self._transfers: Dict[str, TransferProgress] = {}
        self._lock = threading.Lock()
        self._running = False
        self._server_socket = None
        self._heartbeat_thread = None
        self._server_thread = None
        self._on_event: Optional[Callable] = None

        # Create a requests session that ignores SSL verification for self-signed certs
        self._session = requests.Session()
        self._session.verify = False

    def set_event_callback(self, callback: Callable):
        """Set callback for GUI event notifications."""
        self._on_event = callback

    def _emit_event(self, event_type: str, data: dict = None):
        if self._on_event:
            try:
                self._on_event(event_type, data or {})
            except Exception:
                pass

    # --- Discovery Server Communication ---

    def register_with_discovery(self) -> bool:
        """Register this peer with the discovery server."""
        if not self.discovery_url:
            logger.warning("No discovery URL configured")
            return False

        try:
            resp = self._session.post(
                f"{self.discovery_url}/api/register",
                json={
                    "peer_id": self.peer_id,
                    "host": self.host,
                    "port": self.port,
                    "capacity_bytes": self._get_disk_capacity(),
                },
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._auth_token = data["token"]
                logger.info(f"Registered with discovery server as {self.peer_id}")
                self._emit_event("registered", {"peer_id": self.peer_id})
                return True
            else:
                logger.error(f"Registration failed: {resp.text}")
                return False
        except requests.RequestException as e:
            logger.error(f"Failed to connect to discovery server: {e}")
            return False

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._auth_token}"} if self._auth_token else {}

    def _heartbeat_loop(self):
        """Send periodic heartbeats to the discovery server."""
        while self._running:
            try:
                if self.discovery_url and self._auth_token:
                    local_shards = self.shard_manager.get_local_shard_ids()
                    self._session.post(
                        f"{self.discovery_url}/api/heartbeat",
                        headers=self._auth_headers(),
                        json={"shard_ids": local_shards},
                        timeout=5,
                    )
            except Exception as e:
                logger.debug(f"Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def discover_peers(self) -> List[dict]:
        """Get list of alive peers from discovery server."""
        if not self.discovery_url or not self._auth_token:
            return []
        try:
            resp = self._session.get(
                f"{self.discovery_url}/api/peers",
                headers=self._auth_headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                peers = resp.json().get("peers", [])
                with self._lock:
                    self._peers = {p["peer_id"]: p for p in peers}
                return peers
        except Exception as e:
            logger.error(f"Failed to discover peers: {e}")
        return []

    def discover_files(self) -> List[dict]:
        """Get list of available files from discovery server."""
        if not self.discovery_url or not self._auth_token:
            return []
        try:
            resp = self._session.get(
                f"{self.discovery_url}/api/files",
                headers=self._auth_headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json().get("files", [])
        except Exception as e:
            logger.error(f"Failed to discover files: {e}")
        return []

    def get_remote_file_metadata(self, file_id: str) -> Optional[dict]:
        """Fetch full file metadata from the discovery server."""
        if not self.discovery_url or not self._auth_token:
            return None
        try:
            resp = self._session.get(
                f"{self.discovery_url}/api/files/{file_id}",
                headers=self._auth_headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Failed to get file metadata: {e}")
        return None

    def publish_file(self, filepath: str) -> Optional[FileMetadata]:
        """Share a file: shard it, store locally, and publish metadata to discovery."""
        if not os.path.isfile(filepath):
            logger.error(f"File not found: {filepath}")
            return None

        meta = self.shard_manager.shard_file(filepath, self.peer_id, encrypt=True)
        logger.info(
            f"File sharded: {meta.filename} -> {meta.shard_count} shards "
            f"(file_id={meta.file_id})"
        )

        if self.discovery_url and self._auth_token:
            try:
                resp = self._session.post(
                    f"{self.discovery_url}/api/files/publish",
                    headers=self._auth_headers(),
                    json=meta.to_dict(),
                    timeout=10,
                )
                if resp.status_code == 200:
                    logger.info(f"File published to network: {meta.filename}")
                    self._emit_event("file_published", {"file_id": meta.file_id,
                                                         "filename": meta.filename})
            except Exception as e:
                logger.error(f"Failed to publish file: {e}")

        return meta

    def delete_file(self, file_id: str) -> bool:
        """Delete a file from local storage and unpublish from discovery."""
        meta = self.shard_manager.get_file_metadata(file_id)
        if not meta:
            return False

        self.shard_manager.delete_file(file_id)

        if self.discovery_url and self._auth_token:
            try:
                self._session.delete(
                    f"{self.discovery_url}/api/files/{file_id}",
                    headers=self._auth_headers(),
                    timeout=10,
                )
            except Exception:
                pass

        self._emit_event("file_deleted", {"file_id": file_id})
        return True

    # --- Shard Transfer Server ---

    def _start_shard_server(self):
        """Start the TCP server for serving shards to other peers."""
        self.cert_manager.generate_peer_cert(self.peer_id)
        ssl_ctx = self.cert_manager.create_ssl_context(self.peer_id, server_side=True)

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(32)
        self._server_socket.settimeout(1.0)

        wrapped = ssl_ctx.wrap_socket(self._server_socket, server_side=True)

        logger.info(f"Shard server listening on {self.host}:{self.port}")

        while self._running:
            try:
                client_sock, addr = wrapped.accept()
                t = threading.Thread(
                    target=self._handle_client, args=(client_sock, addr), daemon=True
                )
                t.start()
            except socket.timeout:
                continue
            except ssl.SSLError:
                continue
            except Exception as e:
                if self._running:
                    logger.error(f"Server error: {e}")

    def _handle_client(self, sock, addr):
        """Handle an incoming peer connection."""
        try:
            sock.settimeout(30)

            # Expect handshake
            msg_type, payload = recv_message(sock)
            if msg_type != MessageType.HANDSHAKE:
                send_message(sock, MessageType.ERROR, make_error(1, "Expected handshake"))
                return

            hs = parse_handshake(payload)
            remote_peer_id = hs.get("peer_id", "unknown")

            # Send handshake ACK
            send_message(sock, MessageType.HANDSHAKE_ACK, json.dumps({
                "peer_id": self.peer_id, "status": "ok"
            }).encode())

            # Handle requests in a loop
            while self._running:
                try:
                    msg_type, payload = recv_message(sock)
                except ConnectionError:
                    break

                if msg_type == MessageType.SHARD_REQUEST:
                    req = parse_shard_request(payload)
                    shard_id = req["shard_id"]
                    data = self.shard_manager.get_shard_data(shard_id)
                    if data:
                        resp_payload = make_shard_response(shard_id, data)
                        send_message(sock, MessageType.SHARD_RESPONSE, resp_payload)
                    else:
                        send_message(sock, MessageType.ERROR,
                                     make_error(404, f"Shard not found: {shard_id}"))

                elif msg_type == MessageType.SHARD_PUSH:
                    header, data = parse_shard_response(payload)
                    shard_id = header["shard_id"]
                    expected_hash = header.get("sha256", "")
                    actual_hash = hashlib.sha256(data).hexdigest()
                    if expected_hash and actual_hash != expected_hash:
                        send_message(sock, MessageType.ERROR,
                                     make_error(400, "Shard integrity check failed"))
                    else:
                        self.shard_manager.store_shard(shard_id, data)
                        send_message(sock, MessageType.SHARD_ACK,
                                     json.dumps({"shard_id": shard_id, "status": "ok"}).encode())

                elif msg_type == MessageType.FILE_META_REQUEST:
                    req = json.loads(payload.decode())
                    file_id = req["file_id"]
                    meta = self.shard_manager.get_file_metadata(file_id)
                    if meta:
                        send_message(sock, MessageType.FILE_META_RESPONSE,
                                     json.dumps(meta.to_dict()).encode())
                    else:
                        send_message(sock, MessageType.ERROR,
                                     make_error(404, f"File not found: {file_id}"))
                else:
                    break

        except Exception as e:
            logger.debug(f"Client handler error: {e}")
        finally:
            try:
                sock.close()
            except Exception:
                pass

    # --- Shard Download Client ---

    def _connect_to_peer(self, peer_host: str, peer_port: int):
        """Establish a TLS connection to a remote peer and perform handshake."""
        ssl_ctx = self.cert_manager.create_ssl_context(self.peer_id, server_side=False)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(15)
        wrapped = ssl_ctx.wrap_socket(sock, server_hostname=peer_host)
        wrapped.connect((peer_host, peer_port))

        # Send handshake
        send_message(wrapped, MessageType.HANDSHAKE,
                     make_handshake(self.peer_id, self._auth_token or ""))

        # Wait for ACK
        msg_type, payload = recv_message(wrapped)
        if msg_type != MessageType.HANDSHAKE_ACK:
            wrapped.close()
            raise ConnectionError("Handshake failed")

        return wrapped

    def _download_shard_from_peer(self, shard_id: str, peer_info: dict) -> Optional[bytes]:
        """Download a single shard from a specific peer."""
        try:
            sock = self._connect_to_peer(peer_info["host"], peer_info["port"])
            send_message(sock, MessageType.SHARD_REQUEST, make_shard_request(shard_id))
            msg_type, payload = recv_message(sock)
            sock.close()

            if msg_type == MessageType.SHARD_RESPONSE:
                header, data = parse_shard_response(payload)
                return data
            else:
                logger.warning(f"Failed to download shard {shard_id} from {peer_info['peer_id']}")
                return None
        except Exception as e:
            logger.debug(f"Error downloading shard from {peer_info.get('peer_id')}: {e}")
            return None

    def download_file(self, file_id: str) -> Optional[str]:
        """
        Download a file from the network by fetching shards from multiple peers
        in parallel, then reassembling.
        """
        # Get file metadata from discovery
        meta_dict = self.get_remote_file_metadata(file_id)
        if not meta_dict:
            logger.error(f"Could not get metadata for file {file_id}")
            return None

        meta = FileMetadata.from_dict(meta_dict)
        self.shard_manager.register_file_metadata(meta)

        progress = TransferProgress(
            file_id=file_id,
            filename=meta.filename,
            total_shards=meta.shard_count,
            total_bytes=meta.total_size,
            status="downloading",
            started_at=time.time(),
        )
        with self._lock:
            self._transfers[file_id] = progress
        self._emit_event("transfer_started", progress.to_dict())

        # Get available peers
        peers = self.discover_peers()
        peer_map = {p["peer_id"]: p for p in peers}

        # Download shards in parallel
        def download_shard(shard):
            if self.shard_manager.has_shard(shard.shard_id):
                return shard.shard_id, True

            for holder_id in shard.holders:
                peer_info = peer_map.get(holder_id)
                if not peer_info:
                    continue
                data = self._download_shard_from_peer(shard.shard_id, peer_info)
                if data:
                    self.shard_manager.store_shard(shard.shard_id, data)
                    return shard.shard_id, True
            return shard.shard_id, False

        success = True
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TRANSFERS) as pool:
            futures = {
                pool.submit(download_shard, shard): shard
                for shard in meta.shards
            }
            for future in as_completed(futures):
                shard_id, ok = future.result()
                if ok:
                    with self._lock:
                        progress.completed_shards += 1
                        shard = futures[future]
                        progress.transferred_bytes += shard.size
                    self._emit_event("transfer_progress", progress.to_dict())
                else:
                    success = False
                    logger.error(f"Failed to download shard: {shard_id}")

        if not success:
            with self._lock:
                progress.status = "failed"
                progress.error = "Some shards could not be downloaded"
            self._emit_event("transfer_failed", progress.to_dict())
            return None

        # Reassemble file
        try:
            output_path = self.shard_manager.reassemble_file(file_id)
            with self._lock:
                progress.status = "complete"
                progress.completed_at = time.time()
            self._emit_event("transfer_complete", progress.to_dict())
            logger.info(f"File downloaded: {meta.filename} -> {output_path}")
            return output_path
        except Exception as e:
            with self._lock:
                progress.status = "failed"
                progress.error = str(e)
            self._emit_event("transfer_failed", progress.to_dict())
            logger.error(f"Failed to reassemble file: {e}")
            return None

    # --- Lifecycle ---

    def start(self):
        """Start the peer node (shard server + heartbeat)."""
        self._running = True

        self._server_thread = threading.Thread(target=self._start_shard_server, daemon=True)
        self._server_thread.start()

        if self.discovery_url:
            self.register_with_discovery()
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()

        logger.info(f"Peer node started: {self.peer_id}")

    def stop(self):
        """Stop the peer node."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
        if self.discovery_url and self._auth_token:
            try:
                self._session.post(
                    f"{self.discovery_url}/api/unregister",
                    headers=self._auth_headers(),
                    timeout=5,
                )
            except Exception:
                pass
        logger.info(f"Peer node stopped: {self.peer_id}")

    def get_transfers(self) -> List[dict]:
        with self._lock:
            return [t.to_dict() for t in self._transfers.values()]

    def get_local_files(self) -> List[dict]:
        files = self.shard_manager.list_files()
        return [f.to_dict() for f in files]

    def get_peers(self) -> List[dict]:
        with self._lock:
            return list(self._peers.values())

    def _get_disk_capacity(self) -> int:
        try:
            st = os.statvfs(self.shared_dir)
            return st.f_bavail * st.f_frsize
        except Exception:
            return 0
