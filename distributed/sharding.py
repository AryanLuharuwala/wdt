"""
File sharding engine for WDT Distributed File Sharing.
Splits files into fixed-size shards, tracks metadata, and reassembles files.
"""
import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

from config import DOWNLOAD_DIR, META_DIR, SHARD_DIR, SHARD_SIZE
from security import FileEncryptor


@dataclass
class ShardInfo:
    """Metadata for a single shard."""
    shard_id: str
    file_id: str
    index: int
    size: int
    sha256: str
    holders: List[str] = field(default_factory=list)  # peer_ids that hold this shard
    encrypted: bool = False


@dataclass
class FileMetadata:
    """Metadata for a shared file."""
    file_id: str
    filename: str
    total_size: int
    sha256: str
    shard_count: int
    shard_size: int
    shards: List[ShardInfo] = field(default_factory=list)
    owner_peer_id: str = ""
    encryption_key_hex: str = ""  # hex-encoded AES key
    created_at: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["shards"] = [asdict(s) for s in self.shards]
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "FileMetadata":
        shards = [ShardInfo(**s) for s in data.pop("shards", [])]
        meta = cls(**data)
        meta.shards = shards
        return meta


class ShardManager:
    """Manages file sharding, storage, and reassembly."""

    def __init__(self, shard_dir: str = SHARD_DIR, meta_dir: str = META_DIR,
                 download_dir: str = DOWNLOAD_DIR, shard_size: int = SHARD_SIZE):
        self.shard_dir = shard_dir
        self.meta_dir = meta_dir
        self.download_dir = download_dir
        self.shard_size = shard_size
        os.makedirs(shard_dir, exist_ok=True)
        os.makedirs(meta_dir, exist_ok=True)
        os.makedirs(download_dir, exist_ok=True)
        self._file_metadata: Dict[str, FileMetadata] = {}
        self._load_metadata()

    def _load_metadata(self):
        """Load all file metadata from disk."""
        if not os.path.exists(self.meta_dir):
            return
        for fname in os.listdir(self.meta_dir):
            if fname.endswith(".json"):
                path = os.path.join(self.meta_dir, fname)
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                    meta = FileMetadata.from_dict(data)
                    self._file_metadata[meta.file_id] = meta
                except (json.JSONDecodeError, TypeError, KeyError):
                    continue

    def _save_metadata(self, meta: FileMetadata):
        """Persist file metadata to disk."""
        path = os.path.join(self.meta_dir, f"{meta.file_id}.json")
        with open(path, "w") as f:
            json.dump(meta.to_dict(), f, indent=2)

    def shard_file(self, filepath: str, peer_id: str, encrypt: bool = True) -> FileMetadata:
        """Split a file into shards and return metadata."""
        import time

        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        file_id = str(uuid.uuid4())
        filename = os.path.basename(filepath)
        total_size = os.path.getsize(filepath)

        # Calculate file hash
        file_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                file_hash.update(chunk)

        # Generate encryption key if needed
        enc_key = FileEncryptor.generate_key() if encrypt else None

        shards = []
        shard_index = 0

        with open(filepath, "rb") as f:
            while True:
                data = f.read(self.shard_size)
                if not data:
                    break

                shard_id = str(uuid.uuid4())
                shard_hash = hashlib.sha256(data).hexdigest()

                # Encrypt shard if needed
                if enc_key:
                    data = FileEncryptor.encrypt_data(data, enc_key)

                # Write shard to disk
                shard_path = os.path.join(self.shard_dir, shard_id)
                with open(shard_path, "wb") as sf:
                    sf.write(data)

                shard_info = ShardInfo(
                    shard_id=shard_id,
                    file_id=file_id,
                    index=shard_index,
                    size=len(data),
                    sha256=shard_hash,
                    holders=[peer_id],
                    encrypted=encrypt,
                )
                shards.append(shard_info)
                shard_index += 1

        meta = FileMetadata(
            file_id=file_id,
            filename=filename,
            total_size=total_size,
            sha256=file_hash.hexdigest(),
            shard_count=shard_index,
            shard_size=self.shard_size,
            shards=shards,
            owner_peer_id=peer_id,
            encryption_key_hex=enc_key.hex() if enc_key else "",
            created_at=time.time(),
        )

        self._file_metadata[file_id] = meta
        self._save_metadata(meta)
        return meta

    def reassemble_file(self, file_id: str, output_dir: str = None) -> str:
        """Reassemble a file from its shards. Returns the output file path."""
        meta = self._file_metadata.get(file_id)
        if not meta:
            raise ValueError(f"Unknown file_id: {file_id}")

        out_dir = output_dir or self.download_dir
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, meta.filename)

        enc_key = bytes.fromhex(meta.encryption_key_hex) if meta.encryption_key_hex else None

        sorted_shards = sorted(meta.shards, key=lambda s: s.index)

        with open(output_path, "wb") as outf:
            for shard in sorted_shards:
                shard_path = os.path.join(self.shard_dir, shard.shard_id)
                if not os.path.exists(shard_path):
                    raise FileNotFoundError(f"Missing shard: {shard.shard_id}")

                with open(shard_path, "rb") as sf:
                    data = sf.read()

                if enc_key and shard.encrypted:
                    data = FileEncryptor.decrypt_data(data, enc_key)

                outf.write(data)

        # Verify file integrity
        file_hash = hashlib.sha256()
        with open(output_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                file_hash.update(chunk)

        if file_hash.hexdigest() != meta.sha256:
            os.remove(output_path)
            raise ValueError("File integrity check failed: SHA-256 mismatch")

        return output_path

    def get_shard_data(self, shard_id: str) -> Optional[bytes]:
        """Read shard data from disk."""
        shard_path = os.path.join(self.shard_dir, shard_id)
        if not os.path.exists(shard_path):
            return None
        with open(shard_path, "rb") as f:
            return f.read()

    def store_shard(self, shard_id: str, data: bytes):
        """Store shard data to disk."""
        shard_path = os.path.join(self.shard_dir, shard_id)
        with open(shard_path, "wb") as f:
            f.write(data)

    def has_shard(self, shard_id: str) -> bool:
        """Check if a shard exists locally."""
        return os.path.exists(os.path.join(self.shard_dir, shard_id))

    def get_file_metadata(self, file_id: str) -> Optional[FileMetadata]:
        return self._file_metadata.get(file_id)

    def list_files(self) -> List[FileMetadata]:
        return list(self._file_metadata.values())

    def register_file_metadata(self, meta: FileMetadata):
        """Register metadata received from another peer."""
        self._file_metadata[meta.file_id] = meta
        self._save_metadata(meta)

    def get_local_shard_ids(self) -> List[str]:
        """Return IDs of all locally stored shards."""
        if not os.path.exists(self.shard_dir):
            return []
        return [f for f in os.listdir(self.shard_dir) if not f.startswith(".")]

    def delete_file(self, file_id: str):
        """Delete a file and all its shards."""
        meta = self._file_metadata.get(file_id)
        if not meta:
            return
        for shard in meta.shards:
            shard_path = os.path.join(self.shard_dir, shard.shard_id)
            if os.path.exists(shard_path):
                os.remove(shard_path)
        meta_path = os.path.join(self.meta_dir, f"{file_id}.json")
        if os.path.exists(meta_path):
            os.remove(meta_path)
        del self._file_metadata[file_id]
