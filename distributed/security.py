"""
Security module for WDT Distributed File Sharing.
Handles encryption, authentication, token generation, and TLS certificate management.
"""
import hashlib
import hmac
import json
import os
import secrets
import ssl
import time
from typing import Optional, Tuple

from cryptography import x509
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.x509.oid import NameOID

from config import CERT_DIR, SECRET_KEY, TOKEN_EXPIRY_SECONDS

import datetime


class TokenManager:
    """Generates and validates authentication tokens for peers."""

    def __init__(self, secret_key: str = SECRET_KEY):
        self._secret = secret_key.encode() if isinstance(secret_key, str) else secret_key

    def generate_token(self, peer_id: str) -> str:
        """Generate a signed token for a peer."""
        payload = {
            "peer_id": peer_id,
            "issued_at": time.time(),
            "nonce": secrets.token_hex(16),
        }
        data = json.dumps(payload, sort_keys=True).encode()
        sig = hmac.new(self._secret, data, hashlib.sha256).hexdigest()
        token_data = {"payload": payload, "signature": sig}
        return Fernet(self._derive_fernet_key()).encrypt(
            json.dumps(token_data).encode()
        ).decode()

    def validate_token(self, token: str) -> Optional[dict]:
        """Validate a token and return the payload if valid."""
        try:
            decrypted = Fernet(self._derive_fernet_key()).decrypt(token.encode())
            token_data = json.loads(decrypted)
            payload = token_data["payload"]
            sig = token_data["signature"]

            data = json.dumps(payload, sort_keys=True).encode()
            expected_sig = hmac.new(self._secret, data, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(sig, expected_sig):
                return None

            if time.time() - payload["issued_at"] > TOKEN_EXPIRY_SECONDS:
                return None

            return payload
        except Exception:
            return None

    def _derive_fernet_key(self) -> bytes:
        """Derive a Fernet-compatible key from the secret."""
        import base64
        dk = hashlib.sha256(self._secret).digest()
        return base64.urlsafe_b64encode(dk)


class FileEncryptor:
    """Handles file/shard encryption using AES-256-GCM."""

    @staticmethod
    def generate_key() -> bytes:
        """Generate a random 256-bit encryption key."""
        return AESGCM.generate_key(bit_length=256)

    @staticmethod
    def encrypt_data(data: bytes, key: bytes) -> bytes:
        """Encrypt data using AES-256-GCM. Returns nonce + ciphertext."""
        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, data, None)
        return nonce + ciphertext

    @staticmethod
    def decrypt_data(encrypted: bytes, key: bytes) -> bytes:
        """Decrypt data encrypted with AES-256-GCM."""
        nonce = encrypted[:12]
        ciphertext = encrypted[12:]
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(nonce, ciphertext, None)

    @staticmethod
    def encrypt_file(filepath: str, key: bytes, output_path: str):
        """Encrypt a file and write to output_path."""
        with open(filepath, "rb") as f:
            data = f.read()
        encrypted = FileEncryptor.encrypt_data(data, key)
        with open(output_path, "wb") as f:
            f.write(encrypted)

    @staticmethod
    def decrypt_file(filepath: str, key: bytes, output_path: str):
        """Decrypt a file and write to output_path."""
        with open(filepath, "rb") as f:
            encrypted = f.read()
        data = FileEncryptor.decrypt_data(encrypted, key)
        with open(output_path, "wb") as f:
            f.write(data)


class CertificateManager:
    """Manages TLS certificates for secure peer communication."""

    def __init__(self, cert_dir: str = CERT_DIR):
        self.cert_dir = cert_dir
        os.makedirs(cert_dir, exist_ok=True)

    @property
    def ca_key_path(self) -> str:
        return os.path.join(self.cert_dir, "ca.key")

    @property
    def ca_cert_path(self) -> str:
        return os.path.join(self.cert_dir, "ca.crt")

    def generate_ca(self) -> Tuple[str, str]:
        """Generate a self-signed CA certificate."""
        key = ec.generate_private_key(ec.SECP384R1())

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "WDT Distributed CA"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "WDT Network"),
        ])

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650)
            )
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=None),
                critical=True,
            )
            .sign(key, hashes.SHA384())
        )

        with open(self.ca_key_path, "wb") as f:
            f.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ))
        os.chmod(self.ca_key_path, 0o600)

        with open(self.ca_cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

        return self.ca_cert_path, self.ca_key_path

    def generate_peer_cert(self, peer_id: str, hostnames: list = None) -> Tuple[str, str]:
        """Generate a peer certificate signed by the CA."""
        if not os.path.exists(self.ca_key_path):
            self.generate_ca()

        with open(self.ca_key_path, "rb") as f:
            ca_key = serialization.load_pem_private_key(f.read(), password=None)
        with open(self.ca_cert_path, "rb") as f:
            ca_cert = x509.load_pem_x509_certificate(f.read())

        peer_key = ec.generate_private_key(ec.SECP384R1())

        subject = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, peer_id),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "WDT Network"),
        ])

        san_names = [x509.DNSName("localhost"), x509.IPAddress(
            __import__("ipaddress").IPv4Address("127.0.0.1")
        )]
        if hostnames:
            import ipaddress
            for h in hostnames:
                try:
                    san_names.append(x509.IPAddress(ipaddress.ip_address(h)))
                except ValueError:
                    san_names.append(x509.DNSName(h))

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_cert.subject)
            .public_key(peer_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365)
            )
            .add_extension(
                x509.SubjectAlternativeName(san_names),
                critical=False,
            )
            .sign(ca_key, hashes.SHA384())
        )

        key_path = os.path.join(self.cert_dir, f"{peer_id}.key")
        cert_path = os.path.join(self.cert_dir, f"{peer_id}.crt")

        with open(key_path, "wb") as f:
            f.write(peer_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ))
        os.chmod(key_path, 0o600)

        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

        return cert_path, key_path

    def create_ssl_context(self, peer_id: str, server_side: bool = False) -> ssl.SSLContext:
        """Create an SSL context for secure communication."""
        cert_path = os.path.join(self.cert_dir, f"{peer_id}.crt")
        key_path = os.path.join(self.cert_dir, f"{peer_id}.key")

        if not os.path.exists(cert_path):
            self.generate_peer_cert(peer_id)

        if server_side:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        else:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        ctx.load_cert_chain(cert_path, key_path)

        if os.path.exists(self.ca_cert_path):
            ctx.load_verify_locations(self.ca_cert_path)

        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_OPTIONAL
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3

        return ctx
