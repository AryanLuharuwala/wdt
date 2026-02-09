#!/usr/bin/env python3
"""
WDT Distributed File Sharing - Main Entry Point.

Usage:
    # Start a discovery server:
    python run.py discovery [--host 0.0.0.0] [--port 8400] [--no-tls]

    # Start a peer node with GUI:
    python run.py peer [--discovery-url https://localhost:8400] [--port 8401]
                       [--gui-port 8402] [--peer-id my-peer] [--shared-dir ./shared]

    # Start just the GUI (connecting to an existing peer):
    python run.py gui [--gui-port 8402]
"""
import argparse
import logging
import os
import signal
import sys
import threading

# Add the distributed directory to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DISCOVERY_HOST, DISCOVERY_PORT, GUI_HOST, GUI_PORT, PEER_HOST, PEER_PORT


def run_discovery(args):
    """Run the discovery server."""
    from discovery_server import DiscoveryServer

    server = DiscoveryServer(
        host=args.host,
        port=args.port,
        secret_key=args.secret_key or os.urandom(32).hex(),
    )

    def shutdown(sig, frame):
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Starting WDT Discovery Server on {args.host}:{args.port}")
    print(f"TLS: {'enabled' if not args.no_tls else 'disabled'}")
    server.run(use_tls=not args.no_tls)


def run_peer(args):
    """Run a peer node with the web GUI."""
    from gui.app import create_app
    from peer import PeerNode

    peer = PeerNode(
        peer_id=args.peer_id,
        host=args.host,
        port=args.port,
        discovery_url=args.discovery_url,
        shared_dir=args.shared_dir,
    )

    def shutdown(sig, frame):
        print("\nShutting down...")
        peer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Start the peer node
    peer.start()

    # Create and start the GUI
    app = create_app(peer)

    print(f"\n{'=' * 56}")
    print(f"  WDT Distributed File Sharing")
    print(f"{'=' * 56}")
    print(f"  Peer ID:    {peer.peer_id}")
    print(f"  Shard Port: {args.host}:{args.port}")
    print(f"  GUI:        http://{args.gui_host}:{args.gui_port}")
    if args.discovery_url:
        print(f"  Discovery:  {args.discovery_url}")
    print(f"  Shared Dir: {args.shared_dir or peer.shared_dir}")
    print(f"{'=' * 56}\n")

    app.run(
        host=args.gui_host,
        port=args.gui_port,
        debug=False,
        use_reloader=False,
    )


def main():
    parser = argparse.ArgumentParser(
        description="WDT Distributed File Sharing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", help="Component to run")

    # Discovery server subcommand
    disc_parser = subparsers.add_parser("discovery", help="Start a discovery server")
    disc_parser.add_argument("--host", default=DISCOVERY_HOST, help="Bind address")
    disc_parser.add_argument("--port", type=int, default=DISCOVERY_PORT, help="Listen port")
    disc_parser.add_argument("--no-tls", action="store_true", help="Disable TLS")
    disc_parser.add_argument("--secret-key", default=None, help="Shared secret for tokens")

    # Peer node subcommand
    peer_parser = subparsers.add_parser("peer", help="Start a peer node with GUI")
    peer_parser.add_argument("--peer-id", default=None, help="Unique peer identifier")
    peer_parser.add_argument("--host", default=PEER_HOST, help="Bind address for shard server")
    peer_parser.add_argument("--port", type=int, default=PEER_PORT, help="Shard server port")
    peer_parser.add_argument("--gui-host", default=GUI_HOST, help="GUI bind address")
    peer_parser.add_argument("--gui-port", type=int, default=GUI_PORT, help="GUI port")
    peer_parser.add_argument("--discovery-url", default=None,
                             help="Discovery server URL (e.g., https://localhost:8400)")
    peer_parser.add_argument("--shared-dir", default=None, help="Directory for shared files")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    if args.command == "discovery":
        run_discovery(args)
    elif args.command == "peer":
        run_peer(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
