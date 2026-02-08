"""
Web GUI for WDT Distributed File Sharing.
Flask application providing a dashboard to manage files, peers, and transfers.
"""
import logging
import os
import tempfile
import threading

from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

logger = logging.getLogger("wdt.gui")

# The peer node is injected at startup
_peer_node = None


def create_app(peer_node) -> Flask:
    """Create and configure the Flask GUI application."""
    global _peer_node
    _peer_node = peer_node

    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )
    app.secret_key = os.urandom(24)

    @app.route("/")
    def dashboard():
        """Main dashboard showing overview."""
        local_files = _peer_node.get_local_files()
        peers = _peer_node.get_peers()
        transfers = _peer_node.get_transfers()

        stats = {
            "peer_id": _peer_node.peer_id,
            "total_files": len(local_files),
            "total_peers": len(peers),
            "active_transfers": sum(
                1 for t in transfers if t["status"] == "downloading"
            ),
            "total_shared_size": sum(f.get("total_size", 0) for f in local_files),
        }
        return render_template("dashboard.html", stats=stats, transfers=transfers)

    @app.route("/files")
    def files_page():
        """File management page."""
        local_files = _peer_node.get_local_files()
        return render_template("files.html", local_files=local_files)

    @app.route("/network")
    def network_page():
        """Network files available for download."""
        network_files = _peer_node.discover_files()
        local_file_ids = {f["file_id"] for f in _peer_node.get_local_files()}
        # Mark files we already have
        for f in network_files:
            f["have_locally"] = f["file_id"] in local_file_ids
        return render_template("network.html", network_files=network_files)

    @app.route("/peers")
    def peers_page():
        """Peer management page."""
        peers = _peer_node.discover_peers()
        return render_template("peers.html", peers=peers, my_peer_id=_peer_node.peer_id)

    @app.route("/transfers")
    def transfers_page():
        """Transfer monitoring page."""
        transfers = _peer_node.get_transfers()
        return render_template("transfers.html", transfers=transfers)

    # --- API Endpoints ---

    @app.route("/api/upload", methods=["POST"])
    def upload_file():
        """Upload and share a file."""
        if "file" not in request.files:
            flash("No file selected", "error")
            return redirect(url_for("files_page"))

        file = request.files["file"]
        if file.filename == "":
            flash("No file selected", "error")
            return redirect(url_for("files_page"))

        filename = secure_filename(file.filename)
        upload_dir = os.path.join(_peer_node.shared_dir, "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)

        meta = _peer_node.publish_file(filepath)
        if meta:
            flash(f"File shared: {filename} ({meta.shard_count} shards)", "success")
        else:
            flash(f"Failed to share file: {filename}", "error")

        return redirect(url_for("files_page"))

    @app.route("/api/download/<file_id>", methods=["POST"])
    def download_file(file_id):
        """Download a file from the network."""
        def do_download():
            _peer_node.download_file(file_id)

        thread = threading.Thread(target=do_download, daemon=True)
        thread.start()

        flash("Download started. Check the transfers page for progress.", "info")
        return redirect(url_for("transfers_page"))

    @app.route("/api/delete/<file_id>", methods=["POST"])
    def delete_file(file_id):
        """Delete a local file and its shards."""
        if _peer_node.delete_file(file_id):
            flash("File deleted", "success")
        else:
            flash("File not found", "error")
        return redirect(url_for("files_page"))

    @app.route("/api/retrieve/<file_id>", methods=["GET"])
    def retrieve_file(file_id):
        """Retrieve/download a reassembled file to browser."""
        try:
            output_path = _peer_node.shard_manager.reassemble_file(file_id)
            return send_file(output_path, as_attachment=True)
        except Exception as e:
            flash(f"Failed to retrieve file: {e}", "error")
            return redirect(url_for("files_page"))

    # --- JSON API for AJAX ---

    @app.route("/api/status")
    def api_status():
        local_files = _peer_node.get_local_files()
        peers = _peer_node.get_peers()
        transfers = _peer_node.get_transfers()
        return jsonify({
            "peer_id": _peer_node.peer_id,
            "files": len(local_files),
            "peers": len(peers),
            "transfers": transfers,
            "active_transfers": sum(
                1 for t in transfers if t["status"] == "downloading"
            ),
        })

    @app.route("/api/peers")
    def api_peers():
        return jsonify({"peers": _peer_node.discover_peers()})

    @app.route("/api/files")
    def api_files():
        return jsonify({"files": _peer_node.get_local_files()})

    @app.route("/api/network_files")
    def api_network_files():
        return jsonify({"files": _peer_node.discover_files()})

    @app.route("/api/transfers")
    def api_transfers():
        return jsonify({"transfers": _peer_node.get_transfers()})

    @app.template_filter("filesizeformat")
    def filesizeformat(value):
        """Format a file size as human-readable string."""
        try:
            value = int(value)
        except (TypeError, ValueError):
            return "0 B"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(value) < 1024.0:
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{value:.1f} PB"

    @app.template_filter("timeago")
    def timeago(value):
        """Format a timestamp as relative time."""
        import time
        try:
            diff = time.time() - float(value)
        except (TypeError, ValueError):
            return "unknown"
        if diff < 60:
            return f"{int(diff)}s ago"
        if diff < 3600:
            return f"{int(diff / 60)}m ago"
        if diff < 86400:
            return f"{int(diff / 3600)}h ago"
        return f"{int(diff / 86400)}d ago"

    return app
