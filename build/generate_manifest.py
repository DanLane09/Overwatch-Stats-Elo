# build/generate_manifest.py
import hashlib, json, os

OUTPUT_DIR = "dist/OverwatchStatsElo"
MANIFEST_PATH = "dist/manifest.json"

manifest = {}
for root, _, files in os.walk(OUTPUT_DIR):
    for f in files:
        full_path = os.path.join(root, f)
        rel_path = os.path.relpath(full_path, OUTPUT_DIR).replace("\\", "/")
        with open(full_path, "rb") as fh:
            manifest[rel_path] = hashlib.sha256(fh.read()).hexdigest()

with open(MANIFEST_PATH, "w") as f:
    json.dump(manifest, f, indent=2)