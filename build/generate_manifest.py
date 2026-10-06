import hashlib, json, os

OUTPUT_DIR = "dist/OverwatchStatsElo"
MANIFEST_PATH = "dist/manifest.json"

def hash_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def hash_folder(path):
    h = hashlib.sha256()
    for root, _, files in sorted(os.walk(path)):
        for f in sorted(files):
            h.update(hash_file(os.path.join(root, f)).encode())
    return h.hexdigest()

manifest = {
    "exe_hash": hash_file(os.path.join(OUTPUT_DIR, "OWTVstats.exe")),
    "runtime_hash": hash_folder(os.path.join(OUTPUT_DIR, "_internal")),
}

with open(MANIFEST_PATH, "w") as f:
    json.dump(manifest, f, indent=2)