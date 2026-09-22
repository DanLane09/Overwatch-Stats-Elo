import argparse, hashlib, os, shutil, subprocess, sys, time
import requests
from remotezip import RemoteZip
import tkinter as tk
from tkinter import ttk

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--install-dir", required=True)
    p.add_argument("--manifest-url", required=True)
    p.add_argument("--zip-url", required=True)
    p.add_argument("--relaunch", required=True)
    return p.parse_args()

def hash_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_changed_files(install_dir, manifest):
    changed = []
    for rel_path, remote_hash in manifest.items():
        local_path = os.path.join(install_dir, rel_path)
        if not os.path.exists(local_path) or hash_file(local_path) != remote_hash:
            changed.append(rel_path)
    return changed

def main():
    args = parse_args()
    time.sleep(1)  # give the main app a moment to fully exit and release file locks

    root = tk.Tk()
    root.title("Updating OWTV Stats tracker")
    root.geometry("400x100")
    root.resizable(False, False)
    label = tk.Label(root, text="Checking for changes...")
    label.pack(pady=10)
    progress = ttk.Progressbar(root, length=350, mode="determinate")
    progress.pack(pady=10)
    root.update()

    manifest = requests.get(args.manifest_url, timeout=10).json()
    changed_files = get_changed_files(args.install_dir, manifest)
    total = len(changed_files)

    if total == 0:
        label.config(text="Already up to date.")
        root.update()
        time.sleep(1)
    else:
        progress["maximum"] = total
        with RemoteZip(args.zip_url) as zf:
            for i, rel_path in enumerate(changed_files, start=1):
                label.config(text=f"Updating file {i} of {total}...")
                progress["value"] = i
                root.update()
                zf.extract(rel_path, args.install_dir)

    root.destroy()
    subprocess.Popen([args.relaunch])

if __name__ == "__main__":
    main()