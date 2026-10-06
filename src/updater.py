import argparse, os, shutil, subprocess, sys, time, zipfile
import requests
from config import hash_file, hash_folder
import tkinter as tk
from tkinter import ttk

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--install-dir", required=True)
    p.add_argument("--manifest-url", required=True)
    p.add_argument("--exe-url", required=True)
    p.add_argument("--runtime-url", required=True)
    p.add_argument("--relaunch", required=True)
    return p.parse_args()

def download(url, dest_path, on_progress):
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                on_progress(downloaded / total * 100)

def main():
    args = parse_args()
    time.sleep(1)  # let the main app release its file locks

    root = tk.Tk()
    root.title("Updating OWTV Stats tracker")
    root.geometry("400x100")
    root.resizable(False, False)
    label = tk.Label(root, text="Checking for updates...")
    label.pack(pady=10)
    progress = ttk.Progressbar(root, length=350, mode="determinate", maximum=100)
    progress.pack(pady=10)
    root.update()

    manifest = requests.get(args.manifest_url, timeout=10).json()

    exe_path = os.path.join(args.install_dir, "OWTVstats.exe")
    internal_path = os.path.join(args.install_dir, "_internal")

    exe_changed = not os.path.exists(exe_path) or hash_file(exe_path) != manifest["exe_hash"]
    runtime_changed = not os.path.exists(internal_path) or hash_folder(internal_path) != manifest["runtime_hash"]

    def report(pct):
        progress["value"] = pct
        root.update()

    if not exe_changed and not runtime_changed:
        label.config(text="Already up to date.")
        root.update()
        time.sleep(1)
    else:
        if exe_changed:
            label.config(text="Downloading app update...")
            root.update()
            tmp_exe = exe_path + ".new"
            download(args.exe_url, tmp_exe, report)
            os.replace(tmp_exe, exe_path)

        if runtime_changed:
            label.config(text="Downloading dependencies (larger download)...")
            progress["value"] = 0
            root.update()
            tmp_zip = os.path.join(args.install_dir, "runtime_update.zip")
            download(args.runtime_url, tmp_zip, report)

            label.config(text="Installing...")
            root.update()
            if os.path.exists(internal_path):
                shutil.rmtree(internal_path)
            with zipfile.ZipFile(tmp_zip) as zf:
                zf.extractall(internal_path)
            os.remove(tmp_zip)

    root.destroy()
    subprocess.Popen([args.relaunch])

if __name__ == "__main__":
    main()