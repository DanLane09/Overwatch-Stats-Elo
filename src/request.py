import time
import requests

WINDOW = 60

def api_request(method, url, max_retries=5, **kwargs):
    kwargs.setdefault("timeout", 10)
    for attempt in range(max_retries + 1):
        r = requests.request(method, url, **kwargs)

        remaining = r.headers.get("x-ratelimit-remaining", "")
        reset = r.headers.get("x-ratelimit-reset", "")
        wait = 0
        if reset.isdigit():
            wait = min(max(int(reset) - time.time(), 0) + 1, WINDOW + 1)

        if r.status_code == 429:
            time.sleep(wait or min(2 ** attempt, WINDOW))
            continue

        if remaining.isdigit() and int(remaining) <= 5:
            time.sleep(wait)   # budget nearly spent, so wait for the window to reset

        return r
    return r