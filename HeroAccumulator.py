from collections import defaultdict, deque
from copy import deepcopy

class Snapshot:
    def __init__(self, game_time, hero, eliminations, assists, deaths, damage, healing, mitigated):
        self.game_time = game_time
        self.hero = hero
        self.eliminations = eliminations
        self.assists = assists
        self.deaths = deaths
        self.damage = damage
        self.healing = healing
        self.mitigated = mitigated


class HeroAccumulator:
    def __init__(self):
        self.current_hero = None
        self.last_snapshot = None
        self.player_id = None
        self.role = None

        # final per-hero totals (this maps 1:1 to DB rows)
        self.hero_totals = defaultdict(lambda: {
            "seconds": 0,
            "eliminations": 0,
            "assists": 0,
            "deaths": 0,
            "damage": 0,
            "healing": 0,
            "mitigated": 0,
        })

        self.delta_history = deque(maxlen=5)
        self.snapshot_time_history = deque(maxlen=2)
        self.spike_multiplier = 5
        self.fallback_delta = 2

    def ingest(self, snap: Snapshot):
        """Process a new CV snapshot."""
        # First frame
        if self.last_snapshot is None:
            self.current_hero = snap.hero
            self.last_snapshot = snap
            return

        # Compute deltas
        dt = int(snap.game_time) - int(self.last_snapshot.game_time)

        deltas = {
            "eliminations": max(0, int(snap.eliminations) - int(self.last_snapshot.eliminations)),
            "assists": max(0, int(snap.assists) - int(self.last_snapshot.assists)),
            "deaths": max(0, int(snap.deaths) - int(self.last_snapshot.deaths)),
            "damage": max(0, int(snap.damage) - int(self.last_snapshot.damage)),
            "healing": max(0, int(snap.healing) - int(self.last_snapshot.healing)),
            "mitigated": max(0, int(snap.mitigated) - int(self.last_snapshot.mitigated)),
        }

        # Attribute deltas to the PREVIOUS hero
        hero_bucket = self.hero_totals[self.current_hero]
        hero_bucket["seconds"] += dt
        for k, v in deltas.items():
            hero_bucket[k] += v

        # Handle hero change AFTER attribution
        if snap.hero != self.current_hero:
            self.current_hero = snap.hero

        self.last_snapshot = snap

    def finalize(self):
        """Call at end of map. Returns DB-ready rows."""
        return deepcopy(self.hero_totals)

    def set_player_id(self, player_id):
        self.player_id = player_id

    def get_player_id(self):
        return self.player_id

    def get_current_hero(self):
        return self.current_hero

    def set_role(self, role):
        self.role = role

    def get_role(self):
        return self.role