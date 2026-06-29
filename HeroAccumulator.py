from collections import defaultdict, deque
from copy import deepcopy
from typing import Optional, Dict


class Snapshot:
    """
    Represents the scoreboard state extracted for a player from a single timestep.
    """
    def __init__(self, game_time: int, hero: str, eliminations: int, assists: int, deaths: int, damage: int,
                 healing: int, mitigated: int) -> None:
        self.game_time = game_time
        self.hero = hero
        self.eliminations = eliminations
        self.assists = assists
        self.deaths = deaths
        self.damage = damage
        self.healing = healing
        self.mitigated = mitigated


class HeroAccumulator:
    """
    Manages and tallies performance statistics per individual player across hero swaps.
    When a player swaps hero the statistics for the previously played hero are calculated and attributed.
    """
    def __init__(self) -> None:
        self.current_hero= None # Active character on the scoreboard
        self.last_snapshot= None # Previous valid Snapshot to calculate deltas
        self.player_id = None # Database player_id
        self.role = "none"

        # Nested mapping tracks cumulative metrics grouped under character names as keys
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

        self.last_stats = {
            "eliminations": 0,
            "assists": 0,
            "deaths": 0,
            "damage": 0,
            "healing": 0,
            "mitigated": 0,
        }

    def ingest(self, snap: Snapshot) -> None:
        """
        Processes a newly parsed Snapshot entry and computes differences against the previous Snapshot.
        """
        # Baseline setup on start
        if self.last_snapshot is None:
            self.current_hero = snap.hero
            self.last_snapshot = snap
            return

        # Compute deltas between Snapshots
        dt = int(snap.game_time) - int(self.last_snapshot.game_time)
        # Negative numbers are prevented to safeguard against any OCR errors
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

    def finalize(self)-> Dict[str, Dict[str, int]]:
        """
        Call at end of map to finish tracking.
        Returns an isolated, deep copy of gathered aggregates ready for SQL insertion.
        """
        return deepcopy(self.hero_totals)

    # Standard setters and getters for attributes
    def set_player_id(self, player_id: int) -> None:
        self.player_id = player_id

    def get_player_id(self) -> Optional[int]:
        return self.player_id

    def get_current_hero(self) -> Optional[str]:
        return self.current_hero

    def set_role(self, role: str) -> None:
        self.role = role

    def get_role(self) -> str:
        return self.role