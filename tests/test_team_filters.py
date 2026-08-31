import json
import sqlite3
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))

from db import preferences as preferences_dal  # noqa: E402
from filter_integration import event_passes_team_rules, should_include_event  # noqa: E402
from fruit_build_adb_lanes import should_include_event as adb_should_include_event  # noqa: E402
from server.services.filters import _build_active_leagues, _build_active_teams  # noqa: E402


def _event(competitors, sport="Baseball", league="MLB", title="Test game"):
    return {
        "title": title,
        "genres_json": json.dumps([sport]),
        "classification_json": json.dumps([
            {"type": "sport", "value": sport},
            {"type": "league", "value": league},
        ]),
        "raw_attributes_json": json.dumps({
            "sport_name": sport,
            "league_name": league,
            "competitors": competitors,
        }),
    }


class TeamRuleMatchingTest(unittest.TestCase):
    def setUp(self):
        self.cubs = {
            "id": "team-cubs", "name": "Chicago Cubs", "shortName": "Cubs",
            "nickname": "Cubs", "type": "Team",
        }
        self.reds = {"id": "team-reds", "name": "Cincinnati Reds", "type": "Team"}

    def _rule(self, mode, teams, **overrides):
        rule = {
            "sport": "Baseball", "league": "MLB", "mode": mode,
            "teams": teams, "include_unassigned": True,
        }
        rule.update(overrides)
        return rule

    def test_include_mode_keeps_only_selected_team_matchups(self):
        event = _event([self.reds, self.cubs])
        cubs_rule = self._rule("include", [{"team_id": "team-cubs", "name": "Chicago Cubs"}])
        bulls_rule = self._rule("include", [{"team_id": "team-bulls", "name": "Chicago Bulls"}])
        self.assertTrue(event_passes_team_rules(event, [cubs_rule]))
        self.assertFalse(event_passes_team_rules(event, [bulls_rule]))
        self.assertTrue(should_include_event(event, {"team_rules": [cubs_rule]}))
        self.assertFalse(should_include_event(event, {"team_rules": [bulls_rule]}))

    def test_exclude_mode_removes_only_selected_team_matchups(self):
        event = _event([self.reds, self.cubs])
        cubs_rule = self._rule("exclude", [{"team_id": "team-cubs", "name": "Chicago Cubs"}])
        bulls_rule = self._rule("exclude", [{"team_id": "team-bulls", "name": "Chicago Bulls"}])
        self.assertFalse(event_passes_team_rules(event, [cubs_rule]))
        self.assertTrue(event_passes_team_rules(event, [bulls_rule]))

    def test_nickname_is_not_used_for_matching(self):
        buffalo = {"id": "team-buffalo", "name": "Buffalo Bulls", "nickname": "Bulls", "type": "Team"}
        event = _event([buffalo], sport="Football", league="College Football")
        chicago = [{
            "sport": "Football", "league": "College Football", "mode": "include",
            "teams": [{"team_id": "team-chicago", "name": "Chicago Bulls"}],
        }]
        self.assertFalse(event_passes_team_rules(event, chicago))

    def test_name_fallback_is_scoped_by_sport_and_league(self):
        event = _event([self.cubs])
        wrong_context = [{"sport": "Basketball", "league": "NBA", "mode": "include", "teams": [{"name": "Chicago Cubs"}]}]
        right_context = [self._rule("include", [{"name": "Chicago Cubs"}])]
        self.assertTrue(event_passes_team_rules(event, wrong_context), "rules for other leagues must not affect this event")
        self.assertTrue(event_passes_team_rules(event, right_context))

    def test_title_text_and_generic_programs_are_not_inferred(self):
        title_only = _event([], title="Chicago Cubs Pregame")
        keep_unassigned = self._rule("include", [{"team_id": "team-cubs", "name": "Chicago Cubs"}])
        hide_unassigned = self._rule(
            "include", [{"team_id": "team-cubs", "name": "Chicago Cubs"}], include_unassigned=False,
        )
        self.assertTrue(should_include_event(title_only, {"team_rules": [keep_unassigned]}))
        self.assertFalse(should_include_event(title_only, {"team_rules": [hide_unassigned]}))

    def test_adb_lane_filter_uses_same_exact_team_semantics(self):
        event = _event([self.reds, self.cubs])
        include_cubs = self._rule("include", [{"team_id": "team-cubs", "name": "Chicago Cubs"}])
        self.assertFalse(adb_should_include_event(
            event["classification_json"], [], [], event["raw_attributes_json"], [
                self._rule("include", [{"team_id": "team-bulls", "name": "Chicago Bulls"}])
            ],
        ))
        self.assertTrue(adb_should_include_event(
            event["classification_json"], [], [], event["raw_attributes_json"], [include_cubs],
        ))


class ActiveTeamDiscoveryTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        self.conn.execute(
            """
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                end_utc TEXT,
                genres_json TEXT,
                classification_json TEXT,
                raw_attributes_json TEXT
            )
            """
        )

    def _insert(self, event_id, competitors, hours=2):
        event = _event(competitors)
        end = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
        self.conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
            (event_id, end, event["genres_json"], event["classification_json"], event["raw_attributes_json"]),
        )

    def test_returns_active_teams_with_distinct_event_counts(self):
        cubs = {"id": "team-cubs", "name": "Chicago Cubs", "type": "Team"}
        reds = {"id": "team-reds", "name": "Cincinnati Reds", "type": "Team"}
        player = {"id": "person-one", "name": "A Player", "type": "Person"}
        self._insert("event-1", [cubs, reds, player])
        self._insert("event-2", [cubs, cubs])
        self._insert("expired", [cubs], hours=-2)
        self.conn.commit()

        teams = _build_active_teams(self.conn)
        by_id = {team["team_id"]: team for team in teams}
        self.assertEqual(set(by_id), {"team-cubs", "team-reds"})
        self.assertEqual(by_id["team-cubs"]["count"], 2)
        self.assertEqual(by_id["team-reds"]["count"], 1)
        self.assertEqual(by_id["team-cubs"]["sport"], "Baseball")
        self.assertEqual(by_id["team-cubs"]["league"], "MLB")


class ActiveLeagueDiscoveryTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        self.conn.execute(
            """
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                end_utc TEXT,
                genres_json TEXT,
                classification_json TEXT
            )
            """
        )

    def _insert(self, event_id, sport, league, hours=2):
        end = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
        self.conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            (event_id, end, json.dumps([sport]), json.dumps([{"type": "league", "value": league}])),
        )

    def test_returns_sport_associations_and_preserves_event_counts(self):
        self._insert("mls-1", "Soccer", "MLS")
        self._insert("mls-2", "Soccer", "MLS")
        self._insert("expired", "Soccer", "MLS", hours=-2)
        self._insert("shared-1", "Soccer", "Shared League")
        self._insert("shared-2", "Futsal", "Shared League")
        self.conn.commit()

        leagues = {league["name"]: league for league in _build_active_leagues(self.conn)}
        self.assertEqual(leagues["MLS"], {"name": "MLS", "count": 2, "sports": ["Soccer"]})
        self.assertEqual(leagues["Shared League"]["count"], 2)
        self.assertEqual(leagues["Shared League"]["sports"], ["Futsal", "Soccer"])


class TeamPreferenceRoundTripTest(unittest.TestCase):
    def test_team_rules_round_trip(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        rules = [{
            "sport": "Football", "league": "College Football", "mode": "include",
            "teams": [{"team_id": "team-indiana", "name": "Indiana Hoosiers"}],
            "include_unassigned": True,
        }]
        self.assertTrue(preferences_dal.save(conn, {"team_rules": rules}))
        self.assertEqual(preferences_dal.load(conn)["team_rules"], rules)


if __name__ == "__main__":
    unittest.main()
