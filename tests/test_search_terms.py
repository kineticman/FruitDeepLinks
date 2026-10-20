import json
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bin'))
from core.search_terms import DEFAULT_TERMS, PREF_KEY, init_stats_schema, load_terms, normalize_terms
import apple_scraper_db
from apple_scraper_db import record_term_stats


def stats(found):
    return {'seed_count': len(found), 'found_count': len(found), 'new_full': 0, 'refreshed': 0,
            'new_shelf': 0, 'errors': 0, 'duration_s': 1.0}


class NormalizeTermsTests(unittest.TestCase):
    def test_strips_dedupes_and_drops_all(self):
        self.assertEqual(
            normalize_terms([' NBA ', 'nba', 'All', '', 'europa  league', 'a,b', 5, None]),
            ['NBA', 'europa league', 'a b'],
        )


class LoadTermsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

    def save(self, value):
        self.conn.execute('CREATE TABLE IF NOT EXISTS user_preferences (key TEXT PRIMARY KEY, value TEXT, updated_utc TEXT)')
        self.conn.execute('INSERT OR REPLACE INTO user_preferences VALUES (?, ?, ?)', (PREF_KEY, json.dumps(value), 'x'))

    def test_defaults_without_table_or_connection(self):
        self.assertEqual(load_terms(self.conn), DEFAULT_TERMS)
        self.assertEqual(load_terms(None), DEFAULT_TERMS)

    def test_saved_list_wins(self):
        self.save(['nba', 'europa league'])
        self.assertEqual(load_terms(self.conn), ['nba', 'europa league'])

    def test_empty_saved_list_means_defaults(self):
        self.save([])
        self.assertEqual(load_terms(self.conn), DEFAULT_TERMS)


class RecordTermStatsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

    def test_unique_counts_and_hits_replaced_each_run(self):
        record_term_stats(self.conn, 'run1', [
            ('all', stats({'a', 'b'}), {'a', 'b'}),
            ('soccer', stats({'b', 'c', 'd'}), {'b', 'c', 'd'}),
        ])
        rows = dict(self.conn.execute('SELECT term, unique_count FROM search_term_stats').fetchall())
        self.assertEqual(rows, {'all': 1, 'soccer': 2})
        self.assertEqual(self.conn.execute('SELECT total_found FROM search_term_runs').fetchone()[0], 4)

        record_term_stats(self.conn, 'run2', [('nba', stats({'x'}), {'x'})])
        self.assertEqual(self.conn.execute('SELECT term, event_id FROM search_term_hits').fetchall(), [('nba', 'x')])

    def test_old_runs_pruned(self):
        with patch.object(apple_scraper_db, 'KEEP_RUNS', 2):
            for i in range(4):
                record_term_stats(self.conn, f'run{i}', [('all', stats({'a'}), {'a'})])
        self.assertEqual([r[0] for r in self.conn.execute('SELECT run_id FROM search_term_runs ORDER BY run_id')], [3, 4])
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM search_term_stats').fetchone()[0], 2)

    def test_schema_is_idempotent(self):
        init_stats_schema(self.conn)
        init_stats_schema(self.conn)


if __name__ == '__main__':
    unittest.main()
