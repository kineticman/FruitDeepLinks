import sys
import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bin'))
from apple_scraper_db import HybridAPIClient, extract_relevant_playables, init_database, scrape_search_term


def event(eid):
    return {'id': eid, 'type': 'SportingEvent'}


def response(ids, token=None):
    return {'data': {'shelf': {'items': [event(i) for i in ids], 'nextToken': token}}}


class ShelfPaginationTests(unittest.TestCase):
    def setUp(self):
        self.client = HybridAPIClient(Mock(), 'config', 'key', use_hybrid=False)
        self.shelf = {'id': 'sports-related', 'url': '/collection?ctx_league=college&utsk=wrong',
                      'items': [event('early')], 'nextToken': '20'}
        self.data = {'data': {'canvas': {'shelves': [self.shelf]}}}

    def test_continuations_discover_evening_games_and_keep_page_metadata(self):
        second = response(['early', 'clemson'], '40')
        third = response(['lamar', 'ucla'])
        third['data']['playables'] = {'espn': {'canonicalId': 'lamar', 'channelId': 'espn'}}
        self.client.fetch_shelf_v3 = Mock(side_effect=[second, third])
        found = list(self.client.iter_shelf_events(self.data))
        self.assertEqual([i['id'] for i, _ in found], ['early', 'clemson', 'lamar', 'ucla'])
        self.assertEqual(extract_relevant_playables(found[2][1], found[2][0]), third['data']['playables'])
        self.assertEqual([c.args[1] for c in self.client.fetch_shelf_v3.call_args_list], ['20', '40'])
        self.assertEqual(list(self.client.iter_shelf_events(self.data)), [])
        self.assertEqual(self.client.fetch_shelf_v3.call_count, 2)

    def test_repeated_tokens_stop_without_losing_new_items(self):
        self.client.fetch_shelf_v3 = Mock(return_value=response(['late'], '20'))
        self.assertEqual([i['id'] for i, _ in self.client.iter_shelf_events(self.data)], ['early', 'late'])
        self.assertEqual(self.client.fetch_shelf_v3.call_count, 1)

    def test_failed_page_keeps_prior_items_and_can_be_retried(self):
        self.client.fetch_shelf_v3 = Mock(side_effect=[{'error': 'unavailable'}, response(['late'])])
        self.assertEqual([i['id'] for i, _ in self.client.iter_shelf_events(self.data)], ['early'])
        self.assertEqual([i['id'] for i, _ in self.client.iter_shelf_events(self.data)], ['late'])

    def test_empty_page_with_continuation_is_followed(self):
        self.client.fetch_shelf_v3 = Mock(side_effect=[response([], '40'), response(['late'])])
        self.assertEqual([i['id'] for i, _ in self.client.iter_shelf_events(self.data)], ['early', 'late'])

    def test_changing_tokens_are_bounded(self):
        self.client.fetch_shelf_v3 = Mock(side_effect=[response([], str(i)) for i in range(1000, 1100)])
        list(self.client.iter_shelf_events(self.data))
        self.assertEqual(self.client.fetch_shelf_v3.call_count, 100)

    def test_search_persists_later_pages_for_full_detail_upgrade(self):
        self.data['data']['content'] = event('seed')
        late = response(['late'])
        late['data']['channels'] = {'espn': {'name': 'ESPN'}}
        late['data']['playables'] = {'playable': {'canonicalId': 'late', 'channelId': 'espn'}}
        self.client.fetch_event_v3 = Mock(return_value=self.data)
        self.client.fetch_shelf_v3 = Mock(return_value=late)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'apple.db'
            init_database(path)
            with sqlite3.connect(path) as conn, patch('apple_scraper_db.time.sleep'), patch('apple_scraper_db.auto_scroll'), patch('apple_scraper_db.get_event_ids_from_page', return_value={'seed'}):
                self.assertEqual(scrape_search_term(Mock(), conn, 'football', self.client), (1, 2, 0))
                level, blob = conn.execute("SELECT fetch_level, raw_json_gzip FROM apple_events WHERE event_id='late'").fetchone()
                self.assertEqual(level, 'shelf')
                saved = json.loads(gzip.decompress(blob))
                self.assertEqual(saved['channels'], late['data']['channels'])
                self.assertEqual(saved['playables'], late['data']['playables'])

    def test_terminal_shelf_does_not_request_pages(self):
        self.shelf.pop('nextToken')
        self.client.fetch_shelf_v3 = Mock()
        self.assertEqual(len(list(self.client.iter_shelf_events(self.data))), 1)
        self.client.fetch_shelf_v3.assert_not_called()

    def test_shelf_request_keeps_context_and_uses_browser_fallback(self):
        self.client._fetch_via_browser = Mock(return_value=response(['late']))
        result = self.client.fetch_shelf_v3(self.shelf, '20')
        self.assertEqual(result, response(['late']))
        url = urlsplit(self.client._fetch_via_browser.call_args.args[0])
        self.assertEqual(url.netloc, 'tv.apple.com')
        self.assertEqual(url.path, '/api/uts/v3/shelves/sports-related')
        params = parse_qs(url.query)
        self.assertEqual(params['ctx_league'], ['college'])
        self.assertEqual(params['nextToken'], ['20'])
        self.assertEqual(params['utsk'], ['key'])

    def test_http_failure_falls_back_to_browser_for_shelves(self):
        self.client.use_hybrid = True
        self.client.session = Mock()
        self.client.session.get.return_value = Mock(status_code=503, headers={}, text='', content=b'')
        self.client._fetch_via_browser = Mock(return_value=response(['late']))
        self.assertEqual(self.client.fetch_shelf_v3(self.shelf, '20'), response(['late']))
        self.assertEqual(self.client.requests_failures, 1)
        self.client._fetch_via_browser.assert_called_once()


if __name__ == '__main__':
    unittest.main()
