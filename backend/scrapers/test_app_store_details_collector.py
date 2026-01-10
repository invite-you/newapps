import unittest

from backend.scrapers.app_store_details_collector import AppStoreDetailsCollector


class TestAppStoreDetailsCollector(unittest.TestCase):
    def setUp(self):
        self.collector = AppStoreDetailsCollector(verbose=False)

    def test_parse_app_metadata_detects_iap_from_features(self):
        data = {
            'features': ['Family Sharing', 'In-App Purchases'],
            'inAppPurchases': [],
            'hasInAppPurchases': False,
            'price': 1.99,
        }

        result = self.collector.parse_app_metadata(data, 'test.app')

        self.assertTrue(result['has_iap'])

    def test_parse_app_metadata_detects_iap_from_boolean_flag(self):
        data = {
            'features': ['Family Sharing'],
            'inAppPurchases': False,
            'hasInAppPurchases': True,
            'price': 0,
        }

        result = self.collector.parse_app_metadata(data, 'test.app.flag')

        self.assertTrue(result['has_iap'])

    def test_parse_app_metadata_sets_zero_when_iap_fields_empty(self):
        data = {
            'features': [],
            'inAppPurchases': [],
            'hasInAppPurchases': False,
            'price': 0,
        }

        result = self.collector.parse_app_metadata(data, 'test.app.none')

        self.assertFalse(result['has_iap'])


if __name__ == '__main__':
    unittest.main()
