import unittest
from betterweather.stations import import_from_csv
from betterweather import db

class CsvTest(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)

    def test_read_csv(self):
        import_from_csv("/home/thopu/stationen.csv")

    def test_db(self):
        engine = db.setup_db()


if __name__ == '__main__':
    unittest.main()
