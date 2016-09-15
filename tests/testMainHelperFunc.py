import unittest
import hawqbackup.main


class TestHelperFunctions(unittest.TestCase):

    def setUp(self):
        self.possible_valid_inputs = ['"schema"."table"',
                                      'schema."table"',
                                      '"schema".table',
                                      'schema.table',
                                      '"sch.ema".table',
                                      '"sc.he.ma"."ta.bl.e"',
                                      'schema."ta.ble"']
        self.invalid_inputs = ['sch.ema."table"', 'sch.ema.table', '"sch.ema".tab.le']

    def test_is_name_valid(self):

        for name in self.possible_valid_inputs:
            is_valid = hawqbackup.main.is_table_name_valid(name)
            self.assertTrue(is_valid)

    def test_invalid_names(self):
        for invalid_name in self.invalid_inputs:
            is_invalid = hawqbackup.main.is_table_name_valid(invalid_name)
            self.assertFalse(is_invalid)


if __name__ == '__main__':
    unittest.main()
