"""
    Author          :   Siddhi
    Created_date    :   2019/08/20  
    Modified Date   :   2019/09/26
    Description     :   Program utility tests.       
"""

import unittest

from utilities.utility import *
from utilities.global_vars import *

class TestUtilities(unittest.TestCase):
    
    def setUp(self):
        self.QUERY = "select * from MASTER_DATA.md_date"
        self.CON_URL = connection_url
        self.df = read_table_sql(self.QUERY, con = self.CON_URL)

    def test_read_table_sql(self):
        self.assertEqual(read_table_sql(self.QUERY, con = self.CON_URL).shape[0],3822)

    def test_insert_df(self):
        insert_df(
            self.df, 
            'test_test_test',
            con = self.CON_URL,
            schema = 'temp', 
            if_exists='replace')