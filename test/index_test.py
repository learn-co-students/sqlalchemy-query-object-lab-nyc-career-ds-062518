import unittest, sqlite3, sys
sys.path.insert(0, '..')
from queries import *

class TestQueries(unittest.TestCase):
    def test_return_apple(self):
        apple = return_apple()
        result = 'Apple'
        self.assertEqual(apple.company, result)

    def test_return_disneys_industry(self):
        disney = return_disneys_industry()
        result = 'Broadcasting\xa0and\xa0entertainment'
        self.assertEqual(disney, result)

    def test_return_list_of_company_objects_ordered_alphabetically_by_symbol(self):
        companies = return_list_of_company_objects_ordered_alphabetically_by_symbol()
        def format():
            result_arr = []
            for company in companies:
                result_arr.append(company.company)
            return result_arr

        result = ['Apple', 'American Express', 'Boeing', 'Caterpillar', 'Cisco Systems', 'Chevron', 'Walt Disney', 'DowDuPont', 'General Electric', 'Goldman Sachs', 'The Home Depot', 'IBM', 'Intel', 'Johnson & Johnson', 'JPMorgan Chase', 'Coca-Cola', "McDonald's", '3M', 'Merck'
        , 'Microsoft', 'Nike', 'Pfizer', 'Procter & Gamble', 'Travelers', 'UnitedHealth Group', 'United Technologies', 'Visa', 'Verizon', 'Walmart', 'ExxonMobil']

        self.assertEqual(format(), result)

    def test_return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending(self):
        tech = return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending()

        result = [{'company': 'Apple', 'EV': 954.8}, {'company': 'Microsoft', 'EV': 708.61}, {'company': 'Intel', 'EV': 244.6}, {'company': 'IBM', 'EV': 178.63},
        {'company': 'Cisco Systems', 'EV': 174.31}]
        self.assertEqual(tech, result)

    def test_return_list_of_consumer_products_companies_with_EV_above_225(self):
        cos = return_list_of_consumer_products_companies_with_EV_above_225()
        result = [{'name': 'The Home Depot'}, {'name': 'Walmart'}]
        self.assertEqual(cos, result)

    def test_return_conglomerates_and_pharmaceutical_companies(self):
        conglomerceuticals = return_conglomerates_and_pharmaceutical_companies()
        result = ['3M', 'General Electric', 'Johnson & Johnson', 'Merck', 'Pfizer', 'United Technologies']
        self.assertEqual(conglomerceuticals, result)

    def test_count_number_of_tech_companies(self):
        avg = avg_EV_of_dow_companies()
        result = (221.90999999999997,)
        self.assertEqual(avg, result)

    def test_return_industry_and_its_total_EV(self):
        industry_ev = return_industry_and_its_total_EV()
        result = [('Broadcasting\xa0and\xa0entertainment', 172.31), ('Conglomerate', 482.95), ('Consumer products', 1217.87), ('Financial services', 73.86000000000001), ('Managed health care', 233.23), ('Manufacturing', 494.19000000000005), ('Oil & gas', 636.48), ('Pharmaceuticals', 775.82), ('Technology', 2260.95), ('Telecommunication', 309.64)]
        self.assertEqual(industry_ev, result)
