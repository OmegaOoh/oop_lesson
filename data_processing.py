import csv
import os
import copy

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))


class Database:
    def __init__(self):
        self.database = []

    def insert(self, table):
        """
        add table object into database
        :param table: Table Object
        """
        self.database.append(table)

    def search(self, table_name):
        """
        Search for Table by its name
        :param table_name: name of table
        :return: Table object ,None if not found
        """
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None


class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table

    def join(self, other, common_key):
        """
        Joins 2 Table together
        :param other: Table Object
        :param common_key: common key of 2 table
        :return: joined table
        """
        joined_table = Table(self.table_name + '_joins_' + other.table_name,[])
        for i in self.table:
            for j in other.table:
                if i[common_key] == j[common_key]:
                    temp_dict = copy.deepcopy(i)
                    temp_dict.update(copy.deepcopy(j))
                    joined_table.table.append(temp_dict)
        return joined_table

    def filter(self, cond):
        """
        Filter the data of table
        :param cond: condition(function)
        :return: filtered table
        """
        filtered = Table(self.table_name + '_filtered', [])
        for i in self.table:
            if cond(i):
                filtered.table.append(i)
        return filtered

    def aggregate(self, func, key):
        """
        Aggregate the table
        :param func: Functions to aggregate
        :param key: key to aggregate
        :return: output of the function
        """
        temp = []
        for i in self.table:
            temp.append(float(i[key]))
        return func(temp)

    def select(self, attribute_list):
        """
        list of dict that follow attribute list
        :param attribute_list: List of attribute
        :return: Selected List
        """
        temp = []
        for i in self.table:
            temp_dict = {}
            for j in i:
                if j in attribute_list:
                    temp_dict[j] = i[j]
            temp.append(temp_dict)
        return temp

    def __str__(self):
        return self.table_name + ':' + str(self.table)


table1 = Table('cities', cities)
table2 = Table('countries', countries)
my_DB = Database()
my_DB.insert(table1)
my_DB.insert(table2)
my_table1 = my_DB.search('cities')
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
my_table1_selected = my_table1.select(['city', 'latitude'])
print(my_table1)
print()
print(my_table1_selected)

temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps) / len(temps))
print("Using aggregation")
print(my_table1_filtered.aggregate(lambda x: sum(x) / len(x), 'temperature'))

print()
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)

print()
countries_table = my_DB.search('countries')
cities_table = my_DB.search('cities')
combined_table = cities_table.join(countries_table, 'country')
# Filter cities in EU that do not have coastlines
my_table4 = combined_table.filter(lambda x : x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')

# Find and print its max temperature
my_table5_max_temp = my_table4.aggregate(lambda x: max(x), 'temperature')
print()
print("Highest City's Temperature in EU that have no coastline")
print(my_table5_max_temp)

# Find and print its min temperature
my_table5_min_temp = my_table4.aggregate(lambda x: min(x), 'temperature')
print()
print("Lowest City's Temperature in EU that have no coastline")
print(my_table5_min_temp)

countries_names = my_DB.search('countries')
countries_names = countries_names.select('country')
for i in countries_names:
    j = i['country']
    max_lat = combined_table.aggregate(lambda x: max(x), 'latitude')
    min_lat = combined_table.aggregate(lambda x: min(x), 'latitude')
    print()
    print(j, 'highest latitude: ', max_lat)
    print(j, 'lowest latitude: ', min_lat)
