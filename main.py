from os import listdir, remove
from csv import reader
from bs4 import BeautifulSoup
from pathlib import Path
from db_maria import MariaDB
from zipfile import ZipFile
from urllib.request import urlretrieve, urlopen

import logging as lg

_path = Path(__file__).cwd()


class App:
    def __init__(self, **kwargs):
        self.db = None

    def setup(self, db_name, new=False):
        def tables():
            return (
                # ('address',
                #  'id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT, '
                #  'street VARCHAR(30) NOT NULL, '
                #  'unit_number VARCHAR(30) NOT NULL, '
                #  'city VARCHAR(30) NOT NULL, '
                #  'state VARCHAR(30), '
                #  'zip_code VARCHAR(30), '
                #  'country_code KEY (id)'
                #  'comments VARCHAR(1024), '
                #  ),
                ('countries',
                 'id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'name VARCHAR(100) UNIQUE NOT NULL, '
                 'code2 VARCHAR(2) UNIQUE NOT NULL, '
                 'code3 VARCHAR(3) UNIQUE NOT NULL, '
                 'PRIMARY KEY (id)'
                 ),
                ('country_zones',
                 'id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'country_id SMALLINT(4) UNSIGNED NOT NULL, '
                 'code VARCHAR(4) NOT NULL, '
                 'name VARCHAR(100) NOT NULL, '
                 'type VARCHAR(100), '
                 'PRIMARY KEY (id), '
                 'INDEX (country_id), '
                 'CONSTRAINT name_type UNIQUE (code, name), '
                 'FOREIGN KEY (country_id) '
                 'REFERENCES countries (id) '
                 'ON DELETE CASCADE',
                 ),
                ('country_places',
                 'id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'zone_id SMALLINT(4) UNSIGNED NOT NULL, '
                 'code VARCHAR(4) NOT NULL, '
                 'name VARCHAR(100) NOT NULL, '
                 'flags VARCHAR(9), '
                 'coordinates VARCHAR(16), '
                 'PRIMARY KEY (id), '
                 'INDEX (zone_id), '
                 'CONSTRAINT code_name UNIQUE (code, name), '
                 'FOREIGN KEY (zone_id) '
                 'REFERENCES country_zones (id) '
                 'ON DELETE CASCADE',
                 ),
            )

        def init_tables():
            if db.connect(db_name, connection=info):
                for table in tables():
                    table_name, table_sql = table
                    if not db.table_exist(table_name):
                        db.create_table(table_name, table_sql)

                print(f'Connected to database "{info["database"]}" as user "{info["user"]}" successful.')
            else:
                print(f'Could not connected to database "{info["database"]}" as user "{info["user"]}".')

        db = self.db = MariaDB(log_level=lg.ERROR)

        info = {
            'host': 'localhost',
            'port': 3306,
            'user': 'mary',
            'password': 'password',
            'database': db_name,
        }

        init_tables()

    @staticmethod
    def get_country_csv_file():
        with urlopen('https://www.iban.com/country-codes') as f:
            html = f.read()
            soup = BeautifulSoup(html, features="html.parser")
            li = soup.find("table", {"id": "myTable"})
            table_body = li.find('tbody')
            rows = table_body.findChildren("tr")

            with open("countries.csv", "w") as text_file:
                for row in rows:
                    line = ''
                    for idx, column in enumerate(row.text.split('\n')[:4]):
                        if ',' in column:
                            col = column.split(',')
                            column = f'{col[0].strip(" ")} ({col[1].strip(" ")})'
                        line += f'{column},'
                    print(line.strip(','), file=text_file)

    @staticmethod
    def get_country_zone_csv_files():
        with urlopen('http://www.unece.org/cefact/codesfortrade/codes_index.html') as response:
            html = response.read()

        soup = BeautifulSoup(html, features="html.parser")
        parsed_data = soup.find("div", {"id": "c21211"})
        version_number = parsed_data.find_all(['td'])[3].text.split()[-1:][0]
        unlocode_zip_file = 'loc' + version_number.replace('-', '')[2:] + 'csv.zip'

        file = _path.joinpath(unlocode_zip_file)
        if not file.exists():
            url = 'http://www.unece.org/fileadmin/DAM/cefact/locode/' + unlocode_zip_file
            src_file_name = url.split('/')[-1]
            urlretrieve(url, src_file_name)

            with ZipFile(src_file_name, 'r') as z:
                z.extractall('.')

    def update_countries(self):
        file = _path.joinpath('countries.csv')
        if not file.exists():
            self.get_country_csv_file()

        if file.exists():
            with open(str(file.resolve())) as f:
                results = reader(f, delimiter=',', quotechar='"')

                for row in results:
                    sql = 'SELECT id FROM countries WHERE countries.code2=%s;'
                    # Todo: Check the all columns are the same not that only the code2 exists.
                    if self.db.execute(sql, (row[1], )):
                        continue

                    self.db.insert_row('countries', row)

    def update_country_zones(self):
        file = '2019-2 SubdivisionCodes.csv'

        with open(file, errors='ignore') as f:
            results = reader(f, delimiter=',', quotechar='"')

            for row in results:
                if row[3].lower() in (
                    'parish', 'dependency', 'department', 'federal district', 'autonomous district',
                    'autonomous region', 'special administrative region', 'special municipality', 'administration',
                    'metropolitan department', 'council area', 'district council area', 'local council',
                    'Administrative atoll', 'zone', 'autonomous city', 'administrative region',
                    'administrative territory', 'oblast', 'economic prefecture', 'department', 'administrative atoll',
                    'autonomous province', 'autonomous republic', 'department', 'departments',
                    'free communal consortia', 'town council', 'island council', 'outlying area'
                ) or not row[3].strip(' '):
                    continue

                sql = 'SELECT id FROM countries WHERE countries.code2=%s;'
                if self.db.execute(sql, (row[0], )):
                    _id = self.db.fetchone()[0]
                    sql = 'SELECT id FROM country_zones WHERE country_zones.code=%s;'
                    # Todo: Check the all columns are the same not that only the id exists.
                    if _id and not self.db.execute(sql, (row[1],)):
                        self.db.insert_row('country_zones', (_id, row[1], row[2], row[3]))

    def update_country_places(self):
        def get_files():
            file_list = []
            for file_name in listdir('.'):
                if 'UNLOCODE' in file_name and file_name.endswith('.csv'):
                    file_list.append(file_name)
            return tuple(sorted(file_list))

        for file in get_files():
            with open(file, errors='ignore') as f:
                results = reader(f, delimiter=',', quotechar='"')
                for i, row in enumerate(results):
                    zone_code = row[5].strip()
                    location_name = row[3]
                    location_code = row[2]
                    location_flags = row[6]
                    location_coordinates = row[10]
                    sql = 'SELECT id FROM country_zones WHERE country_zones.code=%s;'
                    if self.db.execute(sql, (zone_code, )):
                        _id = self.db.fetchone()[0]
                        if _id:
                            sql = 'SELECT id FROM country_places WHERE country_places.code=%s;'
                            # Todo: Check the all columns are the same not that only the location_code exists.
                            if self.db.execute(sql, (location_code, )):
                                continue

                            self.db.insert_row('country_places', (
                                _id, location_code, location_name, location_flags, location_coordinates))


def main():
    app = App()
    app.setup('mydb')
    app.get_country_zone_csv_files()

    app.update_countries()
    app.update_country_zones()
    app.update_country_places()


if __name__ == '__main__':
    main()
