from os import listdir, remove
from csv import reader
from bs4 import BeautifulSoup
from pathlib import Path
from db_maria import MariaDB
from zipfile import ZipFile
from urllib.request import urlretrieve, urlopen

import string
import chardet
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
                ('country',
                 'id INT UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'name VARCHAR(100) UNIQUE NOT NULL, '
                 'code2 VARCHAR(2) UNIQUE NOT NULL, '
                 'code3 VARCHAR(3) UNIQUE NOT NULL, '
                 'PRIMARY KEY (id)'
                 ),
                ('country_zones',
                 'id INT UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'country_id INT UNSIGNED NOT NULL, '
                 'code VARCHAR(3) NOT NULL, '
                 'name VARCHAR(100) NOT NULL, '
                 'type VARCHAR(60), '
                 'PRIMARY KEY (id), '
                 'INDEX (country_id), '
                 'CONSTRAINT id_code UNIQUE (country_id, code), '
                 'FOREIGN KEY (country_id) '
                 'REFERENCES country (id) '
                 'ON DELETE CASCADE',
                 ),
                ('country_places',
                 'id INT UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'zone_id INT UNSIGNED NOT NULL, '
                 'code VARCHAR(3) NOT NULL, '
                 'name VARCHAR(256) NOT NULL, '
                 'flags VARCHAR(9), '
                 'coordinates VARCHAR(16), '
                 'PRIMARY KEY (id), '
                 'INDEX (zone_id), '
                 'CONSTRAINT id_code UNIQUE (zone_id, code), '
                 'FOREIGN KEY (zone_id) '
                 'REFERENCES country_zones (id) '
                 'ON DELETE CASCADE',
                 ),
            )

        def init_tables():
            for table in tables():
                table_name, table_sql = table
                if not db.table_exist(table_name):
                    db.create_table(table_name, table_sql)

            if db.execute('SELECT COUNT(*) FROM country'):
                if not db.fetchone()[0]:
                    self.update_country()

            if db.execute('SELECT COUNT(*) FROM country_zones'):
                if not db.fetchone()[0]:
                    self.update_country_zones()

            if db.execute('SELECT COUNT(*) FROM country_places'):
                if not db.fetchone()[0]:
                    self.update_country_places()

        db = self.db = MariaDB(log_level=lg.DEBUG)

        info = {
            'host': 'localhost',
            'port': 3306,
            'user': 'mary',
            'password': 'password',
            'database': db_name,
        }
        if db.connect(db_name, connection=info):
            print(f'Connected to database "{info["database"]}" as user "{info["user"]}" successful.')
            if new:
                sql = 'DROP TABLE IF EXISTS country_places, country_zones, country;'
                db.execute(sql)
            init_tables()
        else:
            print(f'Could not connected to database "{info["database"]}" as user "{info["user"]}".')

    @staticmethod
    def get_country_csv_file():
        with urlopen('https://www.iban.com/country-codes') as f:
            html = f.read()
            soup = BeautifulSoup(html, features="html.parser")
            li = soup.find("table", {"id": "myTable"})
            table_body = li.find('tbody')
            rows = table_body.findChildren("tr")

            with open("country.csv", "w") as text_file:
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

    def update_country(self):
        file = _path.joinpath('country.csv')
        if not file.exists():
            self.get_country_csv_file()

        if file.exists():
            with open(str(file.resolve())) as f:
                results = reader(f, delimiter=',', quotechar='"')

                for row in results:
                    sql = 'SELECT id FROM country WHERE country.code2="%s";'
                    if self.db.execute(sql, (row[1].strip(' '), )):
                        continue

                    self.db.insert_row('country', (row[0].strip(' '), row[1].strip(' '), row[2].strip(' ')))

    def update_country_zones(self):
        reject = [
            'parish', 'dependency', 'department', 'federal district', 'autonomous district', 'island council',
            'autonomous region', 'special administrative region', 'special municipality', 'administration',
            'metropolitan department', 'council area', 'district council area', 'local council',
            'administrative atoll', 'zone', 'autonomous city', 'administrative region', 'administrative territory',
            'oblast', 'economic prefecture', 'department', 'departments', 'free communal consortia', 'town council',
        ]

        file = '2019-2 SubdivisionCodes.csv'
        with open(file, errors='ignore') as f:
            results = reader(f, delimiter=',', quotechar='"')

            for idx, row in enumerate(results):
                if row[3].lower() in reject:
                    continue

                for column, value in enumerate(row):
                    j = value.replace('?', '').replace('\n', ' ')
                    row[column] = j

                sql = 'SELECT id FROM country WHERE country.code2=%s;'
                if self.db.execute(sql, (row[0],)):
                    _id = self.db.fetchone()[0]
                    self.db.insert_row('country_zones', (_id, row[1], row[2], row[3]))

    def update_country_places(self):
        def get_files():
            file_list = []
            for file_name in listdir('.'):
                if 'UNLOCODE' in file_name and file_name.endswith('.csv'):
                    file_list.append(file_name)
            return tuple(sorted(file_list))

        for file in get_files():
            with open(file, encoding='iso-8859-1') as f:
                results = reader(f, delimiter=',', quotechar='"')
                for i, row in enumerate(results):
                    zone_code = row[5].strip()
                    place_code = row[2]
                    place_name = row[3]
                    place_flags = row[6]
                    place_coordinates = row[10]
                    sql = 'SELECT id FROM country WHERE country.code2=%s;'

                    if self.db.execute(sql, (row[1], )):
                        country_id = self.db.fetchone()[0]
                        sql = 'SELECT country_zones.id ' \
                              'FROM country_zones ' \
                              'WHERE country_zones.country_id=%s AND country_zones.code=%s;'

                        if self.db.execute(sql, (country_id, zone_code)):
                            zone_id = self.db.fetchone()[0]
                            self.db.insert_row('country_places', (
                                zone_id, place_code, place_name, place_flags, place_coordinates))


def main():
    app = App()
    app.setup('countries', new=True)

    app.get_country_zone_csv_files()
    sql = f'SELECT country.name, country_zones.name, country_zones.code, country_places.name ' \
          f'FROM country ' \
          f'JOIN country_zones ' \
          f'ON country.id=country_zones.country_id ' \
          f'JOIN country_places ' \
          f'ON country_zones.id=country_places.zone_id WHERE country.code2="CA" AND country_zones.code="ON";'

    if app.db.execute(sql):
        for idx, row in enumerate(app.db.fetchall()):
            print(idx, row)


if __name__ == '__main__':
    main()
