
from db_maria import MariaDB


def main():
    db = MariaDB()

    db_name = 'mydb'

    info = {
        'host': 'localhost',
        'port': 3306,
        'user': 'mary',
        'password': 'password',
        'database': db_name,
        'charset': 'utf8',
    }
    tables = (
        ('users',
         'id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT, '
         'first_name VARCHAR(30) NOT NULL, '
         'last_name VARCHAR(30) NOT NULL, '
         'cell_phone VARCHAR(30), '
         'PRIMARY KEY (id)'
         ),
        ('stats',
         'id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT, '
         'user_id SMALLINT(4) UNSIGNED NOT NULL, '
         'time_used INT, '
         'skill_level VARCHAR(30) NOT NULL, '
         'total_points INT, '
         'PRIMARY KEY (id), '
         'INDEX (user_id), '
         'FOREIGN KEY (user_id) '
         'REFERENCES users (id) '
         'ON DELETE CASCADE',
         ),
    )

    if db.connect(db_name, connection=info):
        for table in tables:
            table_name, table_sql = table
            if not db.table_exist(table_name):
                db.create_table(table_name, table_sql)

        print(f'Connected to database "{info["database"]}" as user "{info["user"]}" successful.')

        _id = db.insert_row('users', ('Mary', 'Jane', '1 234-5678'))
        if _id:
            db.insert_row('stats', (_id, 3600, 'Good', 420))
            db.update_row('users', _id, ('Scary', 'Jane', '2 234-5678'))
    else:
        print(f'Could not connected to database "{info["database"]}" as user "{info["user"]}".')


if __name__ == '__main__':
    main()
