from Load_files import *


if __name__ == '__main__':
    all = get_files('data')
    print(all)
    connection = create_connection(
    "SongsDB","postgres", "postgres", "127.0.0.1", "5432"
)

#add

update_load_id_table(connection)
connection.close ()
