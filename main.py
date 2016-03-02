import logging, os

import psycopg2

# settings
database_name = 'postgres_database'
user = 'postgres_user'
password = 'some_password_here_lol'
port = 5432
host = 'postgres_host_normally_localhost'
path_to_gnaf_data = '/path/to/gnaf/data/'

# setup
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)


def get_folder_path(support_text, absolute_path, search_path, search_name, test_name):
    if not search_path and search_name in test_name:
        logging.debug(support_text + absolute_path)
        return absolute_path
    else:
        return search_path


def load_sql_file_into_db(file_path):
    file_ref = open(file_path, "r").read()
    db_cursor.execute(file_ref)
    db_connection.commit()


try:
    db_connection = psycopg2.connect(database=database_name, user=user, password=password, host=host, port=port)
    db_cursor = db_connection.cursor()

    logging.info("Step 0 of 5 : Bootstrapping started...")

    gnaf_parent_path = ''
    extras_path = ''
    table_creation_scripts_path = ''
    example_view_creation_scripts_path = ''
    table_creation_script_path = ''
    foreign_key_script_path = ''
    example_view_script_path = ''
    authority_code_path = ''
    standard_data_path = ''

    gnaf_name = 'G-NAF '
    table_creation_script_folder_name = 'GNAF_TableCreation_Scripts'
    table_creation_script_name = 'create_tables_ansi.sql'
    foreign_key_script_name = 'add_fk_constraints.sql'
    authority_code_name = 'Authority Code'
    standard_data_name = 'Standard'
    psv_file_suffix = "_psv.psv"

    views_script_folder_name = 'GNAF_View_Scripts'
    example_view_script_name = 'address_view.sql'

    SQL_STATEMENT = """ COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS '|'"""

    # find sub folders needed
    for dirname, dirnames, filenames in os.walk(path_to_gnaf_data):
        for subdirname in dirnames:
            absolute_path = os.path.join(dirname, subdirname)

            gnaf_parent_path = get_folder_path("G-NAF parent folder: ", absolute_path, gnaf_parent_path, gnaf_name, subdirname)

            table_creation_scripts_path = get_folder_path("Table creation scripts folder: ", absolute_path, table_creation_scripts_path, table_creation_script_folder_name, subdirname)

            example_view_creation_scripts_path = get_folder_path("Example View creation scripts folder: ", absolute_path, example_view_creation_scripts_path, views_script_folder_name, subdirname)

            authority_code_path = get_folder_path("Authority Code folder: ", absolute_path, authority_code_path, authority_code_name, subdirname)

            standard_data_path = get_folder_path("Standard data folder: ", absolute_path, standard_data_path, standard_data_name, subdirname)

    # find table/fk creation scripts
    for dirname, dirnames, filenames in os.walk(table_creation_scripts_path):
        for filename in filenames:
            absolute_path = os.path.join(table_creation_scripts_path, filename)

            if not table_creation_script_path and table_creation_script_name in filename:
                table_creation_script_path = absolute_path
                logging.debug("Table creation script: " + table_creation_script_path)

            if not foreign_key_script_path and foreign_key_script_name in filename:
                foreign_key_script_path = absolute_path
                logging.debug("Foreign key script: " + foreign_key_script_path)

    # find views creation script
    for dirname, dirnames, filenames in os.walk(example_view_creation_scripts_path):
        for filename in filenames:
            absolute_path = os.path.join(example_view_creation_scripts_path, filename)

            if not example_view_script_path and example_view_script_name in filename:
                example_view_script_path = absolute_path
                logging.debug("Example views script: " + example_view_script_path)

    logging.info("Step 0 of 5 : Bootstrapping finished!")

    logging.info("Step 1 of 5 : Creating Schema started...")

    load_sql_file_into_db(table_creation_script_path)

    logging.info("Step 1 of 5 : Creating Schema finished!")

    logging.info("Step 2 of 5 : Loading Authority Code data started...")

    for dirname, dirnames, filenames in os.walk(authority_code_path):
        num_files = str(len(filenames))
        for index, filename in enumerate(filenames):
            absolute_path = os.path.join(authority_code_path, filename)

            authority_code_prefix = "Authority_Code_"
            authority_code_suffix = psv_file_suffix
            table_name = filename.replace(authority_code_prefix, "")
            table_name = table_name.replace(authority_code_suffix, "")

            logging.info("Importing file " + str(index + 1) + " of " + num_files + ": " + filename + " -> " + table_name)
            db_cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=open(absolute_path))
            db_connection.commit()

    logging.info("Step 2 of 5 : Loading Authority Code data finished!")

    logging.info("Step 3 of 5 : Loading Standard data started...")

    for dirname, dirnames, filenames in os.walk(standard_data_path):
        num_files = str(len(filenames))
        for index, filename in enumerate(filenames):
            absolute_path = os.path.join(standard_data_path, filename)

            standard_data_suffix = psv_file_suffix

            table_name = filename.split('_', 1)[-1]
            table_name = table_name.replace(standard_data_suffix, "")

            logging.info("Importing file " + str(index + 1) + " of " + num_files + ": " + filename + " -> " + table_name)

            db_cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=open(absolute_path))
            db_connection.commit()

    logging.info("Step 3 of 5 : Loading Standard data finished!")

    logging.info("Step 4 of 5 : Creating Foreign Key relationships creation started...")

    load_sql_file_into_db(foreign_key_script_path)

    logging.info("Step 4 of 5 : Creating Foreign Key relationships creation finished!")

    logging.info("Step 5 of 5 : Creating example views creation started...")

    load_sql_file_into_db(example_view_script_path)

    logging.info("Step 5 of 5 : Creating example views creation finished!")

    db_cursor.close()
    db_connection.close()
except Exception as exception:
    logging.error("Exception occurred: " + str(exception))
