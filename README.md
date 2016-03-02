# psma-gnaf-loader

A simple file to help load the Geo-coded National Address File (G-NAF) data from PSMA Australia Limited into a postgresql database. Automatically imports the data following the PSMA's guidance.

Setup instructions
- unpack the data from http://www.data.gov.au/dataset/geocoded-national-address-file-g-naf
- open up the "/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql" file and change the "DROP TABLE" commands to "DROP TABLE IF EXISTS", otherwise the import scripts won't work from a clean database.
- Install Python 3, Postgresql (with Postgis), and psycopg2
- Edit the parameters at the top of the "main.py" script to connect to the database you created in the previous step and the path to the extracted files.
- Run the "main.py" program. On a Macbook Pro mid-2012, 2.6Ghz with SSD the script took about 10 minutes to run.

If I'm (ajosephau) asked I could upload a database export.

Have fun :-).
