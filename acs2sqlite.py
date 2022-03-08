import csv
import logging
import os
import sqlite3
from itertools import groupby


def nullify(data):
    if data in ('', '.'):
        return None
    return float(data)


def main():
    base_dir = '/Users/iandees/Downloads/2018_5yr'
    table_number_lookup_filename = 'ACS_5yr_Seq_Table_Number_Lookup.txt'
    sequence_number_column_name = 'Sequence Number'
    table_id_column_name = 'Table ID'

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    logger = logging.getLogger()

    # Connect to the sqlite3 databae
    conn = sqlite3.connect(os.path.join(base_dir, "test.db"))
    conn.isolation_level = None  # https://stackoverflow.com/a/23634805
    cur = conn.cursor()

    # Create the table and column tables
    cur.execute("BEGIN")
    cur.execute("""
        CREATE TABLE census_table_metadata (
            table_id varchar(10),
            table_title text,
            simple_table_title text,
            subject_area text,
            universe text,
            denominator_column_id varchar(16),
            topics text[],
            PRIMARY KEY (table_id)
        );
    """)
    cur.execute("""
        CREATE TABLE census_column_metadata (
            table_id varchar(10),
            line_number numeric(4,1),
            column_id varchar(16),
            column_title text,
            indent smallint,
            parent_column_id varchar(16),
            PRIMARY KEY (column_id)
        )
    """)
    cur.execute("""
        CREATE TABLE geoheader (
            fileid varchar(6),
            stusab varchar(2),
            sumlevel int,
            component varchar(2),
            logrecno int,
            us varchar(1),
            region varchar(1),
            division varchar(1),
            statece varchar(2),
            state varchar(2),
            county varchar(3),
            cousub varchar(5),
            place varchar(5),
            tract varchar(6),
            blkgrp varchar(1),
            concit varchar(5),
            aianhh varchar(4),
            aianhhfp varchar(5),
            aihhtli varchar(1),
            aitsce varchar(3),
            aits varchar(5),
            anrc varchar(5),
            cbsa varchar(5),
            csa varchar(3),
            metdiv varchar(5),
            macc varchar(1),
            memi varchar(1),
            necta varchar(5),
            cnecta varchar(3),
            nectadiv varchar(5),
            ua varchar(5),
            blank1 varchar(5),
            cdcurr varchar(2),
            sldu varchar(3),
            sldl varchar(3),
            blank2 varchar(6),
            blank3 varchar(3),
            zcta5 varchar(5),
            submcd varchar(5),
            sdelm varchar(5),
            sdsec varchar(5),
            sduni varchar(5),
            ur varchar(1),
            pci varchar(1),
            blank5 varchar(6),
            blank6 varchar(5),
            puma5 varchar(5),
            blank7 varchar(5),
            geoid varchar(40),
            name varchar(200),
            bttr varchar(6),
            btbg varchar(1),
            blank8 varchar(50),
            PRIMARY KEY (geoid)
        )
    """)
    logger.info("Created base tables")

    # Load table lookup
    table_infos = []
    with open(os.path.join(base_dir, table_number_lookup_filename), 'r') as f:
        r = csv.DictReader(f)
        lineno = 1

        for table_id, column_infos in groupby(r, lambda k: k.get(table_id_column_name)):

            column_infos = list(column_infos)
            table_info = {
                "table_id": table_id,
                "table_title": column_infos[0]['Table Title'],
                "subject_area": column_infos[0]['Subject Area'],
                "universe": column_infos[1]['Table Title'][11:],
            }
            cur.execute("""
                INSERT INTO census_table_metadata
                    (table_id, table_title, subject_area, universe)
                VALUES (:table_id, :table_title, :subject_area, :universe)
            """, table_info)

            lineno += 2

            columns = []
            for column_info in column_infos[2:]:
                lineno += 1
                if column_info['Line Number'].endswith('.5'):
                    # Skip over 'median' labels
                    # print("Skipping median line", lineno)
                    continue
                elif column_info['Line Number'] == '':
                    # Skip over blank line numbers that happen when a table spans multiple sequence numbers
                    # print("Skipping blank line", lineno)
                    continue

                columns.append({
                    "sequence_number": int(column_info['Sequence Number']),
                    "table_id": table_id,
                    "line_number": int(column_info['Line Number']),
                    "column_id": "%s%03d" % (table_id, int(column_info['Line Number'])),
                    "column_title": column_info['Table Title'],
                })

            cur.executemany("""
                INSERT INTO census_column_metadata
                    (table_id, line_number, column_id, column_title)
                VALUES (:table_id, :line_number, :column_id, :column_title)
            """, columns)

            table_info['columns'] = columns
            table_infos.append(table_info)
    logger.info("Imported table and column metadata")

    # Load geography data
    geoid_mapping = {}
    g_files_read = set()
    for dirpath, dirnames, filenames in os.walk(base_dir):
        for filename in filenames:
            if not (filename.startswith('g') and filename.endswith('.csv')):
                continue
            if filename in g_files_read:
                continue

            lines = []
            with open(os.path.join(dirpath, filename), 'r', encoding='latin-1') as f:
                for line in csv.reader(f):
                    lines.append(line)

                    # Map (stusab, logrecno) -> geoid
                    # We'll use this mapping while loading sequence tables to insert the geoid for each row.
                    geoid_mapping[(line[1].lower(), line[4])] = line[48]

                cur.executemany("""
                    INSERT INTO geoheader
                        (fileid, stusab, sumlevel, component, logrecno, us, region, division, statece, state, county, cousub, place, tract, blkgrp, concit, aianhh, aianhhfp, aihhtli, aitsce, aits, anrc, cbsa, csa, metdiv, macc, memi, necta, cnecta, nectadiv, ua, blank1, cdcurr, sldu, sldl, blank2, blank3, zcta5, submcd, sdelm, sdsec, sduni, ur, pci, blank5, blank6, puma5, blank7, geoid, name, bttr, btbg, blank8)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, lines)
            g_files_read.add(filename)
    logger.info("Imported geoheader data")

    # Create sequence tables
    with open(os.path.join(base_dir, table_number_lookup_filename), 'r') as f:
        for sqn, lines in groupby(csv.DictReader(f), lambda k: k.get(sequence_number_column_name)):
            sqn = int(sqn)
            lines = list(lines)

            # Estimates for the sequence
            cols = []
            for line in lines:
                if line.get('Line Number') and '.' not in line['Line Number']:
                    cols.append("%s%03d double precision" % (line[table_id_column_name], int(line['Line Number'])))
            column_text = ",\n".join(cols)

            cur.execute("""
                CREATE TABLE seq%04d (
                    fileid varchar(6),
                    filetype varchar(6),
                    stusab varchar(2),
                    chariter varchar(3),
                    seq varchar(4),
                    logrecno int,
                    geoid varchar(40),
                    %s,
                    PRIMARY KEY(geoid)
                )""" % (sqn, column_text))

            # Measurement of Error for the sequence
            cols = []
            for line in lines:
                if line.get('Line Number') and '.' not in line['Line Number']:
                    cols.append("%s%03d_moe double precision" % (line[table_id_column_name], int(line['Line Number'])))
            column_text = ",\n".join(cols)

            cur.execute("""
                CREATE TABLE seq%04d_moe (
                    fileid varchar(6),
                    filetype varchar(6),
                    stusab varchar(2),
                    chariter varchar(3),
                    seq varchar(4),
                    logrecno int,
                    geoid varchar(40),
                    %s,
                    PRIMARY KEY(geoid)
                )""" % (sqn, column_text))
    logger.info("Created sequence tables")

    # Create views for individual tables
    for table_info in table_infos:
        # For just estimates
        sql = "CREATE VIEW %s AS SELECT geoid, " % table_info['table_id']

        sequences = []
        columns = []
        for column_info in table_info['columns']:
            columns.append(column_info['column_id'])
            sequence_table_name = 'seq%04d' % column_info['sequence_number']
            if sequence_table_name not in sequences:
                sequences.append(sequence_table_name)

        sql += ', '.join(columns)

        sql += ' FROM %s' % sequences[0]

        if len(sequences) > 1:
            for sqn in sequences[1:]:
                sql += ' JOIN %s USING (geoid)' % sqn

        cur.execute(sql)

        # For estimates + moe combined in a single view
        sql = "CREATE VIEW %s_moe AS SELECT geoid, " % table_info['table_id']

        sequences = []
        columns = []
        for column_info in table_info['columns']:
            columns.append(column_info['column_id'])
            columns.append(column_info['column_id'] + "_moe")

            sequence_table_name = 'seq%04d' % column_info['sequence_number']
            if sequence_table_name not in sequences:
                sequences.append(sequence_table_name)
                sequences.append(sequence_table_name + "_moe")

        sql += ', '.join(columns)

        sql += ' FROM %s' % sequences[0]

        if len(sequences) > 1:
            for sqn in sequences[1:]:
                sql += ' JOIN %s USING (geoid)' % sqn

        cur.execute(sql)
    logger.info("Created table views")

    # Load estimate and MOE data
    for dirpath, dirnames, filenames in os.walk(base_dir):
        for filename in sorted(filenames):
            if filename.startswith('e') and filename.endswith('.txt'):
                table_name_template = "seq%04d"
            elif filename.startswith('m') and filename.endswith('.txt'):
                table_name_template = 'seq%04d_moe'
            else:
                continue

            full_path = os.path.join(dirpath, filename)
            logger.info("Importing data from %s", full_path)
            with open(full_path, 'r') as f:
                rows = []
                for line in csv.reader(f):
                    sqn = int(line[4])
                    rows.append((
                        line[0],  # fileid
                        line[1],  # filetype
                        line[2],  # stusab
                        line[3],  # chariter
                        line[4],  # seq
                        line[5],  # logrecno
                        geoid_mapping[(line[2], line[5])],  # geoid
                        *map(nullify, line[6:]),  # actual data...
                    ))

                if not rows:
                    continue

                table_name = table_name_template % sqn
                n_data_columns = len(rows[0])
                column_text = ", ".join("?" * n_data_columns)

                cur.executemany("INSERT INTO %s VALUES (%s)" % (table_name, column_text), rows)

    cur.execute("COMMIT")
    cur.close()
    conn.close()
    logger.info("Done")


if __name__ == "__main__":
    main()
