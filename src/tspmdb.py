import os.path
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import wait
from io import TextIOWrapper
import sqlite3
# import pandas as pd
import csv
from zipfile import ZipFile
import os
import tempfile

from tspmdb_workers import worker_SequenceGeneration

class TspmDB:
    """Core object for DB-based TSPM calculations"""

    # ========================================================================================
    def __init__(self, dbfile, destructive=False, parallel_threads=False, reuse_cache=True, max_memory_mb=512):
        """create the TSPM database"""

        # handle the db file/connection
        if os.path.exists(dbfile):
            if destructive is True:
                os.remove(dbfile)
            else:
                if reuse_cache is not True:
                    raise FileExistsError

        self.destructive = destructive
        self.reuse_cache = reuse_cache
        self.db = dbfile
        self.conn = sqlite3.connect(dbfile)
        self.conn.row_factory = sqlite3.Row
        # optimize the DB
        self.conn.execute("PRAGMA locking_mode=NORMAL")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA temp_store=FILE")
#        self.conn.execute("PRAGMA mmap_size=4294967296") # 4GB of mmap for DB file
        self.conn.execute("PRAGMA page_size=4096") # 4k page size (usually an SSD's block size)
        self.memory_limit = int(max_memory_mb * 1048576) # in bytes
        cache_size_in_pages = int(self.memory_limit / 4096)
        self.conn.execute("PRAGMA cache_size=" + str(cache_size_in_pages)) # cache is in number of DB pages

        self.cache_patients = {}
        self.cache_obs = {}

        # create the tables if needed
        self._create_db(self.conn)

        # handle cpu usage
        cpu_count = multiprocessing.cpu_count()
        if parallel_threads is False:
            if cpu_count is None:
                self.max_cpu_core = 1
            else:
                self.max_cpu_core = cpu_count
        else:
            threads = int(parallel_threads)
            if cpu_count is None:
                self.max_cpu_core = 1
            else:
                if threads <= cpu_count:
                    self.max_cpu_core = threads
                else:
                    self.max_cpu_core = cpu_count

    # ========================================================================================
    def close(self):
        self.conn.commit()
        self.conn.close()


    # ========================================================================================
    def ingest_csv(self, csvfile: str, colnames: list, zipfile: str = None, batch_size: int = 10000, show_progress: bool = True, rebuild : bool = False):
        """used to ingest a csv file containing data"""
        # make sure we have required colnames defined
        if "PATIENT" not in colnames:
            raise KeyError
        if "DATE" not in colnames:
            raise KeyError
        if "CODE" not in colnames:
            raise KeyError

        # handle files and get a csvDictReader running
        if zipfile is not None:
            if not os.path.exists(zipfile):
                raise FileNotFoundError
            else:
                data_zip = ZipFile(zipfile, 'r')
                fp = data_zip.open(csvfile, 'r')
                csvreader = csv.DictReader(TextIOWrapper(fp, 'utf-8'))
        else:
            if not os.path.exists(csvfile):
                raise FileNotFoundError
            else:
                fp = open(csvfile, 'r')
                csvreader = csv.DictReader(fp)
                pass

        # got the CSV reader... ingest the data
        db_cur = self.conn.cursor()
        lookup_patients_data = {}
        lookup_codes_data = {}
        insert_batch = []
        inserted_row_count = 0
        patient_num = 0
        code_num = 0

        # but first make sure expected columns exist
        if not colnames["PATIENT"] in csvreader.fieldnames:
            raise KeyError
        if not colnames["DATE"] in csvreader.fieldnames:
            raise KeyError
        if not colnames["CODE"] in csvreader.fieldnames:
            raise KeyError
        if not colnames["TEXT"] in csvreader.fieldnames:
            # the optional TEXT column does not exist in the csv file, do not use it
            del colnames["TEXT"]


        # and load the existing lookup tables' data
        def load_patient_ids():
            patient_num = 0
            results = db_cur.execute("SELECT patient_num, patient_id FROM lookup_patients ORDER BY patient_num ASC")
            for row in results:
                temp_num = int(row['patient_num'])
                self.cache_patients[row['patient_id']] = temp_num
                patient_num = temp_num
            return patient_num

        if self.reuse_cache == True:
            if self.cache_patients is None:
                patient_num = load_patient_ids()
        else:
            self.cache_patients = {}
            patient_num = load_patient_ids()

        def load_obs_ids():
            results = db_cur.execute("SELECT obs_code, obs_code_id, obs_description FROM lookup_observations ORDER BY obs_code_id ASC")
            for row in results:
                temp_num = int(row['obs_code_id'])
                self.cache_obs[row['obs_code']] = {
                    "num": temp_num,
                    "text": row['obs_description'].split(",\n")
                }
                code_num = temp_num
            return code_num

        if self.reuse_cache == True:
            if self.cache_obs is None:
                code_num = load_obs_ids()
        else:
            self.cache_obs = {}
            code_num = load_obs_ids()

        # ingest the data
        for row in csvreader:
            # handle patient lookup
            current_row_patient_data = row[colnames["PATIENT"]]
            if current_row_patient_data not in self.cache_patients:
                patient_num += 1
                current_patients_id = patient_num
                self.cache_patients[current_row_patient_data] = patient_num
            else:
                current_patients_id = self.cache_patients[current_row_patient_data]

            # handle code lookup
            current_row_code_data = row[colnames["CODE"]]
            if current_row_code_data not in lookup_codes_data:
                code_num += 1
                current_patients_code = code_num
                lookup_codes_data[current_row_code_data] = {
                    "num": code_num,
                    "text": []
                }
                if "TEXT" in colnames:
                    for line in row[colnames["TEXT"]].split(",\n"):
                        lookup_codes_data[current_row_code_data]["text"].append(line)
            else:
                current_patients_code = lookup_codes_data[current_row_code_data]["num"]
                if "TEXT" in colnames:
                    current_row_text_data = row[colnames["TEXT"]]
                    # add the code description if it is not yet saved
                    if current_row_text_data not in lookup_codes_data[current_row_code_data]["text"]:
                        lookup_codes_data[current_row_code_data]["text"].append(current_row_text_data)

            # get the observation date
            current_patients_date = row[colnames["DATE"]]

            # save the entry
            insert_batch.append((current_patients_id, current_patients_code, current_patients_date))
            inserted_row_count += 1
            if len(insert_batch) >= batch_size:
                db_cur.executemany("INSERT INTO source_data (patient_num, obs_code, obs_date) VALUES (?,?,?)", insert_batch)
                self.conn.commit()
                insert_batch = []
                if show_progress:
                    print("Inserted row #: " + str(inserted_row_count))

        # commit the last batch of records
        db_cur.executemany("INSERT INTO source_data (patient_num, obs_code, obs_date) VALUES (?,?,?)", insert_batch)
        self.conn.commit()

        # save the patient lookup table
        patient_id_rows = list(self.cache_patients.items())
        db_cur.executemany("INSERT OR IGNORE INTO lookup_patients (patient_id, patient_num) VALUES (?,?)", patient_id_rows)
        self.conn.commit()

        # save the code lookup table
        patient_code_rows = []
        for code in lookup_codes_data:
            text_entry = ",\n".join(lookup_codes_data[code]["text"])
            patient_code_rows.append((lookup_codes_data[code]["num"], code, text_entry))
        db_cur.executemany("INSERT OR IGNORE INTO lookup_observations (obs_code_id, obs_code, obs_description) VALUES (?,?,?)", patient_code_rows)
        self.conn.commit()


    # ========================================================================================
    def ingest_sqlite(self, dbfile: str, query: str, colnames: dict, batch_size=10000, rebuild : bool = False):
        """used to ingest a sqlite3 database file containing data"""
        raise Exception()
        pass


    # ========================================================================================
    def generate_sequences_parallel(self, table_name : str = "", rebuild : bool = False):
        table_names = {
            "SEQ": table_name
        }
        if len(table_names["SEQ"]) < 3:
            table_names["SEQ"] = 'seq_optimized'
        self._create_seq_table(self.conn, table_names["SEQ"])

        # Refresh our query plan statistics
        self.conn.execute("PRAGMA OPTIMIZE")

        # create temp db directory
        temp_dir = tempfile.mkdtemp(prefix="tspmdb-")

        # create the queue and processes
        patientlist_queue = multiprocessing.Queue()
        temp_mem_limit = int(self.memory_limit / self.max_cpu_core)
        process_list = []
        # for process_id in range(0, 1):
        for process_id in range(1, self.max_cpu_core):
            tempdb = os.path.join(temp_dir, f"seq_gen_{process_id}.sqlite")
            p = multiprocessing.Process(target=worker_SequenceGeneration, args=(self.db, tempdb, patientlist_queue, temp_mem_limit, table_names["SEQ"]))
            # process_list.append((self.db, tempdb, patientlist_queue, temp_mem_limit, table_names["SEQ"]))
            process_list.append(p)

        # populate the queue
        cur = self.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients")
        id_list = []
        while True:
            patient_ids = cur.fetchmany(1000)
            if not patient_ids:
                break
            id_list = []
            for row in patient_ids:
                id_list.append(row[0])
            patientlist_queue.put(id_list)
        del patient_ids
        del id_list

        # start the processes
        for proc in process_list:
            proc.start()

        # wait for the processes to finish
        for proc in process_list:
            proc.join()
            proc.close()

        # close the queue
        patientlist_queue.close()

        # create the sequence index AFTER we populate the table
        cur.execute(f"""
            CREATE INDEX idx_{table_names["SEQ"]} ON {table_names["SEQ"]} (
                obs_code_1 ASC,
                obs_code_2 ASC,
                temporal_distance ASC
            );
            """)
        cur.connection.commit()

        # clean up the temp folder
        try:
            os.rmdir(temp_dir)
        except OSError:
            pass

    # ========================================================================================
    def generate_sequences(self, table_name : str = "", rebuild : bool = False):
        table_names = {
            "SEQ": table_name
        }
        if len(table_names["SEQ"]) < 3:
            table_names["SEQ"] = 'seq_optimized'
        self._create_seq_table(self.conn, table_names["SEQ"])

        # handle buckets
        temporal_SQL = "CAST(julianday(t2.occurred_on) - julianday(t1.occurred_on) AS INTEGER) AS time_diff"

        # build the sequence table
        build_SQL = f"""INSERT INTO {table_names["SEQ"]} (patient_num, obs_code_1, obs_code_2, temporal_distance)
           WITH subquery (patient, code, occurred_on) AS (
             SELECT patient_num, obs_code, MIN(obs_date)
             FROM source_data
             GROUP BY patient_num, obs_code
             ORDER BY patient_num, obs_code,MIN(obs_date)
           )
           SELECT
             t1.patient, t1.code, t2.code,
             {temporal_SQL}
           FROM
             subquery AS t1
             JOIN subquery AS t2 ON (t1.patient = t2.patient)
           WHERE
             t1.occurred_on <= t2.occurred_on
             AND t1.code != t2.code;"""

        # Refresh our query plan statistics
        self.conn.execute("PRAGMA OPTIMIZE")

        # execute
        db_cur = self.conn.cursor()
        timer_start = time.perf_counter()

        db_cur.execute(build_SQL)
        db_cur.connection.commit()

        # create the index
        db_cur.execute(f"""
            CREATE INDEX idx_{table_names["SEQ"]} ON {table_names["SEQ"]} (
                obs_code_1 ASC,
                obs_code_2 ASC,
                temporal_distance ASC
            );
            """)

        timer_end = time.perf_counter()
        # print(f"Elapsed time: {timer_end-timer_start} seconds")


    # ========================================================================================
    def get_sequences(self, temporal_buckets : list = [], table_name : str = "", pandas : bool = False, with_names : bool = False):
        """used to generate temporal sequences into a table and return the results"""
        table_names = {
            "SEQ": table_name
        }
        if len(table_names["SEQ"]) < 3:
            table_names["SEQ"] = 'seq_optimized'

        # see if the correct table name is given
        cur = self.db_conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [name[0] for name in cur.fetchall()]
        # create table if it is missing
        if table_names["SEQ"] not in tables:
            raise NameError(f"given sequence table (\"{table_names['SEQ']}\") does not exist")

        # create temporal buckets
        if len(temporal_buckets) == 0:
            temporal_SQL = "temporal_distance"
        else:
            temporal_SQL = "CASE\n"
            bucket_num = 0
            for bucket in temporal_buckets:
                bucket_num += 1
                temporal_SQL += "WHEN temporal_distance BETWEEN " + str(bucket[0]) + " AND " + str(bucket[1]) + " THEN " + str(bucket_num) + "\n"
            temporal_SQL += "ELSE 0\n"
            temporal_SQL += "END AS temporal_distance"

        # build the select statement
        if with_names is False:
            select_SQL = f"""
                SELECT patient_id, obs1.obs_code AS obs_code_1, obs2.obs_code AS obs_code_2, 
                {temporal_SQL}
                FROM {table_names["SEQ"]} seq
                JOIN lookup_observations obs1 ON (seq.obs_code_1 = obs1.obs_code_id)
                JOIN lookup_observations obs2 ON (seq.obs_code_2 = obs2.obs_code_id)
                JOIN lookup_patients pat ON (seq.patient_num = pat.patient_num)
            """
        else:
            select_SQL = f"""
                SELECT patient_id, 
                      obs1.obs_code AS obs_code_1,
                      obs1.obs_description AS obs_name_1,
                      obs2.obs_code AS obs_code_2, 
                      obs2.obs_description AS obs_name_2,
                {temporal_SQL}
                FROM {table_names["SEQ"]} seq
                JOIN lookup_observations obs1 ON (seq.obs_code_1 = obs1.obs_code_id)
                JOIN lookup_observations obs2 ON (seq.obs_code_2 = obs2.obs_code_id)
                JOIN lookup_patients pat ON (seq.patient_num = pat.patient_num)
            """

        # retrieve the data
        if pandas is True:
            return pd.read_sql_query(select_SQL, self.db_conn)
        else:
            cur.execute(select_SQL)
            return cur.fetchall()


    # ========================================================================================
    def get_sequence_frequencies(self, temporal_buckets : list = [], table_name : str = "", seq_table : str = "", pandas : bool = False, with_names : bool = False):
        """used to generate temporal sequence frequencies into a table and return the results"""
        table_names = {
            "SEQ": seq_table,
            "FREQ": table_name
        }
        if len(table_names["SEQ"]) < 3:
            table_names["SEQ"] = 'seq_optimized'
        if len(table_names["FREQ"]) < 3:
            table_names["FREQ"] = 'calc_seq_freq'

        # see if the correct table name is given
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [name[0] for name in cur.fetchall()]
        # create table if it is missing
        if table_names["SEQ"] not in tables:
            raise NameError(f"given sequence table (\"{table_names['SEQ']}\") does not exist")

        self._create_seq_freq_table(self.conn, table_names["FREQ"])

        # create temporal buckets
        if len(temporal_buckets) == 0:
            temporal_SQL = "temporal_distance AS temporal_bucket,"
        else:
            temporal_SQL = "CASE\n"
            bucket_num = 0
            for bucket in temporal_buckets:
                bucket_num += 1
                temporal_SQL += "WHEN temporal_distance BETWEEN " + str(bucket[0]) + " AND " + str(bucket[1]) + " THEN " + str(bucket_num) + "\n"
            temporal_SQL += "ELSE 0\n"
            temporal_SQL += "END AS temporal_bucket,"

        # build the select statement
        build_SQL = f"""
            INSERT INTO {table_names["FREQ"]} (obs_code_1, obs_code_2, temporal_bucket, patients)
            WITH sub_count(code1, code2, temporal_distance, subcount) AS (
                SELECT 
                      seq.obs_code_1,
                      seq.obs_code_2,
                      temporal_distance,
                      COUNT(patient_num)
                FROM seq_optimized seq
                GROUP BY seq.obs_code_1, seq.obs_code_2, temporal_distance
            )
            SELECT 
                code1 AS obs_code_1,
                code2 AS obs_code_2,
                {temporal_SQL}
                SUM(subcount) AS patient_cnt
            FROM sub_count
            GROUP BY code1, code2, temporal_bucket;
        """
        cur.execute(build_SQL)
        self.conn.commit()

        return True

        # retrieve the data
        if with_names is True:
            select_SQL = f"""
                SELECT
                    seq_freq.obs_code_1,
                    obs1.obs_code AS obs_code_1,
                    obs1.obs_description AS obs_description_1,
                    seq_freq.obs_code_2,
                    obs2.obs_code AS obs_code_2,
                    obs2.obs_description AS obs_description_2,
                    seq_freq.temporal_bucket AS temporal_bucket,
                    patients AS patient_count
                FROM {table_names["FREQ"]} AS seq_freq
                JOIN lookup_observations AS obs1 ON (seq_freq.obs_code_1 = obs1.obs_code_id)
                JOIN lookup_observations AS obs2 ON (seq_freq.obs_code_2 = obs2.obs_code_id)
            """
        else:
            select_SQL = f"""
                SELECT
                    seq_freq.obs_code_1,
                    obs1.obs_code AS obs_code_1,
                    seq_freq.obs_code_2,
                    obs2.obs_code AS obs_code_2,
                    seq_freq.temporal_bucket AS temporal_bucket,
                    patients AS patient_count
                FROM {table_names["FREQ"]} AS seq_freq
                JOIN lookup_observations AS obs1 ON (seq_freq.obs_code_1 = obs1.obs_code_id)
                JOIN lookup_observations AS obs2 ON (seq_freq.obs_code_2 = obs2.obs_code_id)
            """

        if pandas is True:
            return pd.read_sql_query(select_SQL, self.db_conn)
        else:
            cur.execute(select_SQL)
            return cur.fetchall()




    # ========================================================================================
    # ----------------------------------------------------------------------------------------
    def _create_seq_freq_table(self, db_conn, freq_table):
        if not isinstance(db_conn, sqlite3.Connection):
            raise SyntaxError("database connection was not passed")
        if len(freq_table) < 3:
            raise SyntaxError("sequence frequency table name is to short")

        cur = db_conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [name[0] for name in cur.fetchall()]

        # create table if it is missing
        if freq_table in tables:
            if self.destructive is not True:
                raise NameError("sequence frequency table already exists (and destructive option not selected)")
            else:
                cur.execute(f"DELETE FROM {freq_table};")
        else:
            cur.execute(f"""
                CREATE TABLE {freq_table} (
                    obs_code_1       INTEGER     NOT NULL,
                    obs_code_2       INTEGER     NOT NULL,
                    temporal_bucket  INTEGER NOT NULL,
                    patients         INTEGER NOT NULL
                );
            """)
            cur.execute(f"""
                CREATE INDEX idx_{freq_table} ON {freq_table} (
                    obs_code_1      ASC,
                    obs_code_2      ASC,
                    temporal_bucket ASC
                );
                """)
            db_conn.commit()

    # ----------------------------------------------------------------------------------------
    def _create_seq_table(self, db_conn, tablename):
        if not isinstance(db_conn, sqlite3.Connection):
            raise SyntaxError("database connection was not passed")
        if len(tablename) < 3:
            raise SyntaxError("sequence table name is to short")

        cur = db_conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [name[0] for name in cur.fetchall()]

        # create table if it is missing
        if tablename in tables:
            if self.destructive is not True:
                raise NameError("sequence table already exists (and destructive option not selected)")
            else:
                cur.execute(f"DELETE FROM {tablename};")
                cur.execute(f"DROP INDEX IF EXISTS {tablename};")
        else:
            cur.execute(f"""
                CREATE TABLE {tablename} (
                    patient_num INTEGER     NOT NULL,
                    obs_code_1  INTEGER     NOT NULL,
                    obs_code_2  INTEGER     NOT NULL,
                    temporal_distance   INTEGER NOT NULL
                );
            """)
        db_conn.commit()

    # ----------------------------------------------------------------------------------------
    def _create_db(self, db_conn):
        if not isinstance(db_conn, sqlite3.Connection):
            raise SyntaxError("database connection was not passed")

        cur = db_conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [name[0] for name in cur.fetchall()]

        if "lookup_patients" not in tables:
            cur.execute("""
                CREATE TABLE lookup_patients (
                    patient_num INTEGER PRIMARY KEY,
                    patient_id  TEXT    UNIQUE NOT NULL
                );
            """)
            db_conn.commit()

        if "lookup_observations" not in tables:
            cur.execute("""
                CREATE TABLE lookup_observations (
                    obs_code_id     INTEGER PRIMARY KEY,
                    obs_code        TEXT    UNIQUE NOT NULL,
                    obs_description TEXT
                );
            """)
            db_conn.commit()

        if "source_data" not in tables:
            cur.execute("""
                CREATE TABLE source_data (
                    patient_num INTEGER NOT NULL,
                    obs_code    INTEGER NOT NULL,
                    obs_date    DATE    NOT NULL
                );
            """)
            cur.execute("""
                CREATE INDEX idx_source_data ON source_data (
                    patient_num ASC,
                    obs_code ASC,
                    obs_date ASC
                );
            """)
            db_conn.commit()
