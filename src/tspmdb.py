#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#
#     TSPM-DB Library - A multiprocessing implementation of the TSPM algorithm using Sqlite3
#     Copyright (C) 2026  Nick Benik <nbenik@gmail.com>
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU Affero General Public License as published
#     by the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

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

from DDLFunctions import Create_Base_DB
from DDLFunctions import Create_SEQUENCES
from tspmdb_workers import worker_SequenceGeneration
from Dataset import Dataset
from Subpopulation import Subpopulation
from Population import Population

class TspmDB:
    """Core object for DB-based TSPM calculations"""

    # ========================================================================================
    def __init__(self, dbfile, destructive=False, parallel_threads=False, reuse_cache=False, max_memory_mb=512):
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
        Create_Base_DB(self.conn, self.destructive)

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

        # additional setup
        self._subpopulation = Subpopulation(self)
        self._dataset = Dataset(self)

    # ========================================================================================
    @property
    def population(self):
        """ returns a specialized subpopulation object that represents all entries in the database """
        # TsmpDb.population.identifier
        # TsmpDb.population.description
        # TsmpDb.population.help()
        # TsmpDb.population.patients()
        # TsmpDb.population.sequences()
        # TsmpDb.population.frequencies()
        # TsmpDb.population.events()
        return Population(self)
        pass


    # ========================================================================================
    @property
    def subpopulation(self):
        """ returns the subpopulation management object """
        # TsmpDb.subpopulation.list()
        # TsmpDb.subpopulation.create()
        # TsmpDb.subpopulation.delete()
        # TsmpDb.subpopulation.get()
        return self._subpopulation

    # ========================================================================================
    @property
    def dataset(self):
        """ returns the dataset management object """
        # TsmpDb.dataset.ingest(cvs=, zipfile=, dataframe=)
        # TsmpDb.dataset.clear()
        # TsmpDb.dataset.calculate(temporal_buckets=, sparsity_threshold=)
        return self._dataset


    # ========================================================================================
    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


    # ========================================================================================
    def help(self):
        print("[HELP] Main TspmDb Object")
        print("tspmdb = TspmDb(dbfile=str, [destructive=boolean, workers=int, max_memory_mb=int])")
        print("------------------------------------------------------------------------------")
        print("tspmdb.help()         Displays this help message")
        print("tspmdb.population     Manage/retreve data of entire population")
        print("tspmdb.subpopulation  Operations to manage/retrieve subpopulations data")
        print("tspmdb.dataset        Operations to manage the dataset")
        print("tspmdb.close()        Close the dataset and release resources")

    # ========================================================================================
    def ingest_sqlite(self, dbfile: str, query: str, colnames: dict, batch_size=10000, rebuild : bool = False):
        """used to ingest a sqlite3 database file containing data"""
        raise Exception()
        pass


    # ========================================================================================
    def generate_sequences_parallel(self, table_name: str = "", rebuild: bool = False, sparsity_threshold: float = 0.05):
        table_names = {
            "SEQ": table_name
        }
        if len(table_names["SEQ"]) < 3:
            table_names["SEQ"] = 'seq_optimized'
        Create_SEQUENCES(self.conn, table_names["SEQ"])

        # Refresh our query plan statistics
        self.conn.execute("PRAGMA OPTIMIZE")

        # create temp db directory
        temp_dir = tempfile.mkdtemp(prefix="tspmdb-")

        # create the queue and processes
        patientlist_queue = multiprocessing.Queue()
        temp_mem_limit = int(self.memory_limit / self.max_cpu_core)
        process_list = []
        temp_db_list = []
        # for process_id in range(0, 1):
        for process_id in range(1, self.max_cpu_core):
            tempdb = os.path.join(temp_dir, f"seq_gen_{process_id}.sqlite")
            temp_db_list.append(tempdb)
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
        Create_SEQUENCES(self.conn, table_names["SEQ"])

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
