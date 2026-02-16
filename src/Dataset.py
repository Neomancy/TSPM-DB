import sqlite3
import tempfile
import os
import multiprocessing
from pandas import DataFrame

from zipfile import ZipFile
import csv
from io import TextIOWrapper
from pathlib import Path, PurePath

import tspmdb
from tspmdb_workers import worker_SequenceGeneration_Step_1, worker_SequenceGeneration_Step_2, worker_SequenceGeneration_Step_3
from DDLFunctions import Create_SEQUENCES, Index_SEQUENCES, Create_FREQUENCIES, Create_Base_DB

class Dataset:

    def __init__(self, tspmdb_ref, memory_limit: int = 0, worker_limit: int = 1):
        self._parent = tspmdb_ref
        self.conn = tspmdb_ref.conn
        self.db = tspmdb_ref.db
        self.memory_limit = tspmdb_ref.memory_limit
        self.worker_limit = tspmdb_ref.max_cpu_core
        self.cache_patients = None
        self.cache_obs = None
        self.destructive = tspmdb_ref.destructive


    # ========================================================================================
    def help(self):
        print("[HELP] TspmDb.Dataset Object")
        print("TspmDb().dataset")
        print("------------------------------------------------------------------------------")
        print(".help()       Displays this help message")
        print(".clear()      Clears all data from the dataset")
        print(".ingest()     Ingests data from CSV/ZIP+CSV files and pandas dataframes")
        print(".calculate()  (Re)calculates all sequences and frequencies")


    # ========================================================================================
    def clear(self, confirm: str = None):
        """ Clears all data from the dataset """
        self.conn.execute('DROP TABLE IF EXISTS source_data;')
        self.conn.execute('DROP TABLE IF EXISTS lookup_patients;')
        self.conn.execute('DROP TABLE IF EXISTS lookup_observations;')
        self.conn.execute('DROP TABLE IF EXISTS subpopulations;')
        self.conn.execute('DROP TABLE IF EXISTS subpopulation_patients;')
        self.conn.execute('DROP TABLE IF EXISTS seq_optimized;')
        Create_Base_DB(self.conn, destructive=self.destructive)
        self.conn.execute("PRAGMA OPTIMIZE")
        self.cache_patients = None
        self.cache_obs = None

    # ========================================================================================
    def ingest(self, data, col_names: list, zip_file: str = None, show_progress: bool = True):
        """ Ingests data from CSV/ZIP+CSV files """
        if isinstance(data, DataFrame):
            # a DataFrame was passed as data
            print("TODO: Import data from dataframe")
        else:
            # we are assuming that a filename was passed as an input
            if zip_file is None:
                self._ingest_csv(data, col_names, show_progress=show_progress)
            else:
                self._ingest_csv(data, col_names, zip_file, show_progress=show_progress)


    # ========================================================================================
    def calculate(self, temporal_buckets: list = [], sparsity_threshold: float = 0.05, temporal_mode = "DAYS"):
        """ Calculates all sequences and frequencies using the specified temporal buckets """
        if "DAY".casefold() in temporal_mode.casefold():
            temporal_mode = "DAYS"
        else:
            temporal_mode = "HOURS"

        # create / truncate tables
        Create_SEQUENCES(self.conn, destructive=True)
        Create_FREQUENCIES(self.conn, destructive=True)

        # Refresh our query plan statistics
        self.conn.execute("PRAGMA OPTIMIZE")

        # --- [CREATE SEQUENCES] -----------------------------------------------------
        # get main db filepathname
        target_path = Path(self.db).resolve()
        # create the queue and processes
        patientlist_queue = multiprocessing.Queue()
        temp_mem_limit = int(self.memory_limit / self.worker_limit)
        process_list = []
        temp_db_list = []
        total_patients = 0

        # for process_id in range(0, 1):
        for process_id in range(1, self.worker_limit):
            tempdb = Path(str(target_path.parent / target_path.stem) + '-sequences-' + str(process_id) + target_path.suffix)
            # if file exists then handle (error or delete)
            if tempdb.is_file():
                if not self.destructive:
                    raise FileExistsError
                tempdb.unlink()

            tempdb = str(tempdb)
            temp_db_list.append(tempdb)
            p = multiprocessing.Process(target=worker_SequenceGeneration_Step_1, args=(self.db, tempdb, patientlist_queue, temp_mem_limit, temporal_buckets))
            process_list.append(p)

        # populate the queue
        cur = self.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients ORDER BY patient_num ASC")
        id_list = []
        while True:
            patient_ids = cur.fetchmany(1000)
            if not patient_ids:
                break
            id_list = []
            for row in patient_ids:
                id_list.append(row[0])
                total_patients += 1
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

        # close the queue and cleanup
        patientlist_queue.close()

        # --- [SUM THE FREQUENCIES ACROSS ALL TEMP DBs] -----------------------------------------------------
        process_list = []
        for tempdb in temp_db_list:
            p = multiprocessing.Process(target=worker_SequenceGeneration_Step_2, args=(self.db, tempdb, temp_mem_limit, temporal_buckets))
            process_list.append(p)

        # start the processes
        for proc in process_list:
            proc.start()
        # wait for the processes to finish
        for proc in process_list:
            proc.join()
            proc.close()

        # --- [FILTER THE FREQUENCIES IN THE MAIN DB BY SPARSITY] -----------------------------------------------------
        cur.execute(f"""
            DELETE FROM frequencies 
            WHERE CAST(patient_cnt AS float) / {total_patients} < {sparsity_threshold}
        """)
        self.conn.commit()

        # --- [COPY OVER THE SEQUENCES OF INTEREST (as determined by the sparsity threshold)] --------------------------
        process_list = []
        for tempdb in temp_db_list:
            p = multiprocessing.Process(target=worker_SequenceGeneration_Step_3, args=(self.db, tempdb, temp_mem_limit, sparsity_threshold, total_patients))
            process_list.append(p)

        # start the processes
        for proc in process_list:
            proc.start()
        # wait for the processes to finish
        for proc in process_list:
            proc.join()
            proc.close()

        # --- [CLEAN UP THE TEMPORARY DATABASES] -----------------------------------------------------
        for tempdb in temp_db_list:
            os.remove(tempdb)

        # --- [CREATE THE INDEX ON THE SEQUENCES TABLE] -----------------------------------------------------
        Index_SEQUENCES(self.conn)

        # Refresh our query plan statistics
        self.conn.execute("PRAGMA OPTIMIZE")


    # ========================================================================================
    def _calculate_sequences(self, temporal_buckets: list = [], sparsity_threshold: float = 0.05, destructive: bool = False):
        pass

        # # create the sequence index AFTER we populate the table
        # cur.execute(f"""
        #     CREATE INDEX idx_{table_name} ON {table_name} (
        #         obs_code_1 ASC,
        #         obs_code_2 ASC,
        #         temporal_distance ASC
        #     );
        #     """)
        # cur.connection.commit()

        # # clean up the temp folder
        # try:
        #     os.rmdir(temp_dir)
        # except OSError:
        #     pass



# ======================================================================================================================
    def _ingest_csv(self, csvfile: str, colnames: list, zipfile: str = None, batch_size: int = 10000, show_progress: bool = True):
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
        if self.cache_patients is None:
            patient_num = 0
            self.cache_patients = {}
            results = db_cur.execute("SELECT patient_num, patient_id FROM lookup_patients ORDER BY patient_num ASC")
            for row in results:
                temp_num = int(row['patient_num'])
                self.cache_patients[row['patient_id']] = temp_num
                patient_num = temp_num
        else:
            self.cache_patients = {}
            patient_num = 0

        if self.cache_obs is None:
            code_num = 0
            self.cache_obs = {}
            results = db_cur.execute("SELECT obs_code, obs_code_id, obs_description FROM lookup_observations ORDER BY obs_code_id ASC")
            for row in results:
                temp_num = int(row['obs_code_id'])
                self.cache_obs[row['obs_code']] = {
                    "num": temp_num,
                    "text": row['obs_description'].split(",\n")
                }
                code_num = temp_num
        else:
            self.cache_obs = {}
            code_num = 0

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
                db_cur.executemany("INSERT OR IGNORE INTO source_data (patient_num, obs_code, obs_date) VALUES (?,?,?)", insert_batch)
                self.conn.commit()
                insert_batch = []
                if show_progress:
                    print("Inserted row #: " + str(inserted_row_count))

        # commit the last batch of records
        db_cur.executemany("INSERT OR IGNORE INTO source_data (patient_num, obs_code, obs_date) VALUES (?,?,?)", insert_batch)
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
