import os.path
import multiprocessing
import sqlite3

#========================================================================================
def worker_SequenceGeneration_only_first_occurences(maindb, tempdb, ingestQueue, mem_limit, seq_table_name):
    # create our temporary db (per process)
    if os.path.exists(tempdb):
        os.remove(tempdb)
    local_db = sqlite3.connect(tempdb, timeout=60000)
    local_db.execute("PRAGMA locking_mode=NORMAL")
    local_db.execute("PRAGMA synchronous=OFF")
    local_db.execute("PRAGMA journal_mode=MEMORY")
    local_db.execute("PRAGMA temp_store=FILE")
    local_db.execute("PRAGMA page_size=4096")  # 4k page size (usually an SSD's block size)
    local_db.execute("PRAGMA cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages

    # connect to the main db
    local_db.execute(f"ATTACH DATABASE '{maindb}' AS source;")
    local_db.execute("PRAGMA source.cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages
    local_db.execute("PRAGMA source.locking_mode=NORMAL")
    local_db.execute("PRAGMA source.synchronous=OFF")
    local_db.execute("PRAGMA source.journal_mode=OFF")
    # local_db.execute("PRAGMA source.journal_mode=WAL")
    # local_db.execute("PRAGMA source.journal_size_limit = 6144000")

    # create our local db table(s)
    local_db.execute(f"""
        CREATE TABLE local_seq (
            patient_num         INTEGER NOT NULL,
            obs_code_1          INTEGER NOT NULL,
            obs_code_2          INTEGER NOT NULL,
            temporal_distance   INTEGER NOT NULL
        );
    """)
    local_db.commit()

    # main processing loop
    while True:
        try:
            patient_list = ingestQueue.get(timeout=10)
            if patient_list is None:
                # no more items - break out of processing
                break
            # print(f"Worker {multiprocessing.current_process().name} received ids [{patient_list[0]}-{patient_list[len(patient_list)-1]}")
            for patient_num in patient_list:
                local_db.execute(f"""
                INSERT INTO local_seq (patient_num, obs_code_1, obs_code_2, temporal_distance)
                   WITH subquery (patient, code, occurred_on) AS (
                     SELECT patient_num, obs_code, MIN(obs_date)
                     FROM source.source_data
                     WHERE patient_num = {str(patient_num)}
                     GROUP BY patient_num, obs_code
                     ORDER BY patient_num, obs_code,MIN(obs_date)
                   )
                   SELECT
                     t1.patient, t1.code, t2.code,
                     CAST(julianday(t2.occurred_on) - julianday(t1.occurred_on) AS INTEGER) AS time_diff
                   FROM
                     subquery AS t1
                     JOIN subquery AS t2 ON (t1.patient = t2.patient)
                   WHERE
                     t1.occurred_on <= t2.occurred_on
                     AND t1.code != t2.code;
                """)
                local_db.commit()
        except multiprocessing.queues.Empty:
            # shutdown
            break

    # transfer our results back to the main db
    is_done = False
    while not is_done:
        try:
            local_db.execute(f"""
                INSERT INTO source.{seq_table_name} (patient_num, obs_code_1, obs_code_2, temporal_distance)
                SELECT 
                    patient_num, 
                    obs_code_1, 
                    obs_code_2, 
                    temporal_distance
                FROM local_seq;
            """)
            local_db.commit()
            is_done = True
        except Exception as e:
            print(f"Worker {multiprocessing.current_process().name} error: {e}")

    # close and delete our temporary db (per process)
    local_db.close()
    os.remove(tempdb)

# ========================================================================================
def worker_SequenceGeneration(maindb, tempdb, ingestQueue, mem_limit):
    seq_table_name = 'sequences'
    # create our temporary db (per process)
    if os.path.exists(tempdb):
        os.remove(tempdb)
    local_db = sqlite3.connect(tempdb, timeout=60000)
    local_db.execute("PRAGMA locking_mode=NORMAL")
    local_db.execute("PRAGMA synchronous=OFF")
    local_db.execute("PRAGMA journal_mode=MEMORY")
    local_db.execute("PRAGMA temp_store=FILE")
    local_db.execute("PRAGMA page_size=4096")  # 4k page size (usually an SSD's block size)
    local_db.execute("PRAGMA cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages

    # connect to the main db
    local_db.execute(f"ATTACH DATABASE '{maindb}' AS source;")
    local_db.execute("PRAGMA source.cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages
    local_db.execute("PRAGMA source.locking_mode=NORMAL")
    local_db.execute("PRAGMA source.synchronous=OFF")
    local_db.execute("PRAGMA source.journal_mode=OFF")
    # local_db.execute("PRAGMA source.journal_mode=WAL")
    # local_db.execute("PRAGMA source.journal_size_limit = 6144000")

    # create our local db table(s)
    local_db.execute(f"""
        CREATE TABLE local_seq (
            patient_num         INTEGER NOT NULL,
            obs_code_1          INTEGER NOT NULL,
            obs_code_2          INTEGER NOT NULL,
            temporal_distance   INTEGER NOT NULL
        );
    """)
    local_db.commit()

    # main processing loop
    while True:
        try:
            patient_list = ingestQueue.get(timeout=10)
            if patient_list is None:
                # no more items - break out of processing
                break
            # print(f"Worker {multiprocessing.current_process().name} received ids [{patient_list[0]}-{patient_list[len(patient_list)-1]}")
            for patient_num in patient_list:
                local_db.execute(f"""
                INSERT INTO local_seq (patient_num, obs_code_1, obs_code_2, temporal_distance)
                   WITH subquery (patient, code, occurred_on) AS (
                     SELECT patient_num, obs_code, obs_date
                     FROM source.source_data
                     WHERE patient_num = {str(patient_num)}
                     ORDER BY patient_num, obs_code, obs_date
                   )
                   SELECT DISTINCT
                     t1.patient, t1.code, t2.code,
                     CAST(julianday(t2.occurred_on) - julianday(t1.occurred_on) AS INTEGER) AS time_diff
                   FROM
                     subquery AS t1
                     JOIN subquery AS t2 ON (t1.patient = t2.patient)
                   WHERE
                     t1.occurred_on <= t2.occurred_on
                     AND t1.code != t2.code;
                """)
                local_db.commit()
        except multiprocessing.queues.Empty:
            # shutdown
            break

    # transfer our results back to the main db
    is_done = False
    while not is_done:
        try:
            local_db.execute(f"""
                INSERT INTO source.{seq_table_name} (patient_num, obs_code_1, obs_code_2, temporal_distance)
                SELECT 
                    patient_num, 
                    obs_code_1, 
                    obs_code_2, 
                    temporal_distance
                FROM local_seq;
            """)
            local_db.commit()
            is_done = True
        except Exception as e:
            print(f"Worker {multiprocessing.current_process().name} error: {e}")

    # close and delete our temporary db (per process)
    local_db.close()
    # os.remove(tempdb)

# ========================================================================================
def worker_SequenceGeneration_Step_1(maindb, tempdb, ingestQueue, mem_limit, temporal_buckets):
    seq_table_name = 'sequences'

    # create our temporary db (per process)
    if os.path.exists(tempdb):
        os.remove(tempdb)
    local_db = sqlite3.connect(tempdb, timeout=60000)
    local_db.execute("PRAGMA locking_mode=NORMAL")
    local_db.execute("PRAGMA synchronous=OFF")
    local_db.execute("PRAGMA journal_mode=MEMORY")
    local_db.execute("PRAGMA temp_store=FILE")
    local_db.execute("PRAGMA page_size=4096")  # 4k page size (usually an SSD's block size)
    local_db.execute("PRAGMA cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages

    # connect to the main db
    local_db.execute(f"ATTACH DATABASE '{maindb}' AS source;")
    local_db.execute("PRAGMA source.cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages
    local_db.execute("PRAGMA source.locking_mode=NORMAL")
    local_db.execute("PRAGMA source.synchronous=OFF")
    local_db.execute("PRAGMA source.journal_mode=OFF")
    # local_db.execute("PRAGMA source.journal_mode=WAL")
    # local_db.execute("PRAGMA source.journal_size_limit = 6144000")

    # create our local db table(s)
    local_db.execute(f"""
        CREATE TABLE local_seq (
            patient_num         INTEGER NOT NULL,
            obs_code_1          INTEGER NOT NULL,
            obs_code_2          INTEGER NOT NULL,
            temporal_distance   INTEGER NOT NULL
        );
    """)
    local_db.commit()

    # main processing loop
    while True:
        try:
            patient_list = ingestQueue.get(timeout=10)
            if patient_list is None:
                # no more items - break out of processing
                break
            # print(f"Worker {multiprocessing.current_process().name} received ids [{patient_list[0]}-{patient_list[len(patient_list)-1]}")
            for patient_num in patient_list:
                local_db.execute(f"""
                INSERT INTO local_seq (patient_num, obs_code_1, obs_code_2, temporal_distance)
                   WITH subquery (patient, code, occurred_on) AS (
                     SELECT patient_num, obs_code, obs_date
                     FROM source.source_data
                     WHERE patient_num = {str(patient_num)}
                     ORDER BY patient_num, obs_code, obs_date
                   )
                   SELECT DISTINCT
                     t1.patient, t1.code, t2.code,
                     CAST(julianday(t2.occurred_on) - julianday(t1.occurred_on) AS INTEGER) AS time_diff
                   FROM
                     subquery AS t1
                     JOIN subquery AS t2 ON (t1.patient = t2.patient)
                   WHERE
                     t1.occurred_on <= t2.occurred_on
                     AND t1.code != t2.code;
                """)
                local_db.commit()
        except multiprocessing.queues.Empty:
            # shutdown
            break


def worker_SequenceGeneration_Step_2(maindb, tempdb, mem_limit, temporal_buckets):
    """ this step updates the seq_sparsity table in the master database """
    # connect to our temporary db (per process)
    local_db = sqlite3.connect(tempdb, timeout=60000)
    local_db.execute("PRAGMA locking_mode=NORMAL")
    local_db.execute("PRAGMA synchronous=OFF")
    local_db.execute("PRAGMA journal_mode=MEMORY")
    local_db.execute("PRAGMA temp_store=FILE")
    local_db.execute("PRAGMA page_size=4096")  # 4k page size (usually an SSD's block size)
    local_db.execute("PRAGMA cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages

    # connect to the main db
    local_db.execute(f"ATTACH DATABASE '{maindb}' AS source;")
    local_db.execute("PRAGMA source.cache_size=" + str(int(mem_limit / 4096)))  # cache is in number of DB pages
    local_db.execute("PRAGMA source.locking_mode=NORMAL")
    local_db.execute("PRAGMA source.synchronous=OFF")
    local_db.execute("PRAGMA source.journal_mode=OFF")
    # local_db.execute("PRAGMA source.journal_mode=WAL")
    # local_db.execute("PRAGMA source.journal_size_limit = 6144000")


    # create temporal buckets
    if len(temporal_buckets) == 0:
        temporal_SQL = "temporal_distance"
    else:
        temporal_SQL = "CASE\n"
        bucket_num = 0
        for bucket in temporal_buckets:
            bucket_num += 1
            temporal_SQL += "WHEN temporal_distance BETWEEN " + str(bucket[0]) + " AND " + str(bucket[1]) + " THEN " + str(
                bucket_num) + "\n"
        temporal_SQL += "ELSE 0\n"
        temporal_SQL += "END AS temporal_distance"




    local_db.execute(f"""
        WITH bucketed (obs_count, patient, obs_1, obs_2, dist) AS (
            SELECT 
                COUNT(*) AS obs_count,
                patient_num,
                obs_code_1,
                obs_code_2,
                {temporal_SQL}
            FROM local_seq
            GROUP BY
                patient_num,
                obs_code_1,
                obs_code_2,
                temporal_distance
        ), intermediate (obs_1, obs_2, dist, count_obs, count_patient) AS (
            SELECT
                obs_1, 
                obs_2, 
                dist,
                SUM(obs_count) AS observation_cnt,
                COUNT(*) AS patient_cnt
            FROM bucketed
            GROUP BY
                obs_1, 
                obs_2, 
                dist
        )
        INSERT INTO source.frequencies (obs_code_1, obs_code_2, temporal_distance, observation_cnt, patient_cnt)
        SELECT obs_1, obs_2, dist, count_obs, count_patient FROM intermediate WHERE true
        ON CONFLICT(obs_code_1, obs_code_2, temporal_distance) DO UPDATE 
        SET observation_cnt = observation_cnt + excluded.observation_cnt, patient_cnt = patient_cnt + excluded.patient_cnt WHERE true;
    """)
    local_db.commit()
