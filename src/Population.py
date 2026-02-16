import tspmdb
from SubpopulationInstance import SubpopulationInstance
from PatientInstance import PatientInstance
from pandas import DataFrame

class Population:
    def __init__(self, tspmdb_ref):
        self._parent = tspmdb_ref


    def help(self):
        print("[HELP] tspmdb.Population Object")
        print("TspmDb().population")
        print("------------------------------------------------------------------------------")
        print(".help()         Displays this help message")
        print(".patients()     List of all patients in the database")
        print(".sequences()    List of calculated patient sequences")
        print(".frequencies()  List of calculated sequences with their frequencies")
        print(".events()       List of all events for all patients")

    def patients(self, as_list: bool = False, as_pandas: bool = False, with_ids: bool = False):
        """
        Returns patients from the database.

        Args:
            as_list: If True, returns a list of patient_id strings (or dicts if with_ids=True).
                     Default is False (returns a list of PatientInstance objects).
            as_pandas: If True, returns a Pandas DataFrame.
                       Default is False.
            with_ids: If True, includes both patient_id and patient_num in the results.
                      Only applies when as_list=True or as_pandas=True.
                      Default is False.

        Returns:
            - If as_list=False and as_pandas=False: List of PatientInstance objects
            - If as_list=True and with_ids=False: List of patient_id strings
            - If as_list=True and with_ids=True: List of dicts with patient_id and patient_db_num
            - If as_pandas=True and with_ids=False: DataFrame with patient_id column
            - If as_pandas=True and with_ids=True: DataFrame with patient_id and patient_db_num columns
        """
        cur = self._parent.conn.cursor()

        if with_ids:
            cur.execute("SELECT patient_id, patient_num FROM lookup_patients ORDER BY patient_num ASC")
            results = cur.fetchall()

            if as_pandas:
                return DataFrame(results, columns=["patient_id", "patient_db_num"])
            elif as_list:
                return [{"patient_id": row[0], "patient_db_num": row[1]} for row in results]
            else:
                # Return PatientInstance objects (with_ids is ignored for PatientInstance)
                return [PatientInstance(self._parent, row[1]) for row in results]
        else:
            cur.execute("SELECT patient_id, patient_num FROM lookup_patients ORDER BY patient_num ASC")
            results = cur.fetchall()

            if as_pandas:
                return DataFrame([row[0] for row in results], columns=["patient_id"])
            elif as_list:
                return [row[0] for row in results]
            else:
                # Return PatientInstance objects
                return [PatientInstance(self._parent, row[1]) for row in results]

    def sequences(self, as_pandas: bool = False, as_iterator: bool = False):
        """
        Returns all calculated patient sequences from the database.

        Args:
            as_pandas: If True, returns a Pandas DataFrame.
                       Default is False (returns a list of dictionaries).
            as_iterator: If True, returns an iterator instead of a list or DataFrame.
                         Default is False (returns a list or DataFrame).

        Returns:
            A list of dictionaries containing sequence data (patient_id, obs_code_1, obs_code_2, time_diff),
            or a Pandas DataFrame if as_pandas=True.
        """
        cur = self._parent.conn.cursor()

        cur.execute("""
            WITH 
                subquery (patient, code, occurred_on) AS (
                    SELECT patient_num, obs_code, obs_date
                    FROM source_data
                    ORDER BY patient_num, obs_code, obs_date
                ),
                main_query (patient, obs_code_1, obs_code_2, time_diff, occurrence_count) AS (
                    SELECT DISTINCT
                        t1.patient, t1.code, t2.code,
                        CAST(julianday(t2.occurred_on) - julianday(t1.occurred_on) AS INTEGER) AS time_diff,
                        COUNT(*) AS occurrence_count
                    FROM
                        subquery AS t1
                        JOIN subquery AS t2 ON (t1.patient = t2.patient)
                    WHERE
                        t1.occurred_on <= t2.occurred_on
                        AND t1.code != t2.code
                    GROUP BY t1.patient, t1.code, t2.code, time_diff
                )
            SELECT
                lp.patient_id,
                lo1.obs_code AS obs_code_1,
                lo2.obs_code AS obs_code_2,
                main_query.time_diff AS time_diff,
                main_query.occurrence_count
            FROM main_query
            JOIN lookup_patients AS lp ON (lp.patient_num = main_query.patient)
            JOIN lookup_observations AS lo1 ON (lo1.obs_code_id = main_query.obs_code_1)
            JOIN lookup_observations AS lo2 ON (lo2.obs_code_id = main_query.obs_code_2)
            ORDER BY main_query.patient ASC, main_query.obs_code_1 ASC, main_query.obs_code_2 ASC, main_query.time_diff ASC
        """)

        if as_iterator:
            def _generator():
                while True:
                    row = cur.fetchone()
                    if row is None:
                        break
                    yield {"patient_id": row[0], "obs_code_1": row[1], "obs_code_2": row[2], "time_diff": row[3],
                           "occurrence_count": row[3]}
            return _generator()
        else:
            results = cur.fetchall()

            if as_pandas:
                return DataFrame(results, columns=["patient_id", "obs_code_1", "obs_code_2", "time_diff", "occurrence_count"])
            else:
                return [{"patient_id": row[0], "obs_code_1": row[1], "obs_code_2": row[2], "time_diff": row[3], "occurrence_count": row[3]} for row in results]



        # TsmpDb.population.frequencies()
        # TsmpDb.population.events()
