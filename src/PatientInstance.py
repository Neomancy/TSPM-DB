from pandas import DataFrame


class PatientInstance:
    """Class representing a single patient instance from the database."""

    def __init__(self, tspmdb_ref, patient_num: int):
        """
        Initializes a PatientInstance.

        Args:
            tspmdb_ref: Reference to the TspmDB object.
            patient_num: The database key (patient_num) for the patient.

        Raises:
            KeyError: If the patient_num does not exist in the lookup_patients table.
        """
        self._parent = tspmdb_ref
        self._identifier = patient_num

        # Verify the patient exists and retrieve the patient_id
        cur = self._parent.conn.cursor()
        cur.execute("SELECT patient_id FROM lookup_patients WHERE patient_num = ?", (patient_num,))
        result = cur.fetchone()

        if result is None:
            raise KeyError(f"Patient with patient_num '{patient_num}' does not exist in lookup_patients")

        self._patient_id = result[0]

    def help(self):
        print("[HELP] Patient Instance Operations")
        print("<multiple>.patient(patient_num)")
        print("------------------------------------------------------------------------------")
        print(" .help()                   Displays this help message")
        print(" .id                       Returns the patient_id (string identifier) for this patient")
        print(" .events()                 Returns the events for this patient")
        print(" .sequences()              Returns the sequences for this patient")
        print(" .frequencies()            Returns the sequence frequencies for this patient")

    @property
    def id(self):
        """Returns the patient_id (string identifier) for this patient."""
        return self._patient_id

    def events(self, as_pandas: bool = False):
        """
        Returns a list of the patient's events from the source_data table.

        Args:
            as_pandas: If True, returns a Pandas DataFrame instead of a Python list.
                       Default is False (returns a Python list of dictionaries).

        Returns:
            A list of dictionaries containing event data (obs_code, obs_description, obs_date),
            or a Pandas DataFrame if as_pandas=True.
        """
        cur = self._parent.conn.cursor()

        cur.execute("""
            SELECT sd.obs_date, lo.obs_code, lo.obs_description
            FROM source_data AS sd
            JOIN lookup_observations AS lo ON (lo.obs_code_id = sd.obs_code)
            WHERE sd.patient_num = ?
            ORDER BY sd.obs_date ASC
        """, (self._identifier,))

        results = cur.fetchall()

        if as_pandas:
            return DataFrame(results, columns=["obs_date", "obs_code", "obs_description"])
        else:
            return [{"obs_date": row[2], "obs_code": row[0], "obs_description": row[1]} for row in results]

    def sequences(self, as_pandas: bool = False, as_iterator: bool = False):
        """
        Returns all calculated sequences for the patient from the database.

        Args:
            as_pandas: If True, returns a Pandas DataFrame.
                       Default is False (returns a list of dictionaries).
            as_iterator: If True, returns an iterator instead of a list or DataFrame.
                         Default is False (returns a list or DataFrame).

        Returns:
            A list of dictionaries containing sequence data (patient_id, obs_code_1, obs_code_2, time_diff, occurrence_count),
            or a Pandas DataFrame if as_pandas=True.
        """
        cur = self._parent.conn.cursor()

        cur.execute("""
                WITH 
                    subquery (patient, code, occurred_on) AS (
                        SELECT patient_num, obs_code, obs_date
                        FROM source_data
                        WHERE patient_num = ?
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
            """, (self._identifier,))
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
                return DataFrame(results,
                                 columns=["patient_id", "obs_code_1", "obs_code_2", "time_diff", "occurrence_count"])
            else:
                return [{"patient_id": row[0], "obs_code_1": row[1], "obs_code_2": row[2], "time_diff": row[3],
                         "occurrence_count": row[3]} for row in results]
