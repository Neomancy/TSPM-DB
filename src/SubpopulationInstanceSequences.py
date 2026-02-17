from pandas import DataFrame


class SubpopulationInstanceSequences:

    def __init__(self, tspmdb_ref, subpop_instance):
        self._parent = tspmdb_ref
        self._subpop_instance = subpop_instance

    def get(self):
        """ gets the list of all sequences for all patients of the subpopulation """
        pass

    def get_bucketed(self):
        """ gets the list of all sequences for all patients of the subpopulation constrained to the passed buckets """
        pass

    def recalculate(self, table_name=""):
        """ recalculates the subpopulation's sequences """
        pass

    def frequencies(self, observation1=None, observation2=None, as_pandas: bool = False, as_iterator: bool = False, with_ids: bool = False):
        """
        Returns frequency data calculated from the sequences table for this subpopulation.

        Args:
            observation1: Optional filter for obs_code_1. Can be a string (single code) or list of strings.
                          If None, all obs_code_1 values are included.
            observation2: Optional filter for obs_code_2. Can be a string (single code) or list of strings.
                          If None, all obs_code_2 values are included.
            as_pandas: If True, returns a Pandas DataFrame. Default is False.
            as_iterator: If True, returns an iterator. Default is False.
            with_ids: If True, returns raw integer IDs instead of translated observation code strings.
                      Default is False.

        Returns:
            A list of dictionaries (or DataFrame/iterator) containing:
            obs_code_1, obs_code_2, temporal_distance, observation_cnt, patient_cnt

        Raises:
            KeyError: If any observation code in observation1 or observation2 does not exist.
        """
        cur = self._parent.conn.cursor()

        # Helper function to validate and convert observation codes to IDs
        # Returns a tuple: (list of IDs, dict mapping ID -> code string)
        def validate_and_get_ids(codes, param_name):
            if codes is None:
                return None, {}

            # Normalize to list
            if isinstance(codes, str):
                codes = [codes]

            # Look up each code and collect IDs and mappings
            code_ids = []
            id_to_code = {}
            not_found = []
            for code in codes:
                cur.execute("SELECT obs_code_id FROM lookup_observations WHERE obs_code = ?", (code,))
                result = cur.fetchone()
                if result is None:
                    not_found.append(code)
                else:
                    code_ids.append(result[0])
                    id_to_code[result[0]] = code

            if not_found:
                raise KeyError(f"Observation code(s) not found in {param_name}: {', '.join(not_found)}")

            return code_ids, id_to_code

        # Validate observation codes upfront and cache the mappings
        obs1_ids, obs1_id_to_code = validate_and_get_ids(observation1, "observation1")
        obs2_ids, obs2_id_to_code = validate_and_get_ids(observation2, "observation2")

        # If not using with_ids and we need to translate, build a complete lookup cache
        id_to_code_cache = {}
        if not with_ids:
            # If filters were provided, we already have those mappings
            id_to_code_cache.update(obs1_id_to_code)
            id_to_code_cache.update(obs2_id_to_code)

            # For unfiltered columns, we need to fetch all mappings
            if obs1_ids is None or obs2_ids is None:
                cur.execute("SELECT obs_code_id, obs_code FROM lookup_observations")
                for row in cur.fetchall():
                    id_to_code_cache[row[0]] = row[1]

        # Build the query - calculate frequencies from sequences table for this subpopulation
        query = """
            SELECT
                s.obs_code_1,
                s.obs_code_2,
                s.temporal_distance,
                SUM(s.occurrence_count) AS observation_cnt,
                COUNT(DISTINCT s.patient_num) AS patient_cnt
            FROM sequences AS s
            JOIN subpopulation_patients AS sp ON (sp.patient_num = s.patient_num)
            WHERE sp.subpop_num = ?
        """
        params = [self._subpop_instance._identifier]

        # Add observation filters
        if obs1_ids is not None:
            placeholders = ",".join("?" * len(obs1_ids))
            query += f" AND s.obs_code_1 IN ({placeholders})"
            params.extend(obs1_ids)

        if obs2_ids is not None:
            placeholders = ",".join("?" * len(obs2_ids))
            query += f" AND s.obs_code_2 IN ({placeholders})"
            params.extend(obs2_ids)

        query += " GROUP BY s.obs_code_1, s.obs_code_2, s.temporal_distance"
        query += " ORDER BY s.obs_code_1 ASC, s.obs_code_2 ASC, s.temporal_distance ASC"

        cur.execute(query, params)

        # Helper to translate IDs to codes if needed
        def translate_row(row):
            if with_ids:
                return {"obs_code_1": row[0], "obs_code_2": row[1], "temporal_distance": row[2],
                        "observation_cnt": row[3], "patient_cnt": row[4]}
            else:
                return {"obs_code_1": id_to_code_cache[row[0]], "obs_code_2": id_to_code_cache[row[1]],
                        "temporal_distance": row[2], "observation_cnt": row[3], "patient_cnt": row[4]}

        if as_iterator:
            def _generator():
                while True:
                    row = cur.fetchone()
                    if row is None:
                        break
                    yield translate_row(row)
            return _generator()
        else:
            results = cur.fetchall()

            if as_pandas:
                if with_ids:
                    return DataFrame(results, columns=["obs_code_1", "obs_code_2", "temporal_distance", "observation_cnt", "patient_cnt"])
                else:
                    translated = [[id_to_code_cache[row[0]], id_to_code_cache[row[1]], row[2], row[3], row[4]] for row in results]
                    return DataFrame(translated, columns=["obs_code_1", "obs_code_2", "temporal_distance", "observation_cnt", "patient_cnt"])
            else:
                return [translate_row(row) for row in results]