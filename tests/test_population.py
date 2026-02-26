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

import pytest

import tempfile
import os.path

import pandas as pd

import tspmdb
from PatientInstance import PatientInstance


class TestPopulationPatients:
    """Tests for the Population.patients() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_population_patients.sqlite3")

        # Delete temp file if it exists
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # Create the database and ingest test data
        test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.dataset.ingest("test_data.csv", col_names)

        yield test_obj, temp_filename

        # Cleanup
        test_obj.close()
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

    def test_patients_returns_patient_instances_by_default(self, setup_db):
        """Test that patients() returns a list of PatientInstance objects by default"""
        test_obj, temp_filename = setup_db

        patients = test_obj.population.patients()

        # Verify it returns a list
        assert isinstance(patients, list)
        # Verify all items are PatientInstance objects
        for patient in patients:
            assert isinstance(patient, PatientInstance)

    def test_patients_as_list_returns_patient_ids(self, setup_db):
        """Test that patients(as_list=True) returns a list of patient_id strings"""
        test_obj, temp_filename = setup_db

        patients = test_obj.population.patients(as_list=True)

        # Verify it returns a list
        assert isinstance(patients, list)
        # Verify all items are strings
        for patient_id in patients:
            assert isinstance(patient_id, str)

    def test_patients_as_list_with_ids(self, setup_db):
        """Test that patients(as_list=True, with_ids=True) returns dicts with both ids"""
        test_obj, temp_filename = setup_db

        patients = test_obj.population.patients(as_list=True, with_ids=True)

        # Verify it returns a list
        assert isinstance(patients, list)
        # Verify all items are dicts with correct keys
        for patient in patients:
            assert isinstance(patient, dict)
            assert "patient_id" in patient
            assert "patient_db_num" in patient
            assert isinstance(patient["patient_id"], str)
            assert isinstance(patient["patient_db_num"], int)

    def test_patients_as_pandas_returns_dataframe(self, setup_db):
        """Test that patients(as_pandas=True) returns a DataFrame"""
        test_obj, temp_filename = setup_db

        patients_df = test_obj.population.patients(as_pandas=True)

        # Verify it returns a DataFrame
        assert isinstance(patients_df, pd.DataFrame)
        # Verify it has the correct column
        assert "patient_id" in patients_df.columns
        # Verify patient_db_num is NOT in columns when with_ids=False
        assert "patient_db_num" not in patients_df.columns

    def test_patients_as_pandas_with_ids(self, setup_db):
        """Test that patients(as_pandas=True, with_ids=True) returns DataFrame with both columns"""
        test_obj, temp_filename = setup_db

        patients_df = test_obj.population.patients(as_pandas=True, with_ids=True)

        # Verify it returns a DataFrame
        assert isinstance(patients_df, pd.DataFrame)
        # Verify it has both columns
        assert "patient_id" in patients_df.columns
        assert "patient_db_num" in patients_df.columns

    def test_patients_count_matches_database(self, setup_db):
        """Test that the number of patients returned matches the database"""
        test_obj, temp_filename = setup_db

        # Get count from database directly
        cur = test_obj.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lookup_patients")
        expected_count = cur.fetchone()[0]

        # Get patients using different methods
        patients_instances = test_obj.population.patients()
        patients_list = test_obj.population.patients(as_list=True)
        patients_df = test_obj.population.patients(as_pandas=True)

        # Verify counts match
        assert len(patients_instances) == expected_count
        assert len(patients_list) == expected_count
        assert len(patients_df) == expected_count

    def test_patients_patient_instance_id_matches_list(self, setup_db):
        """Test that PatientInstance.id matches the patient_id from as_list"""
        test_obj, temp_filename = setup_db

        patients_instances = test_obj.population.patients()
        patients_list = test_obj.population.patients(as_list=True)

        # Verify the ids match
        for instance, patient_id in zip(patients_instances, patients_list):
            assert instance.id == patient_id

    def test_patients_empty_database(self, setup_db):
        """Test patients() on a database with no patients"""
        test_obj, temp_filename = setup_db

        # Clear the database
        test_obj.dataset.clear()

        # Get patients
        patients_instances = test_obj.population.patients()
        patients_list = test_obj.population.patients(as_list=True)
        patients_df = test_obj.population.patients(as_pandas=True)

        # Verify empty results
        assert len(patients_instances) == 0
        assert len(patients_list) == 0
        assert len(patients_df) == 0

    def test_patients_with_ids_patient_db_num_is_valid(self, setup_db):
        """Test that patient_db_num from with_ids can be used to create PatientInstance"""
        test_obj, temp_filename = setup_db

        patients_with_ids = test_obj.population.patients(as_list=True, with_ids=True)

        # Verify each patient_db_num can be used to create a PatientInstance
        for patient_data in patients_with_ids:
            patient_instance = PatientInstance(test_obj, patient_data["patient_db_num"])
            assert patient_instance.id == patient_data["patient_id"]


class TestPopulationSequences:
    """Tests for the Population.sequences() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_population_sequences.sqlite3")

        # Delete temp file if it exists
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # Create the database and ingest test data
        test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.dataset.ingest("test_data.csv", col_names)
        test_obj.dataset.calculate()

        yield test_obj, temp_filename

        # Cleanup
        test_obj.close()
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

    def test_sequences_returns_list_by_default(self, setup_db):
        """Test that sequences() returns a list by default"""
        test_obj, temp_filename = setup_db

        sequences = test_obj.population.sequences()

        # Verify it returns a list
        assert isinstance(sequences, list)

    def test_sequences_returns_pandas_dataframe(self, setup_db):
        """Test that sequences(as_pandas=True) returns a DataFrame"""
        test_obj, temp_filename = setup_db

        sequences_df = test_obj.population.sequences(as_pandas=True)

        # Verify it returns a DataFrame
        assert isinstance(sequences_df, pd.DataFrame)
        # Verify it has the correct columns
        assert "patient_id" in sequences_df.columns
        assert "obs_code_1" in sequences_df.columns
        assert "obs_code_2" in sequences_df.columns
        assert "time_diff" in sequences_df.columns

    def test_sequences_list_contains_correct_keys(self, setup_db):
        """Test that sequences list contains dictionaries with correct keys"""
        test_obj, temp_filename = setup_db

        sequences = test_obj.population.sequences()

        # If there are sequences, verify they have the correct keys
        if len(sequences) > 0:
            for seq in sequences:
                assert "patient_id" in seq
                assert "obs_code_1" in seq
                assert "obs_code_2" in seq
                assert "time_diff" in seq

    def test_sequences_empty_when_no_sequences_calculated(self, setup_db):
        """Test sequences() returns empty results when no sequences have been calculated"""
        test_obj, temp_filename = setup_db

        # Clear the sequences table
        test_obj.conn.execute("DELETE FROM sequences")
        test_obj.conn.commit()

        sequences = test_obj.population.sequences()
        sequences_df = test_obj.population.sequences(as_pandas=True)

        # Verify empty results
        assert len(sequences) == 0
        assert len(sequences_df) == 0

    def test_sequences_patient_id_is_string(self, setup_db):
        """Test that patient_id in sequences is a string (not patient_num)"""
        test_obj, temp_filename = setup_db

        sequences = test_obj.population.sequences()

        # If there are sequences, verify patient_id is a string
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["patient_id"], str)

    def test_sequences_obs_codes_are_strings(self, setup_db):
        """Test that obs_code_1 and obs_code_2 are strings (not obs_code_id integers)"""
        test_obj, temp_filename = setup_db

        sequences = test_obj.population.sequences()

        # If there are sequences, verify obs_codes are strings
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["obs_code_1"], str)
                assert isinstance(seq["obs_code_2"], str)

    def test_sequences_time_diff_is_integer(self, setup_db):
        """Test that time_diff is an integer"""
        test_obj, temp_filename = setup_db

        sequences = test_obj.population.sequences()

        # If there are sequences, verify time_diff is an integer
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["time_diff"], int)

    def test_sequences_as_iterator_returns_generator(self, setup_db):
        """Test that sequences(as_iterator=True) returns a generator"""
        test_obj, temp_filename = setup_db

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Verify it returns a generator/iterator
        import types
        assert isinstance(sequences_iter, types.GeneratorType)

    def test_sequences_as_iterator_yields_correct_keys(self, setup_db):
        """Test that sequences iterator yields dictionaries with correct keys"""
        test_obj, temp_filename = setup_db

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Consume the iterator and verify the items have correct keys
        count = 0
        for seq in sequences_iter:
            assert "patient_id" in seq
            assert "obs_code_1" in seq
            assert "obs_code_2" in seq
            assert "time_diff" in seq
            assert "occurrence_count" in seq
            count += 1

    def test_sequences_as_iterator_patient_id_is_string(self, setup_db):
        """Test that patient_id from iterator is a string (not patient_num)"""
        test_obj, temp_filename = setup_db

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Verify patient_id is a string for each item
        for seq in sequences_iter:
            assert isinstance(seq["patient_id"], str)

    def test_sequences_as_iterator_obs_codes_are_strings(self, setup_db):
        """Test that obs_code_1 and obs_code_2 from iterator are strings"""
        test_obj, temp_filename = setup_db

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Verify obs_codes are strings for each item
        for seq in sequences_iter:
            assert isinstance(seq["obs_code_1"], str)
            assert isinstance(seq["obs_code_2"], str)

    def test_sequences_as_iterator_time_diff_is_integer(self, setup_db):
        """Test that time_diff from iterator is an integer"""
        test_obj, temp_filename = setup_db

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Verify time_diff is an integer for each item
        for seq in sequences_iter:
            assert isinstance(seq["time_diff"], int)

    def test_sequences_as_iterator_occurrence_count_is_integer(self, setup_db):
        """Test that occurrence_count from iterator is an integer"""
        test_obj, temp_filename = setup_db

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Verify occurrence_count is an integer for each item
        for seq in sequences_iter:
            assert isinstance(seq["occurrence_count"], int)

    def test_sequences_as_iterator_matches_list_count(self, setup_db):
        """Test that iterator yields the same number of items as the list"""
        test_obj, temp_filename = setup_db

        # Get sequences as list
        sequences_list = test_obj.population.sequences()

        # Get sequences as iterator and count
        sequences_iter = test_obj.population.sequences(as_iterator=True)
        iter_count = sum(1 for _ in sequences_iter)

        # Verify counts match
        assert iter_count == len(sequences_list)

    def test_sequences_as_iterator_empty_when_no_sequences(self, setup_db):
        """Test that iterator yields nothing when no sequences exist"""
        test_obj, temp_filename = setup_db

        # Clear the source_data table so no sequences can be generated
        test_obj.conn.execute("DELETE FROM source_data")
        test_obj.conn.commit()

        sequences_iter = test_obj.population.sequences(as_iterator=True)

        # Verify iterator yields nothing
        count = sum(1 for _ in sequences_iter)
        assert count == 0


class TestPopulationFrequencies:
    """Tests for the Population.frequencies() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data and frequencies"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_population_frequencies.sqlite3")

        # Delete temp file if it exists
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

        # Create the database and ingest test data
        test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.dataset.ingest("test_data.csv", col_names)

        yield test_obj, temp_filename

        # Cleanup
        test_obj.close()
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

    def test_frequencies_returns_list_by_default(self, setup_db):
        """Test that frequencies() returns a list by default"""
        test_obj, temp_filename = setup_db

        frequencies = test_obj.population.frequencies()

        # Verify it returns a list
        assert isinstance(frequencies, list)

    def test_frequencies_returns_pandas_dataframe(self, setup_db):
        """Test that frequencies(as_pandas=True) returns a DataFrame"""
        test_obj, temp_filename = setup_db

        frequencies_df = test_obj.population.frequencies(as_pandas=True)

        # Verify it returns a DataFrame
        assert isinstance(frequencies_df, pd.DataFrame)
        # Verify it has the correct columns
        assert "obs_code_1" in frequencies_df.columns
        assert "obs_code_2" in frequencies_df.columns
        assert "temporal_distance" in frequencies_df.columns
        assert "observation_cnt" in frequencies_df.columns
        assert "patient_cnt" in frequencies_df.columns

    def test_frequencies_list_contains_correct_keys(self, setup_db):
        """Test that frequencies list contains dictionaries with correct keys"""
        test_obj, temp_filename = setup_db

        frequencies = test_obj.population.frequencies()

        # If there are frequencies, verify they have the correct keys
        if len(frequencies) > 0:
            for freq in frequencies:
                assert "obs_code_1" in freq
                assert "obs_code_2" in freq
                assert "temporal_distance" in freq
                assert "observation_cnt" in freq
                assert "patient_cnt" in freq

    def test_frequencies_obs_codes_are_strings_by_default(self, setup_db):
        """Test that obs_code_1 and obs_code_2 are strings when with_ids=False"""
        test_obj, temp_filename = setup_db

        frequencies = test_obj.population.frequencies()

        # If there are frequencies, verify obs_codes are strings
        if len(frequencies) > 0:
            for freq in frequencies:
                assert isinstance(freq["obs_code_1"], str)
                assert isinstance(freq["obs_code_2"], str)

    def test_frequencies_with_ids_returns_integers(self, setup_db):
        """Test that obs_code_1 and obs_code_2 are integers when with_ids=True"""
        test_obj, temp_filename = setup_db

        frequencies = test_obj.population.frequencies(with_ids=True)

        # If there are frequencies, verify obs_codes are integers
        if len(frequencies) > 0:
            for freq in frequencies:
                assert isinstance(freq["obs_code_1"], int)
                assert isinstance(freq["obs_code_2"], int)

    def test_frequencies_as_iterator_returns_generator(self, setup_db):
        """Test that frequencies(as_iterator=True) returns a generator"""
        test_obj, temp_filename = setup_db

        frequencies_iter = test_obj.population.frequencies(as_iterator=True)

        # Verify it returns a generator/iterator
        import types
        assert isinstance(frequencies_iter, types.GeneratorType)

    def test_frequencies_as_iterator_yields_correct_keys(self, setup_db):
        """Test that frequencies iterator yields dictionaries with correct keys"""
        test_obj, temp_filename = setup_db

        frequencies_iter = test_obj.population.frequencies(as_iterator=True)

        # Consume the iterator and verify the items have correct keys
        for freq in frequencies_iter:
            assert "obs_code_1" in freq
            assert "obs_code_2" in freq
            assert "temporal_distance" in freq
            assert "observation_cnt" in freq
            assert "patient_cnt" in freq

    def test_frequencies_filter_by_observation1_string(self, setup_db):
        """Test filtering frequencies by a single observation1 string"""
        test_obj, temp_filename = setup_db

        # Get all observation codes
        cur = test_obj.conn.cursor()
        cur.execute("SELECT obs_code FROM lookup_observations LIMIT 1")
        result = cur.fetchone()
        if result is None:
            pytest.skip("No observation codes in database")
        obs_code = result[0]

        frequencies = test_obj.population.frequencies(observation1=obs_code)

        # Verify all results have the specified obs_code_1
        for freq in frequencies:
            assert freq["obs_code_1"] == obs_code

    def test_frequencies_filter_by_observation2_string(self, setup_db):
        """Test filtering frequencies by a single observation2 string"""
        test_obj, temp_filename = setup_db

        # Get all observation codes
        cur = test_obj.conn.cursor()
        cur.execute("SELECT obs_code FROM lookup_observations LIMIT 1")
        result = cur.fetchone()
        if result is None:
            pytest.skip("No observation codes in database")
        obs_code = result[0]

        frequencies = test_obj.population.frequencies(observation2=obs_code)

        # Verify all results have the specified obs_code_2
        for freq in frequencies:
            assert freq["obs_code_2"] == obs_code

    def test_frequencies_filter_by_observation1_list(self, setup_db):
        """Test filtering frequencies by a list of observation1 codes"""
        test_obj, temp_filename = setup_db

        # Get observation codes
        cur = test_obj.conn.cursor()
        cur.execute("SELECT obs_code FROM lookup_observations LIMIT 2")
        results = cur.fetchall()
        if len(results) < 2:
            pytest.skip("Not enough observation codes in database")
        obs_codes = [row[0] for row in results]

        frequencies = test_obj.population.frequencies(observation1=obs_codes)

        # Verify all results have obs_code_1 in the specified list
        for freq in frequencies:
            assert freq["obs_code_1"] in obs_codes

    def test_frequencies_filter_by_both_observations(self, setup_db):
        """Test filtering frequencies by both observation1 and observation2"""
        test_obj, temp_filename = setup_db

        # Get observation codes
        cur = test_obj.conn.cursor()
        cur.execute("SELECT obs_code FROM lookup_observations LIMIT 2")
        results = cur.fetchall()
        if len(results) < 2:
            pytest.skip("Not enough observation codes in database")
        obs_code_1 = results[0][0]
        obs_code_2 = results[1][0]

        frequencies = test_obj.population.frequencies(observation1=obs_code_1, observation2=obs_code_2)

        # Verify all results match both filters
        for freq in frequencies:
            assert freq["obs_code_1"] == obs_code_1
            assert freq["obs_code_2"] == obs_code_2

    def test_frequencies_invalid_observation1_raises_keyerror(self, setup_db):
        """Test that invalid observation1 code raises KeyError"""
        test_obj, temp_filename = setup_db

        with pytest.raises(KeyError) as excinfo:
            test_obj.population.frequencies(observation1="NONEXISTENT_CODE")

        assert "NONEXISTENT_CODE" in str(excinfo.value)

    def test_frequencies_invalid_observation2_raises_keyerror(self, setup_db):
        """Test that invalid observation2 code raises KeyError"""
        test_obj, temp_filename = setup_db

        with pytest.raises(KeyError) as excinfo:
            test_obj.population.frequencies(observation2="NONEXISTENT_CODE")

        assert "NONEXISTENT_CODE" in str(excinfo.value)

    def test_frequencies_invalid_code_in_list_raises_keyerror(self, setup_db):
        """Test that invalid code in a list raises KeyError mentioning the invalid code"""
        test_obj, temp_filename = setup_db

        # Get a valid observation code
        cur = test_obj.conn.cursor()
        cur.execute("SELECT obs_code FROM lookup_observations LIMIT 1")
        result = cur.fetchone()
        if result is None:
            pytest.skip("No observation codes in database")
        valid_code = result[0]

        with pytest.raises(KeyError) as excinfo:
            test_obj.population.frequencies(observation1=[valid_code, "INVALID_CODE"])

        assert "INVALID_CODE" in str(excinfo.value)

    def test_frequencies_empty_when_no_frequencies(self, setup_db):
        """Test frequencies() returns empty results when frequencies table is empty"""
        test_obj, temp_filename = setup_db

        # Clear the frequencies table
        test_obj.conn.execute("DELETE FROM frequencies")
        test_obj.conn.commit()

        frequencies = test_obj.population.frequencies()
        frequencies_df = test_obj.population.frequencies(as_pandas=True)

        # Verify empty results
        assert len(frequencies) == 0
        assert len(frequencies_df) == 0

    def test_frequencies_empty_when_no_matching_records(self, setup_db):
        """Test frequencies() returns empty results when filters match nothing"""
        test_obj, temp_filename = setup_db

        # Get two observation codes
        cur = test_obj.conn.cursor()
        cur.execute("SELECT obs_code FROM lookup_observations LIMIT 2")
        results = cur.fetchall()
        if len(results) < 2:
            pytest.skip("Not enough observation codes in database")

        # Clear frequencies and add a specific record
        test_obj.conn.execute("DELETE FROM frequencies")
        test_obj.conn.commit()

        # Query with valid codes but no matching data should return empty
        frequencies = test_obj.population.frequencies(observation1=results[0][0])

        # Verify empty results (not an error)
        assert isinstance(frequencies, list)
        assert len(frequencies) == 0

    def test_frequencies_temporal_distance_is_integer(self, setup_db):
        """Test that temporal_distance is an integer"""
        test_obj, temp_filename = setup_db

        frequencies = test_obj.population.frequencies()

        # If there are frequencies, verify temporal_distance is an integer
        if len(frequencies) > 0:
            for freq in frequencies:
                assert isinstance(freq["temporal_distance"], int)

    def test_frequencies_counts_are_integers(self, setup_db):
        """Test that observation_cnt and patient_cnt are integers"""
        test_obj, temp_filename = setup_db

        frequencies = test_obj.population.frequencies()

        # If there are frequencies, verify counts are integers
        if len(frequencies) > 0:
            for freq in frequencies:
                assert isinstance(freq["observation_cnt"], int)
                assert isinstance(freq["patient_cnt"], int)
