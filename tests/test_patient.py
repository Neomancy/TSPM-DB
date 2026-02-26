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


class TestPatientInstance:
    """Tests for the PatientInstance class"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_patient_instance.sqlite3")

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

    def test_patient_instance_creation_valid_patient(self, setup_db):
        """Test creating a PatientInstance with a valid patient_num"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with a patient to ensure patient exists
        my_subpop = test_obj.subpopulation.create("TEST_PI_1", ["TEST_PATIENT_1"], "Test subpopulation")

        # Get the patient_num
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        assert len(patient_nums) == 1
        patient_num = patient_nums[0]

        # Create a PatientInstance
        patient_instance = PatientInstance(test_obj, patient_num)

        # Verify the id property returns the correct patient_id
        assert patient_instance.id == "TEST_PATIENT_1"

    def test_patient_instance_invalid_patient_num_raises_keyerror(self, setup_db):
        """Test that creating a PatientInstance with an invalid patient_num raises KeyError"""
        test_obj, temp_filename = setup_db

        # Try to create a PatientInstance with a non-existent patient_num
        with pytest.raises(KeyError):
            PatientInstance(test_obj, 99999)

    def test_patient_instance_id_property_is_readonly(self, setup_db):
        """Test that the id property is read-only"""
        test_obj, temp_filename = setup_db

        # Create a patient
        my_subpop = test_obj.subpopulation.create("TEST_PI_2", ["READONLY_PATIENT"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_num = patient_nums[0]

        patient_instance = PatientInstance(test_obj, patient_num)

        # Verify the id property returns the correct value
        assert patient_instance.id == "READONLY_PATIENT"

        # Attempting to set the id property should raise an AttributeError
        with pytest.raises(AttributeError):
            patient_instance.id = "NEW_ID"

    def test_patient_instance_multiple_patients(self, setup_db):
        """Test creating multiple PatientInstance objects"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with multiple patients
        my_subpop = test_obj.subpopulation.create("TEST_PI_3", ["PATIENT_A", "PATIENT_B", "PATIENT_C"], "Test subpopulation")

        # Get the patient_nums
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_ids = my_subpop.patients.list()

        # Create PatientInstance for each patient
        for patient_num, expected_id in zip(patient_nums, patient_ids):
            patient_instance = PatientInstance(test_obj, patient_num)
            assert patient_instance.id == expected_id


class TestPatientInstanceEvents:
    """Tests for the PatientInstance.events() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_patient_events.sqlite3")

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

    def test_events_returns_list_by_default(self, setup_db):
        """Test that events() returns a list by default"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get events
        patient_instance = PatientInstance(test_obj, patient_num)
        events = patient_instance.events()

        # Verify it returns a list
        assert isinstance(events, list)

    def test_events_returns_pandas_dataframe(self, setup_db):
        """Test that events(as_pandas=True) returns a DataFrame"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get events as DataFrame
        patient_instance = PatientInstance(test_obj, patient_num)
        events_df = patient_instance.events(as_pandas=True)

        # Verify it returns a DataFrame with correct columns
        assert isinstance(events_df, pd.DataFrame)
        assert "obs_code" in events_df.columns
        assert "obs_description" in events_df.columns
        assert "obs_date" in events_df.columns

    def test_events_list_contains_correct_keys(self, setup_db):
        """Test that events list contains dictionaries with correct keys"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get events
        patient_instance = PatientInstance(test_obj, patient_num)
        events = patient_instance.events()

        # If there are events, verify they have the correct keys
        if len(events) > 0:
            for event in events:
                assert "obs_code" in event
                assert "obs_description" in event
                assert "obs_date" in event

    def test_events_patient_with_no_events(self, setup_db):
        """Test events() for a patient with no events in source_data"""
        test_obj, temp_filename = setup_db

        # Create a new patient that has no events
        my_subpop = test_obj.subpopulation.create("TEST_EVENTS_1", ["NEW_PATIENT_NO_EVENTS"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_num = patient_nums[0]

        # Create a PatientInstance and get events
        patient_instance = PatientInstance(test_obj, patient_num)
        events = patient_instance.events()

        # Verify it returns an empty list
        assert isinstance(events, list)
        assert len(events) == 0

    def test_events_patient_with_no_events_as_pandas(self, setup_db):
        """Test events(as_pandas=True) for a patient with no events"""
        test_obj, temp_filename = setup_db

        # Create a new patient that has no events
        my_subpop = test_obj.subpopulation.create("TEST_EVENTS_2", ["NEW_PATIENT_NO_EVENTS_2"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_num = patient_nums[0]

        # Create a PatientInstance and get events as DataFrame
        patient_instance = PatientInstance(test_obj, patient_num)
        events_df = patient_instance.events(as_pandas=True)

        # Verify it returns an empty DataFrame with correct columns
        assert isinstance(events_df, pd.DataFrame)
        assert len(events_df) == 0
        assert "obs_code" in events_df.columns
        assert "obs_description" in events_df.columns
        assert "obs_date" in events_df.columns


class TestPatientInstanceSequences:
    """Tests for the PatientInstance.sequences() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_patient_sequences.sqlite3")

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

    def test_sequences_returns_list_by_default(self, setup_db):
        """Test that sequences() returns a list by default"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # Verify it returns a list
        assert isinstance(sequences, list)

    def test_sequences_returns_pandas_dataframe(self, setup_db):
        """Test that sequences(as_pandas=True) returns a DataFrame"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences as DataFrame
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences_df = patient_instance.sequences(as_pandas=True)

        # Verify it returns a DataFrame with correct columns
        assert isinstance(sequences_df, pd.DataFrame)
        assert "patient_id" in sequences_df.columns
        assert "obs_code_1" in sequences_df.columns
        assert "obs_code_2" in sequences_df.columns
        assert "time_diff" in sequences_df.columns
        assert "occurrence_count" in sequences_df.columns

    def test_sequences_list_contains_correct_keys(self, setup_db):
        """Test that sequences list contains dictionaries with correct keys"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # If there are sequences, verify they have the correct keys
        if len(sequences) > 0:
            for seq in sequences:
                assert "patient_id" in seq
                assert "obs_code_1" in seq
                assert "obs_code_2" in seq
                assert "time_diff" in seq
                assert "occurrence_count" in seq

    def test_sequences_patient_id_is_string(self, setup_db):
        """Test that patient_id in sequences is a string (not patient_num)"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # If there are sequences, verify patient_id is a string
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["patient_id"], str)

    def test_sequences_obs_codes_are_strings(self, setup_db):
        """Test that obs_code_1 and obs_code_2 are strings (not obs_code_id integers)"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # If there are sequences, verify obs_codes are strings
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["obs_code_1"], str)
                assert isinstance(seq["obs_code_2"], str)

    def test_sequences_time_diff_is_integer(self, setup_db):
        """Test that time_diff is an integer"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # If there are sequences, verify time_diff is an integer
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["time_diff"], int)

    def test_sequences_occurrence_count_is_integer(self, setup_db):
        """Test that occurrence_count is an integer"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # If there are sequences, verify occurrence_count is an integer
        if len(sequences) > 0:
            for seq in sequences:
                assert isinstance(seq["occurrence_count"], int)

    def test_sequences_patient_with_no_events(self, setup_db):
        """Test sequences() for a patient with no events in source_data"""
        test_obj, temp_filename = setup_db

        # Create a new patient that has no events
        my_subpop = test_obj.subpopulation.create("TEST_SEQ_1", ["NEW_PATIENT_NO_SEQUENCES"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_num = patient_nums[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # Verify it returns an empty list (no events means no sequences)
        assert isinstance(sequences, list)
        assert len(sequences) == 0

    def test_sequences_patient_with_no_events_as_pandas(self, setup_db):
        """Test sequences(as_pandas=True) for a patient with no events"""
        test_obj, temp_filename = setup_db

        # Create a new patient that has no events
        my_subpop = test_obj.subpopulation.create("TEST_SEQ_2", ["NEW_PATIENT_NO_SEQUENCES_2"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_num = patient_nums[0]

        # Create a PatientInstance and get sequences as DataFrame
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences_df = patient_instance.sequences(as_pandas=True)

        # Verify it returns an empty DataFrame with correct columns
        assert isinstance(sequences_df, pd.DataFrame)
        assert len(sequences_df) == 0
        assert "patient_id" in sequences_df.columns
        assert "obs_code_1" in sequences_df.columns
        assert "obs_code_2" in sequences_df.columns
        assert "time_diff" in sequences_df.columns
        assert "occurrence_count" in sequences_df.columns

    def test_sequences_as_iterator(self, setup_db):
        """Test that sequences(as_iterator=True) returns an iterator"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences as iterator
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences_iter = patient_instance.sequences(as_iterator=True)

        # Verify it returns a generator/iterator
        import types
        assert isinstance(sequences_iter, types.GeneratorType)

        # Consume the iterator and verify the items have correct keys
        for seq in sequences_iter:
            assert "patient_id" in seq
            assert "obs_code_1" in seq
            assert "obs_code_2" in seq
            assert "time_diff" in seq
            assert "occurrence_count" in seq

    def test_sequences_patient_id_matches_instance_id(self, setup_db):
        """Test that patient_id in sequences matches the PatientInstance.id"""
        test_obj, temp_filename = setup_db

        # Get a patient from the ingested test data
        cur = test_obj.conn.cursor()
        cur.execute("SELECT patient_num FROM lookup_patients LIMIT 1")
        result = cur.fetchone()
        assert result is not None
        patient_num = result[0]

        # Create a PatientInstance and get sequences
        patient_instance = PatientInstance(test_obj, patient_num)
        sequences = patient_instance.sequences()

        # If there are sequences, verify patient_id matches the instance id
        if len(sequences) > 0:
            for seq in sequences:
                assert seq["patient_id"] == patient_instance.id
