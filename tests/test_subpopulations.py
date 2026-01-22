import pytest

import tempfile
import os.path
import sqlite3

import pandas as pd
import numpy as np

import tspmdb
from Subpopulation import Subpopulation
from SubpopulationInstance import SubpopulationInstance


class TestSubpopulationInstancePatientsAdd:
    """Tests for the SubpopulationInstancePatients.add() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_subpop_add.sqlite3")

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

    def test_add_single_string_patient(self, setup_db):
        """Test adding a single patient as a string"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_ADD_1", [], "Test subpopulation for add")
        assert isinstance(my_subpop, SubpopulationInstance)

        # Add a single patient as string
        my_subpop.patients.add("TEST_PATIENT")

        # Verify the patient was added
        patients = my_subpop.patients.list()
        assert len(patients) == 1
        assert "TEST_PATIENT" in patients

    def test_add_new_patient_creates_lookup_entry(self, setup_db):
        """Test adding a patient that does not exist in lookup_patients creates a new entry"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_ADD_2", [], "Test subpopulation for add")

        # Add a new patient that doesn't exist yet
        my_subpop.patients.add("NEW_PATIENT_123")

        # Verify the patient was added to lookup_patients
        with sqlite3.connect(temp_filename) as con:
            cur = con.execute("SELECT patient_id FROM lookup_patients WHERE patient_id = ?", ("NEW_PATIENT_123",))
            result = cur.fetchone()
            assert result is not None
            assert result[0] == "NEW_PATIENT_123"

        # Verify the patient was added to the subpopulation
        patients = my_subpop.patients.list()
        assert "NEW_PATIENT_123" in patients

    def test_add_multiple_patients_as_list(self, setup_db):
        """Test adding multiple patients as a list"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_ADD_3", [], "Test subpopulation for multiple adds")

        # Add multiple patients as a list
        my_subpop.patients.add(["PATIENT_A", "PATIENT_B", "PATIENT_C"])

        # Verify all patients were added
        patients = my_subpop.patients.list()
        assert len(patients) == 3
        assert "PATIENT_A" in patients
        assert "PATIENT_B" in patients
        assert "PATIENT_C" in patients

    def test_add_single_patient_one_at_a_time(self, setup_db):
        """Test adding multiple patients one at a time"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_ADD_4", [], "Test subpopulation")

        # Add patients one at a time
        my_subpop.patients.add("PATIENT_X")
        my_subpop.patients.add("PATIENT_Y")
        my_subpop.patients.add("PATIENT_Z")

        patients = my_subpop.patients.list()
        assert len(patients) == 3
        assert "PATIENT_X" in patients
        assert "PATIENT_Y" in patients
        assert "PATIENT_Z" in patients

    def test_add_patient_to_subpopulation_with_existing_patients(self, setup_db):
        """Test adding a patient to a subpopulation that already has patients"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with an initial patient
        my_subpop = test_obj.subpopulation.create("TEST_ADD_5", ["INITIAL_PATIENT"], "Test subpopulation with initial patient")

        # Add another patient
        my_subpop.patients.add("ADDED_PATIENT")

        # Verify both patients are in the subpopulation
        patients = my_subpop.patients.list()
        assert len(patients) == 2
        assert "INITIAL_PATIENT" in patients
        assert "ADDED_PATIENT" in patients

    def test_add_with_no_id_translation_valid_patient_num(self, setup_db):
        """Test adding a patient using patient_num with no_id_translation=True"""
        test_obj, temp_filename = setup_db

        # First, create a patient in lookup_patients and get its patient_num
        my_subpop = test_obj.subpopulation.create("TEST_ADD_6", ["EXISTING_PATIENT"], "Test subpopulation")

        # Get the patient_num for the existing patient
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        assert len(patient_nums) == 1
        existing_patient_num = patient_nums[0]

        # Create a new subpopulation and add the patient using patient_num
        my_subpop2 = test_obj.subpopulation.create("TEST_ADD_6B", [], "Another test subpopulation")
        my_subpop2.patients.add(existing_patient_num, no_id_translation=True)

        # Verify the patient was added
        patients = my_subpop2.patients.list()
        assert len(patients) == 1
        assert "EXISTING_PATIENT" in patients

    def test_add_with_no_id_translation_invalid_patient_num_raises_keyerror(self, setup_db):
        """Test that adding a non-existent patient_num with no_id_translation=True raises KeyError"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_ADD_7", [], "Test subpopulation")

        # Try to add a patient_num that doesn't exist
        with pytest.raises(KeyError):
            my_subpop.patients.add(99999, no_id_translation=True)

    def test_add_multiple_patient_nums_with_no_id_translation(self, setup_db):
        """Test adding multiple patient_nums as a list with no_id_translation=True"""
        test_obj, temp_filename = setup_db

        # Create patients first
        my_subpop = test_obj.subpopulation.create("TEST_ADD_8", ["PATIENT_1", "PATIENT_2"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        assert len(patient_nums) == 2

        # Create a new subpopulation and add patients using patient_nums
        my_subpop2 = test_obj.subpopulation.create("TEST_ADD_8B", [], "Another test subpopulation")
        my_subpop2.patients.add(patient_nums, no_id_translation=True)

        # Verify the patients were added
        patients = my_subpop2.patients.list()
        assert len(patients) == 2
        assert "PATIENT_1" in patients
        assert "PATIENT_2" in patients

    def test_add_mixed_valid_invalid_patient_nums_raises_keyerror(self, setup_db):
        """Test that adding a list with one invalid patient_num raises KeyError"""
        test_obj, temp_filename = setup_db

        # Create a patient first
        my_subpop = test_obj.subpopulation.create("TEST_ADD_9", ["VALID_PATIENT"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        valid_patient_num = patient_nums[0]

        # Create a new subpopulation and try to add valid and invalid patient_nums
        my_subpop2 = test_obj.subpopulation.create("TEST_ADD_9B", [], "Another test subpopulation")
        with pytest.raises(KeyError):
            my_subpop2.patients.add([valid_patient_num, 99999], no_id_translation=True)

    def test_add_single_integer_patient_num(self, setup_db):
        """Test adding a single patient_num as an integer with no_id_translation=True"""
        test_obj, temp_filename = setup_db

        # Create a patient first
        my_subpop = test_obj.subpopulation.create("TEST_ADD_10", ["INT_PATIENT"], "Test subpopulation")
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        patient_num = patient_nums[0]

        # Create a new subpopulation and add the patient using integer patient_num
        my_subpop2 = test_obj.subpopulation.create("TEST_ADD_10B", [], "Another test subpopulation")
        my_subpop2.patients.add(patient_num, no_id_translation=True)

        patients = my_subpop2.patients.list()
        assert len(patients) == 1
        assert "INT_PATIENT" in patients


class TestSubpopulationInstancePatientsList:
    """Tests for the SubpopulationInstancePatients.list() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_subpop_list.sqlite3")

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

    def test_list_patients_empty_subpopulation(self, setup_db):
        """Test listing patients from an empty subpopulation"""
        test_obj, temp_filename = setup_db

        # Create an empty subpopulation
        my_subpop = test_obj.subpopulation.create("TEST_LIST_EMPTY", [], "Empty subpopulation")

        # List patients - should return empty list
        patients = my_subpop.patients.list()
        assert isinstance(patients, list)
        assert len(patients) == 0

    def test_list_patients_with_id_translation(self, setup_db):
        """Test listing patients with default id translation (patient_id strings)"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with patients
        my_subpop = test_obj.subpopulation.create("TEST_LIST_1", ["PATIENT_A", "PATIENT_B", "PATIENT_C"], "Test subpopulation")

        # List patients with default settings (id translation enabled)
        patients = my_subpop.patients.list()
        assert isinstance(patients, list)
        assert len(patients) == 3
        assert "PATIENT_A" in patients
        assert "PATIENT_B" in patients
        assert "PATIENT_C" in patients

    def test_list_patients_no_id_translation(self, setup_db):
        """Test listing patients without id translation (raw patient_num values)"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with patients
        my_subpop = test_obj.subpopulation.create("TEST_LIST_2", ["PATIENT_X", "PATIENT_Y"], "Test subpopulation")

        # List patients without id translation
        patients = my_subpop.patients.list(no_id_translation=True)
        assert isinstance(patients, list)
        assert len(patients) == 2
        # Should be integers (patient_num values)
        for patient_num in patients:
            assert isinstance(patient_num, int)

    def test_list_patients_as_pandas_dataframe(self, setup_db):
        """Test listing patients as a Pandas DataFrame"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with patients
        my_subpop = test_obj.subpopulation.create("TEST_LIST_3", ["PATIENT_1", "PATIENT_2"], "Test subpopulation")

        # List patients as DataFrame
        patients_df = my_subpop.patients.list(as_pandas=True)
        assert isinstance(patients_df, pd.DataFrame)
        assert len(patients_df) == 2
        assert "patient_id" in patients_df.columns
        assert "PATIENT_1" in patients_df["patient_id"].values
        assert "PATIENT_2" in patients_df["patient_id"].values

    def test_list_patients_as_pandas_no_id_translation(self, setup_db):
        """Test listing patients as a Pandas DataFrame without id translation"""
        test_obj, temp_filename = setup_db

        # Create a subpopulation with patients
        my_subpop = test_obj.subpopulation.create("TEST_LIST_4", ["PATIENT_M", "PATIENT_N"], "Test subpopulation")

        # List patients as DataFrame without id translation
        patients_df = my_subpop.patients.list(no_id_translation=True, as_pandas=True)
        assert isinstance(patients_df, pd.DataFrame)
        assert len(patients_df) == 2
        assert "patient_num" in patients_df.columns
        # Should contain integer values
        for val in patients_df["patient_num"].values:
            assert isinstance(patients_df["patient_num"].values[0], np.int64)

    def test_list_patients_after_adding(self, setup_db):
        """Test listing patients after adding patients via patients.add()"""
        test_obj, temp_filename = setup_db

        # Create an empty subpopulation
        my_subpop = test_obj.subpopulation.create("TEST_LIST_5", [], "Test subpopulation")

        # Add patients using the new API
        my_subpop.patients.add("ADDED_PATIENT_1")
        my_subpop.patients.add("ADDED_PATIENT_2")

        # List patients
        patients = my_subpop.patients.list()
        assert isinstance(patients, list)
        assert len(patients) == 2
        assert "ADDED_PATIENT_1" in patients
        assert "ADDED_PATIENT_2" in patients



class TestSubpopulationInstancePatientsRemove:
    """Tests for the SubpopulationInstancePatients.remove() method"""

    @pytest.fixture
    def setup_db(self):
        """Create a temporary database with test data"""
        temp_dir = tempfile.gettempdir()
        temp_filename = os.path.join(temp_dir, "test_subpop_remove.sqlite3")

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

    def test_remove_single_patient_string(self, setup_db):
        """Test removing a single patient using patient_id string"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_1", ["PATIENT_A", "PATIENT_B", "PATIENT_C"], "Test subpopulation")

        # Remove a single patient
        my_subpop.patients.remove("PATIENT_B")

        # Verify the patient was removed
        patients = my_subpop.patients.list()
        assert len(patients) == 2
        assert "PATIENT_A" in patients
        assert "PATIENT_B" not in patients
        assert "PATIENT_C" in patients

    def test_remove_multiple_patients_as_list(self, setup_db):
        """Test removing multiple patients as a list"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_2", ["PATIENT_1", "PATIENT_2", "PATIENT_3", "PATIENT_4"], "Test subpopulation")

        # Remove multiple patients as a list
        my_subpop.patients.remove(["PATIENT_2", "PATIENT_4"])

        # Verify the patients were removed
        patients = my_subpop.patients.list()
        assert len(patients) == 2
        assert "PATIENT_1" in patients
        assert "PATIENT_2" not in patients
        assert "PATIENT_3" in patients
        assert "PATIENT_4" not in patients

    def test_remove_nonexistent_patient_silently_skipped(self, setup_db):
        """Test that removing a non-existent patient_id is silently skipped"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_3", ["PATIENT_X", "PATIENT_Y"], "Test subpopulation")

        # Remove a patient that doesn't exist - should not raise an error
        my_subpop.patients.remove("NONEXISTENT_PATIENT")

        # Verify original patients are still there
        patients = my_subpop.patients.list()
        assert len(patients) == 2
        assert "PATIENT_X" in patients
        assert "PATIENT_Y" in patients

    def test_remove_all_patients(self, setup_db):
        """Test removing all patients from a subpopulation"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_4", ["PATIENT_A", "PATIENT_B"], "Test subpopulation")

        # Remove all patients
        my_subpop.patients.remove(["PATIENT_A", "PATIENT_B"])

        # Verify subpopulation is now empty
        patients = my_subpop.patients.list()
        assert len(patients) == 0

    def test_remove_with_no_id_translation_valid_patient_num(self, setup_db):
        """Test removing a patient using patient_num with no_id_translation=True"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_5", ["PATIENT_M", "PATIENT_N"], "Test subpopulation")

        # Get the patient_nums
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        assert len(patient_nums) == 2

        # Remove one patient using patient_num
        my_subpop.patients.remove(patient_nums[0], no_id_translation=True)

        # Verify only one patient remains
        patients = my_subpop.patients.list()
        assert len(patients) == 1

    def test_remove_with_no_id_translation_invalid_patient_num_raises_keyerror(self, setup_db):
        """Test that removing a non-existent patient_num with no_id_translation=True raises KeyError"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_6", ["PATIENT_P"], "Test subpopulation")

        # Try to remove a patient_num that doesn't exist
        with pytest.raises(KeyError):
            my_subpop.patients.remove(99999, no_id_translation=True)

    def test_remove_multiple_patient_nums_with_no_id_translation(self, setup_db):
        """Test removing multiple patient_nums as a list with no_id_translation=True"""
        test_obj, temp_filename = setup_db

        my_subpop = test_obj.subpopulation.create("TEST_REMOVE_7", ["PATIENT_1", "PATIENT_2", "PATIENT_3"], "Test subpopulation")

        # Get the patient_nums
        patient_nums = my_subpop.patients.list(no_id_translation=True)
        assert len(patient_nums) == 3

        # Remove two patients using patient_nums
        my_subpop.patients.remove([patient_nums[0], patient_nums[2]], no_id_translation=True)

        # Verify only one patient remains
        patients = my_subpop.patients.list()
        assert len(patients) == 1

    def test_remove_patient_not_in_subpopulation(self, setup_db):
        """Test removing a patient that exists in lookup_patients but not in the subpopulation"""
        test_obj, temp_filename = setup_db

        # Create two subpopulations with different patients
        my_subpop1 = test_obj.subpopulation.create("TEST_REMOVE_8A", ["PATIENT_A"], "Subpopulation 1")
        my_subpop2 = test_obj.subpopulation.create("TEST_REMOVE_8B", ["PATIENT_B"], "Subpopulation 2")

        # Try to remove PATIENT_B from subpop1 (it exists in lookup_patients but not in subpop1)
        my_subpop1.patients.remove("PATIENT_B")

        # Verify PATIENT_A is still in subpop1
        patients1 = my_subpop1.patients.list()
        assert len(patients1) == 1
        assert "PATIENT_A" in patients1

        # Verify PATIENT_B is still in subpop2
        patients2 = my_subpop2.patients.list()
        assert len(patients2) == 1
        assert "PATIENT_B" in patients2