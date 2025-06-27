import pytest

import tspmdb
import tempfile
import os.path
import sqlite3

def test_initialization_nofile():
    with pytest.raises(Exception):
        test_obj = tspmdb.TspmDB()
        test_obj.close()

def test_initialization_w_filename():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # delete temp file if it exists
    if os.path.exists(temp_filename):
        os.remove(temp_filename)
    # create object - creating new db file
    test_obj = tspmdb.TspmDB(temp_filename)
    assert isinstance(test_obj, tspmdb.TspmDB)
    assert os.path.exists(temp_filename)

    # cleanup
    test_obj.close()
    os.remove(temp_filename)

def test_initialization_existing_file_nondestructive():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # delete temp file if it exists
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # create sqlite3 db
    with sqlite3.connect(temp_filename) as con:
        assert os.path.exists(temp_filename)

        with pytest.raises(FileExistsError):
            # create object - defaults to non-destructive
            test_obj = tspmdb.TspmDB(temp_filename)

        with pytest.raises(FileExistsError):
            # create object - don't overwrite db file explicit
            test_obj = tspmdb.TspmDB(temp_filename, destructive=False)
    # cleanup
    con.close()
    os.remove(temp_filename)


def test_initialization_existing_file_destructive():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # delete temp file if it exists
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # create sqlite3 db
    with sqlite3.connect(temp_filename) as con:
        assert os.path.exists(temp_filename)
    con.close()

    # create object - overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    assert isinstance(test_obj, tspmdb.TspmDB)

    # cleanup
    test_obj.close()
    os.remove(temp_filename)


def test_ingest_csv_file_missing():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    # ingest non-existent zip file
    with pytest.raises(FileNotFoundError):
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.ingest_csv("non-existent.csv", col_names)
    # cleanup
    test_obj.close()
    os.remove(temp_filename)



def test_ingest_csv_zip_file_missing():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    # ingest non-existent zip file
    with pytest.raises(FileNotFoundError):
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.ingest_csv("test_data.csv", col_names, zipfile="./non-existent.zip")
    # cleanup
    test_obj.close()
    os.remove(temp_filename)

def test_ingest_csv_not_in_zip():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    # ingest non-existent zip file
    with pytest.raises(KeyError):
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.ingest_csv("non-existent.csv", col_names, zipfile="./test_data.zip")
    # cleanup
    test_obj.close()
    os.remove(temp_filename)

def test_ingest_csv_in_zip():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    # ingest csv file within a zip file
    col_names = {
        "PATIENT": "PatientID",
        "DATE": "ObservationDate",
        "CODE": "ObservationCode",
        "TEXT": "Description"
    }
    test_obj.ingest_csv("test_data.csv", col_names, zipfile="./test_data.zip")

    # TODO: See if correct records are in the table

    # cleanup
    test_obj.close()
    os.remove(temp_filename)

def test_ingest_csv_missing_colnames():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    # ingest non-existent zip file
    with pytest.raises(KeyError):
        col_names = {
            "missing_PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.ingest_csv("test_data.csv", col_names)
    with pytest.raises(KeyError):
        col_names = {
            "PATIENT": "PatientID",
            "missing_DATE": "ObservationDate",
            "CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.ingest_csv("test_data.csv", col_names)
    with pytest.raises(KeyError):
        col_names = {
            "PATIENT": "PatientID",
            "DATE": "ObservationDate",
            "missing_CODE": "ObservationCode",
            "TEXT": "Description"
        }
        test_obj.ingest_csv("test_data.csv", col_names)
    # cleanup
    test_obj.close()
    os.remove(temp_filename)


def test_ingest_sqlite():
    pass

def test_sequence_generation():
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "testing_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=True)
    # ingest csv file within a zip file
    col_names = {
        "PATIENT": "PatientID",
        "DATE": "ObservationDate",
        "CODE": "ObservationCode",
        "TEXT": "Description"
    }
    test_obj.ingest_csv("test_data.csv", col_names)
    test_obj.generate_sequences()

    # confirm data
    # with sqlite3.connect(temp_filename) as con:

    # cleanup
    test_obj.close()
    os.remove(temp_filename)

def test_seqgen_named_table():
    pass

def test_seqgen_temporal_bucket():
    pass


def test_INGEST_ALL_THE_DATA():

    ZIPFILE = "D:/RESEARCH/TSPM+/test_data/100k_synthea_covid19_csv.zip"

    import time
    temp_dir = tempfile.gettempdir()
    temp_filename = os.path.join(temp_dir, "actual_tspmdb.sqlite3")
    # create object - don't overwrite db file explicit
    test_obj = tspmdb.TspmDB(temp_filename, destructive=False, parallel_threads=4)

    # ingest all the data
    # ingest_start = time.perf_counter()
    # col_names = {
    #     "PATIENT": "PATIENT",
    #     "DATE": "START",
    #     "CODE": "CODE",
    #     "TEXT": "DESCRIPTION"
    # }
    # test_obj.ingest_csv("100k_synthea_covid19_csv/medications.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # test_obj.ingest_csv("100k_synthea_covid19_csv/devices.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # test_obj.ingest_csv("100k_synthea_covid19_csv/allergies.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # test_obj.ingest_csv("100k_synthea_covid19_csv/conditions.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # col_names = {
    #     "PATIENT": "PATIENT",
    #     "DATE": "DATE",
    #     "CODE": "CODE",
    #     "TEXT": "DESCRIPTION"
    # }
    # test_obj.ingest_csv("100k_synthea_covid19_csv/procedures.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # test_obj.ingest_csv("100k_synthea_covid19_csv/immunizations.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # test_obj.ingest_csv("100k_synthea_covid19_csv/observations.csv", col_names, zipfile=ZIPFILE, show_progress=False)
    # ingest_end = time.perf_counter()
    # elapsed = ingest_end - ingest_start
    # print(f"Ingest Time: {elapsed:0.4f} seconds")

    test_obj.generate_sequences()
    raise SyntaxError(f"Ingest Time: {elapsed:0.4f} seconds")


    # cleanup
    test_obj.close()
    # os.remove(temp_filename)
