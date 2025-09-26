import pytest
import tempfile
import os
import tspmdb

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

    test_obj.dataset.ingest("test_data.csv", col_names, zip_file="./test_data.zip")
    # TODO: See if correct records are in the table

    temporal_config = [
            (0, 1),
            (1, 3),
            (3, 7),
            (7, 15)
        ]
    test_obj.dataset.calculate(temporal_config, temporal_mode="DAYS")


    # cleanup
    test_obj.close()
#    os.remove(temp_filename)
