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

from pandas import DataFrame


class SubpopulationInstancePatients:
    def __init__(self, tspmdb_ref, subpop_instance):
        self._parent = tspmdb_ref
        self._subpop_instance = subpop_instance

    def help(self):
        print("[HELP] Subpopulation Instance Patients Operations")
        print("TspmDb.subpopulation.get(identifier).patients")
        print("------------------------------------------------------------------------------")
        print(" .help()                                Displays this help message")
        print(" .list()                                List of patients in the subpopulation")
        print(" .add(patient_id)                       Add a patient to the subpopulation")
        print(" .remove(patient_id)                    Remove a patient from the subpopulation")

    def list(self, no_id_translation: bool = False, as_pandas: bool = False):
        """
        Gets a list of patients in the subpopulation.

        Args:
            no_id_translation: If True, returns raw database patient_num values instead of patient_id strings.
                               Default is False (returns translated patient_id values).
            as_pandas: If True, returns a Pandas DataFrame instead of a Python list.
                       Default is False (returns a Python list).

        Returns:
            A list of patient identifiers (or patient_num if no_id_translation=True),
            or a Pandas DataFrame if as_pandas=True.
        """
        cur = self._parent.conn.cursor()
        subpop_num = self._subpop_instance.identifier

        if no_id_translation:
            # Return raw patient_num values
            cur.execute("""
                SELECT sp.patient_num
                FROM subpopulation_patients AS sp
                WHERE sp.subpop_num = ?
            """, (subpop_num,))
            results = cur.fetchall()
            patient_list = [row[0] for row in results]
        else:
            # Return translated patient_id values
            cur.execute("""
                SELECT lp.patient_id
                FROM subpopulation_patients AS sp
                JOIN lookup_patients AS lp ON (lp.patient_num = sp.patient_num)
                WHERE sp.subpop_num = ?
            """, (subpop_num,))
            results = cur.fetchall()
            patient_list = [row[0] for row in results]

        if as_pandas:
            column_name = "patient_num" if no_id_translation else "patient_id"
            return DataFrame(patient_list, columns=[column_name])
        else:
            return patient_list

    def add(self, patient_ids, no_id_translation: bool = False):
        """
        Adds patient(s) to the subpopulation.

        Args:
            patient_ids: A single patient identifier (string or integer) or a list of patient identifiers.
                         If no_id_translation is False, these should be patient_id strings.
                         If no_id_translation is True, these should be patient_num integers (database keys).
            no_id_translation: If False (default), patient_ids are treated as patient_id strings that need
                               to be looked up in lookup_patients. If a patient_id is not found, a new
                               record is created in lookup_patients.
                               If True, patient_ids are treated as patient_num database keys. They must
                               exist in lookup_patients or a KeyError is raised.

        Raises:
            KeyError: If no_id_translation is True and a patient_num does not exist in lookup_patients.
        """
        cur = self._parent.conn.cursor()
        subpop_num = self._subpop_instance.identifier

        # Normalize input to a list
        if isinstance(patient_ids, (str, int)):
            patient_ids = [patient_ids]

        insert_list = []

        if no_id_translation:
            # patient_ids are database keys (patient_num) - verify they exist
            for patient_num in patient_ids:
                cur.execute("SELECT patient_num FROM lookup_patients WHERE patient_num = ?", (patient_num,))
                result = cur.fetchone()
                if result is None:
                    raise KeyError(f"Patient with patient_num '{patient_num}' does not exist in lookup_patients")
                insert_list.append((subpop_num, patient_num))
        else:
            # patient_ids are patient_id strings - look up or create
            for patient_id in patient_ids:
                cur.execute("SELECT coalesce(max(patient_num), -1) FROM lookup_patients WHERE patient_id = ?", (patient_id,))
                patient_num = cur.fetchone()[0]

                if patient_num == -1:
                    # Patient id does not exist, add it to lookup_patients
                    cur.execute("INSERT INTO lookup_patients (patient_id) VALUES (?)", (patient_id,))
                    patient_num = cur.lastrowid

                insert_list.append((subpop_num, patient_num))

        # Insert all patients into the subpopulation
        cur.executemany("INSERT INTO subpopulation_patients (subpop_num, patient_num) VALUES (?, ?)", insert_list)
        self._parent.conn.commit()

    def remove(self, patient_ids, no_id_translation: bool = False):
        """
        Removes patient(s) from the subpopulation.

        Args:
            patient_ids: A single patient identifier (string or integer) or a list of patient identifiers.
                         If no_id_translation is False, these should be patient_id strings.
                         If no_id_translation is True, these should be patient_num integers (database keys).
            no_id_translation: If False (default), patient_ids are treated as patient_id strings that need
                               to be looked up in lookup_patients. If a patient_id is not found, it is
                               silently skipped (nothing to remove).
                               If True, patient_ids are treated as patient_num database keys. They must
                               exist in lookup_patients or a KeyError is raised.

        Raises:
            KeyError: If no_id_translation is True and a patient_num does not exist in lookup_patients.
        """
        cur = self._parent.conn.cursor()
        subpop_num = self._subpop_instance.identifier

        # Normalize input to a list
        if isinstance(patient_ids, (str, int)):
            patient_ids = [patient_ids]

        delete_list = []

        if no_id_translation:
            # patient_ids are database keys (patient_num) - verify they exist
            for patient_num in patient_ids:
                cur.execute("SELECT patient_num FROM lookup_patients WHERE patient_num = ?", (patient_num,))
                result = cur.fetchone()
                if result is None:
                    raise KeyError(f"Patient with patient_num '{patient_num}' does not exist in lookup_patients")
                delete_list.append((subpop_num, patient_num))
        else:
            # patient_ids are patient_id strings - look up
            for patient_id in patient_ids:
                cur.execute("SELECT patient_num FROM lookup_patients WHERE patient_id = ?", (patient_id,))
                result = cur.fetchone()
                if result is not None:
                    delete_list.append((subpop_num, result[0]))
                # If patient_id not found, silently skip (nothing to remove)

        # Delete patients from the subpopulation
        if delete_list:
            cur.executemany("DELETE FROM subpopulation_patients WHERE subpop_num = ? AND patient_num = ?", delete_list)
            self._parent.conn.commit()

    def get(self, patient_id, no_id_translation: bool = False):
        """ gets a single patient from the subpopulation """
        pass

    def event_counts(self, no_id_translation: bool = False, as_pandas: bool = True):
        """ gets a list of patient_ids with the number of their events in the database """
        pass