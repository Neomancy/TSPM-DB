import tspmdb
from SubpopulationInstance import SubpopulationInstance


class Subpopulation:
    """ class used to create and retrieve subpopulations """
    def __init__(self, tspmdb_ref):
        self.tspmdb = tspmdb_ref
        self.db = tspmdb_ref.db

    # ----------------------------------------------------------------------------------------
    def help(self):
        print("[HELP] Subpopulation Operations")
        print("TspmDb.subpopulation")
        print("------------------------------------------------------------------------------")
        print(".help()                             Displays this help message")
        print(".list()                             Lists all existing subpopulations in the database")
        print(".get(identifier)                    Gets an existing subpopulation from the database")
        print(".create(identifier, id_list, [name])  Generates a new subpopulation in the database")
        return None

    # ----------------------------------------------------------------------------------------
    def create(self, identifier:str, patient_ids:list, description:str = "", destructive:bool = False):
        """ Creates a new subpopulation and returns its representation """
        cur = self.tspmdb.conn.cursor()
        cur.execute("SELECT subpop_num FROM subpopulations WHERE subpop_id LIKE ?", (identifier,))
        results = cur.fetchall()
        if len(results) != 0:
            if not destructive:
                raise NameError("Subpopulation already exists")
            # destroy the existing definition
            self.delete(identifier, destructive=True)
        # create the subpopulation
        cur.execute("INSERT INTO subpopulations (subpop_id, description) VALUES (?, ?)", (identifier, description))
        subpop_num = cur.lastrowid

        # translate the patient_ids to DB's patient_nums
        if len(patient_ids) > 0:
            insert_list = []
            for patient_id in patient_ids:
                # lookup the patient_nums from the patient_ids
                cur.execute("SELECT coalesce(max(patient_num), -1) FROM lookup_patients WHERE patient_id = ?", (patient_id,))
                patient_num = cur.fetchone()[0]
                if patient_num == -1:
                    # patient id does not exist, lets add it
                    cur.execute("INSERT INTO lookup_patients (patient_id) VALUES (?)", (patient_id,))
                    patient_num = cur.lastrowid
                insert_list.append((subpop_num, patient_num))
            # insert list of passed patients
            cur.executemany("INSERT INTO subpopulation_patients (subpop_num, patient_num) VALUES (?, ?)", insert_list)
        self.tspmdb.conn.commit()

        return SubpopulationInstance(self.tspmdb, subpop_num)

    # ----------------------------------------------------------------------------------------
    def delete(self, identifier:str, destructive:bool = False):
        """ Deletes a subpopulation from the database """
        cur = self.tspmdb.conn.cursor()
        cur.execute("SELECT subpop_num FROM subpopulations WHERE subpop_id LIKE ?", (identifier,))
        results = cur.fetchall()
        if len(results) == 0:
            raise NameError(f"Subpopulation \"${identifier}\" does not exist")

        if not destructive:
            raise NameError("Cannot delete subpopulations unless you specify \"destructive=True\" as a parameter")

        target_num = results[0][0]
        cur.execute("DELETE FROM subpopulations WHERE subpop_num = ?", (target_num,))
        cur.execute("DELETE FROM subpopulation_patients WHERE subpop_num = ?", (target_num,))
        self.tspmdb.conn.commit()

    # ----------------------------------------------------------------------------------------
    def get(self, identifier: str):
        """ Retrieves a subpopulation and returns its representation """
        # see if the subpopulation identifier exists
        cur = self.db.cursor()
        results = cur.execute("SELECT subpop_num FROM subpopulation WHERE subpop_id LIKE ?", (identifier,))
        if len(results) > 0:
            return tspmdb.SubpopulationInstance(self.tspmdb, results[0][0])
        return None

    # ----------------------------------------------------------------------------------------
    def list(self, search: str = ""):
        """ Lists all subpopulations in the database """
        # search for the subpopulation identifier
        search_str = search + "%"

        cur = self.db.cursor()
        results = cur.execute("""
            SELECT 
                subpop_id AS id,
                description,
                COUNT(*) AS patient_count
            FROM subpopulation AS s 
            JOIN subpopulation_patients AS sp ON (s.subpop_num = sp.subpop_num)
            WHERE subpop_id LIKE "?"
            GROUP BY subpop_id;""", (search_str,))

        ret = []
        for row in results:
            ret.append((row["id"], row["patient_count"], row["description"]))

        return ret
