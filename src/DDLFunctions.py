import sqlite3


# ----------------------------------------------------------------------------------------
def Create_PATIENTS(db_conn, destructive:bool = False):
    """ create the patients lookup table in the database """
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    if "lookup_patients" not in tables:
        # create the missing table
        cur.execute("""
            CREATE TABLE lookup_patients (
                patient_num INTEGER PRIMARY KEY,
                patient_id  TEXT    UNIQUE NOT NULL
            );
        """)
    else:
        if destructive:
            # deleted all records
            cur.execute("DELETE FROM source_data WHERE patient_num IN (SELECT patient_num FROM lookup_patients);")
            cur.execute("DELETE FROM lookup_patients;")
        else:
            raise Exception("LOOKUP_PATIENTS table already exists")
    db_conn.commit()


# ----------------------------------------------------------------------------------------
def Create_OBSERVATIONS(db_conn, destructive:bool = False):
    """ create the observations lookup table in the database """
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    if "lookup_observations" not in tables:
        cur.execute("""
            CREATE TABLE lookup_observations (
                obs_code_id     INTEGER PRIMARY KEY,
                obs_code        TEXT    UNIQUE NOT NULL,
                obs_description TEXT
            );
        """)
    else:
        if destructive:
            # deleted all records
            cur.execute("DELETE FROM source_data WHERE obs_code IN (SELECT obs_code_id FROM lookup_observations);")
            cur.execute("DELETE FROM lookup_observations;")
        else:
            raise Exception("LOOKUP_OBSERVATIONS table already exists")
    db_conn.commit()


# ----------------------------------------------------------------------------------------
def Create_SOURCEDATA(db_conn, destructive:bool = False):
    """ create the observations lookup table in the database """
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    if "source_data" not in tables:
        cur.execute("""
            CREATE TABLE source_data (
                patient_num INTEGER NOT NULL,
                obs_code    INTEGER NOT NULL,
                obs_date    DATE    NOT NULL
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX idx_source_data ON source_data (
                patient_num ASC,
                obs_code ASC,
                obs_date ASC
            );
        """)
    else:
        if destructive:
            # deleted all records
            cur.execute("DELETE FROM source_data;")
        else:
            raise Exception("SOURCE_DATA table already exists")
    db_conn.commit()


# ----------------------------------------------------------------------------------------
def Create_SUBPOPULATIONS(db_conn, destructive:bool = False):
    """ create the subpopulation tables in the database """
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    if "subpopulations" not in tables:
        cur.execute("""
            CREATE TABLE subpopulations (
                subpop_num INTEGER PRIMARY KEY,
                subpop_id  TEXT UNIQUE NOT NULL,
                description TEXT
            );
        """)
        cur.execute("CREATE UNIQUE INDEX idx_subpopulations ON subpopulations (subpop_id)")
    else:
        if destructive:
            # deleted all records
            cur.execute("DELETE FROM subpopulations;")
        else:
            raise Exception("SUBPOPULATIONS table already exists")

    if "subpopulation_patients" not in tables:
        cur.execute("""
            CREATE TABLE subpopulation_patients (
                subpop_num INTEGER,
                patient_num INTEGER
            );
        """)
        cur.execute("CREATE INDEX idx_subpopulation_patients ON subpopulation_patients (subpop_num)")
    else:
        if destructive:
            # deleted all records
            cur.execute("DELETE FROM subpopulation_patients;")
        else:
            raise Exception("SUBPOPULATION_PATIENTS table already exists")
    db_conn.commit()


# ----------------------------------------------------------------------------------------
def Create_SEQUENCES(db_conn, destructive:bool = False):
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    tablename = 'sequences'

    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    # create table if it is missing
    if tablename in tables:
        if destructive is not True:
            raise NameError("sequence table already exists (and destructive option not selected)")
        else:
            cur.execute(f"DELETE FROM {tablename};")
            cur.execute(f"DROP INDEX IF EXISTS idx_{tablename};")
    else:
        cur.execute(f"""
            CREATE TABLE {tablename} (
                patient_num INTEGER     NOT NULL,
                obs_code_1  INTEGER     NOT NULL,
                obs_code_2  INTEGER     NOT NULL,
                temporal_distance   INTEGER NOT NULL
            );
        """)
    db_conn.commit()


# ----------------------------------------------------------------------------------------
def Index_SEQUENCES(db_conn, destructive:bool = False):
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    tablename = 'sequences'

    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    cur.execute(f"""
        CREATE INDEX idx_{tablename} ON {tablename} (
            obs_code_1 ASC,
            obs_code_2 ASC,
            temporal_distance ASC
        );
    """)
    db_conn.commit()



# ----------------------------------------------------------------------------------------
def Create_FREQUENCIES(db_conn, destructive:bool = False):
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")
    tablename = 'frequencies'

    cur = db_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [name[0] for name in cur.fetchall()]

    # create table if it is missing
    if tablename in tables:
        if destructive is not True:
            raise NameError("sequence table already exists (and destructive option not selected)")
        else:
            cur.execute(f"DELETE FROM {tablename};")
    else:
        cur.execute(f"""
            CREATE TABLE {tablename} (
                obs_code_1  INTEGER     NOT NULL,
                obs_code_2  INTEGER     NOT NULL,
                temporal_distance       INTEGER NOT NULL,
                observation_cnt         INTEGER NOT NULL DEFAULT 0,
                patient_cnt             INTEGER NOT NULL DEFAULT 0
            );
        """)
        cur.execute(f"""
            CREATE UNIQUE INDEX idx_{tablename} ON {tablename} (
                obs_code_1,
                obs_code_2,
                temporal_distance
            );
        """)
    db_conn.commit()



# ----------------------------------------------------------------------------------------
def Create_Base_DB(db_conn, destructive:bool = False):
    if not isinstance(db_conn, sqlite3.Connection):
        raise SyntaxError("database connection was not passed")

    Create_PATIENTS(db_conn, destructive)
    Create_OBSERVATIONS(db_conn, destructive)
    Create_SOURCEDATA(db_conn, destructive)
    Create_SUBPOPULATIONS(db_conn, destructive)
    Create_SEQUENCES(db_conn, destructive)
    Create_FREQUENCIES(db_conn, destructive)