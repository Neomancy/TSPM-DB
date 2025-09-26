import tspmdb
from SubpopulationInstance import SubpopulationInstance

class Population(SubpopulationInstance):
    def __init__(self, tspmdb_ref):
        super(Population, self).__init__(tspmdb_ref, "ALL", "All patients in the database")
        self._parent = tspmdb_ref
        pass

    def help(self):
        print("[HELP] tspmdb.Population Object")
        print("TspmDb().population")
        print("------------------------------------------------------------------------------")
        print(".help()         Displays this help message")
        print(".identifier     The identifier representing the entire population (\"ALL\")")
        print(".description    Read-only description")
        print(".patients()     List of all patient identifiers")
        print(".sequences()    List of calculated patient sequences")
        print(".frequencies()  List of calculated sequences with their frequencies")

    @property
    def identifier(self):
        return super(Population, self).identifier

    @property
    def description(self):
        return self._parent.description

    def patients(self, pandas : bool = False, with_ids : bool = False):
        """ return a list of all patient identifiers (or internal ids) as list (or dataframe) """
        pass


        # TsmpDb.population.sequences()
        # TsmpDb.population.frequencies()
        # TsmpDb.population.events()
