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