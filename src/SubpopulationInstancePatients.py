


class SubpopulationInstancePatients:
    def __init__(self, subpop_instance):
        self.db = subpop_instance
        self.subpop_instance = subpop_instance

    def list(self, no_id_translation: bool = False, as_pandas: bool = True):
        """ gets a list of patients in the subpopulation """
        pass

    def add(self, patient_ids, no_id_translation: bool = False):
        """ adds the passed patients to the subpopulation"""
        pass

    def remove(self):
        """ removes the passed patients from the subpopulation"""
        pass

    def event_counts(self, no_id_translation: bool = False, as_pandas: bool = True):
        """ gets a list of patient_ids with the number of their events in the database """
        pass