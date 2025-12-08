from SubpopulationInstancePatients import SubpopulationInstancePatients

class SubpopulationInstance:
    """ Main class for manipulation of a subpopulation """

    def __init__(self, tspmdb_ref, identifier:str, description:str =""):
        self._parent = tspmdb_ref
        self._identifier = identifier
        self._description = description
        self._ref_instance_patients = SubpopulationInstancePatients(self._parent)

    @property
    def identifier(self):
        return self._identifier

    @property
    def description(self):
        return self._description


    def help(self):
        print("[HELP] Subpopulation Instance Operations")
        print("TspmDb.subpopulation.get(identifier)")
        print("------------------------------------------------------------------------------")
        print(" .help()                                Displays this help message")
        print(" .patients.list()                       List of patients in the subpopulation")
        print(" .patients.add()                        Add a patient to the subpopulation")
        print(" .patients.remove()                     Remove a patient from the subpopulation")
        print(" .patients.get()                        Get a single patient from the subpopulation")
        print(" .patients.event_counts()               List all patients in the subpopulation with their number of events")
        print(" .sequences.list(sparcity_level)        List of unique sequences in the subpopulation along with the number of patients who have it")
        print(" .sequences.get_counts(sparcity_level)  Gets a list of sequence occurance frequencies ")
        # print(" .sequences.get_date_range()   ")
        #        print(" .sequences.get_dates(sparcity_level=0.05)   ")
        #        patient1, obs_1, obs_1_date, obs_2, obs_2_date

    def recalculate(self, sequences: bool = False, frequencies: bool = False):
        """ recalculates the sequences and/or sequence frequencies for the subpopulation """
        pass

    @property
    def patients(self):
        return self._ref_instance_patients

    def sequences(self):
        pass


    def get_sequences(self, no_id_translation: bool = False, as_pandas: bool = True, include_actual_duration=False):
        pass

    def get_frequencies(self, observation=False, no_id_translation: bool = False, as_pandas: bool = True):
        """ gets the frequencies of all event sequences for the subpopulation """
        pass
