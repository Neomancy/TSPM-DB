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

from SubpopulationInstancePatients import SubpopulationInstancePatients
from SubpopulationInstanceSequences import SubpopulationInstanceSequences

class SubpopulationInstance:
    """ Main class for manipulation of a subpopulation """

    def __init__(self, tspmdb_ref, identifier:str, description:str =""):
        self._parent = tspmdb_ref
        self._identifier = identifier
        self._description = description
        self._ref_instance_patients = SubpopulationInstancePatients(tspmdb_ref, self)
        self._ref_instance_sequences = SubpopulationInstanceSequences(tspmdb_ref, self)

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
        print(" .sequences.get_frequencies(sparcity_level)  Gets a list of sequence occurrance frequencies")
        # print(" .sequences.get_date_range()   ")
        #        print(" .sequences.get_dates(sparcity_level=0.05)   ")
        #        patient1, obs_1, obs_1_date, obs_2, obs_2_date

    def recalculate(self, sequences: bool = False, frequencies: bool = False):
        """ recalculates the sequences and/or sequence frequencies for the subpopulation """
        pass

    @property
    def patients(self):
        return self._ref_instance_patients

    @property
    def sequences(self):
        return self._ref_instance_sequences

    # def get_frequencies(self, observation=False, no_id_translation: bool = False, as_pandas: bool = True):
    #     """ gets the frequencies of all event sequences for the subpopulation """
    #     pass
