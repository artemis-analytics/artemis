#    AUSTRALIAN NATIONAL UNIVERSITY OPEN SOURCE LICENSE (ANUOS LICENSE)
#    VERSION 1.3

#    The contents of this file are subject to the ANUOS License Version 1.3
#    (the "License"); you may not use this file except in compliance with
#    the License. You may obtain a copy of the License at:

#      https://sourceforge.net/projects/febrl/

#    Software distributed under the License is distributed on an "AS IS"
#    basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
#    the License for the specific language governing rights and limitations
#    under the License.

"""
Modifier class to generate errors given a record.
This class is a wrapper of the FEBRL data generator modifier functions.

Febrl (Freely Extensible Biomedical Record Linkage) is a freely available tool
that enables record linkage through a GUI.
The tool written in python which offers both a
programming interface as well as GUI,
supporting several record linakge
algorithms. In addition, the tool includes a data generator
which generates two record data sets suitable for performing
record linkage. The data generator creates two datasets with
the following random variables:
field given a frequency table (histogram)
date
phone
identification

The following probabilities are defined to modify
The second (duplicate) dataset draws randomly from first (original).
Each field defines a dictionary of probabilities:
Modify a given field
Mispell
Insert a character
Delete character
Substiture character
Swap two characters
Swap two fields
Swap words in field
Split a word
Merge words
Null field
Insert new value

PDFs for number of duplicates for each record:
Uniform
Poisson
Zipf

Each duplicate apply the modifications up to:
(Fixed) Max N modifications for a given record
(Random) Max N modifications for a given field

Straightforward to implement.
Requires suitable dictionaries for generating proper Canadian addresses.
Requires dictionary of commonly mispelled words (with list of misspellings).
This will also serve well for the Postal data.

Require a way to select from a probability distribution:
https://eli.thegreenplace.net/2010/01/22/weighted-random-generation-python

FEBRLGEN Comments
All fields the following keys must be given:
# - select_prob    Probability of selecting a field for introducing one or
#                  more modifications (set this to 0.0 if no modifications
#                  should be introduced into this field ever). Note: The sum
#                  of these select probabilities over all defined fields must
#                  be 100.
# - misspell_prob  Probability to swap an original value with a randomly
#                  chosen misspelling from the corresponding misspelling
#                  dictionary (can only be set to larger than 0.0 if such a
#                  misspellings dictionary is defined for the given field).
# - ins_prob       Probability to insert a character into a field value.
# - del_prob       Probability to delete a character from a field value.
# - sub_prob       Probability to substitute a character in a field value with
#                  another character.
# - trans_prob     Probability to transpose two characters in a field value.
# - val_swap_prob  Probability to swap the value in a field with another
#                  (randomly selected) value for this field (taken from this
#                  field's look-up table).
# - wrd_swap_prob  Probability to swap two words in a field (given there are
#                  at least two words in a field).
# - spc_ins_prob   Probability to insert a space into a field value (thus
#                  splitting a word).
# - spc_del_prob   Probability to delete a space (if available) in a field (and
#                  thus merging two words).
# - miss_prob      Probability to set a field value to missing (empty).
# - new_val_prob   Probability to insert a new value given the original value
#                  was empty.
#

# Note: The sum over the probabilities ins_prob, del_prob, sub_prob,
#       trans_prob, val_swap_prob, wrd_swap_prob, spc_ins_prob, spc_del_prob,
#       and miss_prob for each defined field must be 1.0; or 0.0 if no
#       modification should be done at all on a given field.
#
# ============================================================================="""

from collections import OrderedDict
import string
import logging
from pprint import pformat

from artemis.logger import Logger


@Logger.logged
class Modifier(object):
    """
    Base modification class for row of data

    """

    def __init__(self, fake, generators, schema, modifiers):
        """
        Requires frequencies for field modification
        Requires modification probabilities
        Pass tuple of lists or list
        """
        # Fixed probabilities
        print("Modifier init")
        self.__logger = logging.getLogger(__name__)
        self.__logger.info("Modifier init")
        self.__logger.debug("DEBUG Message")
        self.single_typo_prob = {"same_row": 0.40, "same_col": 0.30}

        # Additional dictionaries required
        # field swap probabilities, e.g.
        # field_swap_prob = {('address_1', 'address_2'):0.02,
        #                    ('given_name', 'surname'):0.05,
        #                    ('postcode', 'suburb'):0.01}

        self.max_modifications_in_record = modifiers.max_modifications_in_record
        self.max_field_modifiers = modifiers.max_field_modifiers
        self.max_record_modifiers = modifiers.max_record_modifiers

        self.modification_fcns = {
            "insert": self.insert,
            "delete": self.delete,
            "substitute": self.substitute,
            "misspell": self.misspell,
            "transpose": self.transpose,
            "replace": self.replace,
            "swap": self.swap,
            "split": self.split,
            "merge": self.merge,
            "nullify": self.nullify,
            "fill": self.fill,
        }
        self.counters = {
            "insert": 0,
            "delete": 0,
            "substitute": 0,
            "misspell": 0,
            "transpose": 0,
            "replace": 0,
            "swap": 0,
            "split": 0,
            "merge": 0,
            "nullify": 0,
            "fill": 0,
        }

        self.generator_fcns = generators
        self.num_mods_in_record = 0
        self.field_mod_count = {}

        self.fake = fake
        self.randcntr = 0
        self.schema = schema
        self.modifiers = OrderedDict()

        for field in modifiers.fields:
            probabilities = {}
            for x, y in field.probabilities.ListFields():
                if x.name == "misspell":
                    continue
                probabilities[x.name] = round(y, 4)
            self.modifiers[field.name] = {
                "select": field.selection,
                "probabilities": probabilities,
            }
        self.prob_fields = []
        self.prob_modifiers = OrderedDict({})
        self.pos_fields = {}
        prob_sum = 0.0
        for key in self.modifiers:
            self.prob_fields.append((key, prob_sum))
            prob_sum += self.modifiers[key]["select"]

        prob_sum = 0.0
        self.__logger.debug("Creating CDF for modifiers")
        for key in self.modifiers:
            self.prob_modifiers[key] = []
            prob_sum = 0.0
            for name in self.modifiers[key]["probabilities"]:
                self.prob_modifiers[key].append((name, prob_sum))
                prob_sum += self.modifiers[key]["probabilities"][name]

        for pos, key in enumerate(self.schema):
            for field in self.modifiers:
                if key == field:
                    self.pos_fields[key] = int(pos)
                    self.field_mod_count[key] = 0
        self.__logger.info("Modifier configured")

    def get_stats(self):
        self.__logger.info("Modifier Statistics")
        self.__logger.info(pformat(self.counters))

    def reset_fake(self, fake):
        self.fake = None
        self.fake = fake

    def validate(self):
        """
        Ensure defined metafields probabilitites sum to 1
        """
        pass

    def _reset(self):
        """
        Reset per record counters
        """
        self.num_mods_in_record = 0
        for key in self.field_mod_count:
            self.field_mod_count[key] = 0

    def field_pdf(self):
        """
        Create a map of duplicates and probabilities
        according to a pdf, i.e. uniform
        and store for re-use on each original event
        current version taken directly from FEBRL
        needs review b/c number of duplicates stored starts at 2?
        """
        num_dup = 1
        prob_sum = 0.0
        prob_list = [(num_dup, prob_sum)]
        max_dups = self.duplicate_cfg["Max_duplicate"]
        uniform_val = 1.0 / float(max_dups)
        self.__logger.debug("Maximum number of duplicatesi %d", max_dups)
        for i in range(max_dups - 1):
            num_dup += 1
            prob_list.append((num_dup, uniform_val + prob_list[-1][1]))
        return prob_list

    def random_select(self, prob):
        ind = -1
        while prob[ind][1] > self.fake.random.random():
            ind -= 1
            self.randcntr += 1
        return prob[ind][0]

    def modify(self, row):
        """
        modify given a row (or tuple of rows)
        loop over number of fields to modify
        random select field to modify
        random number of modifications in field
        random select field to modify

        e.g. mod = random_select(field_dict['prob_list'])

        selects modification according to pdf
        apply modifications in field
        """
        while self.num_mods_in_record < self.max_record_modifiers:
            field = self.random_select(self.prob_fields)

            # continue selecting new field if max modifications reached
            while self.field_mod_count[field] == self.max_field_modifiers:
                field = self.random_select(self.prob_fields)

            pos = self.pos_fields[field]
            if self.max_field_modifiers == 1:
                num_field_mods = 1
            else:
                num_field_mods = self.fake.randint(1, self.max_field_modifiers)

            expected_rec_mods = self.max_record_modifiers - self.num_mods_in_record
            if num_field_mods > expected_rec_mods:
                num_field_mods = expected_rec_mods
            # print('Modify field with n mods:', field, num_field_mods)
            for _ in range(num_field_mods):
                row[pos] = self._modify(field, row[pos])
                self.field_mod_count[field]
                self.num_mods_in_record += 1
        self._reset()

    def _modify(self, field, value):
        """
        determine whether to modify a field in a row
        select modification
        apply modification
        """
        self.__logger.debug("_modify")
        self.__logger.debug("%s" % self.prob_modifiers[field])
        modifier = self.random_select(self.prob_modifiers[field])
        self.__logger.info("Modifier: %s" % modifier)
        return self.modification_fcns[modifier](field, value)

    def character_range(self, data):
        """
        FEBRL defines the character type in the original configuration
        this should be implemented from the data model ourselves.
        For now, we brute force look up of data type each time.
        Also assumes everything is a string :(
        in the future we likely want proper data types
        """
        if data.isdigit():
            return string.digits
        elif data.isalpha():
            return string.digits + string.ascii_lowercase
        else:
            return string.ascii_lowercase

    def insert(self, field, data):
        """
        insert single character according to type
        """
        self.__logger.debug("insert")
        self.counters["insert"] += 1
        pos = self.select_position(data, +1)
        char_range = self.character_range(data)
        char = ""
        if data.isdigit():
            char = self.fake.random.choice(char_range)
        elif data.isalpha():
            char = self.fake.random.choice(char_range)
        else:
            char = self.fake.random.choice(char_range)

        value = data[:pos] + char + data[pos:]
        return value

    def delete(self, field, data):
        self.__logger.debug("delete")
        self.counters["delete"] += 1
        pos = self.select_position(data, 0)
        value = data[:pos] + data[pos + 1 :]
        return value

    def substitute(self, field, data):
        """
        substitute random character
        """
        self.__logger.debug("substitute")
        self.counters["substitute"] += 1
        pos = self.select_position(data, 0)
        char_range = self.character_range(data)
        value = ""
        if pos is None:
            return data
        else:
            char = self.error_character(data[pos], char_range)
            value = data[:pos] + char + data[pos:]
        return value

    def misspell(self, field, data):
        """
        Dictionary of commonly misspelled words
        """
        self.__logger.debug("misspell")
        self.counters["misspell"] += 1
        return "Blah"

    def transpose(self, field, data):
        """
        transpose two characters
        """
        self.__logger.debug("transpose")
        self.counters["transpose"] += 1
        if len(data) == 1:
            return data
        else:
            pos = self.select_position(data, -1)
            chars1 = data[pos : pos + 2]
            chars2 = chars1[1] + chars1[0]  # transpose characters
            value = data[:pos] + chars2 + data[pos + 2 :]
            return value

    def replace(self, field, data):
        """
        replace
        """
        self.__logger.debug("replace")
        self.counters["replace"] += 1
        # TODO
        # Implement random generation of dependent values?
        if self.generator_fcns[field][1] is None:
            value = self.generator_fcns[field][0]()
            return value
        else:
            return self.generator_fcns[field][0](self.generator_fcns[field][1])

    def swap(self, field, data):
        """
        swap -- randomly swap two words if field has at least two words
        """
        self.__logger.debug("swap")
        self.counters["swap"] += 1
        words = data.split(" ")
        nwords = len(words)
        if nwords > 2:
            idx = 0
        else:
            idx = self.fake.random.randint(0, nwords - 2)
        tmp = words[idx]
        words[idx] = words[idx + 1]
        words[idx + 1] = tmp
        value = " ".join(words)
        return value

    def split(self, field, data):
        """
        split word
        """
        self.__logger.debug("split")
        self.counters["split"] += 1
        if len(data) > 1:
            pos = self.select_position(data, 0)
            while (data[pos - 1] == " ") or (data[pos] == " "):
                pos = self.select_position(data, 0)
        value = data[:pos] + " " + data[pos:]
        return value

    def merge(self, field, data):
        """
        merge one or more words
        """
        self.__logger.debug("merge")
        self.counters["merge"] += 1

        nspaces = field.count(" ")
        if nspaces == 0:
            value = data
            return value

        if nspaces == 1:
            ind = field.index(" ")
        else:
            rspace = self.fake.random.randint(1, nspaces - 1)
            ind = field.index(" ", 0)  # Index of first space
            for _ in range(rspace):
                ind = field.index(" ", ind)
        value = field[:ind] + field[ind + 1 :]
        return value

    def nullify(self, field, data):
        """
        random null value
        """
        self.__logger.debug("nullify")
        self.counters["nullify"] += 1
        return None

    def fill(self, field, data):
        """
        fill
        """
        self.__logger.debug("fill")
        self.counters["fill"] += 1
        if self.generator_fcns[field][1] is None:
            value = self.generator_fcns[field][0]()
            return value
        else:
            return self.generator_fcns[field][0](self.generator_fcns[field][1])

    def select_position(self, input_string, len_offset):
        """
        dsgen::error_position
        randomly select position of character for a string
        to introduce an error
        FEBRL description:
        function that randomly calculates an error position within the given
        input string and returns the position as integer number 0 or larger.
        The argument 'len_offset' can be set to an integer (e.g. -1, 0, or 1).
        Provides an offset relative to the string length of the maximal error
        position that can be returned.
        Errors do not likely appear at the beginning of a word.
        Gaussian distribution is used with the mean being one position
        behind half the string length (simga = 1.0) to simulate errors.
        """

        str_len = len(input_string)
        # Maximal position to be returned
        max_return_pos = str_len - 1 + len_offset
        if str_len == 0:
            return None  # Empty input string

        mid_pos = (str_len + len_offset) / 2 + 1
        random_pos = self.fake.random.gauss(float(mid_pos), 1.0)
        # Make it integer and 0 or larger
        random_pos = max(0, int(round(random_pos)))
        return min(random_pos, max_return_pos)

    def error_character(self, input_char, char_range):

        """
        A function which returns a character created randomly. It uses row and
        column keyboard dictionaires.
        Directly taken from FEBRL dsgen
        """
        # Keyboard substitutions gives two dictionaries
        # with the neigbouring keys for
        # all letters both for rows and columns (based on ideas implemented by
        # Mauricio A. Hernandez in his dbgen).

        rows = {
            "a": "s",
            "b": "vn",
            "c": "xv",
            "d": "sf",
            "e": "wr",
            "f": "dg",
            "g": "fh",
            "h": "gj",
            "i": "uo",
            "j": "hk",
            "k": "jl",
            "l": "k",
            "m": "n",
            "n": "bm",
            "o": "ip",
            "p": "o",
            "q": "w",
            "r": "et",
            "s": "ad",
            "t": "ry",
            "u": "yi",
            "v": "cb",
            "w": "qe",
            "x": "zc",
            "y": "tu",
            "z": "x",
            "1": "2",
            "2": "13",
            "3": "24",
            "4": "35",
            "5": "46",
            "6": "57",
            "7": "68",
            "8": "79",
            "9": "80",
            "0": "9",
        }

        cols = {
            "a": "qzw",
            "b": "gh",
            "c": "df",
            "d": "erc",
            "e": "d",
            "f": "rvc",
            "g": "tbv",
            "h": "ybn",
            "i": "k",
            "j": "umn",
            "k": "im",
            "l": "o",
            "m": "jk",
            "n": "hj",
            "o": "l",
            "p": "p",
            "q": "a",
            "r": "f",
            "s": "wxz",
            "t": "gf",
            "u": "j",
            "v": "fg",
            "w": "s",
            "x": "sd",
            "y": "h",
            "z": "as",
        }
        # Create a random number between 0 and 1
        rand_num = self.fake.random.random()

        if char_range == "digit":
            # Randomly chosen neigbouring key in the same keyboard row
            if (input_char.isdigit()) and (
                rand_num <= self.single_typo_prob["same_row"]
            ):
                output_char = self.fake.random.choice(rows[input_char])
            else:
                # TODO following line not understood in original implementation
                # choice_str =  string.replace(string.digits, input_char, '')
                # A randomly choosen digit
                output_char = self.fake.random.choice(string.digit)
        elif char_range == "alpha":
            # A randomly chosen neigbouring key in the same keyboard row
            if (input_char.isalpha()) and (
                rand_num <= self.single_typo_prob["same_row"]
            ):
                output_char = self.fake.random.choice(rows[input_char])

            # A randomly chosen neigbouring key in the same keyboard column
            elif (input_char.isalpha()) and (
                rand_num
                <= (
                    self.single_typo_prob["same_row"]
                    + self.single_typo_prob["same_col"]
                )
            ):
                output_char = self.fake.random.choice(cols[input_char])

            else:
                # TODO Following line not understood in original implementation
                # Just do choice of character set
                # choice_str = string.replace(string.lowercase, input_char, '')
                # output_char = self.fake.random.choice(choice_str)
                # A randomly choosen letter
                output_char = self.fake.random.choice(string.ascii_lowercase)

        else:  # Both letters and digits possible
            # A randomly chosen neigbouring key in the same keyboard row
            #
            if rand_num <= self.single_typo_prob["same_row"]:
                if input_char in rows:
                    output_char = self.fake.random.choice(rows[input_char])
                else:
                    # TODO
                    # Following line not understood in original implementation
                    # choice_str = \
                    #   string.replace(string.lowercase+string.digits, \
                    #                     input_char, '')
                    # output_char = self.fake.random.choice(choice_str)
                    # A randomly choosen character
                    output_char = self.fake.random.choice(
                        string.ascii_lowercase + string.digits
                    )
            # A randomly chosen neigbouring key in the same keyboard column
            elif rand_num <= (
                self.single_typo_prob["same_row"] + self.single_typo_prob["same_col"]
            ):
                if input_char in cols:
                    output_char = self.fake.random.choice(cols[input_char])
                else:
                    # TODO
                    # Following line not understood in original implementation
                    # choice_str = \
                    # string.replace(string.lowercase+string.digits, \
                    #                     input_char, '')
                    # output_char = self.fake.random.choice(choice_str)
                    # A randomly choosen character
                    output_char = self.fake.random.choice(
                        string.ascii_lowercase + string.digits
                    )

            else:
                # TODO
                # Following line not understood in original implementation
                # choice_str =
                # string.replace(string.lowercase+string.digits, \
                #                       input_char, '')
                # output_char = self.fake.random.choice(choice_str)
                # A randomly choosen character
                output_char = self.fake.random.choice(
                    string.ascii_lowercase + string.digits
                )

        return output_char
