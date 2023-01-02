import pandas as pd
from Levenshtein import distance as levenshtein_distance
from polyfuzz import PolyFuzz
from utils.db_utils import norm_db_reader_conn
from polyfuzz.models import RapidFuzz, Embeddings
import re
pd.options.mode.chained_assignment = None  # default='warn'


def fuzz_sim(s1, s2):
    if s1 == s2:
        return 1.0
    rapidfuzz_matcher = RapidFuzz(n_jobs=1)
    pfmodel = PolyFuzz(rapidfuzz_matcher).match([s1.lower()], [s2.lower()])
    fuzz_sim = pfmodel.get_matches().iloc[0]['Similarity']
    return fuzz_sim

def find_word_matches(string, substring):
    matches = []
    words = string.split()
    for word in words:
        if substring in word:
            matches.append(word)
        else:
            suffix = word[-4:]
            if levenshtein_distance(suffix, substring) <= 2:
                matches.append([word, suffix])
    return matches

def multiple_word_edit_distance(string, department):
    matches = None
    dept_words = department.split()
    dept_word_len = len(dept_words)
    input_words = string.split()
    input_word_len = len(input_words)

    # create substring of N len for every possible N length word
    ngrams = filter(
        lambda x: len(x) == dept_word_len,
        [input_words[i:i + dept_word_len] for i in range(input_word_len)]
    )

    # iterate through each ngrams and check for word distaance
    for ngram in ngrams:
        match_lookup = (
            [single_word_edit_distance(ngram[i], dept_words[i]) for i in range(dept_word_len)]
        )
        if all(match_lookup):
            match_string = ' '.join(ngram)
            matches = (department, match_string)

    return matches if matches else []

def no_meaningful_diff_substr(s1, s2, record):
    cond_one = s1 in s2
    cond_two = s2 in s1
    entity_diffs = record['entity_differences']
    if any([cond_one, cond_two]) and (len(entity_diffs) == 0):
        return True
    else:
        return False

def no_meaningful_diff(s1, s2, record):
    s1_words, s2_words = set(s1.split()), set(s2.split())
    cond_one = all([word in s2_words for word in s1_words])
    cond_two = all([word in s1_words for word in s2_words])
    total_diffs = len(record['entity_differences'])
    if any([cond_one, cond_two]) and total_diffs == 0:
        return True
    else:
        return False

def single_word_edit_distance(string, department):
    edit_distance = levenshtein_distance(string, department)
    if edit_distance <= 1:
        return True
    else:
        return False

class EntityStringParsing():
    def __init__(self, medical_entities, departments, fields):
        self.medical_entities = medical_entities
        self.departments = departments
        self.fields = fields

    def find_high_level_entities(self, mapping):
        med_entities = list(self.medical_entities.keys()) + ['hospital', 'hospitalists', 'hospitals', 'hospital group', 'hospitalist']
        high_level = set(i[0] for i in mapping if i[0] in med_entities)
        return high_level

    def find_department_entities(self, mapping):
        departments = list(self.departments.keys())
        depts = set(i[0] for i in mapping if i[0] in departments)
        return depts

    def find_specialties(self, mapping):
        specialties = self.fields
        special = set(i[0] for i in mapping if i[0] in specialties)
        return special

    def diff_elements(self, x, y):
        return list(x.difference(y).union(y.difference(x)))

    def common_elements(self, x, y):
        return list(x.intersection(y))

    def replace_elements(self, string, elem):
        new_str = re.sub(r'|'.join(map(re.escape, elem)), '', string).strip()
        return new_str

    def parse_medical_entities(self, string, entity_type):
        assert entity_type in self.medical_entities.keys(), f'Entity Type must be in {self.medical_entities.keys()}'
        # set the inital match to false
        match = False
        first_word, second_word = entity_type.split()[0], entity_type.split()[1]
        # pull all the input matches from the data and check for exact match
        base_matches = self.medical_entities[entity_type]
        for bm in base_matches:
            if re.search(r'\b' + bm + r'\b', string):
                match = True
                word = bm

        # if none match, check to see if partial match
        if not match:
            words = string.split()
            for i in range(len(words) - 1):
                # calclate the distance for the first two words (medical and something else)
                first_word_dist = levenshtein_distance(first_word, words[i])
                second_word_dist = levenshtein_distance(second_word, words[i + 1])
                if first_word_dist <= 2 and second_word_dist <= 2:
                    match = True
                    word = words[i] + ' ' + words[i + 1]

        return (entity_type, word) if match else []

    def parse_hospitals(self, string):
        # look for the word hospital, or any common misspellings
        base_matches = ['hospital', 'hospt']
        match = False
        for bm in base_matches:
            if bm in string:
                match = True
                match_word = bm

        if not match:
            words = string.split()
            for word in words:
                dist = levenshtein_distance(word, 'hospital')
                if dist <= 2:
                    match = True
                    match_word = word

        return [('hospital', match_word)] if match else []

    def parse_specialty(self, string, cutoff=1):
        roots = [
            i.replace('logy', '').replace('logist', '') for i in self.fields
        ]
        # first, try to match the exact string
        vals = []
        specialties = []
        full_names = (
                [i + 'logy' for i in roots] +
                [i + 'logist' for i in roots] +
                [i + 'logists' for i in roots]
        )
        for full_name, root in zip(full_names, roots):
            if full_name in string.split():
                vals.append((root + 'logy', full_name))

        # then check if the root of the word resembles
        matches = find_word_matches(string=string, substring='logy')
        for match in matches:
            if isinstance(match, str):
                index_string = match.find('logy')
                # last four resemble common roots
            elif isinstance(match, list):
                # if theres a partial match of the string
                index_string = match[0].find(match[1])
                match = match[0]
            else:
                for field in roots:
                    # make sure you're comparing the different parts of words
                    string_root = match[:index_string].strip()
                    sim = levenshtein_distance(field, string_root)
                    if sim == cutoff:
                        vals.append((field + 'logy', match))

        # certain field matches - more can be added here as needed
        words = string.split()
        for word in words:
            if word.endswith('sia') and word.startswith('a'):
                vals.append(('anesthesiology', word))
            if 'obgyn' in word or 'ob-gyn' in word or word == 'ob' or word == 'gyn':
                vals.append(('obgyn', word))
            if word.startswith('ortho'):
                vals.append(('orthopedic', word))
            if word.startswith('pedi'):
                vals.append(('pedatric', word))
            if word.startswith('geri'):
                vals.append(('geriatric', word))
            if word.startswith('endos'):
                vals.append(('endoscopy', word))
            if word.startswith('cardio'):
                vals.append(('cardiology', word))
            if word.startswith('pulmon'):
                vals.append('pulmonology')
            if word == 'ent' or word.startswith('otorhino'):
                vals.append(('ent', word))

            for root in roots:
                root_word = f'{root}log'
                if root_word not in [v[0] for v in vals] and root_word not in ['urolog', 'neurolog']:
                    if root_word in word:
                        vals.append((root_word + 'y', word))
                    if levenshtein_distance(root + 'logy', word) <= 1 and root[0] == word[0]:
                        vals.append((root + 'logy', word))


        return vals if vals else []

    def parse_departments(self, string):
        departs = []
        words = string.split()
        departments = self.departments
        for department, matchers in departments.items():
            dept_words = len(department.split())

            # first check to see if there's an exact match
            if department in string:
                # make sure your not involutary matching substring
                match_string = re.search(fr'{department}', string).group(0)
                departs.append((department, match_string))

            # next, check to see if any of the potential looks are in the string
            for match in matchers:
                if re.search(fr'\b{match}\b', string):
                    match_string = re.search(fr'{match}', string).group(0)
                    departs.append((department, match_string))

            # now, check to see if there's an edit distance of one for the offical name
            # first, check for one word entities
            if dept_words == 1:
                for word in words:
                    if single_word_edit_distance(department, word):
                        match_string = word
                        departs.append((department, match_string))

            # finally, check to see if there's an edit distance of one for every word
            # in the multiple department words
            if dept_words > 1:
                if multiple_word_edit_distance(department, string):
                    results = multiple_word_edit_distance(department, string)
                    departs.append(results)

        return list(set(departs)) if departs else []

    def find_identifiers(self, string):
        ids = []
        med_entities = self.medical_entities.keys()
        for med_entity in med_entities:
            ent = self.parse_medical_entities(string, med_entity)
            ids.append(ent)

        hospitals = self.parse_hospitals(string)
        specialtites = self.parse_specialty(string)
        departments = self.parse_departments(string)
        vals = ids + hospitals + specialtites + departments
        # parse out the useful ids in the values
        vals = [v for v in vals if v]
        matches = [(v[0], v[1]) for v in vals] if vals else []
        return matches

    def create_parsed_record(self, string):
        ids = self.find_identifiers(string)
        record = {
            'string': string,
            'mapping': {i[0]: i[1] for i in ids},
            'total_entities': set(i[0] for i in ids),
            'medical_entities': self.find_high_level_entities(ids),
            'department_entities': self.find_department_entities(ids),
            'speciality_entities': self.find_specialties(ids)
        }

        return record


    def keyword_overlap(self, s1_record, s2_record, entity_mapping):
        s1_keyword = {
            s1_record['mapping'][n] for n in s1_record[entity_mapping]
            if n in s2_record[entity_mapping]
        }

        s2_keywords = {
            s2_record['mapping'][n] for n in s2_record[entity_mapping]
            if n in s1_record[entity_mapping]
        }

        lookup = {'s1_keywords': s1_keyword, 's2_keywords': s2_keywords}
        return lookup

    def create_comparison_records(self, string_one, string_two):
        s1_record = self.create_parsed_record(string_one.lower())
        s2_record = self.create_parsed_record(string_two.lower())
        entity_keyword_overlap = self.keyword_overlap(s1_record, s2_record, 'total_entities')
        medical_keyword_overlap = self.keyword_overlap(s1_record, s2_record, 'medical_entities')
        department_keyword_overlap = self.keyword_overlap(s1_record, s2_record, 'department_entities')
        specialty_keyword_overlap = self.keyword_overlap(s1_record, s2_record, 'speciality_entities')
        # business_logic_match = self.determine_buisness_match()
        record = {
            'string_one': s1_record['string'],
            'string_two': s2_record['string'],
            'fuzz_sim': fuzz_sim(s1_record['string'], s2_record['string']),
            # high level metrics
            's1_total_entities': s1_record['total_entities'],
            's2_total_entities': s2_record['total_entities'],
            'entity_overlap': self.common_elements(
                x=s1_record['total_entities'],
                y=s2_record['total_entities']
            ),
            'entity_differences': self.diff_elements(
                x=s1_record['total_entities'],
                y=s2_record['total_entities']
            ),

            's1_total_keyword_overlap': entity_keyword_overlap['s1_keywords'],
            's2_total_keyword_overlap': entity_keyword_overlap['s2_keywords'],
            's1_overlap_new_string': self.replace_elements(
                string=s1_record['string'],
                elem=entity_keyword_overlap['s1_keywords']
            ),
            's2_overlap_new_string': self.replace_elements(
                string=s2_record['string'],
                elem=entity_keyword_overlap['s2_keywords']
            ),

            # medical element metrics
            's1_medical_entities': s1_record['medical_entities'],
            's2_medical_entities': s2_record['medical_entities'],
            'common_medical_entities': self.common_elements(
                x=s1_record['medical_entities'],
                y=s2_record['medical_entities']
            ),

            'diff_medical_elements': self.diff_elements(
                x=s1_record['medical_entities'],
                y=s2_record['medical_entities']
            ),
            's1_med_keyword_overlap': medical_keyword_overlap['s1_keywords'],
            's2_med_keyword_overlap': medical_keyword_overlap['s2_keywords'],
            's1_med_overlap_new_string': self.replace_elements(
                string=s1_record['string'],
                elem=medical_keyword_overlap['s1_keywords']
            ),
            's2_med_overlap_new_string': self.replace_elements(
                string=s2_record['string'],
                elem=medical_keyword_overlap['s2_keywords']
            ),
            's1_no_medical_elements': self.replace_elements(
                string=s1_record['string'],
                elem=s1_record['medical_entities']
            ),
            's2_no_medical_elements': self.replace_elements(
                string=s2_record['string'],
                elem=s2_record['medical_entities']
            ),
            # department metrics
            's1_department_entities': s1_record['department_entities'],
            's2_department_entities': s2_record['department_entities'],
            'common_department_entities': self.common_elements(
                x=s1_record['department_entities'],
                y=s2_record['department_entities']
            ),

            'diff_department_elements': self.diff_elements(
                x=s1_record['department_entities'],
                y=s2_record['department_entities']
            ),

            's1_dept_keyword_overlap': department_keyword_overlap['s1_keywords'],
            's2_dept_keyword_overlap': department_keyword_overlap['s2_keywords'],
            's1_dept_overlap_new_string': self.replace_elements(
                string=s1_record['string'],
                elem=department_keyword_overlap['s1_keywords']
            ),
            's2_dept_overlap_new_string': self.replace_elements(
                string=s2_record['string'],
                elem=department_keyword_overlap['s2_keywords']
            ),

            # specialty metrics
            's1_speciality_entities': s1_record['speciality_entities'],
            's2_speciality_entities': s2_record['speciality_entities'],
            'common_speciality_entities': self.common_elements(
                x=s1_record['speciality_entities'],
                y=s2_record['speciality_entities']
            ),
            'diff_speciality_elements': self.diff_elements(
                x=s1_record['speciality_entities'],
                y=s2_record['speciality_entities']
            ),
            's1_specialty_keyword_overlap': specialty_keyword_overlap['s1_keywords'],
            's2_specialty_keyword_overlap': specialty_keyword_overlap['s2_keywords'],
            's1_specialty_overlap_new_string': self.replace_elements(
                string=s1_record['string'],
                elem=specialty_keyword_overlap['s1_keywords']
            ),
            's2_specialty_overlap_new_string': self.replace_elements(
                string=s2_record['string'],
                elem=specialty_keyword_overlap['s2_keywords']
            ),

        }

        # add the new fuzzy similarities in
        record['overlap_fuzz_sim'] = fuzz_sim(
            record['s1_overlap_new_string'], record['s2_overlap_new_string']
        )
        record['med_overlap_fuzz_sim'] = fuzz_sim(
            record['s1_med_overlap_new_string'], record['s2_med_overlap_new_string']
        )

        record['dept_overlap_fuzz_sim'] = fuzz_sim(
            record['s1_dept_overlap_new_string'], record['s2_dept_overlap_new_string']
        )

        record['specialty_overlap_fuzz_sim'] = fuzz_sim(
            record['s1_specialty_overlap_new_string'], record['s2_specialty_overlap_new_string']
        )

        record['no_med_elements_fuzz_sim'] = fuzz_sim(
            record['s1_no_medical_elements'], record['s2_no_medical_elements']
        )

        record['business_logic_match'] = {
            'entity_overlap_string_sim': True if (
                    (len(record['entity_differences']) == 0) &
                    (len(record['s1_total_entities']) + len(record['s2_total_entities']) > 0) &
                    (record['overlap_fuzz_sim'] > .90)
            ) else False
            ,
            'mismatched_specialty_flag': True if (
                (len(record['diff_speciality_elements']) > 0)
            ) else False
            ,
            'mismatched_department_flag': True if (
                (len(record['diff_department_elements']) > 0)
            ) else False
            ,
            'dept_vs_speciality_flag': True if (
                    ((
                             (len(record['s1_department_entities']) > 0) &
                             (len(record['s2_speciality_entities']) == 0)
                     ) |
                     (
                             (len(record['s2_department_entities']) > 0) &
                             (len(record['s1_speciality_entities']) == 0)
                     ))
                    &
                    ((
                             record['s1_department_entities'] != record['s2_department_entities']
                     )
                     |
                     (
                             record['s1_speciality_entities'] != record['s2_speciality_entities']
                     ))
            ) else False
            ,
            'dual_medical_entities_flag': True if (
                    (len(record['s1_medical_entities']) > 0) &
                    (len(record['s2_medical_entities']) > 0)
            ) else False,

            'medical_entities_sole_diff': True if record['no_med_elements_fuzz_sim'] > .98 else False
        }

        return record

