from threading import Thread
import itertools
import json
import os
import random
from sklearn.utils import shuffle


class GenerateQuestions(object):

    def __init__(self, dir_relations, dir_entities, dir_commands, dir_operators):
        self.dir_relations = dir_relations
        self.dir_entities = dir_entities
        self.dir_commands = dir_commands
        self.dir_operators = dir_operators

        self.relations = {}
        self.entities = {}
        self.commands = {}
        self.operators = {}
        self.questions_list = []

    @staticmethod
    def q_parts(part: dict):
        return [[value, [key, len(value.split())]] for key, i in part.items() for value in i]

    def load_json_dicts(self):
        dir_list = [self.dir_relations,
                    self.dir_entities,
                    self.dir_commands,
                    self.dir_operators]

        value = [list()]*len(dir_list)

        for item in range(0, len(dir_list)):
            with open(dir_list[item]) as json_file:
                value[item] = json.load(json_file)

        self.relations = value[0]
        self.entities = value[1]
        self.commands = value[2]
        self.operators = value[3]

    def stack(self):
        questions_cache = []

        def combinations(commands: dict, relation: dict, ent_op: dict):
            for x in itertools.product(self.q_parts(commands), self.q_parts(relation), self.q_parts(ent_op)):
                questions_cache.append(x)

        for part in [self.entities, self.operators]:
            combinations(self.commands, self.relations, part)

        def randomize(q_list, iterations):
            cache_ = random.sample(population=q_list, k=iterations)
            return cache_

        questions = []
        for question in questions_cache:
            questions.append(question)
            # randomize order between command/operator and entity
            q = randomize([question[0], question[2]], 2)
            # r = randomize([question[1], question[1][0]+" of"], 1)
            # place relation between command/operator and entity
            questions.append([q[0], question[1], q[1]])

        for j in range(0, len(questions)):
            cache = [list(), list(), list()]
            counter = 0
            for i in questions[j]:
                cache[0].append(i[0].split())
                if i[1][0] in [key for key in self.relations.keys()]:
                    counter += i[1][1]
                    cache[2].append({"type": i[1][0], "head": 1, "tail": 0})
                else:
                    if (i[0] == "") and (len(cache[1]) > 0):
                        counter += i[1][1]
                        cache[1].append({"type": i[1][0], "start": counter - 2, "end": counter - 1})
                    elif (i[0] == "") and (len(cache[1]) == 0):
                        counter += i[1][1]
                        cache[1].append({"type": i[1][0], "start": counter, "end": counter + 1})
                    else:
                        counter += i[1][1]
                        cache[1].append({"type": i[1][0], "start": counter - i[1][1], "end": counter})
            self.questions_list.append(
                {"tokens": [item for sublist in cache[0] for item in sublist],
                 "entities": cache[1],
                 "relations": cache[2],
                 "orig_id": j})

    def train_dev_test_split(self):
        #  entity_types = {"company": {"short": "company", "verbose": "company"},
        #                 "interrogative_word": {"short": "interrogative word", "verbose": "interrogative word"}}
        #  types_dict = {"entities": entity_types, "relations": [key for key in self.relations.keys()]}
        self.questions_list = shuffle(self.questions_list, random_state=42)
        questions_train = self.questions_list[:int(len(self.questions_list) * 0.6)]
        questions_dev = self.questions_list[int(len(self.questions_list) * 0.6):int(len(self.questions_list) * 0.8)]
        questions_val = self.questions_list[int(len(self.questions_list) * 0.8):]

        output_dict: dict
        output_dict = {"questions_train": questions_train,
                       "questions_dev": questions_dev,
                       "questions_val": questions_val}
                       # "types_": types_dict}

        for key, value in output_dict.items():
            with open('train_dev/{}.json'.format(key), 'w') as fp:
                json.dump(value, fp)

    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.load_json_dicts()).start()
            Thread(target=self.stack()).start()
            Thread(target=self.train_dev_test_split()).start()


if __name__ == "__main__":
    relations_dict = os.path.realpath(os.path.join(os.getcwd(), "question_generation/input/relations_dict.json"))
    entities_dict = os.path.realpath(os.path.join(os.getcwd(), "question_generation/input/entities_dict.json"))
    commands_dict = os.path.realpath(os.path.join(os.getcwd(), "question_generation/input/commands_dict.json"))
    operators_dict = os.path.realpath(os.path.join(os.getcwd(), "question_generation/input/operators_dict.json"))
    GenerateQuestions(relations_dict, entities_dict, commands_dict, operators_dict).runall()
