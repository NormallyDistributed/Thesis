from threading import Thread
import itertools
import json
import os

class GenerateQuestions(object):

    def __init__(self, dir_relations, dir_entities, dir_commands):
        self.dir_relations = dir_relations
        self.dir_entities = dir_entities
        self.dir_commands = dir_commands

        self.relations = {}
        self.entities = {}
        self.commands = {}
        self.questions_list = []

    @staticmethod
    def q_parts(part: dict):
        return [[value, [key, len(value.split())]] for key, i in part.items() for value in i]

    def load_json_dicts(self):
        with open(self.dir_relations) as json_file:
            self.relations = json.load(json_file)
        with open(self.dir_entities) as json_file:
            self.entities = json.load(json_file)
        with open(self.dir_commands) as json_file:
            self.commands = json.load(json_file)

    def stack(self):
        questions = []
        for x in itertools.product(self.q_parts(self.commands), self.q_parts(self.relations),
                                   self.q_parts(self.entities)):
            questions.append(x)

        for j in range(0, len(questions)):
            cache = []
            cache2 = []
            cache3 = []
            counter = 0
            for i in questions[j]:
                cache.append(i[0].split())
                if i[1][0] in [key for key in self.relations.keys()]:
                    counter += i[1][1]
                    cache3.append({"type": i[1][0], "head": 1, "tail": 0})
                else:
                    counter += i[1][1]
                    cache2.append({"type": i[1][0], "start": counter - i[1][1], "end": counter})
            self.questions_list.append(
                {"tokens": [item for sublist in cache for item in sublist], "entities": cache2, "relations": cache3,
                 "orig_id": j})

    def train_dev_test_split(self):
        entity_types = {"company": {"short": "company", "verbose": "company"},
                        "interrogative_word": {"short": "interrogative word", "verbose": "interrogative word"}}
        types_dict = {"entities": entity_types, "relations": [key for key in self.relations.keys()]}

        questions_train = self.questions_list[:int(len(self.questions_list) * 0.6)]
        questions_dev = self.questions_list[int(len(self.questions_list) * 0.6):int(len(self.questions_list) * 0.8)]
        questions_val = self.questions_list[int(len(self.questions_list) * 0.8):]

        output_dict: dict
        output_dict = {"questions_train": questions_train,
                       "questions_dev": questions_dev,
                       "questions_val": questions_val,
                       "types": types_dict}

        for key, value in output_dict.items():
            with open('{}.json'.format(key), 'w') as fp:
                json.dump(value, fp)

    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.load_json_dicts()).start()
            Thread(target=self.stack()).start()
            Thread(target=self.train_dev_test_split()).start()


if __name__ == "__main__":
    relations_dict = os.path.realpath(os.path.join(os.getcwd(), "/input/relations_dict.json"))
    entities_dict = os.path.realpath(os.path.join(os.getcwd(), "/input/entities_dict.json"))
    commands_dict = os.path.realpath(os.path.join(os.getcwd(), "/input/commands_dict.json"))
    GenerateQuestions(relations_dict, entities_dict, commands_dict).runall()
