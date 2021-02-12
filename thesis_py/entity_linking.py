from threading import Thread
import json
from fuzzywuzzy import fuzz
'''''''''INPUT: json_file -> dict'''''''''''''''

class entity_linking(object):

    def __init__(self, dir_prediction):
        self.dir_prediction = dir_prediction
        self.prediction = None
        self.relation = None
        self.entity = None

    def load_prediction(self):
        with open(self.dir_prediction) as json_file:
            self.prediction = json.load(json_file)

    def extract_relation(self):
        for i in self.prediction:
            for j in range(0, len(i["relations"])):
                self.relation = i["relations"][0]["type"]
                print(self.relation)

    def extract_entity(self):
        for i in self.prediction:
            for j in range(0, len(i["entities"])):
                if i["entities"][j]["type"] == "company":
                    start_end = (i["entities"][j]["start"], i["entities"][j]["end"])
        entity_cache = []

        for i in self.prediction:
            for j in range(start_end[0], start_end[1]):
                entity_cache.append(i["tokens"][j])
            self.entity = " ".join(entity_cache)
            return print(self.entity)

    def entity_matching(self):

        knowledge_graph = ["Volkswagen AG", "Gamestop AG", "BMW"]

        for entity in knowledge_graph:
            print(entity)

            fuzz.token_set_ratio()


    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.load_prediction()).start()
            Thread(target=self.extract_relation()).start()
            Thread(target=self.extract_entity()).start()


if __name__ == "__main__":
    prediction_dir = "/Users/mlcb/Desktop/spert-master-3/data/predictions.json"
    entity_linking(prediction_dir).runall()