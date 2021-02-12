from threading import Thread
import json
from fuzzywuzzy import fuzz


class EntityLinking(object):

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

        start_end = None
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

        knowledge_graph = ["Volkswagen AG", "Gamestop", "BMW"]

        threshold = 0.85

        best_candidate = "No Match"

        for entity_candidate in knowledge_graph:
            if fuzz.token_set_ratio(self.entity, entity_candidate) > threshold:
                threshold = fuzz.token_set_ratio(self.entity, entity_candidate)
                best_candidate = entity_candidate

        return print(best_candidate)

    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.load_prediction()).start()
            Thread(target=self.extract_relation()).start()
            Thread(target=self.extract_entity()).start()
            Thread(target=self.entity_matching()).start()


if __name__ == "__main__":
    prediction_dir = "/Users/mlcb/Desktop/spert-master-3/data/predictions.json"
    EntityLinking(prediction_dir).runall()
