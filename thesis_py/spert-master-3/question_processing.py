import subprocess
import json
from spacy.lang.en import English


nlp = English()
tokenizer = nlp.tokenizer

question_asked = "What is the ISIN of AXA AG?"

def questions(question: str):
    nlp = English()
    tokenizer = nlp.tokenizer
    with open('/Users/mlcb/PycharmProjects/Thesis/thesis_py/question.json', 'w') as fp:
        json.dump([{"tokens": ["{}".format(i) for i in tokenizer(question)]}], fp)

questions(question_asked)


subprocess.call(["python", "spert.py", "predict", "--config", "/Users/mlcb/PycharmProjects/Thesis/thesis_py/spert-master-3/configs/example_predict.conf"])

