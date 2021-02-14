import json
from spacy.lang.en import English
nlp = English()
tokenizer = nlp.tokenizer

test = "What is the ISIN of AXA AG?"

def questions(question: str):
    nlp = English()
    tokenizer = nlp.tokenizer
    with open('questions_prediction_example.json', 'w') as fp:
        json.dump([{"tokens": ["{}".format(i) for i in tokenizer(question)]}], fp)

questions(test)
