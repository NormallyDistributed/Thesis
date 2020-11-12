import json

with open('/Users/mlcb/PycharmProjects/Thesis/thesis_py/data/prospectus__undefined__en__2018__godewind immobilien__180309 Godewind-Immobilien-AG_Approved-Prospectus.pdf.json') as f:
  data = json.load(f)

#for i in range(0,len(data["blobs"])):
#  print(data["blobs"][i]["text"])


extract = "Responsibility for liquidity risk management lies with the Management Board, which has established an appropriate concept for managing short-term, medium-term and long-term financing and liquidity requirements. The Company manages liquidity risks by keeping adequate reserves, via loans between related parties and by constantly monitoring the projected and actual cash flows and matching the maturity profiles of financial assets and liabilities."

import spacy

nlp = spacy.load("en_core_web_sm")

doc = nlp(extract)

for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)


for token in doc:
    print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
          token.shape_, token.is_alpha, token.is_stop)