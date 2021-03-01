import argparse
from threading import Thread
from fuzzywuzzy import fuzz
import json
import os
import csv
import rdflib.graph as g
from spacy.lang.en import English
import subprocess
import time
import rdflib
import isodate
from SPARQLWrapper import SPARQLWrapper, JSON


class EntityLinking(object):

    def __init__(self, dir_prediction: str, initial_threshold: int, question: str):
        self.dir_prediction = dir_prediction
        self.threshold = initial_threshold
        self.question = question
        self.prediction = None
        self.relation = None
        self.relation_uri = None
        self.entity_uri = None
        self.entity = None
        self.operator = None
        self.query_command = None
        self.label = None
        self.value = None
        self.best_candidate = "No Match"
        self.graph = None
        self.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    def questions(self):
        nlp = English()
        tokenizer = nlp.tokenizer
        with open('/Users/mlcb/PycharmProjects/Thesis/thesis_py/question.json', 'w') as fp:
            json.dump([{"tokens": ["{}".format(i) for i in tokenizer(self.question)]}], fp)
        subprocess.call(["python",
                         "/Users/mlcb/PycharmProjects/Thesis/thesis_py/spert-master-3/spert.py",
                         "predict",
                         "--config",
                         "/Users/mlcb/PycharmProjects/Thesis/thesis_py/spert-master-3/configs/example_predict.conf"])

    def load_prediction(self):
        with open(self.dir_prediction) as json_file:
            self.prediction = json.load(json_file)
        print(self.prediction)

    def extract_relation(self):
        # import relations
        fibo_prefixes = {}
        prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "wd": "https://wikidata.org/entity/",
            "omg-spec": "http://www.omg.org/techprocess/ab/SpecificationMetadata/",
            "omg-lr": "https://www.omg.org/spec/LCC/Languages/LanguageRepresentation/",
            "omg-cc": "https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
            "fibo-ph": "https://spec.edmcouncil.org/fibo/ontology/placeholder/"
        }
        with open(os.path.join(self.location, "prefixes_fibo.csv"), newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in reader:
                fibo_prefixes.update({row[1].replace(":", ""): row[2]})
        prefix_dict = {**fibo_prefixes, **prefixes}
        mapping_dict = {
            "accounting_standards": [{"fibo-be-le-lei": "hasAccountingStandard"},
                                     {"datatype": "string"}],
            "audit_report": [{"fibo-sec-sec-lst": "hasAuditReport"},
                             {"datatype": "string"}],
            "audited_financial_statements": [{"fibo-sec-sec-lst": "hasAuditedFinancialStatement"},
                                             {"datatype": "string"}],
            "competitive_strength": [{"fibo-fnd-arr-rt": "hasCompetitiveStrength"},
                                     {"datatype": "string"}],
            "complex_financials": [{"wd": "Q192907"},
                                   {"datatype": "string"}],  # uri+string
            "country_of_origin/headquarters": [{"fibo-fnd-org-fm": "isDomiciledIn"},
                                               {"datatype": "string"}],
            "country_of_registration/incorporation": [{"fibo-fbc-fct-mkt": "operatesInCountry"},
                                                      {"datatype": "string"}],
            "description_of_the_business": [{"omg-spec": "hasDescription"},
                                            {"datatype": "string"}],
            "detailed_transaction": [{"fibo-bp-iss-ipo": "FilingDetails"},
                                     {"datatype": "string"}],
            "dividend_policy": [{"fibo-sec-eq-eq": "hasDividendPolicy"},
                                {"datatype": "string"}],
            "emphasis_of_matter": [{"fibo-ph": "emphasisOfMatter"},
                                   {"datatype": "string"}],
            "expected_gross_proceeds": [{"fibo-ph": "hasExpectedGrossProceeds"},
                                        {"datatype": "decimal"}],
            "expected_net_proceeds": [{"fibo-ph": "hasExpectedNetProceeds"},
                                      {"datatype": "decimal"}],
            "external_auditor": [{"wd": "P8571"},
                                 {"datatype": "string"}],
            "filing_date": [{"fibo-fnd-agr-ctr": "hasExecutionDate"},
                            {"datatype": "date"}],
            "financial_advisor": [{"fibo-bp-iss-muni": "hasFinancialAdvisor"},
                                  {"datatype": "string"}],
            "gaas": [{"fibo-be-le-lei": "AccountingFramework"},
                     {"datatype": "string"}],
            "industry": [{"wd": "P452"},
                         {"datatype": "string"}],
            "initial_price_range": [{"fibo-fbc-pas-fpas": "hasOfferingPrice"},
                                    {"datatype": "decimal"}],
            "investment_bank": [{"fibo-fbc-fct-fse": "InvestmentBank"},
                                {"datatype": "string"}],
            "isin": [{"wd": "P946"},
                     {"datatype": "string"}],
            "issuer_name": [{"rdfs": "label"}, {"datatype": "string"}],
            "key_element_strategy": [{"fibo-fnd-gao-obj": "BusinessStrategy"},
                                     {"datatype": "string"}],  # placeholder
            "key_factor_affecting_results": [{"fibo-fnd-pty-pty": "isAffectedBy"},
                                             {"datatype": "string"}],  # placeholder
            "key_line_item_income_statement": [{"wd": "Q243460"},
                                               {"datatype": "string"}],  # placeholder
            "listing_venue": [{"fibo-sec-sec-lst": "isTradedOn"},
                              {"datatype": "string"}],
            "lock-up_period": [{"fibo-sec-dbt-bnd": "hasLockoutPeriod"},
                               {"datatype": "duration"}],
            "non-gaap_measure": [{"wd": "Q330153"},
                                 {"datatype": "string"}],  # placeholder
            "non-gaap_measure;_definition": [{"wd": "Q330153"},
                                             {"datatype": "string"}],  # placeholder
            "non-recurring_item": [{"wd": "Q192907"},
                                   {"datatype": "string"}],  # string  # placeholder
            "number_of_reportable_segments": [{"wd": "Q192907"},
                                              {"datatype": "string"}],  # placeholder
            "offering_costs": [{"wd": "Q185142"},
                               {"datatype": "decimal"}],  # placeholder
            "periods_of_audited_financial_statements": [{"wd": "Q740419"},
                                                        {"datatype": "string"}],  # placeholder
            "periods_of_pffi": [{"wd": "Q1166072"},
                                {"datatype": "date"}],  # placeholder
            "periods_of_unaudited_financial_statements": [{"wd": "Q192907"},
                                                          {"datatype": "string"}],  # placeholder
            "periods_of_unaudited_interim_fs": [{"wd": "Q192907"},
                                                {"datatype": "date"}],  # placeholder
            "pro_forma_financial_information": [{"wd": "Q2481549"},
                                                {"datatype": "string"}],  # placeholder
            "profit_forecast": [{"wd": "Q748250"},
                                {"datatype": "string"}],  # placeholder
            "reasons_for_the_offering": [{"fibo-be-le-lp": "ProfitObjective"},
                                         {"datatype": "string"}],  # placeholder
            "regulation_s_applies": [{"fibo-sec-sec-rst": "RegulationS"}, {"datatype": "string"}],
            "risk_factor": [{"wd": "Q1337875"},
                            {"datatype": "string"}],  # placeholder
            "rule_144a_applies": [{"wd": "Q7378915"},
                                  {"datatype": "boolean"}],
            "shareholder_transaction": [{"wd": "Q1166072"},
                                        {"datatype": "string"}],  # placeholder
            "shareholders_transaction": [{"wd": "Q1166072"},
                                         {"datatype": "string"}],  # placeholder
            "ticker": [{"wd": "P249"},
                       {"datatype": "string"}],
            "transaction": [{"wd": "Q1166072"},
                            {"datatype": "string"}],
            "unaudited_financial_statements": [{"wd": "Q192907"},
                                               {"datatype": "string"}],  # placeholder
            "unaudited_interim_financial_statements": [{"wd": "Q192907"},
                                                       {"datatype": "string"}],  # placeholder
            "underwriters_incentive_fee": [{"fibo-fbc-fct-fse": "UnderwritingArrangement"},
                                           {"datatype": "decimal"}],  # placeholder
            "underwriting_fees": [{"fibo-fbc-fct-fse": "UnderwritingArrangement"},
                                  {"datatype": "decimal"}],  # placeholder
            "use_of_proceeds": [{"fibo-civ-fnd-civ": "InvestmentStrategy"},
                                {"datatype": "string"}],
            "working_capital_statement": [{"fibo-loan-typ-prod": "WorkingCapitalPurpose"},
                                          {"datatype": "string"}]
        }

        def triple_predicate(item):
            predicate = None
            if item in mapping_dict:
                for key in mapping_dict[item][0]:
                    predicate = "<{}{}>".format(prefix_dict[key].replace(">", "").replace("<", ""),
                                                *mapping_dict[item][0].values())
            else:
                pass
            return predicate

        for i in self.prediction:
            for j in range(0, len(i["relations"])):
                self.relation = i["relations"][0]["type"]
                self.relation_uri = triple_predicate(self.relation.replace(" ", "_"))

        if self.relation_uri is None:
            print("Answer unknown. Reason: The mentioned relation is not present in the database.")
            exit()

    def extract_entity(self):
        start_end = None
        for i in self.prediction:
            for j in range(0, len(i["entities"])):
                if i["entities"][j]["type"] == "company":
                    start_end = (i["entities"][j]["start"], i["entities"][j]["end"])
                if i["entities"][j]["type"] in ["argmin_max", "count", "aggregate"]:
                    self.operator = i["entities"][j]["type"]

        entity_cache = []
        try:
            for i in self.prediction:
                for j in range(start_end[0], start_end[1]):
                    entity_cache.append(i["tokens"][j])
                self.entity = " ".join(entity_cache)
        except TypeError:
            pass

    def graph_init(self):
        self.graph = g.Graph()
        self.graph.parse('KnowledgeBaseUpdate.nt', format='ttl')

    def entity_matching(self):

        self.label = rdflib.URIRef("http://www.w3.org/2000/01/rdf-schema#label")
        self.value = rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#value")
        alt_label = rdflib.URIRef("http://www.w3.org/2004/02/skos/core#altLabel")

        def get_entity_irl(graphs, label_uri, altlabel_uri):
            sparql_query_entity: str = """
            SELECT *
            WHERE {
            {?s ?label ?o .} UNION {?s ?altlabel ?o.}
            }
            """
            return graphs.query(sparql_query_entity, initBindings={'label': label_uri, "altlabel": altlabel_uri})

        output_entity = get_entity_irl(self.graph, label_uri=self.label, altlabel_uri=alt_label)

        candidate_list = list()
        for row in output_entity:
            candidate_list.append([str(row.asdict()["o"].toPython()), str(row.asdict()["s"].toPython())])

        threshold: int = self.threshold

        for entity_candidate in candidate_list:
            if fuzz.token_set_ratio(self.entity, entity_candidate[0]) > threshold:
                threshold = fuzz.token_set_ratio(self.entity, entity_candidate)
                self.best_candidate = entity_candidate

        if self.best_candidate != "No Match":
            self.entity_uri = self.best_candidate[1]
        if self.operator is None and self.best_candidate == "No Match":
            print("Answer unknown. Reason: The mentioned company is not present in the database.")
            exit()

    def get_missing_object(self, graphs, predicate_uri, subject_uri, operator):

        answer_list = list()
        arg = None
        query_command = None

        if operator is None:
            arg = "c"
            query_command = """
            PREFIX xsd: <https://www.w3.org/2001/XMLSchema#>
            PREFIX fibo: <https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/>
            SELECT ?c ?e
            WHERE {
            {?s ?p ?o. FILTER (datatype(?o) = xsd:string ||
            datatype(?o) = xsd:decimal ||
            datatype(?o) = xsd:date ||
            datatype(?o) = xsd:duration) BIND(?o as ?c)}
            UNION {?s ?p ?o. ?o ?label ?d. BIND(?d as ?c)}
            UNION{?s ?p ?o. ?o ?value ?d. BIND(?d as ?c) OPTIONAL{?o fibo:isDenominatedIn ?e.}}
            } ORDER BY ASC(?c)
            """

        elif operator == "count":
            arg = "count"
            query_command = """
            SELECT ?p (COUNT(?o) as ?count)
            WHERE
            {
              ?s ?p ?o .
            }
            """
        output = graphs.query(query_command, initBindings={'p': predicate_uri,
                                                           's': subject_uri,
                                                           'label': self.label,
                                                           'value': self.value,
                                                           })

        if operator is None:
            for row in output:
                if "https://www.w3.org/2001/XMLSchema#duration" in str(row):
                    answer_list.append(str(isodate.parse_duration(row[arg])).split(",")[0])
                elif "wikidata" in str(row):
                    sparql_cache = row["e"].split("/entity/")[1].split("/")[0]
                    answer_list.append(row["c"]+" "+self.wiki_query(sparql_cache))
                    continue
                else:
                    answer_list.append(row.asdict()[arg].toPython())
            answer_list = ", ".join(answer_list)

        if operator == "count":
            for row in output:
                answer_list = row.asdict()[arg].toPython()

        if not answer_list:
            return print("This information is not in the database.")
        else:
            return print("The {} of {} is: {}.".format(self.relation.replace("_", " "),
                                                       self.best_candidate[0],
                                                       answer_list))

    def query(self):

        def strip(irl):
            return irl.replace("<", "").replace(">", "")

        if self.operator is None:
            self.get_missing_object(self.graph, subject_uri=rdflib.URIRef(self.entity_uri),
                                    predicate_uri=rdflib.URIRef(strip(self.relation_uri)), operator=None)

        elif self.operator == "count":
            self.get_missing_object(self.graph, subject_uri=rdflib.URIRef(self.entity_uri),
                                    predicate_uri=rdflib.URIRef(strip(self.relation_uri)), operator="count")

    def wiki_query(self, sparql_output):
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        sparql.setQuery("""
        SELECT ?item ?itemLabel
        WHERE {
          wd:""" + sparql_output + """ rdfs:label ?item.
          FILTER (lang(?item) = 'en')
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        }
        """)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()["results"]["bindings"][0]["itemLabel"]["value"]

    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.questions()).start()
            Thread(target=self.load_prediction()).start()
            Thread(target=self.extract_relation()).start()
            Thread(target=self.graph_init()).start()
            Thread(target=self.extract_entity()).start()
            Thread(target=self.entity_matching()).start()
            Thread(target=self.query()).start()


if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("question",
                        help="Ask a question. For instance: What is the ticker symbol of Hello Fresh?",
                        type=str)
    args = parser.parse_args()
    prediction_dir = "/Users/mlcb/PycharmProjects/Thesis/thesis_py/prediction.json"
    threshold_value = 85
    EntityLinking(prediction_dir, threshold_value, args.question).runall()
    print("--- %s seconds ---" % (time.time() - start_time))
