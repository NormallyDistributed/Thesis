import json
import os
import csv
import rdflib
import rdflib.graph as g
from rdflib.namespace import XSD
from spacy.lang.en import English
import time
import isodate
from rich.console import Console
from rich.progress import track
from urllib.error import HTTPError
from SPARQLWrapper import SPARQLWrapper, JSON
from rich import pretty
from spert.spert_predict import *
from threading import Thread
from fuzzywuzzy import fuzz
pretty.install()
console = Console()
from decomposition.rule_based.run_model import *


class EntityLinking(object):

    def __init__(self, dir_prediction: str, initial_threshold: int, question: str):
        self.answer_list = list()
        self.dir_prediction = dir_prediction
        self.threshold = initial_threshold
        self.question = question
        self.prediction = list()
        self.relation = None
        self.relation_uri = list()
        self.entity_uri = list()
        self.entity = None
        self.operator = None
        self.query_command = None
        self.label = None
        self.value = rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#value")
        self.alt_label = None
        self.best_candidate = "No Match"
        self.graph = None
        self.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.complex = False

    def decomposition(self):
        decomposed_questions = main(self.question)
        print(decomposed_questions)
        if len(decomposed_questions) > 1:
            self.complex = True
        if len(decomposed_questions) == 3:
            #decomposed_questions[1] = "of @@1@@"
            decomposed_questions = [str(decomposed_questions[2]), "".join(str(decomposed_questions[0])+" "+str(decomposed_questions[1]))]
        self.question = [str(question) for question in decomposed_questions]

    def predict_questions(self):

        nlp = English()
        tokenizer = nlp.tokenizer

        for question in self.question:
            with open('/Users/mlcb/PycharmProjects/Thesis/thesis_py/question.json', 'w') as fp:
                json.dump([{"tokens": ["{}".format(i) for i in tokenizer(question)]}], fp)
            predict()
            with open(self.dir_prediction) as json_file:
                self.prediction.append(json.load(json_file))
        print(self.prediction)

    def extract_relation(self):

        self.alt_label = rdflib.URIRef("http://www.w3.org/2004/02/skos/core#altLabel")
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

        for question in range(0,len(self.prediction)):
            for i in track(self.prediction[question], description="Extract relation ... "):
                for j in range(0, len(i["relations"])):
                    self.relation = i["relations"][0]["type"]
                    self.relation_uri.append(triple_predicate(self.relation.replace(" ", "_")))
        if len(self.relation_uri) < len(self.question):
            console.print("Answer unknown. Reason: The mentioned relation is not present in the database.",
                          style="bold red")
            exit()
        console.print(">>> {}".format(self.relation.replace("_", " ")), style="bold red")

    def object_matching(self):

        def strip(irl):
            return irl.replace("<", "").replace(">", "")

        self.label = rdflib.URIRef(strip(self.relation_uri))
        #self.value = rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#value")
        #self.alt_label = rdflib.URIRef("http://www.w3.org/2004/02/skos/core#altLabel")

        def get_entity_irl(graphs, label_uri, altlabel_uri):
            sparql_query_entity: str = """
            SELECT *
            WHERE {
            {?s ?label ?o .} UNION {?o ?altlabel ?d.}
            }
            """
            return graphs.query(sparql_query_entity, initBindings={'label': label_uri, "altlabel": altlabel_uri})

        output_entity = get_entity_irl(self.graph, label_uri=self.label, altlabel_uri=self.alt_label)

        candidate_list = list()
        for row in output_entity:
            candidate_list.append([str(row.asdict()["o"].toPython()), str(row.asdict()["s"].toPython())])

        threshold: int = self.threshold
        candidate = None

        for entity_candidate in track(candidate_list, description="Entity extraction ..."):
            if fuzz.token_set_ratio(self.entity, entity_candidate[0]) > threshold:
                threshold = fuzz.token_set_ratio(self.entity, entity_candidate)
                self.best_candidate = entity_candidate

        if self.best_candidate != "No Match":
            self.entity_uri = self.best_candidate[1]

        else:
            with open("wiki_companies.json") as json_file:
                wiki_companies = json.load(json_file)
            for entry in wiki_companies:
                if "altlabel" not in entry:
                    entry["altlabel"] = None
            if self.operator is None and self.best_candidate == "No Match":
                for entry in wiki_companies:
                    if (fuzz.token_set_ratio(self.entity,
                                             entry["label"]) or fuzz.token_set_ratio(self.entity,
                                                                                     entry["altlabel"])) > threshold:
                        threshold = fuzz.token_set_ratio(self.entity, entry["altlabel"])
                        candidate = [entry["label"], entry["item"], threshold]
            if candidate is not None:
                self.entity_uri = candidate[1]
                self.best_candidate = candidate[0]
            else:
                console.print("Answer unknown. Reason: The mentioned company is not present in the database.",
                              style="bold red")
                exit()
        console.print(f">>> {self.entity}", style="bold red")

    def extract_entity(self):
        start_end = None
        for i in self.prediction[0]:
            for j in range(0, len(i["entities"])):
                if i["entities"][j]["type"] == "company":
                    start_end = (i["entities"][j]["start"], i["entities"][j]["end"])
                if i["entities"][j]["type"] in ["argmin_max", "count", "aggregate"]:
                    self.operator = i["entities"][j]["type"]

        entity_cache = []
        try:
            for i in self.prediction[0]:
                for j in range(start_end[0], start_end[1]):
                    entity_cache.append(i["tokens"][j])
                self.entity = " ".join(entity_cache)
        except TypeError:
            pass
        print(self.operator)

    def graph_init(self):
        self.graph = g.Graph()
        self.graph.parse('KnowledgeBaseUpdate.nt', format='ttl')

    def complex_question(self):
        subject = list()
        if self.complex is True:
            def strip(irl):
                return irl.replace("<", "").replace(">", "")

            predicate_uri = rdflib.URIRef(strip(self.relation_uri[0]))
            object_uri = rdflib.Literal(str(self.entity), datatype=XSD.string)
            print(predicate_uri, object_uri, self.entity)
            object = self.entity
            if self.entity is None:
                query_command = """
                            SELECT *
                            WHERE
                            {
                              ?s ?p ?o .
                            }
                            """
                output = self.graph.query(query_command, initBindings={'p': predicate_uri,
                                                                       })
            else:
                query_command = """
                SELECT ?s
                WHERE
                {
                  ?s ?p ?o .
                }
                """
                output = self.graph.query(query_command, initBindings={'p': predicate_uri,
                                                                       'o': object_uri})

            for row in output:
                subject.append(row.asdict()["s"].toPython())

            for entry in subject:
                self.get_missing_object(self.graph, subject_uri=rdflib.URIRef(entry),
                                        predicate_uri=rdflib.URIRef(strip(self.relation_uri[1])), operator=None)
            else:
                pass
            self.answer_list = ", ".join(str(v) for v in self.answer_list)

            if self.entity is None:
                console.print("The {} of companies with {} {} is: {}.".format(self.relation_uri[1].replace("_", " "),
                                                            self.relation_uri[0].replace("_", " "),
                                                            object,
                                                            self.answer_list).replace("..", "."),
                              style="bold green")
            else:
                console.print("The {} of {} is: {}.".format(self.relation.replace("_", " "),
                                                        self.entity,
                                                        self.answer_list).replace("..", "."),
                          style="bold green")
                #self.answer_list = list()

    def entity_matching(self):

        if self.complex is True:
            pass
        else:
            self.label = rdflib.URIRef("http://www.w3.org/2000/01/rdf-schema#label")
            self.value = rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#value")
            self.alt_label = rdflib.URIRef("http://www.w3.org/2004/02/skos/core#altLabel")

            def get_entity_irl(graphs, label_uri, altlabel_uri):
                sparql_query_entity: str = """
                SELECT *
                WHERE {
                {?s ?label ?o .} UNION {?s ?altlabel ?o.}
                }
                """
                return graphs.query(sparql_query_entity, initBindings={'label': label_uri, "altlabel": altlabel_uri})

            output_entity = get_entity_irl(self.graph, label_uri=self.label, altlabel_uri=self.alt_label)

            candidate_list = list()
            for row in output_entity:
                candidate_list.append([str(row.asdict()["o"].toPython()), str(row.asdict()["s"].toPython())])

            threshold: int = self.threshold
            candidate = None

            for entity_candidate in track(candidate_list, description="Entity extraction ..."):
                if fuzz.token_set_ratio(self.entity, entity_candidate[0]) > threshold:
                    threshold = fuzz.token_set_ratio(self.entity, entity_candidate)
                    self.best_candidate = entity_candidate

            if self.best_candidate != "No Match":
                self.entity_uri = self.best_candidate[1]

            else:
                with open("wiki_companies.json") as json_file:
                    wiki_companies = json.load(json_file)
                for entry in wiki_companies:
                    if "altlabel" not in entry:
                        entry["altlabel"] = None
                if self.operator is None and self.best_candidate == "No Match":
                    for entry in wiki_companies:
                        if (fuzz.token_set_ratio(self.entity,
                                                 entry["label"]) or fuzz.token_set_ratio(self.entity,
                                                                                         entry["altlabel"])) > threshold:
                            threshold = fuzz.token_set_ratio(self.entity, entry["altlabel"])
                            candidate = [entry["label"], entry["item"], threshold]
                if candidate is not None:
                    self.entity_uri = candidate[1]
                    self.best_candidate = candidate[0]
                else:
                    console.print("Answer unknown. Reason: The mentioned company is not present in the database.",
                                  style="bold red")
                    exit()
            console.print(f">>> {self.entity}", style="bold red")

    def get_missing_object(self, graphs, predicate_uri, subject_uri, operator):

        arg = None
        query_command = None

        if operator is None:
            arg = "c"
            query_command = """
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX fibo: <https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/>
            SELECT ?c ?e
            WHERE {
            {?s ?p ?o. FILTER (datatype(?o) = xsd:string ||
            datatype(?o) = xsd:decimal ||
            datatype(?o) = xsd:date ||
            datatype(?o) = xsd:duration ||
            datatype(?o) = xsd:boolean
            ) BIND(?o as ?c)}
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

        if self.complex is True:
            output = graphs.query(query_command, initBindings={'p': predicate_uri,
                                                               's': subject_uri,
                                                               'value': self.value
                                                               })

        else:
            output = graphs.query(query_command, initBindings={'p': predicate_uri,
                                                               's': subject_uri,
                                                               'label': self.label,
                                                               'value': self.value
                                                               })

        if operator is None:
            for row in track(output, description="Execute query ...    "):
                if "http://www.w3.org/2001/XMLSchema#duration" in str(row):
                    self.answer_list.append(str(isodate.parse_duration(row[arg])).split(",")[0])
                    break
                if "http://www.w3.org/2001/XMLSchema#boolean" in str(row):
                    self.operator = "boolean"
                    self.answer_list.append(row.asdict()[arg].toPython())
                elif "wikidata" in str(row):
                    sparql_cache = row["e"].split("/entity/")[1].split("/")[0]
                    self.answer_list.append(row["c"] + " " + self.wikidata_query(subject=sparql_cache,
                                                                                 predicate=None,
                                                                                 condition=False))
                    continue
                else:
                    self.answer_list.append(row.asdict()[arg].toPython())
            #self.answer_list = ", ".join(str(v) for v in self.answer_list)

        if self.operator == "count":
            for row in output:
                self.answer_list = row.asdict()[arg].toPython()

    def wikidata_query(self, subject, predicate, condition):
        if condition is True:
            entity = subject.split("/entity/")[1].split("/")[0]
            relation = "wdt:{}".format(predicate.split("/entity/")[1].split("/")[0].replace(">", ""))
        else:
            entity = subject
            relation = "rdfs:label"

        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        sparql.setQuery("""
        SELECT ?item ?itemLabel
        WHERE {
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
          """ + "wd:{} {} ?item.".format(entity, relation) + """
          BIND(?item as ?itemLabel)
        OPTIONAL{FILTER(lang(?itemLabel)="en")}
        } """)
        try:
            sparql.setReturnFormat(JSON)
        except HTTPError:
            time.sleep(4)
        sparql.setReturnFormat(JSON)
        answer = list()

        if condition is False:
            return sparql.query().convert()["results"]["bindings"][0]["itemLabel"]["value"]
        else:
            if len(sparql.query().convert()["results"]["bindings"]) != 0:
                for row in range(len(sparql.query().convert())):
                    answer.append(sparql.query().convert()["results"]["bindings"][0]["itemLabel"]["value"])
                return console.print("The {} of {} is: {}.".format(self.relation.replace("_", " "), self.best_candidate,
                                                                   *list(set(answer))), style="bold cyan")
            else:
                return console.print("This information is not in the database.", style="bold red")

    def query(self):

        if self.complex is True:
            pass
        else:
            def strip(irl):
                return irl.replace("<", "").replace(">", "")

            if "wikidata" in self.entity_uri:
                self.wikidata_query(subject=self.entity_uri, predicate=self.relation_uri, condition=True)
            else:
                if self.operator is None:
                    self.get_missing_object(self.graph, subject_uri=rdflib.URIRef(self.entity_uri),
                                            predicate_uri=rdflib.URIRef(strip(self.relation_uri[0])), operator=None)
                elif self.operator == "count":
                    self.get_missing_object(self.graph, subject_uri=rdflib.URIRef(self.entity_uri),
                                            predicate_uri=rdflib.URIRef(strip(self.relation_uri[0])), operator="count")
                Thread(target=self.output_text()).start()

    def output_text(self):

        if not self.answer_list:
            return console.print("This information is not in the database.", style="bold red")
        else:
            if self.operator == "count":
                return console.print("The number of {}'s {}s is {}.".format(
                                                               self.best_candidate[0],
                                                               self.relation.replace("_", " "),
                                                               self.answer_list), style="bold green")
            if self.operator == "boolean":
                return console.print("The statement that {} to {} is {}.".format(
                    self.relation.replace("_", " "),
                    self.best_candidate[0],
                    self.answer_list.lower()), style="bold green")
            else:
                return console.print("The {} of {} is: {}.".format(self.relation.replace("_", " "),
                                                                   self.best_candidate[0],
                                                                   self.answer_list).replace("..", "."),
                                     style="bold green")

    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.decomposition()).start()
            Thread(target=self.predict_questions()).start()
            Thread(target=self.extract_relation()).start()
            Thread(target=self.graph_init()).start()
            Thread(target=self.extract_entity()).start()
            Thread(target=self.complex_question()).start()
            Thread(target=self.entity_matching()).start()
            Thread(target=self.query()).start()

#  listing venue: P414


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
    console.print("runtime: %s seconds" % (round(time.time() - start_time, 2)), style="bold blue")