from threading import Thread
from fuzzywuzzy import fuzz
import json
import os
import csv
import rdflib.graph as g


class EntityLinking(object):

    def __init__(self, dir_prediction: str, initial_threshold: int):
        self.dir_prediction = dir_prediction
        self.threshold = initial_threshold
        self.prediction = None
        self.relation = None
        self.relation_uri = None
        self.entity_uri = None
        self.entity = None
        self.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    def load_prediction(self):
        with open(self.dir_prediction) as json_file:
            self.prediction = json.load(json_file)

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
                    predicate = "<{}{}>".format(prefix_dict[key].replace(">", ""), *mapping_dict[item][0].values())
            else:
                pass
            return predicate

        for i in self.prediction:
            for j in range(0, len(i["relations"])):
                self.relation = i["relations"][0]["type"]
                self.relation_uri = triple_predicate(self.relation)

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

    def kg_entity_query(self):
        pass

    def entity_matching(self):

        # placeholder KG
        knowledge_graph_entities = ["Volkswagen AG", "Gamestop", "BMW", "Hello Fresh SE"]

        best_candidate = "No Match"

        threshold: int = self.threshold

        for entity_candidate in knowledge_graph_entities:
            if fuzz.token_set_ratio(self.entity, entity_candidate) > threshold:
                threshold = fuzz.token_set_ratio(self.entity, entity_candidate)
                best_candidate = entity_candidate

    def query(self):
        graph = g.Graph()
        graph.parse('data/CovestroAG.nt', format='ttl')

        self.entity_uri = "<http://example.org/entity/#CovestroAG>"

        query_command = str(self.entity_uri + " " + str(self.relation_uri) + " " + "?object")

        query_result = graph.query("""
          SELECT ?object WHERE {
          """ + query_command + """ .
          }""")
        for row in query_result:
            print("The {} of {} is {}.".format(self.relation, self.entity, row[0]))

    def runall(self):
        if __name__ == '__main__':
            Thread(target=self.load_prediction()).start()
            Thread(target=self.extract_relation()).start()
            Thread(target=self.extract_entity()).start()
            Thread(target=self.entity_matching()).start()
            Thread(target=self.query()).start()


if __name__ == "__main__":
    prediction_dir = "/Users/mlcb/Desktop/spert-master-3/data/predictions.json"
    threshold_value = 85
    EntityLinking(prediction_dir, threshold_value).runall()
