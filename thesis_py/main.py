from threading import Thread
import json
from typing import Dict, Any
from fuzzywuzzy import fuzz
import re
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time
import csv
from operator import itemgetter
from datetime import datetime
from urllib.error import HTTPError
from rdflib import URIRef, BNode
from price_parser import Price
import datefinder


class kg_construction(object):

    input_list = {
                "accounting_standards": "{?item wdt:P31 wd:Q317623. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q1779838. ?item skos:altLabel ?altlabel.}",
                "complex_financials": "{?item wdt:P279 wd:Q192907. ?item skos:altLabel ?altlabel.}",
                "country_of_origin/headquarters": "?item wdt:P31 wd:Q6256. ?item skos:altLabel ?altlabel.",
                "country_of_registration/incorporation": "?item wdt:P31 wd:Q6256. ?item skos:altLabel ?altlabel.",
                "external_auditor": "?item wdt:P452 wd:Q23700345. ?item skos:altLabel ?altlabel.",
                "financial_advisor": "{ {?item wdt:P31 wd:Q613142.} OPTIONAL { ?item skos:altLabel ?altlabel.} } UNION { {?item wdt:P31 wd:Q4830453.} OPTIONAL {?item skos:altLabel ?altlabel.} }",
                "industry": "{?item wdt:P31 wd:Q8148. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q268592. ?item skos:altLabel ?altlabel.}",
                "initial_price_range": "{?item wdt:P31 wd:Q8142. ?item skos:altLabel ?altlabel.}",
                "investment_bank": "{?item wdt:P31 wd:Q319845. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q22687. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q568041. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q670792. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q4830453. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q2111088. ?item skos:altLabel ?altlabel.}",
                "isin": "?item wdt:P946 ?object. ?item skos:altLabel ?altlabel.",
                "listing_venue": "?item wdt:P31 wd:Q11691. ?item skos:altLabel ?altlabel.",
                "non-gaap_measure": "{{?item wdt:P31 wd:Q832161.} OPTIONAL {?item skos:altLabel ?altlabel.}}"
                 }

    def __init__(self, dir):
        self.dir = dir
        self.data = None
        self.input = str()
        self.results_list = []
        self.data_graph = []
        self.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    def parse_pdf(self):
        pass

    def load_parsed_pdf(self):

        with open(self.dir) as json_file:
            self.data = json.load(json_file)

    def preprocessing(self):

        # extract annotations from parsed pdf
        placeholder: Dict[Any, Any] = {}
        for annotation in range(0, len(self.data["blobs"])):
            if len(self.data["blobs"][annotation]["annotations"]) != 0:
                base = self.data["blobs"][annotation]["annotations"]
                placeholder.update(base)
        self.data = placeholder

        # Date extractor
        def xsd_date(item):
            try:
                for i in self.data[item]:
                    matches = datefinder.find_dates(i)
                    if matches is not None:
                        self.data[item][self.data[item].index(i)] = [str(datetime.date(match)) for match in matches]
            except KeyError:
                pass

        # Duration extractor
        numbers_dict = {
            'zero': 0,
            'one': 1,
            'two': 2,
            'three': 3,
            'four': 4,
            'five': 5,
            'six': 6,
            'seven': 7,
            'eight': 8,
            'nine': 9,
            'ten': 10,
            'eleven': 11,
            'twelve': 12,
            'thirteen': 13,
            'fourteen': 14,
            'fifteen': 15,
            'sixteen': 16,
            'seventeen': 17,
            'eighteen': 18,
            'nineteen': 19,
            'twenty': 20,
            'thirty': 30,
            'forty': 40,
            'fifty': 50,
            'sixty': 60,
            'seventy': 70,
            'eighty': 80,
            'ninety': 90,
            'hundred': 100,
            'thousand': 1000,
            'million': 1000000,
            'billion': 1000000000,
            'point': '.'
        }

        def xsd_duration(var):
            try:
                for item in self.data[var]:
                    for entry in numbers_dict:
                        if entry in item:
                            self.data[var][self.data[var].index(item)] = item.replace(entry, str(
                                numbers_dict.get(entry)))

                for item in range(0, len(self.data[var])):
                    cache = []
                    for element in self.data[var][item].split():
                        element = [string for string in re.split(r'([1-9])', element) if string != ""]
                        for pos in element:
                            if pos in ["days", "months", "years"] or pos.isdigit() == True:
                                cache.append(pos)
                        self.data[var][item] = "".join(cache)

                # normalization
                self.data[var] = [item.lower().replace("s", "") for item in self.data[var]]
                self.data[var] = [
                    "P" + item.replace("day", "D").replace("month", "nM").replace("year", "Y").replace(" ", "") for item in
                    self.data[var]]
                for item in self.data[var]:
                    if item == "P" or len(item) > 7:  # replace with sequence
                        self.data[var][self.data[var].index(item)] = "unknown"
            except KeyError:
                pass

        # Monetary values extractor
        def monetary_values(key):
            try:
                for item in self.data[key]:
                    cache = []
                    if "million" in item:
                        for number in re.findall(r"([0-9.,]*[0-9]+)", item):
                            if "," in number and "." not in number:
                                cache.append([item, number.replace(",", "") + "0" * 3, currency_mapper(Price.fromstring(item).currency)])
                            if "," in number and "." in number:
                                zeros1 = abs(Price.fromstring(number).amount.as_tuple().exponent)
                                cache.append([item, number.replace(".", "").replace(",", "") + "0" * (3 - int(zeros1)),
                                             currency_mapper(Price.fromstring(item).currency)])
                            if "." in number and "," not in number:
                                zeros2 = abs(Price.fromstring(number).amount.as_tuple().exponent)
                                cache.append([item, number.replace(".", "") + "0" * (6 - int(zeros2)),
                                              currency_mapper(Price.fromstring(item).currency)])
                            if "." not in number and "," not in number:
                                cache.append([item, number + "0" * 6, currency_mapper(Price.fromstring(item).currency)])
                        self.data[key] = cache
                    else:
                        if "%" in item or "percentage" in item.lower():
                            for number in re.findall(r"(\d*\.?\d+)%", item.replace(" ", "")):
                                cache.append([item, number, "percentage"])
                            self.data[key] = cache
                        else:
                            cache.append([item, str(Price.fromstring(item).amount), currency_mapper(Price.fromstring(item).currency)])
                            self.data[key] = cache
                return self.data[key]
            except KeyError:
                pass

        # Monetary values extractor
        currencies = {
            "GBP": "\u00a3",
            "CNY": "\u00a5",
            "EUR": "\u20ac",
            "INR": "\u20B9",
            "JPY": "\u00a5",
            "PLN": "z\u0142",
            "RUB": "\u20BD",
            "USD": "\u0024",
        }
        currency_map = {
            "GBP": "<https://www.wikidata.org/entity/Q25224>",
            "CNY": "<https://www.wikidata.org/entity/Q39099>",
            "EUR": "<https://www.wikidata.org/entity/Q4916>",
            "INR": "<https://www.wikidata.org/entity/Q80524>",
            "JPY": "<https://www.wikidata.org/entity/Q8146>",
            "PLN": "<https://www.wikidata.org/entity/Q123213>",
            "RUB": "<https://www.wikidata.org/entity/Q41044>",
            "USD": "<https://www.wikidata.org/entity/Q4917>",
        }

        def currency_mapper(symbol):
            try:
                for key in currencies:
                    symbol = symbol.replace(currencies[key], key)
                for key in currency_map:
                    symbol = symbol.replace(key, currency_map[key])
                return symbol
            except AttributeError:
                pass

        def decimal_values(key):
            try:
                for item in self.data[key]:
                    cache = []
                    for number in re.findall(r"([0-9.,]*[0-9]+)", item):
                        cache.append([item, str(Price.fromstring(number).amount), currency_mapper(str(Price.fromstring(item).currency))])
                        self.data[key] = cache
                return self.data[key]
            except KeyError:
                pass

        # Assign boolean value "true" if rule applies; either direct or fuzzy match
        def xsd_boolean(var):
            try:
                for item in [re.sub('[^A-Za-z0-9]+', '', item).replace(" ", "").lower() for item in
                             self.data[var]]:
                    if (var.replace("applies", "").replace("_", "").replace(" ", "") in item) or (fuzz.token_set_ratio(var.replace("applies", "").replace("_", "").replace(" ", ""), item) > 85) or (item in ["yes", "true", "1"]):
                        self.data[var] = [str(True)]
            except KeyError:
                pass

        # Accounting standards categorization
        def accounting_categorization(var):
            try:
                for item in self.data[var]:
                    for element in item.lower().replace(" ", "").split():
                        if "ifrs" in element:
                            self.data[var][self.data[var].index(item)] = "IFRS"
                            break
                        if "gaap" in element:
                            self.data[var][self.data[var].index(item)] = "GAAP"
                            break
                        else: pass
            except KeyError:
                pass

        # ISIN check
        def isin_check(var):
            # mandatory costraint: [A-Z]{2}[A-Z\d]{9}\d (wikidata)
            sequence = re.compile(r"[A-Za-z]{2}[a-zA-Z0-9]{10}$")
            try:
                for item in range(0, len(self.data[var])):
                    self.data[var][item], *tail = ["".join(x) for x in re.findall(pattern=sequence, string=self.data[var][item].replace(" ", "").replace(".", "").replace(",", ""))]
            except ValueError:
                pass
            except KeyError:
                pass

        # Complex-Financials: 2 Types -> {"Combined Financial Statements": "shows financial results of different subsidiary companies from that of the parent company", "Consolidated Financial Statements": "aggregate the financial position of a parent company and its subsidiaries"}
        def complex_categorization(var):
            try:
                for item in [element.lower().strip() for element in self.data[var]]:
                    for i in range(0, len(item.split())):
                        if ("combined" in item.split()[i]) and (item.split()[i-1] != "no"):
                            self.data[var].append("combined financial statement")
                        if ("consolidated" in item.split()[i]) and (item.split()[i-1] != "no"):
                            self.data[var].append("consolidated financial statement")
            except KeyError:
                pass

        # Audit-Report: 4 Types -> {"qualified": "company’s financial records have not been maintained in accordance with GAAP but no misrepresentations are identified, an auditor will issue a qualified opinion", "unqualified": "audit report that is issued when an auditor determines that each of the financial records provided by the small business is free of any misrepresentations+in accordance with GAAP", "adverse": "indicates that the firm’s financial records do not conform to GAAP + the financial records provided by the business have been grossly misrepresented", "disclaimer": "auditor is unable to complete an accurate audit report"}
        # def audit_categorization(var): #qualified, unqualified/without-/no qualification(s), adverse, disclaimer
        #     try:
        #         for item in [element.lower().strip().replace("s","") for element in self.data[var]]:
        #             for key in audit_categories:
        #                 for element in audit_categories[key]:
        #                     if re.search(r'\bunqualified\b', item):
        #                         print(item)
        #                     #print("match: {}, item: {}".format(re.search(r'\bunqualified\b', element),item))
        #                         #print(element,item)
        #     except KeyError:
        #         pass
        #
        # audit_categories = {
        #                     "qualified opinion": ["qualified", "with qualification"],
        #                     "unqualified opinion": ["without qualification", "no qualification", "unqualified"],
        #                     "adverse opinion": ["adverse"],
        #                     "disclaimer of opinion": ["disclaimer"]
        #                     }



        # dictionary = {"variable_to_be_preprocessed": "corresponding_xsd_standard"}
        preprocessing_dict = {"lock-up_period": xsd_duration,
                              "periods_of_unaudited_interim_fs": xsd_date,
                              "periods_of_unaudited_financial_statements": xsd_date,
                              "periods_of_pffi": xsd_date,
                              "filing_date": xsd_date,
                              "underwriting_fees": monetary_values,
                              "underwriters_incentive_fee": monetary_values,
                              "accounting_standards": accounting_categorization,
                              "isin": isin_check,
                              "regulation_s_applies": xsd_boolean,
                              "rule_144a_applies": xsd_boolean,
                              "offering_costs": monetary_values,
                              "initial_price_range": decimal_values,
                              "expected_gross_proceeds": monetary_values,
                              "expected_net_proceeds": monetary_values,
                              "complex_financials": complex_categorization
                              }

        # apply preprocessing for all keys in preprocessing_dict
        for key in preprocessing_dict.keys():
            preprocessing_dict[key](key)

        try:
            print(self.data["underwriting_fees"])
        except KeyError:
            pass

    def query(self):

        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        query_command = """
         SELECT ?item ?itemLabel ?object ?altlabel WHERE {
         """ + self.input + """
         SERVICE wikibase:label { bd:serviceParam wikibase:language "en,de".}
         }"""

        sparql.setQuery(query_command)

        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        self.results_list = []
        for i in results["results"]["bindings"]:
            if query_command.count("?object") > 1:
                item = {'label': i["itemLabel"]["value"], 'object': i["object"]["value"], 'value': i["item"]["value"], "altlabel": i["altlabel"]["value"]}
                self.results_list.append({self.output: item})
            else:
                if "altlabel" not in i.keys():
                    i.update({"altlabel": {"value": "unknown altabel"}})
                item = {'label': i["itemLabel"]["value"], 'value': i["item"]["value"], "altlabel": i["altlabel"]["value"]}
                self.results_list.append({self.output: item})

        with open(os.path.join(self.location,"queries/query_{}.json").format(self.output.replace("/", "")), 'w') as outfile:
            json.dump(self.results_list, outfile)

    def run_query(self):
        for item in self.input_list:
            self.output = item
            self.input = self.input_list.get(item)
            if os.path.isfile(os.path.join(self.location,"queries/query_{}.json").format(self.output.replace("/", ""))) == True:
                kg_construction.fuzzy_matching(self)
            else:
                try:
                    kg_construction.query(self)
                except HTTPError:
                    time.sleep(8)
                kg_construction.fuzzy_matching(self)
        print(self.data)

    def fuzzy_matching(self):
        # link extracted values to wikidata database

        # open extracted wikidata entries from wikidata sparql query
        with open(os.path.join(self.location,"queries/query_{}.json").format(self.output.replace("/", "")), "r") as json_file:
            output_file = json_file.read()
            output_file = output_file.replace("'", "").replace(',}', '}').replace(',]', ']').replace('\n', '').replace('\t', '')
            self.results_list = json.loads(output_file)

        placeholder_list = list(map(itemgetter(self.output), list(filter(lambda x: self.output in x, self.results_list))))

        if placeholder_list != [] and "object" in placeholder_list[0]:
            form = "object"
        else:
            form = "label"

        def norm(item):
            return str(item).lower().replace(" ", "").replace(".", "")

        ## lower case and strip all whitespaces when comparing
        ## Pseudocode: Find match (key,value) pair in self.data to wikidata sparql query result
        # IF identical match: use wikidata identifier
        # ELSE: find closest match to value:
        #       (i)  if success: use wikidata identifier of closest match
        #       (ii) else: pass
        try:
            for item in [self.data[self.output][entry] for entry in range(0,len(self.data[self.output]))]:
                #cache = [{"label": item}]
                match = next((x for x in placeholder_list if norm(item) in norm(x[form]) or norm(item) in norm(x["altlabel"])),None)
                if match != None:
                    if match["value"] not in self.data[self.output]:
                        self.data[self.output].append(match["value"])
                else:
                    if self.output != "isin":
                        threshold = 85
                        for element in self.data[self.output]:
                            for x in placeholder_list:
                                if (fuzz.ratio(norm(x["label"]), norm(element)) or fuzz.ratio(norm(x["altlabel"]), norm(element))) > threshold:
                                    threshold = max(fuzz.ratio(norm(x["label"]), norm(element)), fuzz.ratio(norm(x["altlabel"]), norm(element)))
                                    closest_match = x
                                    if closest_match.get("value") not in self.data[self.output]:
                                        self.data[self.output].append(closest_match.get("value"))


        except KeyError:
            pass

    def generate_triples(self):

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

        # prefix = {
        #                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        #                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        #                "wd": "https://wikidata.org/entity/",
        #                "omg-spec": "http://www.omg.org/techprocess/ab/SpecificationMetadata/",
        #                "omg-lr": "https://www.omg.org/spec/LCC/Languages/LanguageRepresentation/",
        #                "omg-cc": "https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
        #                "fibo-fbc-fi-fi": "https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/",
        #                "fibo-sec-sec-lst": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesListings/",
        #                "fibo-fnd-org-fm": "https://spec.edmcouncil.org/fibo/ontology/FND/Organizations/FormalOrganizations/",
        #                "fibo-fbc-pas-fpas": "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/FinancialProductsAndServices/",
        #                "fibo-sec-sec-id": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesIdentification/",
        #                "fibo-fbc-fct-fse": "https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/FinancialServicesEntities/",
        #                "fibo-fnd-agr-ctr": "https://spec.edmcouncil.org/fibo/ontology/FND/Agreements/Contracts/",
        #                "fibo-fbc-fct-mkt": "https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/Markets/",
        #                "fibo-be-le-lei": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LEIEntities/",
        #                "fibo-be-le-lp": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/ProfitObjective",
        #                "fibo-be-oac-exec": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Executives/",
        #                "fibo-sec-dbt-bnd": "https://spec.edmcouncil.org/fibo/ontology/SEC/Debt/Bonds/",
        #                "fibo-sec-eq-eq": "https://spec.edmcouncil.org/fibo/ontology/SEC/Equities/EquityInstruments/",
        #                "fibo-sec-sec-rst": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesRestrictions/",
        #                "fibo-fnd-arr-cls": "https://spec.edmcouncil.org/fibo/ontology/FND/Arrangements/ClassificationSchemes/IndustrySectorClassifier/",
        #                "fibo-sec-sec-iss": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesIssuance/",
        #                "fibo-civ-fnd-civ": "https://spec.edmcouncil.org/fibo/ontology/CIV/Funds/CIV/",
        #                "fibo-fnd-gao-obj": "https://spec.edmcouncil.org/fibo/ontology/FND/GoalsAndObjectives/Objectives/InvestmentObjective",
        #                "fibo-fnd-pty-pty": "https://spec.edmcouncil.org/fibo/ontology/FND/Parties/Parties/",
        #                "fibo-bp-iss-muni": "https://spec.edmcouncil.org/fibo/ontology/BP/SecuritiesIssuance/MuniIssuance/",
        #                "fibo-bp-iss-ipo": "https://spec.edmcouncil.org/fibo/ontology/BP/SecuritiesIssuance/EquitiesIPOIssuance/FilingDetails",
        #                "fibo-loan-typ-prod": "https://spec.edmcouncil.org/fibo/ontology/LOAN/LoanTypes/LoanProducts/",
        #                "fibo-fnd-arr-rt": "https://spec.edmcouncil.org/fibo/ontology/FND/Arrangements/Ratings/",
        #                "fibo-ph": "https://spec.edmcouncil.org/fibo/ontology/placeholder/"
        #                }

        with open(os.path.join(self.location,"prefixes_fibo.csv"), newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in reader:
                fibo_prefixes.update({row[1].replace(":",""): row[2]})

        prefix_dict = {**fibo_prefixes, **prefixes}

        mapping_dict = {"accounting_standards": [{"fibo-be-le-lei": "hasAccountingStandard"}, {"datatype": "string"}],
                        "audit_report": [{"fibo-sec-sec-lst": "hasAuditReport"}, {"datatype": "string"}],
                        "audited_financial_statements": [{"fibo-sec-sec-lst": "hasAuditedFinancialStatement"}, {"datatype": "string"}],
                        "competitive_strength": [{"fibo-fnd-arr-rt": "hasCompetitiveStrength"}, {"datatype": "string"}],
                        "complex_financials": [{"wd": "Q192907"}, {"datatype": "string"}], # uri+string
                        "country_of_origin/headquarters": [{"fibo-fnd-org-fm": "isDomiciledIn"}, {"datatype": "string"}],
                        "country_of_registration/incorporation": [{"fibo-fbc-fct-mkt": "operatesInCountry"}, {"datatype": "string"}],
                        "description_of_the_business": [{"omg-spec": "hasDescription"}, {"datatype": "string"}],
                        "detailed_transaction": [{"fibo-bp-iss-ipo": "FilingDetails"}, {"datatype": "string"}],
                        "dividend_policy": [{"fibo-sec-eq-eq": "hasDividendPolicy"},{"datatype": "string"}],
                        "emphasis_of_matter": [{"fibo-ph": "emphasisOfMatter"}, {"datatype": "string"}],
                        "expected_gross_proceeds": [{"fibo-ph": "hasExpectedGrossProceeds"}, {"datatype": "decimal"}],
                        "expected_net_proceeds": [{"fibo-ph": "hasExpectedNetProceeds"}, {"datatype": "decimal"}],
                        "external_auditor": [{"wd": "P8571"}, {"datatype": "string"}],
                        "filing_date": [{"fibo-fnd-agr-ctr": "hasExecutionDate"}, {"datatype": "date"}],
                        "financial_advisor": [{"fibo-bp-iss-muni": "hasFinancialAdvisor"}, {"datatype": "string"}],
                        "gaas": [{"fibo-be-le-lei": "AccountingFramework"}, {"datatype": "string"}],
                        "industry": [{"wd": "P452"}, {"datatype": "string"}],
                        "initial_price_range": [{"fibo-fbc-pas-fpas": "hasOfferingPrice"}, {"datatype": "decimal"}],
                        "investment_bank": [{"fibo-fbc-fct-fse": "InvestmentBank"}, {"datatype": "string"}],
                        "isin": [{"wd": "P946"}, {"datatype": "string"}],
                        "issuer_name": [{"rdfs": "label"}, {"datatype": "string"}],
                        "key_element_strategy": [{"fibo-fnd-gao-obj": "BusinessStrategy"}, {"datatype": "string"}],#placeholder
                        "key_factor_affecting_results": [{"fibo-fnd-pty-pty": "isAffectedBy"}, {"datatype": "string"}], #placeholder
                        "key_line_item_income_statement": [{"wd": "Q243460"}, {"datatype": "string"}], #placeholder
                        "listing_venue": [{"fibo-sec-sec-lst": "isTradedOn"}, {"datatype": "string"}],
                        "lock-up_period": [{"fibo-sec-dbt-bnd":"hasLockoutPeriod"}, {"datatype": "duration"}],
                        "non-gaap_measure": [{"wd": "Q330153"}, {"datatype": "string"}], #placeholder
                        "non-gaap_measure;_definition": [{"wd": "Q330153"}, {"datatype": "string"}], #placeholder
                        "non-recurring_item": [{"wd": "Q192907"}, {"datatype": "string"}], #string #placeholder
                        "number_of_reportable_segments": [{"wd": "Q192907"}, {"datatype": "string"}], #placeholder
                        "offering_costs": [{"wd": "Q185142"}, {"datatype": "decimal"}], #placeholder
                        "periods_of_audited_financial_statements": [{"wd": "Q740419"}, {"datatype": "string"}], #placeholder
                        "periods_of_pffi": [{"wd": "Q1166072"}, {"datatype": "date"}], #placeholder
                        "periods_of_unaudited_financial_statements": [{"wd": "Q192907"}, {"datatype": "string"}], #placeholder
                        "periods_of_unaudited_interim_fs": [{"wd": "Q192907"}, {"datatype": "date"}], #placeholder
                        "pro_forma_financial_information": [{"wd": "Q2481549"}, {"datatype": "string"}], #placeholder
                        "profit_forecast": [{"wd": "Q748250"}, {"datatype": "string"}], #placeholder
                        "reasons_for_the_offering": [{"fibo-be-le-lp": "ProfitObjective"}, {"datatype": "string"}], #placeholder
                        "regulation_s_applies": [{"fibo-sec-sec-rst": "RegulationS"}, {"datatype": "string"}],
                        "risk_factor": [{"wd": "Q1337875"},{"datatype": "string"}], #placeholder
                        "rule_144a_applies": [{"wd": "Q7378915"}, {"datatype": "boolean"}],
                        "shareholder_transaction": [{"wd": "Q1166072"}, {"datatype": "string"}], # placeholder
                        "shareholders_transaction": [{"wd": "Q1166072"}, {"datatype": "string"}], # placeholder
                        "ticker": [{"wd": "P249"}, {"datatype": "string"}],
                        "transaction": [{"wd": "Q1166072"}, {"datatype": "string"}],
                        "unaudited_financial_statements": [{"wd": "Q192907"}, {"datatype": "string"}], #placeholder
                        "unaudited_interim_financial_statements": [{"wd": "Q192907"}, {"datatype": "string"}], #placeholder
                        "underwriters_incentive_fee": [{"fibo-fbc-fct-fse": "UnderwritingArrangement"}, {"datatype": "decimal"}], #placeholder
                        "underwriting_fees": [{"fibo-fbc-fct-fse": "UnderwritingArrangement"}, {"datatype": "decimal"}], #placeholder
                        "use_of_proceeds": [{"fibo-civ-fnd-civ": "InvestmentStrategy"}, {"datatype": "string"}],
                        "working_capital_statement": [{"fibo-loan-typ-prod": "WorkingCapitalPurpose"}, {"datatype": "string"}]
                        }

        def triple_subject():
            subject = triple_format("{}#{}".format(URIRef("http://example.org/entity/"),self.data["issuer_name"][0].replace(" ", "")))
            return subject

        def triple_predicate(item):
            # for item in self.data:
            global predicate
            if item in mapping_dict:
                for key in mapping_dict[item][0]:
                    predicate = "<{}{}>".format(prefix_dict[key], *mapping_dict[item][0].values())
            else:
                pass
            return predicate

        def triple_format(item):
            return "<{}>".format(item)

        def xsd_type(input):
            return "^^<https://www.w3.org/2001/XMLSchema#{}>.".format(input)

        def object(item):
            subject = triple_subject()
            predicate = triple_predicate(item)
            output_path = "/Users/mlcb/PycharmProjects/Thesis/thesis_py/output/output_{}.nt".format(''.join(e for e in self.data["issuer_name"][0] if e.isalnum()))
            f = open(output_path,"a")
            if item in mapping_dict:
                for element in [item for item in self.data[item]]:
                    if "date" in mapping_dict[item][1].values():
                        object = "'{}'{}".format(element[0],xsd_type("date"))
                    elif "http" in element:
                        object = triple_format(element)+"."
                    elif "decimal" in mapping_dict[item][1].values():
                        if "%" == element[0] or element[2] == "percentage":
                            object = BNode().n3()+"."
                            print(object.replace(".",""), "<http://www.w3.org/1999/02/22-rdf-syntax-ns#value>","'{}'^^<https://www.w3.org/2001/XMLSchema#decimal>.".format(element[1]), file=f)
                            print(object.replace(".",""),"<http://www.w3.org/2000/01/rdf-schema#property>","<https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/Analytics/Percentage>.", file=f)
                            print(object.replace(".",""), "<http://www.w3.org/1999/02/22-rdf-syntax-ns#comment>","'{}'.".format(element[0]), file=f)
                        else:
                            object = BNode().n3()+"."
                            print(object.replace(".",""),"<http://www.w3.org/1999/02/22-rdf-syntax-ns#value>", "'{}'^^<https://www.w3.org/2001/XMLSchema#decimal>.".format(element[1]), file=f)
                            if element[2] != None:
                                print(object.replace(".",""), "<https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/isDenominatedIn>","{}.".format(element[2]), file=f)
                            print(object.replace(".",""), "<http://www.w3.org/1999/02/22-rdf-syntax-ns#comment>","'{}'^^<http://www.w3.org/2001/XMLSchema#string>.".format(element[0].replace("'","").replace('"', "")), file=f)
                    elif "duration" in mapping_dict[item][1].values():
                        object = "'{}'{}".format(element,xsd_type("duration"))
                    elif element in [str(True),str(False)]:
                        object = "'{}'{}".format(element, xsd_type("boolean"))
                    else:
                        try:
                            object = "'{}'^^<http://www.w3.org/2001/XMLSchema#string>.".format(element.replace("'", "").replace('"', ""))
                        except AttributeError:
                            object = "'{}'^^<http://www.w3.org/2001/XMLSchema#string>.".format([elem.replace("'", "").replace('"', "") for elem in element])
                    print(subject, predicate, object, file=f)
            f.close()

        for instance in self.data:
            object(instance)

    def runall(self):
        if __name__ == '__main__':
            Thread(target = self.load_parsed_pdf()).start()
            Thread(target = self.preprocessing()).start()
            Thread(target = self.run_query()).start()
            Thread(target = self.generate_triples()).start()
            #Thread(target = self.metadata()).start()

if __name__ == "__main__":
    path = os.path.realpath(os.path.join(os.getcwd(), "data"))
    with os.scandir(path) as it:
        for entry in it:
            if entry.name.endswith(".json") and entry.is_file():
                kg_construction(entry.path).runall()
                #break






