from threading import Thread
import json
from typing import Dict, Any
from fuzzywuzzy import fuzz
import re
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time
from operator import itemgetter
from datetime import datetime
from urllib.error import HTTPError
from rdflib import URIRef
from price_parser import Price
import datefinder

class kg_construction(object):

    map_dir = "/Users/mlcb/Desktop/mapping" # directory for input/output files

    input_list = {
                "accounting_standards": "{?item wdt:P31 wd:Q317623. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q1779838. ?item skos:altLabel ?altlabel.}",
                "complex_financials": "{?item wdt:P279 wd:Q192907. ?item skos:altLabel ?altlabel.}",
                "country_of_origin/headquarters": "?item wdt:P31 wd:Q6256. ?item skos:altLabel ?altlabel.",
                "country_of_registration/incorporation": "?item wdt:P31 wd:Q6256. ?item skos:altLabel ?altlabel.",
                "external_auditor": "?item wdt:P452 wd:Q23700345. ?item skos:altLabel ?altlabel.",
                "financial_advisor": "{ {?item wdt:P31 wd:Q613142.} OPTIONAL { ?item skos:altLabel ?altlabel.} } UNION { {?item wdt:P31 wd:Q4830453.} OPTIONAL {?item skos:altLabel ?altlabel.} }",
                "industry":"{?item wdt:P31 wd:Q8148. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q268592. ?item skos:altLabel ?altlabel.}",
                "investment_bank": "{?item wdt:P31 wd:Q319845. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q22687. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q568041. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q670792. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q4830453. ?item skos:altLabel ?altlabel.} UNION {?item wdt:P31 wd:Q2111088. ?item skos:altLabel ?altlabel.}",
                "isin":"?item wdt:P946 ?object. ?item skos:altLabel ?altlabel.",
                "listing_venue": "?item wdt:P31 wd:Q11691. ?item skos:altLabel ?altlabel.",
                "non-gaap_measure": "{{?item wdt:P31 wd:Q832161.} OPTIONAL {?item skos:altLabel ?altlabel.}}"
                 }

    def __init__(self): #,dir
        self.dir = dir
        self.data = None
        self.input = str()
        self.results_list = []
        self.data_graph = []

    def parse_pdf(self):
        pass

    def load_parsed_pdf(self):

        with open(self.dir) as json_file:
            self.data = json.load(json_file)

    def preprocessing(self):

        ## extract annotations from parsed pdf
        placeholder: Dict[Any, Any] = {}
        for annotation in range(0, len(self.data["blobs"])):
            if len(self.data["blobs"][annotation]["annotations"]) != 0:
                base = self.data["blobs"][annotation]["annotations"]
                placeholder.update(base)
        self.data = placeholder



        ## Date extractor
        def xsd_date(item):
            try:
                for i in self.data[item]:
                    matches = datefinder.find_dates(i)
                    if matches != None:
                        self.data[item][self.data[item].index(i)] = [str(datetime.date(match)) for match in matches]
            except KeyError:
                pass

        ## Duration extractor
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
                    if item == "P" or len(item) > 7:  ## replace with sequence
                        self.data[var][self.data[var].index(item)] = "unknown"
            except KeyError:
                pass

        ## Monetary values extractor
        def monetary_values(key):
            try:
                for item in self.data[key]:
                    cache = []
                    if "million" in item:
                        for number in re.findall(r"([0-9.,]*[0-9]+)", item):
                            if "," in number and "." not in number:
                                cache.append([number.replace(",", "") + "0" * 3, Price.fromstring(item).currency])
                            if "," in number and "." in number:
                                zeros1 = abs(Price.fromstring(number).amount.as_tuple().exponent)
                                cache.append([number.replace(".", "").replace(",", "") + "0" * (3 - int(zeros1)),
                                              Price.fromstring(item).currency])
                            if "." in number and "," not in number:
                                zeros2 = abs(Price.fromstring(number).amount.as_tuple().exponent)
                                cache.append([number.replace(".", "") + "0" * (6 - int(zeros2)),
                                              Price.fromstring(item).currency])
                            if "." not in number and "," not in number:
                                cache.append([number + "0" * 6, Price.fromstring(item).currency])
                        self.data[key] = cache
                    else:
                        cache.append([str(Price.fromstring(item).amount), Price.fromstring(item).currency, item])
                        self.data[key] = cache

                return self.data[key]
            except KeyError:
                pass

        ## Assign boolean value "true" if rule applies; either direct or fuzzy match
        def xsd_boolean(var):
            try:
                for item in [re.sub('[^A-Za-z0-9]+', '', item).replace(" ", "").lower() for item in
                             self.data[var]]:
                    if (var.replace("applies","").replace("_","").replace(" ","") in item) or (fuzz.token_set_ratio(var.replace("applies","").replace("_","").replace(" ",""), item) > 85) or (item in ["yes", "true", "1"]):
                        self.data[var] = "true"
            except KeyError:
                pass

        ## Accounting standards categorization
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

        ## ISIN check
        def isin_check(var):
            sequence = re.compile(r"[A-Za-z]{2}[a-zA-Z0-9]{10}$")
            try:
                for item in range(0,len(self.data[var])):
                    self.data[var][item], *tail = ["".join(x) for x in re.findall(pattern=sequence, string=self.data[var][item].replace(" ", "").replace(".","").replace(",",""))]
            except ValueError:
                pass
            except KeyError:
                pass

        ## Underwriting fees
        # try:
        #     self.data["underwriting_fees"] = re.compile(r"[^\d.,]+").sub('', str(self.data["underwriting_fees"]))
        # except KeyError:
        #     pass

        # dictionary = {"variable_to_be_preprocessed": "corresponding_xsd_standard"}
        preprocessing_dict = {"lock-up_period": xsd_duration,
                              "periods_of_unaudited_interim_fs": xsd_date,
                              "periods_of_unaudited_financial_statements": xsd_date,
                              "periods_of_pffi": xsd_date,
                              "filing_date": xsd_date,
                              "underwriting_fees": monetary_values, # need to add percentages
                              "accounting_standards": accounting_categorization,
                              "isin": isin_check,
                              "regulation_s_applies": xsd_boolean,
                              "rule_144a_applies": xsd_boolean,
                              "offering_costs": monetary_values,
                              "initial_price_range": monetary_values,
                              "expected_gross_proceeds": monetary_values,
                              "expected_net_proceeds": monetary_values
                              }

        # apply preprocessing for all keys in preprocessing_dict
        for key in preprocessing_dict.keys():
            preprocessing_dict[key](key)


    # ## Audit-Report: 4 Types -> {"qualified": "company’s financial records have not been maintained in accordance with GAAP but no misrepresentations are identified, an auditor will issue a qualified opinion",
    # #                            "unqualified": "audit report that is issued when an auditor determines that each of the financial records provided by the small business is free of any misrepresentations+in accordance with GAAP",
    # #                            "adverse": "indicates that the firm’s financial records do not conform to GAAP + the financial records provided by the business have been grossly misrepresented",
    # #                            "disclaimer": "auditor is unable to complete an accurate audit report"}
    # # try:
    # #     for item in self.data["audit_report"]:
    # #         if
    # # except KeyError:
    # #     pass
    #
    # # Complex-Financials: 2 Types -> {"Combined Financial Statements": "shows financial results of different subsidiary companies from that of the parent company",
    # #                                 "Consolidated Financial Statements": "aggregate the financial position of a parent company and its subsidiaries"}
    # try:
    #     for item in [element.lower().strip() for element in self.data["complex_financials"]]:
    #         for i in range(0,len(item.split())):
    #             if ("combined" in item.split()[i]) and (item.split()[i-1] != "no"):
    #                 self.data["complex_financials"].append("combined financial statement")
    #             if ("consolidated" in item.split()[i]) and (item.split()[i-1] != "no"):
    #                 self.data["complex_financials"].append("consolidated financial statement")
    # except KeyError:
    #     pass


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
                item = {'label': i["itemLabel"]["value"], 'object': i["object"]["value"], 'value': i["item"]["value"],"altlabel": i["altlabel"]["value"]}
                self.results_list.append({self.output: item})
            else:
                if "altlabel" not in i.keys():
                    i.update({"altlabel": {"value": "unknown altabel"}})
                item = {'label': i["itemLabel"]["value"], 'value': i["item"]["value"], "altlabel": i["altlabel"]["value"]}
                self.results_list.append({self.output: item})

        with open("query_{}.json".format(self.output.replace("/", "")), 'w') as outfile:
            json.dump(self.results_list, outfile)

    def run_query(self):
        for item in self.input_list:
            self.output = item
            self.input = self.input_list.get(item)
            if os.path.isfile("./query_{}.json".format(self.output.replace("/", ""))) == True:
                kg_construction.fuzzy_matching(self)
            else:
                try:
                    kg_construction.query(self)
                except HTTPError:
                    time.sleep(8)
                kg_construction.fuzzy_matching(self)

        try:
            print(self.data)
        except KeyError:
            pass

    def fuzzy_matching(self):
        # link extracted values to wikidata database

        # open extracted wikidata entries from wikidata sparql query
        with open("./query_{}.json".format(self.output.replace("/", "")),"r") as json_file:
            output_file = json_file.read()
            output_file = output_file.replace("'","").replace(',}','}').replace(',]',']').replace('\n','').replace('\t','')
            self.results_list = json.loads(output_file)

        placeholder_list = list(map(itemgetter(self.output), list(filter(lambda x: self.output in x, self.results_list))))

        if placeholder_list != [] and "object" in placeholder_list[0]:
            form = "object"
        else:
            form = "label"

        def norm(item):
            return str(item).lower().replace(" ","").replace(".","")

        ## lower case and strip all whitespaces when comparing
        ## Pseudocode: Find match (key,value) pair in self.data to wikidata sparql query result
        # IF identical match: use wikidata identifier
        # ELSE: find closest match to value:
        #       (i)  if success: use wikidata identifier of closest match
        #       (ii) else: pass
        try:
            for item in [self.data[self.output][item] for item in range(0,len(self.data[self.output]))]:
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

        prefix_dict = {"rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                       "wd": "https://wikidata.org/entity/",
                       "omg-spec": "http://www.omg.org/techprocess/ab/SpecificationMetadata/",
                       "omg-lr": "https://www.omg.org/spec/LCC/Languages/LanguageRepresentation/",
                       "omg-cc": "https://www.omg.org/spec/LCC/Countries/ISO3166-1-CountryCodes/",
                       "fibo-fbc-fi-fi": "https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/",
                       "fibo-sec-sec-lst": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesListings/",
                       "fibo-fnd-org-fm": "https://spec.edmcouncil.org/fibo/ontology/FND/Organizations/FormalOrganizations/",
                       "fibo-fbc-pas-fpas": "https://spec.edmcouncil.org/fibo/ontology/FBC/ProductsAndServices/FinancialProductsAndServices/",
                       "fibo-sec-sec-id": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesIdentification/",
                       "fibo-fbc-fct-fse": "https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/FinancialServicesEntities/",
                       "fibo-fnd-agr-ctr": "https://spec.edmcouncil.org/fibo/ontology/FND/Agreements/Contracts/",
                       "fibo-fbc-fct-mkt": "https://spec.edmcouncil.org/fibo/ontology/FBC/FunctionalEntities/Markets/",
                       "fibo-be-le-lei": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LEIEntities/",
                       "fibo-be-le-lp": "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/ProfitObjective",
                       "fibo-be-oac-exec": "https://spec.edmcouncil.org/fibo/ontology/BE/OwnershipAndControl/Executives/",
                       "fibo-sec-dbt-bnd": "https://spec.edmcouncil.org/fibo/ontology/SEC/Debt/Bonds/",
                       "fibo-sec-eq-eq": "https://spec.edmcouncil.org/fibo/ontology/SEC/Equities/EquityInstruments/",
                       "fibo-sec-sec-rst": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesRestrictions/",
                       "fibo-fnd-arr-cls": "https://spec.edmcouncil.org/fibo/ontology/FND/Arrangements/ClassificationSchemes/IndustrySectorClassifier/",
                       "fibo-sec-sec-iss": "https://spec.edmcouncil.org/fibo/ontology/SEC/Securities/SecuritiesIssuance/",
                       "fibo-civ-fnd-civ": "https://spec.edmcouncil.org/fibo/ontology/CIV/Funds/CIV/",
                       "fibo-fnd-gao-obj": "https://spec.edmcouncil.org/fibo/ontology/FND/GoalsAndObjectives/Objectives/InvestmentObjective",
                       "fibo-fnd-pty-pty": "https://spec.edmcouncil.org/fibo/ontology/FND/Parties/Parties/",
                       "fibo-bp-iss-muni": "https://spec.edmcouncil.org/fibo/ontology/BP/SecuritiesIssuance/MuniIssuance/",
                       "fibo-bp-iss-ipo": "https://spec.edmcouncil.org/fibo/ontology/BP/SecuritiesIssuance/EquitiesIPOIssuance/FilingDetails",
                       "fibo-loan-typ-prod": "https://spec.edmcouncil.org/fibo/ontology/LOAN/LoanTypes/LoanProducts/"
                       }

        mapping_dict = {"accounting_standards": [{"fibo-be-le-lei": "AccountingFramework"}, {"datatype": "string"}], #
                        "audit_report": [{"wd": "Q740419"}, {"datatype": "string"}], #
                        "audited_financial_statements": [{"wd": "Q192907"}, {"datatype": "string"}], #still to do
                        "competitive_strength": [{"wd": "Q963465"}, {"datatype": "string"}], # should be fine
                        "complex_financials": [{"wd": "Q192907"}, {"datatype": "string"}],
                        "country_of_origin/headquarters": [{"fibo-fnd-org-fm": "isDomiciledIn"}, {"datatype": "string"}], #
                        "country_of_registration/incorporation": [{"fibo-fbc-fct-mkt": "operatesInCountry"}, {"datatype": "string"}], #
                        "description_of_the_business": [{"omg-spec": "hasDescription"}, {"datatype": "string"}],
                        "detailed_transaction": [{"fibo-bp-iss-ipo": "FilingDetails"}, {"datatype": "string"}], # placeholder
                        "dividend_policy": [{"fibo-sec-eq-eq": "Dividend"},{"datatype": "string"}],
                        "emphasis_of_matter": [{"wd": "Q5373973"}, {"datatype": "string"}],
                        "expected_gross_proceeds": [{"wd": "Q850210"}, {"datatype": "decimal"}], #placeholder + triples
                        "expected_net_proceeds": [{"wd": "Q850210"}, {"datatype": "decimal"}], #placeholder + triples
                        "external_auditor": [{"fibo-be-oac-exec": "Auditor"}, {"datatype": "string"}],
                        "filing_date": [{"fibo-fnd-agr-ctr": "hasExecutionDate"}, {"datatype": "date"}],
                        "financial_advisor": [{"fibo-bp-iss-muni": "hasFinancialAdvisor"}, {"datatype": "string"}],
                        "gaas": [{"fibo-be-le-lei": "AccountingFramework"}, {"datatype": "string"}],
                        "industry": [{"wd": "Q8148"}, {"datatype": "string"}],
                        "initial_price_range": [{"fibo-fbc-pas-fpas": "hasOfferingPrice"}, {"datatype": "decimal"}], #update
                        "investment_bank": [{"fibo-fbc-fct-fse": "InvestmentBank"}, {"datatype": "string"}],
                        "isin": [{"fibo-sec-sec-id": "InternationalSecuritiesIdentificationNumber"}, {"datatype": "string"}],
                        "issuer_name": [{"rdfs": "label"}, {"datatype": "string"}],
                        "key_element_strategy": [{"fibo-fnd-gao-obj": "BusinessStrategy"}, {"datatype": "string"}],
                        "key_factor_affecting_results": [{"fibo-fnd-pty-pty": "isAffectedBy"}, {"datatype": "string"}],
                        "key_line_item_income_statement": [{"wd": "Q243460"}, {"datatype": "string"}],
                        "listing_venue": [{"fibo-fbc-fct-mkt": "Exchange"}, {"datatype": "string"}],
                        "lock-up_period": [{"fibo-sec-dbt-bnd":"hasLockoutPeriod"}, {"datatype": "duration"}],
                        "non-gaap_measure": [{"wd": "Q330153"}, {"datatype": "string"}], #
                        "non-gaap_measure;_definition": [{"wd": "Q330153"}, {"datatype": "string"}], #
                        #"non-recurring_item": [{"key": ""}, {"datatype": "string"}], #string
                        #"number_of_reportable_segments": [{"key": ""}, {"datatype": "string"}],
                        "offering_costs": [{"wd": "Q185142"}, {"datatype": "decimal"}], #placeholder
                        "periods_of_audited_financial_statements": [{"wd": "Q740419"}, {"datatype": "string"}],
                       # "periods_of_pffi": [{"key": ""}, {"datatype": "date"}], #extract dates
                       # "periods_of_unaudited_financial_statements": [{"": ""}, {"datatype": "string"}],
                       # "periods_of_unaudited_interim_fs": [{"key": ""}, {"datatype": "date"}],
                        "pro_forma_financial_information": [{"wd": "Q2481549"}, {"datatype": "string"}],
                        "profit_forecast": [{"wd": "Q748250"}, {"datatype": "string"}],
                        "reasons_for_the_offering": [{"fibo-be-le-lp": "ProfitObjective"}, {"datatype": "string"}],
                        "regulation_s_applies": [{"fibo-sec-sec-rst": "RegulationS"}, {"datatype": "string"}],
                        "risk_factor": [{"wd": "Q1337875"},{"datatype": "string"}],
                        "rule_144a_applies": [{"wd": "Q7378915"}, {"datatype": "boolean"}],
                       # "shareholder_transaction": [{"key": ""}, {"datatype": "string"}],
                       # "shareholders_transaction": [{"key": ""}, {"datatype": "string"}],
                        "ticker": [{"fibo-sec-sec-id": "TickerSymbol"}, {"datatype": "string"}],
                        "transaction": [{"wd": "Q1166072"}, {"datatype": "string"}],
                      # "unaudited_financial_statements": [{"key": ""}, {"datatype": "string"}],
                      # "unaudited_interim_financial_statements": [{"key": ""}, {"datatype": "string"}],
                        "underwriters_incentive_fee": [{"fibo-fbc-fct-fse": "UnderwritingArrangement"}, {"datatype": "decimal"}],
                        "underwriting_fees": [{"fibo-fbc-fct-fse": "UnderwritingArrangement"}, {"datatype": "decimal"}],
                        "use_of_proceeds": [{"fibo-civ-fnd-civ": "InvestmentStrategy"}, {"datatype": "string"}],
                        "working_capital_statement": [{"fibo-loan-typ-prod": "WorkingCapitalPurpose"}, {"datatype": "string"}]
                        }

        # def subject():
        #     if ("isin" in self.data.keys()) and (len(self.data["isin"]) > 1):
        #         subject = triple_format(self.data["isin"][1])
        #     else:
        #         subject = "<{}#{}>".format(URIRef("http://example.org/entity/"),
        #                                    self.data["issuer_name"][0].replace(" ", ""))
        #     return subject

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

        def xsd_type(type):
            "^^<https://www.w3.org/2001/XMLSchema#{}>.".format(type)


        def object(item):
            subject = triple_subject()
            predicate = triple_predicate(item)
            object = []
            if item in mapping_dict:
                for element in [self.data[item]]:
                    for x in element:
                        if "date" in mapping_dict[item][1].values():
                            object.append("'{}'{}".format(x,xsd_type(date)))
                        elif "http" in x:
                            object.append(triple_format(x))
                        elif "decimal" in mapping_dict[item][1].values():
                            object.append("'{}'^^<http://www.w3.org/2001/XMLSchema#decimal>.".format("".join(x)))
                        elif "duration" in mapping_dict[item][1].values():
                            object.append("'{}'{}".format(x,xsd_type(duration)))
                        else:
                            object.append("'{}'^^<http://www.w3.org/2001/XMLSchema#string>.".format(x))
            return subject, predicate

        print(triple_subject(),triple_predicate("listing_venue"), object("listing_venue"))



        # # Subject
        # if ("isin" in self.data.keys()) and (len(self.data["isin"])>1):
        #     subject = "<{}>".format(self.data["isin"][1])
        # else:
        #     subject = "<{}#{}>".format(URIRef("http://example.org/entity/"),self.data["issuer_name"][0].replace(" ",""))
        # # Predicate
        # for item in self.data:
        #      for element in [self.data[item]]:
        #          for x in element:
        #              if item in mapping_dict:
        #                   for key in mapping_dict[item][0]:
        #                         predicate = "<{}{}>".format(prefix_dict[key], *mapping_dict[item][0].values())
        #                         if "date" in mapping_dict[item][1].values():
        #                             objekt = "'{}'^^<http://www.w3.org/2001/XMLSchema#date>.".format(x)
        #                         elif "http" in x:
        #                             objekt = "<{}>.".format(x)
        #                         elif "decimal" in mapping_dict[item][1].values():
        #                             objekt = "'{}'^^<http://www.w3.org/2001/XMLSchema#decimal>.".format("".join(x))
        #                         elif "duration" in mapping_dict[item][1].values():
        #                             objekt = "'{}'^^<http://www.w3.org/2001/XMLSchema#duration>.".format(x)
        #                         else:
        #                             objekt = "'{}'^^<http://www.w3.org/2001/XMLSchema#string>.".format(x)
        #

        #                   import sys
        #                   sys.stdout = open('log.nt', 'a')
        #                   #print(subject,predicate,objekt)
        # sys.stdout.close()
                          #self.data_graph.append([subject,predicate,objekt])

    def metadata(self):
            pass
        # dublin_core_namespace = {"dc": "http://purl.org/dc/elements/1.1/"}
        # for subject in self.data_graph:
        #     print(self.data_graph)
        #     #print("subject: {}".format(subject))
        # pass

    def runall(self):
        if __name__ == '__main__':
            Thread(target = self.load_parsed_pdf()).start()
            Thread(target = self.preprocessing()).start()
            Thread(target = self.run_query()).start()
            Thread(target = self.generate_triples()).start()
            #Thread(target = self.metadata()).start()


if __name__ == "__main__":
    path = "/thesis_py/data/"
    with os.scandir(path) as it:
        for entry in it:
            if entry.name.endswith(".json") and entry.is_file():
                kg_construction(entry.path).runall()
                break




