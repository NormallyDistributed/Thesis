import rdflib.graph as g
import os

print(os.path.realpath(os.path.join(os.getcwd())))

graph = g.Graph()

graph.parse('KnowledgeBase.nt', format='ttl')

query_command = "?subject <http://www.w3.org/2000/01/rdf-schema#label> ?object"

query_result = graph.query(
    """
  SELECT * WHERE {
  """ + query_command + """ .
  }"""
)

candidate_list = list()
for row in query_result:
    candidate_list.append([str(row.asdict()["object"].toPython()), str(row.asdict()["subject"].toPython())])

print(candidate_list)

