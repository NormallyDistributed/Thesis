import os
path = os.path.realpath(os.path.join(os.getcwd(), "output"))

with open('KnowledgeBase.nt', 'w') as outfile:
    with os.scandir(path) as it:
        for entry in it:
            if entry.name.endswith(".nt") and entry.is_file():
                    with open(entry) as infile:
                        outfile.write(infile.read())