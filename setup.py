from setuptools import setup, find_packages

setup(name='thesis_py',
      version='0.0.1',
      author='Marius Bulla',
      packages=find_packages(),
      python_requires='>=3.6',
      install_requires=[
          'fuzzywuzzy',
          'isodate',
          'jinja2',
          'numpy',
          'python-Levenshtein',
          'rdflib',
          'rich',
          'scikit-learn',
          'spacy',
          'sparqlwrapper',
          'torch',
          'tqdm',
          'transformers',
      ],
)