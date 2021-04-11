from copy import deepcopy
from itertools import product
from typing import Dict, List, Tuple


def find_list_in_dict(obj: Dict, param_grid: List) -> List:
    for key in obj:
        if isinstance(obj[key], list):
            param_grid.append([val for val in obj[key]])
        elif isinstance(obj[key], dict):
            find_list_in_dict(obj[key], param_grid)
        else:
            continue
    return param_grid


def replace_list_in_dict(obj: Dict, obj_copy: Dict, comb: Tuple, counter: List) -> Tuple[Dict, List]:
    for key, key_copy in zip(obj, obj_copy):
        if isinstance(obj[key], list):
            obj_copy[key_copy] = comb[len(counter)]
            counter.append(1)
        elif isinstance(obj[key], dict):
            replace_list_in_dict(obj[key], obj_copy[key_copy], comb, counter)
        else:
            continue
    return obj_copy, counter


def split_gs_config(config_grid_search: Dict) -> List[Dict]:
    param_grid = []
    param_grid = find_list_in_dict(config_grid_search, param_grid)
    config_copy = deepcopy(config_grid_search)
    individual_configs = []
    for comb in product(*param_grid):
        counter = []
        individual_config = replace_list_in_dict(config_grid_search, config_copy, comb, counter)[0]
        individual_config = deepcopy(individual_config)
        individual_configs.append(individual_config)
    return individual_configs


def main():
    config = {'a': [[1, 1], [1, 2, 4]],
              'b': [3, 4],
              'c': {'A': 1,
                    'B': [[5, 6]],
                    'C': {'1': 5,
                          '2': ['hallo', 'test']}},
              'd': 3,
              'e': [True, False]}

    individual_configs = split_gs_config(config)
    for i in individual_configs:
        print(i)


if __name__ == '__main__':
    main()
