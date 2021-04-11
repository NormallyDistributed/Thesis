import json
import random

with open("/Users/mlcb/PycharmProjects/Thesis/thesis_py/train_dev/questions_train.json") as json_file:
    train = json.load(json_file)

with open("/Users/mlcb/PycharmProjects/Thesis/thesis_py/train_dev/questions_dev.json") as json_file:
    dev = json.load(json_file)

train_sample = random.sample(train, 1000)
dev_sample = random.sample(dev, 200)

with open('/Users/mlcb/PycharmProjects/Thesis/thesis_py/train_dev/train_sample.json', 'w') as fp:
    json.dump(train_sample, fp)

with open('/Users/mlcb/PycharmProjects/Thesis/thesis_py/train_dev/dev_sample.json', 'w') as fp:
    json.dump(dev_sample, fp)

