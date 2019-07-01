import configparser
import random

# nr of rows
rows = 100

# read from config.ini
config = configparser.ConfigParser()
config.read('config.ini')
section = config.sections()[0]
col_names = config.options(section)

with open('generated.csv', 'w') as f:
    f.write(','.join(col_names) + "\n")
    for i in range(1, rows):
        line = []
        for col in col_names:
            item = config.get(section, col)
            # define as many conditions you like...

            # a large random number
            if item == "highrandom":
                line.append(str(random.randrange(1000000, 9999999)))
            # a medium random number
            if item == "medrandom":
                line.append(str(random.randrange(10000, 99999)))
            # a low random number
            if item == "lowrandom":
                line.append(str(random.randrange(100, 999)))
            # generate random choice from given set of values
            if "," in item:
                choice = random.choice(item.split(","))
                line.append(str(choice))
        f.write(','.join(line) + "\n")

f.close()
