import csv, time, random, os
def check_lines():
    r = random.seed(time.time())
    towrite = []
    with open('labeled.csv', 'rb') as f:
        reader = csv.reader(f)
        header = True
        with open('/Users/kple/IdeaProjects/MapReduceFinalProject/randomchosen.csv', 'wb') as rst:
            writer = csv.writer(rst)
            cols_to_write = filter(lambda i: i not in [953, 1016, 954, 1017], range(1657))
            for row in reader:
                if header:
                    content = [row[i] for i in cols_to_write]
                    writer.writerow(content)
                    header = False
                else:
                    s = random.random()
                    if s < 10**-4:
                        content = [row[i] for i in cols_to_write]
                        writer.writerow(content)

    # with open('randomchosen.csv', 'wb') as f:
    #     writer = csv.writer(f)
    #     writer.writerows(towrite)


def read_file(name):
    counter = 0
    with open(name, 'rb') as f:
        reader = csv.reader(f)

        try:
            for row in reader:
                print e[26]
                # counter += 1
                # s = random.random()
                # if s < 10**-2:
                #     print s
                    # print row
        except csv.Error as e:
            print e

    print counter



def print_first_row():
    f = open("/Users/kple/IdeaProjects/MapReduceFinalProject/randomchosen.csv", 'r')
    line1 = f.readline()
    line2 = f.readline()


    rst = map(lambda i: i.split(','), [line1, line2])
    # for k in zip(map(lambda i: i.split(','), [line1, line2])):
    # 	print k
    skip = [1, 2, 3, 4, 9, 10, 11, 12, 16, 1015] + range(20, 27) + range(28, 964)
    cate = range(962, 967) + range(1016, 1081)

    rstoutput = []
    for i, k in enumerate(rst[0]):
        # print k,
        j = i + 1
        if j in skip:
            rstoutput += k + ', SKIP',
        elif j in cate:
            rstoutput += k + ', CATE',
        elif j == 27:
            rstoutput += k + ', LABEL',
        else:
            rstoutput += k + ', NUM',

    output = open("/Users/kple/IdeaProjects/MapReduceFinalProject/data_type.txt", 'wb')
    for r in rstoutput:
        output.write(r + '\n')
    output.write('')
    output.close()

check_lines()
# read_file('randomchosen.csv')
print_first_row()