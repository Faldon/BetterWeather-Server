import sqlite3
import csv, re


def import_from_csv(path_to_file, db):
    try:
        with open(path_to_file,'r') as station_list:
            csv_reader = csv.reader(station_list, delimiter=',', quotechar='"')
            for row in csv_reader:
                id_match = re.match(r"[0-9a-zA-Z]{4,5}\s", row[0])
                if id_match:
                    s = id_match.string.split(' ')
                    print(s[0])
                    print(id_match.string)
                    # print(id_match)
                    # print(row[1])
    except FileNotFoundError:
        print("File not found")
        return False
    except IOError:
        print("IO Error")
        return False
    return True
