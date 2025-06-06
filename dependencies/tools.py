import constants
import csv
import os
from dependencies import log


class Tools:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def read_csv(self, input_csv):
        try:
            webs_list = []

            with open(input_csv, encoding='utf-8', newline='') as csvfile:
                data = csv.reader(csvfile, delimiter=';')
                try:
                    for row in data:
                        # webs_list.append(row[0])
                        webs_list.append(row)
                except:
                    pass
                webs_list.pop(0)
                return webs_list

        except Exception as e:
            self.__logger.error(f" - Error reading csv - {e}")

    def save_csv_name(self, dict, name_csv):
        # open file
        name_to_save = f'{name_csv}.csv'
        try:
            with open(name_to_save, mode='a', encoding='utf-8') as csv_file:
                headers2 = list(dict.keys())
                writer = csv.DictWriter(csv_file, fieldnames=headers2, delimiter=';', lineterminator='\n')
                # create headers
                if os.stat(name_to_save).st_size == 0:
                    writer.writeheader()

                # save data
                writer.writerow(dict)
        except Exception as e:
            pass

    def clean_country_supply(self, supply_list):
        for elem in supply_list:
            if elem[1] == 'Iran, Islamic Republic of':
                elem[1] = 'Iran (Islamic Republic of)'
            elif elem[1] == 'Viet Nam':
                elem[1] = 'Vietnam'
            elif elem[1] == 'Venezuela, Bolivarian Republic of':
                elem[1] = 'Venezuela (Bolivarian Republic of)'
            elif elem[1] == 'US':
                elem[1] = 'United States'
            elif elem[1] == 'UK':
                elem[1] = 'United Kingdom'
        return supply_list








