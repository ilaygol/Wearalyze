import json
import logging
import sqlite3
import pickle
import datetime

logging.basicConfig(level=logging.DEBUG)
# ../Extras/readings.db
connector = sqlite3.connect(":memory:")
cursor = connector.cursor()
file_path = "../Extras/readings-debug.csv"

reading_lines_count = 1
all_cmd = set()

# ------userId to shortUserId dictionary (for mapping) ----------------
mapping_dict = {'6ad70fdf-64d6-45aa-a138-db324bbc0412': '101', '6214ab65-2483-41e6-889f-82809290cb1e': '102', '11e75103-409e-467d-8204-4a280e14b19b': '105',
                '5285e697-2756-41ef-bca5-baf7d0ac6482': '107', '41ba6a74-69bd-406d-8e60-be8327399212': '108', '88349677-cea8-42ae-b5ff-5ff8766ce065': '109',
                'c961a5fc-345c-43ee-b92e-de501684e24e': '110', 'f524b07a-4494-45e2-9862-e5d36c82c997': '111', '176c1aa0-04a0-437a-9416-6063b8ab121f': '112',
                '755dd5c2-565e-4287-9667-97ff7e492935': '113', 'e545908c-31bb-4def-b82e-75046939e8d1': '115', 'fd3abdc2-54b5-4d2b-b388-578fb171fb78': '116',
                '9d221a6e-3c43-4a13-8996-1c0f4c4c2c03': '117', '5010e504-b107-4dfe-8b12-3b11c4da496d': '118', 'a0f2faa9-1074-4fa9-860c-553c88ce172d': '119',
                'cc46738e-7c17-4329-8999-8fe41fe2d766': '121', 'a001a7d8-ffdc-43bb-a508-9e474be51842': '122', 'c506b332-33fc-44ab-a407-9531b4b4052a': '123',
                'c5fc9ab7-a49d-4c00-8a62-92c00d9c575a': '124', '1ac37cdc-8e5e-454d-9598-8f7cc8e0be3a': '125', '1fdff219-dc8c-40ba-b7f8-c39929675405': '126',
                '30fd9157-9146-42e1-baeb-cf090b3c7159': '127', 'dde4f9e1-6ac3-4ca1-8310-f203167a71ff': '129', 'b35ac630-2ce2-45c9-9a74-e1324149a98b': '130',
                '7e67d313-23a6-483a-a230-a337d79b57b1': '131', '51c2755a-6759-4f63-a183-c8dd6a54a5e9': '201', 'ea3e2345-fe31-423c-9ffc-d4b5f12dfd7e': '202',
                '8b8635ee-b016-4239-a577-da9bfb50a27f': '203', 'ebcb7483-562c-4707-9d5a-bc7d54a67d6b': '204', 'bf1d4486-7034-4881-aaaf-eb625df5fc93': '205',
                '4842e17a-f7c1-4c9d-b57e-c90793ec5abb': '206', '597f09cf-421b-480d-b582-5fef55bc7af3': '207', '20d00c09-50f3-4398-b717-ffb8e7d8cd50': '208',
                'cfd2c79f-68bb-4b38-a976-562cdc98dcac': '209', '7638e6ba-7c98-4979-8dff-71791dabe2b3': '210', '9017dd49-b0ef-44bb-9d21-0f78a6a8aaa4': '211',
                '70794891-11ab-4ecb-9b22-611c0c05e653': '213', '8c353ee8-9b7a-4377-bbbf-617097a1e9e0': '214', '8b7dd2f1-cd34-4179-8ff3-8711dd25985f': '215',
                'b7aef7b9-1787-46a6-a5be-8dd17a5d8f14': '216', 'db310af4-c94b-4a0a-8527-75a008e2ee4a': '217', 'c060bc68-dd6e-4516-b832-9318ea40c6a3': '218',
                '6ac442c8-bcf8-48b0-962e-57698915c0b0': '219', 'a82596c1-3e58-4cbb-83bd-4f46f80b1072': '220', '4bbeada6-7b60-4510-ba3b-da06386eaaaf': '221',
                'fef51300-4513-4890-8b87-4fd3f18978c8': '222', 'cd391202-e65b-4188-bc69-80f0a866cf7c': '223', '541b548b-2775-468b-b84b-1f85f2f2616c': '224',
                '28cd3b94-db72-43ba-89dc-e6aa5d4d0718': '225', '04dc12ee-fcb6-4738-b0bc-747bcbcbc290': '227', '60c5c7f3-4820-42e9-b823-1b2afd600ece': '228',
                'b61a19ce-d7ea-4f85-9e75-1be73f2a6dd3': '229'}
# -------------------------------------------------------------------------


def insert_new_line_to_database(input_cmd, table, user_id):
    if input_cmd not in all_cmd:
        cursor.execute(*input_cmd)
        connector.commit()
        all_cmd.add(input_cmd)
        logging.debug("new line with user id " + user_id + " was added to " + table + " table. (reading count: " + str(
            reading_lines_count) + ")")
    else:
        logging.error("inserting line with user id " + user_id + " to " + table + " table failed, since it was a duplicate of previous line. (reading count: " + str(
            reading_lines_count) + ")")


def add_graphs_data_to_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        if len(v_dict.get('timeOffsetHeartRateSamples')) != 0:
            user_id = mapping_dict.get(v_dict.get('userAccessToken'), "999")
            hours_keys_samples_dict = get_hours_samples_dictionary(v_dict.get('timeOffsetHeartRateSamples'))

            cmd = ("INSERT INTO graphs_data VALUES (?,?,?,?)",
                   (user_id, v_dict.get('calendarDate', '0'), 'HeartRateSamples', pickle.dumps(hours_keys_samples_dict)))

            insert_new_line_to_database(cmd, 'graphs_data', user_id)


def get_hours_samples_dictionary(seconds_keys_samples_dict):
    hours_keys_samples_dict = dict()
    for key, value in seconds_keys_samples_dict.items():
        new_key = int(key)/3600
        hours_keys_samples_dict[new_key] = int(value)

    return hours_keys_samples_dict


def fill_table_from_csv(csv_file_path=file_path):
    global reading_lines_count

    with open(file_path, "r") as file:
        for line in file:
            measurement = json.loads(line)
            measurement_attribute = list(measurement.keys())[0]
            v_dict_arr = list(measurement.values())[0]
            reading_lines_count += 1
            try:
                match measurement_attribute:
                    case "dailies":
                        add_graphs_data_to_table(v_dict_arr)
            except KeyError:
                reading_lines_count += 1
                logging.error(
                    "failed to insert row to " + measurement_attribute + " table, unMatching attributes. (reading count: " + str(
                        reading_lines_count) + ")")
    logging.info("database was filled with '" + file_path + "' file data successfully")


def init_graphs_data_table():
    cursor.execute("""CREATE TABLE graphs_data (
                                    userId text,
                                    calendarDate text,
                                    graphType text,
                                    serializedData text)""")
    connector.commit()
    logging.debug("graphs_data table was created successfully")


def start_program():
    init_graphs_data_table()
    fill_table_from_csv(csv_file_path=file_path)

    cursor.execute("SELECT serializedData FROM graphs_data")


if __name__ == "__main__":
    start_program()
