import json
import logging
import sqlite3
import pickle
import datetime

# comments -------------------------------

# ----------------------------------------

logging.basicConfig(level=logging.DEBUG)
# ../Extras/readings.db
connector = sqlite3.connect("../Extras/readings.db")
cursor = connector.cursor()

file_name = "../Extras/readings-clean.csv"

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
# ---------------------------------------------------------------------

# ------------------------Attributes arrays ---------------------------
manual_upd_act_keys = ['userId', 'userAccessToken', 'summaryId', 'activityId', 'activityName', 'durationInSeconds', 'startTimeInSeconds', 'startTimeOffsetInSeconds', 'activityType', 'averageHeartRateInBeatsPerMinute', 'averageRunCadenceInStepsPerMinute', 'averageSpeedInMetersPerSecond', 'averagePaceInMinutesPerKilometer', 'activeKilocalories', 'deviceName', 'distanceInMeters', 'maxHeartRateInBeatsPerMinute', 'maxPaceInMinutesPerKilometer', 'maxRunCadenceInStepsPerMinute', 'maxSpeedInMetersPerSecond', 'manual']
act_files_keys = ['userId', 'userAccessToken', 'summaryId', 'fileType', 'callbackURL', 'startTimeInSeconds', 'activityId', 'activityName', 'manual']
sleeps_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'durationInSeconds', 'startTimeInSeconds', 'validation']
pulse_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'startTimeInSeconds', 'durationInSeconds', 'startTimeOffsetInSeconds', 'timeOffsetSpo2Values', 'onDemand']
permission_keys = ['userId', 'userAccessToken', 'summaryId', 'permissions', 'changeTimeInSeconds']
# not all lines has 'averageHeartRateInBeatsPerMinute' attribute, set default to 0 (same lines has 0 steps - logic)
dailies_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'durationInSeconds', 'steps', 'averageHeartRateInBeatsPerMinute', 'averageStressLevel']
metrics_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'vo2Max', 'fitnessAge', 'enhanced']
move_iq_act_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'startTimeInSeconds', 'durationInSeconds', 'activityType', 'offsetInSeconds']
body_comps_keys = ['userId', 'userAccessToken', 'summaryId', 'weightInGrams', 'measurementTimeInSeconds', 'measurementTimeOffsetInSeconds']
# not all lines has 'timeOffsetBodyBatteryValues' attribute, set default to empty dict
stress_details_keys = ['userId', 'userAccessToken', 'summaryId', 'startTimeInSeconds', 'startTimeOffsetInSeconds', 'durationInSeconds', 'calendarDate', 'timeOffsetStressLevelValues', 'timeOffsetBodyBatteryValues']
all_day_respiration_keys = ['userId', 'userAccessToken', 'summaryId', 'startTimeInSeconds', 'durationInSeconds', 'startTimeOffsetInSeconds', 'timeOffsetEpochToBreaths']
# not all lines has 'durationInSeconds' 'activeKilocalories' 'maxHeartRateInBeatsPerMinute' attributes, set default to 0
activities_keys = ['userId', 'userAccessToken', 'summaryId', 'activityId', 'activityName', 'durationInSeconds', 'startTimeInSeconds', 'startTimeOffsetInSeconds', 'activityType', 'averageHeartRateInBeatsPerMinute', 'activeKilocalories', 'deviceName', 'maxHeartRateInBeatsPerMinute']
activity_details_keys = ['userId', 'userAccessToken', 'summaryId', 'activityId', 'summary', 'samples', 'laps']
epochs_keys = ['userId', 'userAccessToken', 'summaryId', 'activityType', 'activeKilocalories', 'steps', 'distanceInMeters', 'durationInSeconds', 'activeTimeInSeconds', 'startTimeInSeconds', 'startTimeOffsetInSeconds', 'met', 'intensity', 'meanMotionIntensity', 'maxMotionIntensity']
# ---------------------------------------------------------------------


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


def init_manually_updated_activities_table():
    cursor.execute("""CREATE TABLE manuallyupdatedactivities (
                        shortUserAccessToken text,
                        userId text,
                        userAccessToken text,
                        summaryId text,
                        activityId text,
                        activityName text,
                        durationInSeconds text,
                        startTimeInSeconds text,
                        startTimeOffsetInSeconds text,
                        activityType text,
                        averageHeartRateInBeatsPerMinute text,
                        averageRunCadenceInStepsPerMinute text,
                        averageSpeedInMetersPerSecond text,
                        averagePaceInMinutesPerKilometer text,
                        activeKilocalories text,
                        deviceName text,
                        distanceInMeters text,
                        maxHeartRateInBeatsPerMinute text,
                        maxPaceInMinutesPerKilometer text,
                        maxRunCadenceInStepsPerMinute text,
                        maxSpeedInMetersPerSecond text,
                        manual text)""")
    connector.commit()
    logging.debug("manuallyupdatedactivities table was created successfully")


def insert_rows_to_updated_activities_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO manuallyupdatedactivities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[manual_upd_act_keys[1]], "999"),
                v_dict[manual_upd_act_keys[0]], v_dict[manual_upd_act_keys[1]], v_dict[manual_upd_act_keys[2]], v_dict[manual_upd_act_keys[3]],
                v_dict[manual_upd_act_keys[4]], v_dict[manual_upd_act_keys[5]], v_dict[manual_upd_act_keys[6]], v_dict[manual_upd_act_keys[7]],
                v_dict[manual_upd_act_keys[8]], v_dict[manual_upd_act_keys[9]], v_dict[manual_upd_act_keys[10]], v_dict[manual_upd_act_keys[11]],
                v_dict[manual_upd_act_keys[12]], v_dict[manual_upd_act_keys[13]], v_dict[manual_upd_act_keys[14]], v_dict[manual_upd_act_keys[15]],
                v_dict[manual_upd_act_keys[16]], v_dict[manual_upd_act_keys[17]], v_dict[manual_upd_act_keys[18]], v_dict[manual_upd_act_keys[19]],
                v_dict[manual_upd_act_keys[20]]))

        insert_new_line_to_database(cmd, 'manuallyupdatedactivities',mapping_dict.get(v_dict[manual_upd_act_keys[1]], "999"))


def init_activity_files_table():
    cursor.execute("""CREATE TABLE activityfiles (
                            shortUserAccessToken text,
                            userId text,
                            userAccessToken text,
                            summaryId text,
                            fileType text,
                            callbackURL text,
                            startTimeInSeconds text,
                            activityId text,
                            activityName text,
                            manual text)""")
    connector.commit()
    logging.debug("activityfiles table was created successfully")


def insert_rows_to_activity_files_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO activityfiles VALUES (?,?,?,?,?,?,?,?,?,?)",
            (mapping_dict.get(v_dict[act_files_keys[1]], "999"),
             v_dict[act_files_keys[0]], v_dict[act_files_keys[1]],
             v_dict[act_files_keys[2]], v_dict[act_files_keys[3]],
             v_dict[act_files_keys[4]], v_dict[act_files_keys[5]],
             v_dict[act_files_keys[6]], pickle.dumps(v_dict[act_files_keys[7]]),
             pickle.dumps(v_dict[act_files_keys[8]])))

        insert_new_line_to_database(cmd, 'activityfiles', mapping_dict.get(v_dict[act_files_keys[1]], "999"))


def init_sleeps_table():
    cursor.execute("""CREATE TABLE sleeps (
                                shortUserAccessToken text,
                                userId text,
                                userAccessToken text,
                                calendarDate text,
                                startTime text,
                                durationInHours real,
                                wakingHour text,
                                validation text,
                                startTimeInSeconds text,
                                durationInSeconds integer,
                                wakingInSeconds text)""")
    connector.commit()
    logging.debug("sleeps table was created successfully")


def insert_rows_to_sleeps_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        sleeping_date = datetime.datetime.fromtimestamp(int(v_dict[sleeps_keys[5]]))
        sleeping_duration = float("{:.2f}".format(int(v_dict[sleeps_keys[4]])/3600))
        waking_date = datetime.datetime.fromtimestamp(int(v_dict[sleeps_keys[5]]) + int(v_dict[sleeps_keys[4]]))
        cmd = ("INSERT INTO sleeps VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[sleeps_keys[1]], "999"),
                v_dict[sleeps_keys[0]], v_dict[sleeps_keys[1]], v_dict[sleeps_keys[3]],
                sleeping_date, sleeping_duration, waking_date.strftime('%H:%M'), v_dict.get(sleeps_keys[6], 'NO-VALIDATION'),
                int(v_dict[sleeps_keys[5]]), int(v_dict[sleeps_keys[4]]), str(int(v_dict[sleeps_keys[5]]) + int(v_dict[sleeps_keys[4]]))))

        insert_new_line_to_database(cmd, 'sleeps', mapping_dict.get(v_dict[sleeps_keys[1]], "999"))


def init_pulseox_table():
    cursor.execute("""CREATE TABLE pulseox (
                                   shortUserAccessToken text,
                                   userId text,
                                   userAccessToken text,
                                   summaryId text,
                                   calendarDate text,
                                   startTimeInSeconds text,
                                   durationInSeconds text,
                                   startTimeOffsetInSeconds text,
                                   timeOffsetSpo2Values text,
                                   onDemand text)""")
    connector.commit()
    logging.debug("pulseox table was created successfully")


def insert_rows_to_pulseox_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO pulseox VALUES (?,?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[pulse_keys[1]], "999"),
                v_dict[pulse_keys[0]], v_dict[pulse_keys[1]],
                v_dict[pulse_keys[2]], v_dict[pulse_keys[3]],
                v_dict[pulse_keys[4]], v_dict[pulse_keys[5]],
                v_dict[pulse_keys[6]], pickle.dumps(v_dict[pulse_keys[7]]),
                v_dict[pulse_keys[8]]))

        insert_new_line_to_database(cmd, 'pulseox', mapping_dict.get(v_dict[pulse_keys[1]], "999"))


def init_user_permissions_change_table():
    cursor.execute("""CREATE TABLE userpermissionschange (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       permissions text,
                                       changeTimeInSeconds text)""")
    connector.commit()
    logging.debug("userpermissionschange table was created successfully")


def insert_rows_to_user_permissions_change_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO userpermissionschange VALUES (?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[permission_keys[1]], "999"),
                v_dict[permission_keys[0]], v_dict[permission_keys[1]], v_dict[permission_keys[2]],
                pickle.dumps(v_dict[permission_keys[3]]), v_dict[permission_keys[4]]))

        insert_new_line_to_database(cmd, 'userpermissionschange', mapping_dict.get(v_dict[permission_keys[1]], "999"))


def init_dailies_table():
    cursor.execute("""CREATE TABLE dailies (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       calendarDate text,
                                       durationInSeconds integer,
                                       steps integer,
                                       averageHeartRateInBeatsPerMinute integer,
                                       averageStressLevel integer)""")
    connector.commit()
    logging.debug("dailies table was created successfully")


def insert_rows_to_dailies_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO dailies VALUES (?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict.get(dailies_keys[1]), "999"),
                v_dict.get(dailies_keys[0], '0'), v_dict.get(dailies_keys[1], '0'), v_dict.get(dailies_keys[3], '0'),
                int(v_dict.get(dailies_keys[4], 0)), (v_dict.get(dailies_keys[5], 0)), int(v_dict.get(dailies_keys[6], 0)),
                int(v_dict.get(dailies_keys[7], 0))))

        insert_new_line_to_database(cmd, 'dailies', mapping_dict.get(v_dict[dailies_keys[1]], "999"))


def init_user_metrics_table():
    cursor.execute("""CREATE TABLE usermetrics (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       calendarDate text,
                                       vo2Max text,
                                       fitnessAge text,
                                       enhanced text)""")
    connector.commit()
    logging.debug("usermetrics table was created successfully")


def insert_rows_to_user_metrics_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO usermetrics VALUES (?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[metrics_keys[1]], "999"),
                v_dict[metrics_keys[0]], v_dict[metrics_keys[1]],
                v_dict[metrics_keys[2]], v_dict[metrics_keys[3]],
                v_dict[metrics_keys[4]], v_dict.get(metrics_keys[5], "-1"),
                v_dict[metrics_keys[6]]))

        insert_new_line_to_database(cmd, 'usermetrics', mapping_dict.get(v_dict[metrics_keys[1]], "999"))


def init_move_iq_activities_table():
    cursor.execute("""CREATE TABLE moveiqactivities (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       calendarDate text,
                                       startTimeInSeconds text,
                                       durationInSeconds text,
                                       activityType text,
                                       offsetInSeconds text)""")
    connector.commit()
    logging.debug("moveiqactivities table was created successfully")


def insert_rows_to_move_iq_activities_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO moveiqactivities VALUES (?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[move_iq_act_keys[1]], "999"),
                v_dict[move_iq_act_keys[0]], v_dict[move_iq_act_keys[1]],
                v_dict[move_iq_act_keys[2]], v_dict[move_iq_act_keys[3]],
                v_dict[move_iq_act_keys[4]], v_dict[move_iq_act_keys[5]],
                v_dict[move_iq_act_keys[6]], v_dict[move_iq_act_keys[7]]))

        insert_new_line_to_database(cmd, 'moveiqactivities', mapping_dict.get(v_dict[move_iq_act_keys[1]], "999"))


def init_body_comps_table():
    cursor.execute("""CREATE TABLE bodycomps (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       weightInGrams text,
                                       measurementTimeInSeconds text,
                                       measurementTimeOffsetInSeconds text)""")
    connector.commit()
    logging.debug("bodycomps table was created successfully")


def insert_rows_to_body_comps_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO bodycomps VALUES (?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[body_comps_keys[1]], "999"),
                v_dict[body_comps_keys[0]], v_dict[body_comps_keys[1]],
                v_dict[body_comps_keys[2]], v_dict[body_comps_keys[3]],
                v_dict[body_comps_keys[4]], v_dict[body_comps_keys[5]]))

        insert_new_line_to_database(cmd, 'bodycomps', mapping_dict.get(v_dict[body_comps_keys[1]], "999"))


def init_stress_details_table():
    cursor.execute("""CREATE TABLE stressdetails (
                                shortUserAccessToken text,
                                userId text,
                                userAccessToken text,
                                summaryId text,
                                startTimeInSeconds text,
                                startTimeOffsetInSeconds text,
                                durationInSeconds text,
                                calendarDate text,
                                timeOffsetStressLevelValues text,
                                timeOffsetBodyBatteryValues text)""")
    connector.commit()
    logging.debug("stressdetails table was created successfully")


def insert_rows_to_stress_details_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO stressdetails VALUES (?,?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[stress_details_keys[1]], "999"),
                v_dict[stress_details_keys[0]], v_dict[stress_details_keys[1]],
                v_dict[stress_details_keys[2]], v_dict[stress_details_keys[3]],
                v_dict[stress_details_keys[4]], v_dict[stress_details_keys[5]],
                v_dict[stress_details_keys[6]], pickle.dumps(v_dict[stress_details_keys[7]]),
                pickle.dumps(v_dict.get(stress_details_keys[8], dict()))))

        insert_new_line_to_database(cmd, 'stressdetails', mapping_dict.get(v_dict[stress_details_keys[1]], "999"))


def init_all_day_respiration_table():
    cursor.execute("""CREATE TABLE alldayrespiration (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       startTimeInSeconds text,
                                       durationInSeconds text,
                                       startTimeOffsetInSeconds text,
                                       timeOffsetEpochToBreaths text)""")
    connector.commit()
    logging.debug("alldayrespiration table was created successfully")


def insert_rows_to_all_day_respiration_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO alldayrespiration VALUES (?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[all_day_respiration_keys[1]], "999"),
                v_dict[all_day_respiration_keys[0]], v_dict[all_day_respiration_keys[1]],
                v_dict[all_day_respiration_keys[2]], v_dict[all_day_respiration_keys[3]],
                v_dict[all_day_respiration_keys[4]], v_dict[all_day_respiration_keys[5]],
                pickle.dumps(v_dict[all_day_respiration_keys[6]])))

        insert_new_line_to_database(cmd, 'alldayrespiration', mapping_dict.get(v_dict[all_day_respiration_keys[1]], "999"))


def init_activities_table():
    cursor.execute("""CREATE TABLE activities (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       activityId text,
                                       activityName text,
                                       durationInSeconds text,
                                       startTimeInSeconds text,
                                       startTimeOffsetInSeconds text,
                                       activityType text,
                                       averageHeartRateInBeatsPerMinute text,
                                       activeKilocalories text,
                                       deviceName text,
                                       maxHeartRateInBeatsPerMinute text)""")
    connector.commit()
    logging.debug("activities table was created successfully")


def insert_rows_to_activities_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO activities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[activities_keys[1]], "999"),
                v_dict[activities_keys[0]], v_dict[activities_keys[1]], v_dict[activities_keys[2]], v_dict[activities_keys[3]],
                v_dict[activities_keys[4]], v_dict.get(activities_keys[5], '0'), v_dict[activities_keys[6]], v_dict[activities_keys[7]],
                v_dict[activities_keys[8]], v_dict.get(activities_keys[9], '0'), v_dict.get(activities_keys[10], '0'), v_dict[activities_keys[11]],
                v_dict.get(activities_keys[12], '0')))

        insert_new_line_to_database(cmd, 'activities', mapping_dict.get(v_dict[activities_keys[1]], "999"))


def init_activity_details_table():
    cursor.execute("""CREATE TABLE activitydetails (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       activityId text,
                                       summary text,
                                       samples text,
                                       laps text)""")
    connector.commit()
    logging.debug("activitydetails table was created successfully")


def insert_rows_to_activity_details_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO activitydetails VALUES (?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[activity_details_keys[1]], "999"),
                v_dict[activity_details_keys[0]], v_dict[activity_details_keys[1]],
                v_dict[activity_details_keys[2]], v_dict[activity_details_keys[3]],
                pickle.dumps(v_dict[activity_details_keys[4]]), pickle.dumps(v_dict[activity_details_keys[5]]),
                pickle.dumps(v_dict[activity_details_keys[6]])))

        insert_new_line_to_database(cmd, 'activitydetails', mapping_dict.get(v_dict[activity_details_keys[1]], "999"))


def init_epochs_table():
    cursor.execute("""CREATE TABLE epochs (
                                       shortUserAccessToken text,
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       activityType text,
                                       activeKilocalories text,
                                       steps text,
                                       distanceInMeters text,
                                       durationInSeconds text,
                                       activeTimeInSeconds text,
                                       startTimeInSeconds text,
                                       startTimeOffsetInSeconds text,
                                       met text,
                                       intensity text,
                                       meanMotionIntensity text,
                                       maxMotionIntensity text)""")
    connector.commit()
    logging.debug("epochs table was created successfully")


def insert_rows_to_epochs_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cmd = ("INSERT INTO epochs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
               (mapping_dict.get(v_dict[epochs_keys[1]], "999"),
                v_dict[epochs_keys[0]], v_dict[epochs_keys[1]], v_dict[epochs_keys[2]], v_dict[epochs_keys[3]],
                v_dict[epochs_keys[4]], v_dict[epochs_keys[5]], v_dict[epochs_keys[6]], v_dict[epochs_keys[7]],
                v_dict[epochs_keys[8]], v_dict[epochs_keys[9]], v_dict[epochs_keys[10]], v_dict[epochs_keys[11]],
                v_dict[epochs_keys[12]], v_dict[epochs_keys[13]], v_dict[epochs_keys[14]]))

        insert_new_line_to_database(cmd, 'epochs', mapping_dict.get(v_dict[epochs_keys[1]], "999"))


def init_database_tables():
    init_sleeps_table()
    init_dailies_table()
    #init_manually_updated_activities_table()
    #init_activity_files_table()
    #init_pulseox_table()
    #init_user_permissions_change_table()
    #init_user_metrics_table()
    #init_move_iq_activities_table()
    #init_body_comps_table()
    #init_stress_details_table()
    #init_all_day_respiration_table()
    #init_activities_table()
    #init_activity_details_table()
    #init_epochs_table()
    logging.info("all tables were created successfully")


def fill_database_from_file(filename: str = file_name):
    global reading_lines_count

    with open(filename, "r") as file:
        for line in file:
            measurement = json.loads(line)
            measurement_attribute = list(measurement.keys())[0]
            v_dict_arr = list(measurement.values())[0]
            reading_lines_count += 1
            try:
                match measurement_attribute:
                    case "sleeps":
                        insert_rows_to_sleeps_table(v_dict_arr)
                    case "dailies":
                        insert_rows_to_dailies_table(v_dict_arr)
                    case "manuallyUpdatedActivities":
                        continue
                        insert_rows_to_updated_activities_table(v_dict_arr)
                    case "activityFiles":
                        continue
                        insert_rows_to_activity_files_table(v_dict_arr)
                    case "pulseox":
                        continue
                        insert_rows_to_pulseox_table(v_dict_arr)
                    case "userPermissionsChange":
                        continue
                        insert_rows_to_user_permissions_change_table(v_dict_arr)
                    case "userMetrics":
                        continue
                        insert_rows_to_user_metrics_table(v_dict_arr)
                    case "moveIQActivities":
                        continue
                        insert_rows_to_move_iq_activities_table(v_dict_arr)
                    case "bodyComps":
                        continue
                        insert_rows_to_body_comps_table(v_dict_arr)
                    case "stressDetails":
                        continue
                        insert_rows_to_stress_details_table(v_dict_arr)
                    case "allDayRespiration":
                        continue
                        insert_rows_to_all_day_respiration_table(v_dict_arr)
                    case "activities":
                        continue
                        insert_rows_to_activities_table(v_dict_arr)
                    case "activityDetails":
                        continue
                        insert_rows_to_activity_details_table(v_dict_arr)
                    case "epochs":
                        continue
                        insert_rows_to_epochs_table(v_dict_arr)
                    case _:
                        logging.error("invalid measurement attribute")
                        continue
            except KeyError:
                reading_lines_count += 1
                logging.error("failed to insert row to "+measurement_attribute+" table, unMatching attributes. (reading count: "+str(reading_lines_count)+")")
    logging.info("database was filled with '"+filename+"' file data successfully")


def init_dailies_measurements_table():
    cursor.execute("""CREATE TABLE dailies_measurements
                      AS 
                      SELECT shortUserAccessToken AS id, calendarDate AS date, steps, averageHeartRateInBeatsPerMinute AS average_heart_rate, averageStressLevel AS average_stress_level
                      FROM dailies
                      WHERE shortUserAccessToken != '999' AND durationInSeconds = 86400
                      ORDER BY shortUserAccessToken asc, calendarDate asc""")
    connector.commit()
    logging.info("dailies_measurements table was created successfully")


def init_sleeps_measurements_table():
    cursor.execute("""CREATE TABLE sleeps_measurements
                      AS
                      SELECT shortUserAccessToken as id, calendarDate as date, startTime as sleep_start_time, max(durationInHours) as sleeping_duration, wakingHour as awake_time
                      FROM 'sleeps' 
                      WHERE validation LIKE 'ENHANCED%' 
                      GROUP BY shortUserAccessToken, calendarDate;""")
    connector.commit()
    logging.info("sleeps_measurements table was created successfully")


def create_final_table():
    cursor.execute("""CREATE TABLE results_1
                      AS
                      SELECT *
                      FROM dailies_measurements left outer join sleeps_measurements
                      USING (id, date)
                      WHERE dailies_measurements.steps > 0 and dailies_measurements.id != '999'
                      ORDER BY dailies_measurements.id asc, dailies_measurements.date asc""")
    connector.commit()
    logging.info("created final results table successfully")
    cursor.execute("""CREATE TABLE results_2
                          AS
                          SELECT *
                          FROM dailies_measurements  join sleeps_measurements
                          USING (id, date)
                          WHERE dailies_measurements.steps > 0 and dailies_measurements.id != '999'
                          ORDER BY dailies_measurements.id asc, dailies_measurements.date asc""")
    connector.commit()
    logging.info("created final results table successfully")


def create_research_tables():
    init_dailies_measurements_table()
    init_sleeps_measurements_table()
    create_final_table()


def start_program():
    init_database_tables()
    fill_database_from_file()
    create_research_tables()


if __name__ == "__main__":
    start_program()


