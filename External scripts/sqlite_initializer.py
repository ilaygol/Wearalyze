import json
import logging
import sqlite3
import pickle

# comments -------------------------------
# we have a problem (different number of attributes in each dictionary) in:
# dailies table
# stressDetails table
#
# ----------------------------------------

logging.basicConfig(level=logging.INFO)

connector = sqlite3.connect(":memory:")
cursor = connector.cursor()

file_name = "readings-clean.csv"

# ------------------------Attributes arrays ---------------------------
manual_upd_act_keys = ['userId', 'userAccessToken', 'summaryId', 'activityId', 'activityName', 'durationInSeconds', 'startTimeInSeconds', 'startTimeOffsetInSeconds', 'activityType', 'averageHeartRateInBeatsPerMinute', 'averageRunCadenceInStepsPerMinute', 'averageSpeedInMetersPerSecond', 'averagePaceInMinutesPerKilometer', 'activeKilocalories', 'deviceName', 'distanceInMeters', 'maxHeartRateInBeatsPerMinute', 'maxPaceInMinutesPerKilometer', 'maxRunCadenceInStepsPerMinute', 'maxSpeedInMetersPerSecond', 'manual']
act_files_keys = ['userId', 'userAccessToken', 'summaryId', 'fileType', 'callbackURL', 'startTimeInSeconds', 'activityId', 'activityName', 'manual']
sleeps_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'durationInSeconds', 'startTimeInSeconds']
pulse_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'startTimeInSeconds', 'durationInSeconds', 'startTimeOffsetInSeconds', 'timeOffsetSpo2Values', 'onDemand']
permission_keys = ['userId', 'userAccessToken', 'summaryId', 'permissions', 'changeTimeInSeconds']
# not all lines has 'averageHeartRateInBeatsPerMinute' attribute, set default to 0 (same lines has 0 steps - logic)
dailies_keys = ['userId', 'userAccessToken', 'summaryId', 'calendarDate', 'steps', 'averageHeartRateInBeatsPerMinute', 'averageStressLevel']
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


def init_manually_updated_activities_table():
    cursor.execute("""CREATE TABLE manuallyupdatedactivities (
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
        cursor.execute(
            "INSERT INTO manuallyupdatedactivities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (v_dict[manual_upd_act_keys[0]], v_dict[manual_upd_act_keys[1]], v_dict[manual_upd_act_keys[2]], v_dict[manual_upd_act_keys[3]],
             v_dict[manual_upd_act_keys[4]], v_dict[manual_upd_act_keys[5]], v_dict[manual_upd_act_keys[6]], v_dict[manual_upd_act_keys[7]],
             v_dict[manual_upd_act_keys[8]], v_dict[manual_upd_act_keys[9]], v_dict[manual_upd_act_keys[10]], v_dict[manual_upd_act_keys[11]],
             v_dict[manual_upd_act_keys[12]], v_dict[manual_upd_act_keys[13]], v_dict[manual_upd_act_keys[14]], v_dict[manual_upd_act_keys[15]],
             v_dict[manual_upd_act_keys[16]], v_dict[manual_upd_act_keys[17]], v_dict[manual_upd_act_keys[18]], v_dict[manual_upd_act_keys[19]],
             v_dict[manual_upd_act_keys[20]]))
        connector.commit()


def init_activity_files_table():
    cursor.execute("""CREATE TABLE activityfiles (
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
        cursor.execute(
            "INSERT INTO activityfiles VALUES (?,?,?,?,?,?,?,?,?)",
            (v_dict[act_files_keys[0]], v_dict[act_files_keys[1]],
             v_dict[act_files_keys[2]], v_dict[act_files_keys[3]],
             v_dict[act_files_keys[4]], v_dict[act_files_keys[5]],
             v_dict[act_files_keys[6]], pickle.dumps(v_dict[act_files_keys[7]]),
             pickle.dumps(v_dict[act_files_keys[8]])))
        connector.commit()


def init_sleeps_table():
    cursor.execute("""CREATE TABLE sleeps (
                                userId text,
                                userAccessToken text,
                                summaryId text,
                                calendarDate text,
                                durationInSeconds text,
                                startTimeInSeconds text)""")
    connector.commit()
    logging.debug("sleeps table was created successfully")


def insert_rows_to_sleeps_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cursor.execute(
            "INSERT INTO sleeps VALUES (?,?,?,?,?,?)",
            (v_dict[sleeps_keys[0]], v_dict[sleeps_keys[1]], v_dict[sleeps_keys[2]], v_dict[sleeps_keys[3]],
             v_dict[sleeps_keys[4]], v_dict[sleeps_keys[5]]))
        connector.commit()


def init_pulseox_table():
    cursor.execute("""CREATE TABLE pulseox (
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
        cursor.execute(
            "INSERT INTO pulseox VALUES (?,?,?,?,?,?,?,?,?)",
            (v_dict[pulse_keys[0]], v_dict[pulse_keys[1]],
             v_dict[pulse_keys[2]], v_dict[pulse_keys[3]],
             v_dict[pulse_keys[4]], v_dict[pulse_keys[5]],
             v_dict[pulse_keys[6]], pickle.dumps(v_dict[pulse_keys[7]]),
             v_dict[pulse_keys[8]]))
        connector.commit()


def init_user_permissions_change_table():
    cursor.execute("""CREATE TABLE userpermissionschange (
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
        cursor.execute(
            "INSERT INTO userpermissionschange VALUES (?,?,?,?,?)",
            (v_dict[permission_keys[0]], v_dict[permission_keys[1]], v_dict[permission_keys[2]],
             pickle.dumps(v_dict[permission_keys[3]]), v_dict[permission_keys[4]]))
        connector.commit()


def init_dailies_table():
    cursor.execute("""CREATE TABLE dailies (
                                       userId text,
                                       userAccessToken text,
                                       summaryId text,
                                       calendarDate text,
                                       steps text,
                                       averageHeartRateInBeatsPerMinute text,
                                       averageStressLevel text)""")
    connector.commit()
    logging.debug("dailies table was created successfully")


def insert_rows_to_dailies_table(dict_arr):
    for i in range(len(dict_arr)):
        v_dict = dict_arr[i]
        cursor.execute(
            "INSERT INTO dailies VALUES (?,?,?,?,?,?,?)",
            (v_dict.get(dailies_keys[0], '0'), v_dict.get(dailies_keys[1], '0'), v_dict.get(dailies_keys[2], '0'),
             v_dict.get(dailies_keys[3], '0'), v_dict.get(dailies_keys[4], '0'), v_dict.get(dailies_keys[5], '0'),
             v_dict.get(dailies_keys[6], '0')))
        connector.commit()


def init_user_metrics_table():
    cursor.execute("""CREATE TABLE usermetrics (
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
        cursor.execute(
            "INSERT INTO usermetrics VALUES (?,?,?,?,?,?,?)",
            (v_dict[metrics_keys[0]], v_dict[metrics_keys[1]],
             v_dict[metrics_keys[2]], v_dict[metrics_keys[3]],
             v_dict[metrics_keys[4]], v_dict.get(metrics_keys[5], "-1"),
             v_dict[metrics_keys[6]]))
        connector.commit()


def init_move_iq_activities_table():
    cursor.execute("""CREATE TABLE moveiqactivities (
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
        cursor.execute(
            "INSERT INTO moveiqactivities VALUES (?,?,?,?,?,?,?,?)",
            (v_dict[move_iq_act_keys[0]], v_dict[move_iq_act_keys[1]],
             v_dict[move_iq_act_keys[2]], v_dict[move_iq_act_keys[3]],
             v_dict[move_iq_act_keys[4]], v_dict[move_iq_act_keys[5]],
             v_dict[move_iq_act_keys[6]], v_dict[move_iq_act_keys[7]]))
        connector.commit()


def init_body_comps_table():
    cursor.execute("""CREATE TABLE bodycomps (
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
        cursor.execute(
            "INSERT INTO bodycomps VALUES (?,?,?,?,?,?)",
            (v_dict[body_comps_keys[0]], v_dict[body_comps_keys[1]],
             v_dict[body_comps_keys[2]], v_dict[body_comps_keys[3]],
             v_dict[body_comps_keys[4]], v_dict[body_comps_keys[5]]))
        connector.commit()


def init_stress_details_table():
    cursor.execute("""CREATE TABLE stressdetails (
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
        cursor.execute(
            "INSERT INTO stressdetails VALUES (?,?,?,?,?,?,?,?,?)",
            (v_dict[stress_details_keys[0]], v_dict[stress_details_keys[1]],
             v_dict[stress_details_keys[2]], v_dict[stress_details_keys[3]],
             v_dict[stress_details_keys[4]], v_dict[stress_details_keys[5]],
             v_dict[stress_details_keys[6]], pickle.dumps(v_dict[stress_details_keys[7]]),
             pickle.dumps(v_dict.get(stress_details_keys[8], dict()))))
        connector.commit()


def init_all_day_respiration_table():
    cursor.execute("""CREATE TABLE alldayrespiration (
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
        cursor.execute(
            "INSERT INTO alldayrespiration VALUES (?,?,?,?,?,?,?)",
            (v_dict[all_day_respiration_keys[0]], v_dict[all_day_respiration_keys[1]],
             v_dict[all_day_respiration_keys[2]], v_dict[all_day_respiration_keys[3]],
             v_dict[all_day_respiration_keys[4]], v_dict[all_day_respiration_keys[5]],
             pickle.dumps(v_dict[all_day_respiration_keys[6]])))
        connector.commit()


def init_activities_table():
    cursor.execute("""CREATE TABLE activities (
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
        cursor.execute(
            "INSERT INTO activities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (v_dict[activities_keys[0]], v_dict[activities_keys[1]], v_dict[activities_keys[2]], v_dict[activities_keys[3]],
             v_dict[activities_keys[4]], v_dict.get(activities_keys[5], '0'), v_dict[activities_keys[6]], v_dict[activities_keys[7]],
             v_dict[activities_keys[8]], v_dict.get(activities_keys[9], '0'), v_dict.get(activities_keys[10], '0'), v_dict[activities_keys[11]],
             v_dict.get(activities_keys[12], '0')))
        connector.commit()


def init_activity_details_table():
    cursor.execute("""CREATE TABLE activitydetails (
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
        cursor.execute(
            "INSERT INTO activitydetails VALUES (?,?,?,?,?,?,?)",
            (v_dict[activity_details_keys[0]], v_dict[activity_details_keys[1]],
             v_dict[activity_details_keys[2]], v_dict[activity_details_keys[3]],
             pickle.dumps(v_dict[activity_details_keys[4]]), pickle.dumps(v_dict[activity_details_keys[5]]),
             pickle.dumps(v_dict[activity_details_keys[6]])))
        connector.commit()


def init_epochs_table():
    cursor.execute("""CREATE TABLE epochs (
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
        cursor.execute(
            "INSERT INTO epochs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (v_dict[epochs_keys[0]], v_dict[epochs_keys[1]], v_dict[epochs_keys[2]], v_dict[epochs_keys[3]],
             v_dict[epochs_keys[4]], v_dict[epochs_keys[5]], v_dict[epochs_keys[6]], v_dict[epochs_keys[7]],
             v_dict[epochs_keys[8]], v_dict[epochs_keys[9]], v_dict[epochs_keys[10]], v_dict[epochs_keys[11]],
             v_dict[epochs_keys[12]], v_dict[epochs_keys[13]], v_dict[epochs_keys[14]]))
        connector.commit()


def init_database_tables():
    init_manually_updated_activities_table()
    init_activity_files_table()
    init_sleeps_table()
    init_pulseox_table()
    init_user_permissions_change_table()
    init_dailies_table()
    init_user_metrics_table()
    init_move_iq_activities_table()
    init_body_comps_table()
    init_stress_details_table()
    init_all_day_respiration_table()
    init_activities_table()
    init_activity_details_table()
    init_epochs_table()
    logging.info("all tables were created successfully")


def fill_database_from_file(filename: str = file_name):
    with open(filename, "r") as file:
        for line in file:
            measurement = json.loads(line)
            measurement_attribute = list(measurement.keys())[0]
            v_dict_arr = list(measurement.values())[0]
            try:
                match measurement_attribute:
                    case "manuallyUpdatedActivities":
                        insert_rows_to_updated_activities_table(v_dict_arr)
                    case "activityFiles":
                        insert_rows_to_activity_files_table(v_dict_arr)
                    case "sleeps":
                        insert_rows_to_sleeps_table(v_dict_arr)
                    case "pulseox":
                        insert_rows_to_pulseox_table(v_dict_arr)
                    case "userPermissionsChange":
                        insert_rows_to_user_permissions_change_table(v_dict_arr)
                    case "dailies":
                        insert_rows_to_dailies_table(v_dict_arr)
                    case "userMetrics":
                        insert_rows_to_user_metrics_table(v_dict_arr)
                    case "moveIQActivities":
                        insert_rows_to_move_iq_activities_table(v_dict_arr)
                    case "bodyComps":
                        insert_rows_to_body_comps_table(v_dict_arr)
                    case "stressDetails":
                        insert_rows_to_stress_details_table(v_dict_arr)
                    case "allDayRespiration":
                        insert_rows_to_all_day_respiration_table(v_dict_arr)
                    case "activities":
                        insert_rows_to_activities_table(v_dict_arr)
                    case "activityDetails":
                        insert_rows_to_activity_details_table(v_dict_arr)
                    case "epochs":
                        insert_rows_to_epochs_table(v_dict_arr)
                    case _:
                        logging.error("invalid measurement attribute")
                        continue
                logging.debug("new "+str(len(v_dict_arr))+" lines with user id " + v_dict_arr[0][
                    "userId"] + " were added to " + measurement_attribute.lower() + " table")
            except:
                logging.error("failed to insert row to "+measurement_attribute+" table, unMatching attributes")
    logging.info("database was filled with '"+filename+"' file data successfully")


def start_program():
    init_database_tables()
    fill_database_from_file()


if __name__ == "__main__":
    start_program()
