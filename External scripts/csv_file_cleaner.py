import json

good_lines_counter = 0
bad_lines_counter = 0
readings_file = open("../Extras/readings.csv", "r", encoding="utf8")
new_file = open("../Extras/readings-clean.csv", "a")

for line in readings_file:
    try:
        json.loads(line)
        new_file.write(line)
        good_lines_counter += 1
    except:
        bad_lines_counter += 1

readings_file.close()
new_file.close()
print("good lines: ", good_lines_counter)
print("bad lines: ", bad_lines_counter)
