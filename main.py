import sqlite3

connector = sqlite3.connect("Extras/readings.db")
cursor = connector.cursor()

# cursor.execute("""create view resultsview
#                   as
#                   select distinct *
#                       from
#                         (
#                                 (select shortUserAccessToken, calendarDate, durationInSeconds, startTimeInSeconds
#                                 from sleeps)
#                             join
#                                 (select shortUserAccessToken, steps, averageHeartRateInBeatsPerMinute, averageStressLevel
#                                 from dailies
#                                 where steps > 0)
#                             using(shortUserAccessToken)
#                         )
#                         where shortUserAccessToken!='999'
#                         order by shortUserAccessToken asc, calendarDate asc
#                          """)
# connector.commit()
#
# cursor.execute("""create table resulttable
#                   as select * from resultsview""")
# connector.commit()

# cursor.execute("""drop view resultsview""")
# cursor.execute("""drop table resulttable""")

cursor.execute("""create view resultsview
                  as select distinct dailies.shortUserAccessToken, dailies.calendarDate, dailies.averageHeartRateInBeatsPerMinute, dailies.steps, dailies.averageStressLevel, sleeps.startTimeInSeconds, sleeps.durationInSeconds
                  from dailies join sleeps
                  using(shortUserAccessToken, calendarDate)
                  where dailies.steps > 0 and dailies.shortUserAccessToken != '999'
                  order by dailies.shortUserAccessToken asc, dailies.calendarDate asc""")
connector.commit()

cursor.execute("""create table resulttable
                   as select * from resultsview""")

connector.commit()

cursor.execute("""drop view resultsview""")