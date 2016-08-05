# PaytmLabs/WeblogChallenge
# Aug 4, 2016
# Author: Josh Philip
#
# The code in PaytmLabs_challenge.py is a solution to the Paytm Labs challenge written in Python
# and using Apache Spark.
# Code is run by calling:
#      bin/spark-submit <working_dir_path>/code/PaytmLabs_challenge.py  <optional params>
# The input web log data file is expected to be in /<working_dir_path>/data/ dir.
# Output files are written to /<working_dir_path>/out/ dir.
#
# A brief overview of the goals, solution and output is summarized below and in code comments.
# Full details will be discussed during the interview.
#
# Goals of Challenge:
# 1) Sessionize web log by IP
# 2) Determine Avg Session time
# 3) Determine unique URL visits per session
# 4) Find the most engaged users.
#
# Solution:
# The default session period is set to 15 min and can be set via a program parameter. There is
# also an option to have this automatically determined using a heuristic that examines how the
# percentage of sessions that have 1 (unique) URL view varies as we increase the session window.
# Details of the heuristic is commented in the getOptimalSessionWindow() function.
# Running this heuristic produces two suggested optimal window session sizes of 13 min and 34 min
# for the sample data.
#
# Current output for this program is based on sessionizing the data using a 13 min window. The
# sessionization is written to out/Sessionized_Customer_File.txt. Each line has the format:
#   <IP [[list of durations between URL calls], [list of page URLs],
                                     [list of timestamps as a python datetime representation]]>
# Each line represents one session for a customer IP and so there can be many lines with the same
# IP representing distinct sessions.
#
# The Avg Session Time for the sample data based on the 13 min window was found to be: 1.52 mins.
# The total number of sessions found were 111,954 and the sum of all session times for all users
# was 170,508.20 mins. Total number of customer IPs found were 90,544.
#
# The number of unique URL visits per session is stored in out/Unique_URL_Visits_by_Session.txt
# and has the format:
#   <'IP' '# of unique URL visits for session'>
# The file is sorted in descending order by the number of unique URL visits.
# The top 5 IPs with most URL visits for a session were:
# [[52.74.219.71, 9532], [119.81.61.166, 8016], [52.74.219.71, 5478], [106.186.23.95. 4656],
# [119.81.61.166. 3928]]
#
# The most engaged users, defined as those who have the longest duration in their session time
# when summed over all their sessions, is written to out/Customer_Session_Duration.txt. The
# file has the format:
#   <'IP' 'total session duration'>
# There is only one line for each IP and entries are sorted in descending order by duration.
# The top 10 most engaged users found with their total session duration (in mins) were:
# [('220.226.206.7', 98.34405411666665), ('52.74.219.71', 87.65345023333349),
#  ('119.81.61.166', 87.5433599333334), ('54.251.151.39', 87.27981941666657),
#  ('121.58.175.128', 83.15654343333331), ('106.186.23.95', 82.19450494999998),
#  ('125.19.44.66', 77.54476624999995), ('54.169.191.85', 77.03162288333337),
#  ('207.46.13.22', 75.55636493333333), ('180.179.213.94', 75.21950798333334)]
#
# Some additional processing was done to look at an alternate definition of user 'engagement'
# across several dimensions including total page views across sessions, total time across
# sessions, avg page views across sessions, avg time across sessions, and total number of
# sessions. These stats were written (in this order) to the file out/User_Engagement_Stats.txt.
# Many of these stats are correlated but we can pick 2 or 3 to give a better indicator of user
# engagement and then visualize this in a 2D or 3D plot. A 3D scatterplot was written to
# out/User_Engagement_Plot.pdf (using the fields avg page views, avg session time, and total
# number of sessions).
###########################################################################################################################
