# PaytmLabs/WeblogChallenge
# Aug 4, 2016
# Author: Josh Philip
#
# The code below is a solution to the Paytm Labs challenge written in Python and using Apache Spark
# Code is run by calling: bin/spark-submit <working_dir_path>/code/PaytmLabs_challenge.py  <optional params>
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
# percentage of sessions that have 1 (unique) URL view varies as we increase the session window. Details of
# the heuristic is commented in the getOptimalSessionWindow() function.
# Running this heuristic produces two suggested optimal window session sizes of 13 min and 34 min for the sample data.
#
# Current output for this program is based on sessionizing the data using a 13 min window. The sessionization
# by IP is written to out/Sessionized_Customer_File.txt. Each line of the file has format:
#   <IP [[list of durations between URL calls], [list of page URLs], [list of timestamps as a python datetime representation]]>
# Each line represents one session for a customer IP and so there can be many lines with the same IP representing distinct
# sessions.
#
# The Avg Session Time for the sample data based on the 13 min window was found to be : 1.52 minutes.
# The total number of sessions found were 111,954 and the sum of all session times for all users was 170,508.20 mins.
# Total number of customer IPs found were 90,544.
#
# The number of unique URL visits per session is stored in out/Unique_URL_Visits_by_Session.txt and has the format:
#   <'IP' '# of unique URL visits for session'>
# The file is sorted in descending order by the number of unique URL visits.
# The top 5 IPs with most URL visits for a session were:
# [[52.74.219.71, 9532], [119.81.61.166, 8016], [52.74.219.71, 5478], [106.186.23.95. 4656], [119.81.61.166. 3928]]
#
# The most engaged users, defined as those who have the longest duration in their session time when summed over all
# their sessions, is written to out/Customer_Session_Duration.txt. The file has the format:
#   <'IP' 'total session duration'>
# There is only one line for each IP and entries are sorted in descending order by session duration.
# The top 10 most engaged users found with their total session duration (in mins) were:
# [('220.226.206.7', 98.34405411666665), ('52.74.219.71', 87.65345023333349),
#  ('119.81.61.166', 87.5433599333334), ('54.251.151.39', 87.27981941666657),
#  ('121.58.175.128', 83.15654343333331), ('106.186.23.95', 82.19450494999998),
#  ('125.19.44.66', 77.54476624999995), ('54.169.191.85', 77.03162288333337),
#  ('207.46.13.22', 75.55636493333333), ('180.179.213.94', 75.21950798333334)]
#
# Some additional processing was done to look at an alternate definition of user 'engagement' across several dimensions
# including total page views across sessions, total time across sessions, avg page views across sessions, avg time across
# sessions, and total number of sessions. These stats were written (in this order) to the file out/User_Engagement_Stats.txt.
# Many of these stats are correlated but we can pick 2 or 3 to give a better indicator of user engagement and then visualize
# this in a 2D or 3D plot. A 3D scatterplot was written to out/User_Engagement_Plot.pdf (using the fields avg page views,
# avg session time, and total number of sessions).
###########################################################################################################################


#!/bin/bash
import logging
import argparse
import re
import sys
import shlex
import dateutil.parser as date_parser
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from pyspark import SparkContext


log = logging.getLogger('root')

# if calculating optimal session window size heuristically, specify
# max window size to test -- tests all windows between 1 and this #
SESSION_WINDOW_MAX = 40

# indices of IP, url and timestamp fields in web log file
WEB_LOG_IP_IND = 2
WEB_LOG_URL_IND = 11
WEB_LOG_TS_IND = 0

# temp file created in local dir to show how the % of sessions with one URL
# changes in the 5 minutes before and after time indices determined as optimal
one_pg_sess_change_fl = 'one_pg_sess_changes.pdf'

# ----------------------------------------------------------------------
# Purpose: Parse the customer IP, timestamp and URL from a line of the
#   the input file
# Input: Line of web log input file
# Output: A tuple with the customer IP as first element and a list
#   as the second element consisting of a single list made up of a
#   Python datetime object and a string URL.
#   The line is skipped if there are any errors in processing
#   (e.g. error parsing the date)
def getLines(lin):

    try :
           # escape quote chars in url portion of line so can parse with shlex
           # without errors.
           # find url by doing an approximate match on 'http' keywords
        url_pattern = 'http.*HTTP'
        match = re.search(url_pattern , lin)
        if match:
            rep_str = match.group(0)
            rep_str = rep_str.replace("\'", "\\'")
            rep_str = rep_str.replace('\"', '\\"')
            lin = re.sub(url_pattern , rep_str, lin)

        dat = shlex.split(lin)
           # retrieve client IP and strip out port
        cust_id = dat[WEB_LOG_IP_IND_BC.value]
        if ':' in cust_id:
            cust_id = cust_id[:(cust_id.index(':'))]
           # retrieve timestamp and convert to python datetime
           # object - assumes time in ISO 8601 format
        ts_dat = dat[WEB_LOG_TS_IND_BC.value]
        dt = date_parser.parse(ts_dat)
           # retrieve URL field
        url_dat = dat[WEB_LOG_URL_IND_BC.value]
    except:
        print 'Error : processing line : ' + str(lin)
        return []

    return [(cust_id, [[dt, url_dat]])];



# ----------------------------------------------------------------------
# Purpose: Determine the durations between page visits for a customer. A
#   customer with only a single page visit will have a duration of zero.
# Input: Line of input with customer IP as key and a list of
#   timestamp and url pairs as values
# Output: A tuple with the customer IP as first element
#   and a list as the second element consisting of a
#   list of durations (in mins), a list of Python datetime objects,
#   and a list of string URLs for that IP.
def getPageDurations(lin):

    cid = lin[0]
    dat = lin[1]
    # create a list of duration between page visits in minutes
    # first element of list is always 0
    ldurs = [0]
    # also create a list of timestamps and list of urls with
    # first elements initialized to first ts and first url the
    # customer visited
    lts = [dat[0][0]]
    lurls = [dat[0][1]]
    # iterate over pages visited and calculate duration between their
    # timestamps
    for i, j in zip(dat[:-1], dat[1:]):
        td = j[0] - i[0]
        ldurs.append(td.total_seconds() / 60.0 ) # put duration in mins
        lurls.append(j[1])
        lts.append([j[0]])

    return (cid, [lurls, lts, ldurs])


# ----------------------------------------------------------------------
# Purpose: Vary the session window and collect stats on the number of
#   sessions and unique URLs in each session for a user's data for each
#   window.
# Input: A line of customer data with IP as key, and a list as value
#   consisting of a list of all urls visited, a list of timestamps for
#   each page visited, and a list of durations between page visits.
# Output: A tuple with customer IP as key and a list of lists as value,
#   with each list comprising stats on the number of sessions and unique
#   URLs for each session for a particular session window. There are 7
#   entries in each list with the first entry representing number of
#   sessions with 1 URL hit, 2nd entry is sessions with 2 URL hits, etc.
#   The 6th entry is sessions with 6 or more URL hits, and the 7th entry
#   is total number of sessions for the user for that window.
#
#   eg.(182.71.254.138, [[3, 0, 0, 0, 0, 0, 3], [0, 0, 1, 0, 0, 0, 1]])
#   for user 182.71.254.138, there are 2 session windows (2 lists)
#   1st session window -- 3 total sessions, with each having 1 page view
#   2nd session window -- 1 total session with 3 hits for that 1 session
#
#   Session windows are varied between 1 and SESSION_WINDOW_MAX_BC param
def getSessionsURLHits(lin):

    cid = lin[0]
    dat = lin[1]

    lall_sess_url_hits = []

    # lperiods = [0.1, 0.2]
    lperiods = range(1, SESSION_WINDOW_MAX_BC.value)
    for period in lperiods:
        lsess_urls = []
        lsession_hits = []
        for url, dur in zip(dat[0], dat[2]):
            # if duration between last page visited and current page
            # is less than session period, then include url as part
            # of current session
            if dur < period:
                lsess_urls.append(url)

            # otherwise, record number of all previous unique url entries
            # entries since last session as a distinct session, and
            # re-initialize url list with current url entry to reflect
            # a new session
            else:
                   # get unique # of urls and add to session hits list
                lsession_hits.append(len(set(lsess_urls)))
                lsess_urls= [url]

        lsession_hits.append(len(set(lsess_urls)))

         # create lpage_hits array with 7 entries
         # entries 1 to 5 are counts of sessions with # of unique urls equal
         # to the entry number, entry 6 is 6 or more urls, and entry 7 is total
         # number of sessions
        lpage_hits = [0] * 7
        lpage_hits[6] += len(lsession_hits)
        for spages in lsession_hits:
            if spages <= 5:
                lpage_hits[spages-1] += 1
            else:
                lpage_hits[5] += 1
        lall_sess_url_hits.append(lpage_hits)

    return (cid, lall_sess_url_hits)

# ----------------------------------------------------------------------
# Purpose: Collect customer page visits into separate sessions based
#   on the session period broadcast variable.
# Input: A line of customer data with IP as key, and a list as value
#   consisting of a list of all urls visited, a list of timestamps for
#   each page visited, and a list of durations between page visits.
# Output: A list with one or more elements representing different sessions
#   for that customer. Each element is a tuple with customer IP as key and
#   a list as value consisting of the list of durations in that session,
#   list of urls in the session, and list of session timestamps.
def getSessions(lin):

    cid = lin[0]
    dat = lin[1]

    lsessions = []
    lsess_durs = []
    lsess_urls = []
    lsess_ts = []
    for url, ts, dur in zip(dat[0], dat[1], dat[2]):
          # if duration between last page visted and current page
          # is less than session period, then include duration, url
          # and timestamp as part of current session
        if dur < SESSION_WINDOW_BC.value:
            lsess_durs.append(dur)
            lsess_urls.append(url)
            lsess_ts.append(ts)
          # otherwise, mark all previous entries since last session
          # as a distinct session, and re-initialize duration, ts and url
          # lists with current entries to reflect a new session (duration
          # is always init to 0)
        else:
            lsessions.append((cid, [lsess_durs, lsess_urls, lsess_ts ]))
            lsess_durs, lsess_urls, lsess_ts = [0], [url], [ts]

    lsessions.append((cid, [lsess_durs, lsess_urls, lsess_ts]))
    return lsessions

# ----------------------------------------------------------------------
# Purpose: Calculate the full duration of each session. As a side effect,
#   also calculate the sum of all session times and the total number of
#   sessions using Accumulator vars.
# Input: A line of customer session data with IP as key, and a list as
#   value consisting of a list of durations between session pages, a
#   list of urls visited, a list of timestamps for each page visited.
# Output: A tuple with a customer IP as key and the duration of the
#   session as value. As a side effect also add this data to the
#   accumulator vars representing the sum of all session times and
#   total number of sessions
def getSessionTime(lin):
    global SUM_SESSION_TIME_ACC, TOTAL_SESSIONS_ACC

    cid = lin[0]
    dat = lin[1]
    dur = sum(dat[0])
    SUM_SESSION_TIME_ACC += dur
    TOTAL_SESSIONS_ACC += 1

    return (cid, dur)

# ----------------------------------------------------------------------
# Purpose: Heuristic for determining optimal session window.
# Input: A dict of the window stats indexed by number of URL hits
#   (1,2,3,4,5,6=6+) and with the value being a list holding the percentage
#   of sessions with those hits for each of the session windows.
# Output: A list of optimal session windows given as a list of numbers
#   in minutes. Return an empty list if no good windows found.
#   Each number represents a different cohort but the first (or 2nd)
#   number is suggested as the best session window size. As a side effect
#   we also plot a line graph of how the percentage changes in the 5 mins
#   before and after these optimal points.

# General strategy and rationale for determining this heuristic:
#
# Examine how % of sessions that have 1 (unique) URL hit varies as we increase
# the session window.
# Two opposing forces at play:
# 1) As window increases, many groups of URLs for a user deemed to be separate
#   sessions in a smaller window become consolidated. The net effect is that
#   the total number of sessions decreases. By itself, this should also have
#   the effect of making the number of sessions with only one page view a larger
#   percentage. But this is opposed by 2)
# 2) As window increases, actual number of sessions with 1 page views goes down
#    (the actual percentage may go up or down). By itself this should have the
#    effect of decreasinng the percentage of 1 page sessions.
#
# Generally, # of 1 page sessions (in 2)) will decrease faster than # of total
#    sessions (in 1)) and so percentages tend to drop as window size increases.
#    However, there are some points at which 1) decreases faster than 2).
#    We deem these points to be indicative of different cohorts which suggests
#    that these are optimal points to cut off the session period. The first point
#    occurs at the time index when the percentage of 1 page sessions is greater
#    than the percentage at the previous time index.
# Note: Selecting the percentage of 1 page sessions as opposed to 2-page sessions,
#    3-page sessions, etc. is somewhat arbitrary. But results are generally
#    consistent across different number of URL hits.
def getOptimalSessionWindow(lpg_sess_stats, one_pg_sess_change_fl):
      # set the heuristic to look at sessions with one URL hit (first or zero index)
    pg_vw = 0
      # total number of cohorts to search for (ie total number of time indices to find)
    num_cohorts = 2
    lind = []
    num_ind = 0
      # find all time indices when percentage increases from the previous time index
      # up to the number of cohorts specified.
      # also plot these percentages in the vicinity of these time indices
    for k in range(len(lpg_sess_stats[pg_vw]) - 1):
        if lpg_sess_stats[pg_vw][k + 1] > lpg_sess_stats[pg_vw][k]:
            num_ind += 1
            lind.append(k)
            plt.plot(map(lambda x: x - 5, range(len(lpg_sess_stats[pg_vw][k - 5:k + 6]))), \
                     lpg_sess_stats[pg_vw][k - 5:k + 6], label=str(k) + ' Min Session Period')
            if num_ind >= num_cohorts:
                break
    if len(lind) > 0:
        plt.legend()
        plt.xlabel('5 Min Before and After Optimal Time Index')
        plt.ylabel('Percentage of Sessions')
        plt.title('Plot of Session Percentage by Period Index For ' + str(pg_vw + 1) + ' URL(s) in Session')
        plt.savefig(one_pg_sess_change_fl, bbox_inches='tight')
        plt.close()
    return lind

# ----------------------------------------------------------------------
# Purpose: Collect stats on how many X-page sessions there are for each
#   of the different session window sizes.
# Input: A list of lists with the outer list representing different
#   window sizes and the inner list holding 7 numbers. The first 6 numbers
#   are the number of sessions holding 1, 2, 3, 4, 5, and 6+ URLs
#   respectively, and the 7th entry holds the total number of sessions for
#   that window size.
# Output: Return a dict of the window stats indexed by number of URL hits
#   (1,2,3,4,5,6=6+) and with the value being a list holding the
#   percentage of sessions with those hits for each of the session windows.
def getSessionWindowStats(lsess_window_stats):

    page_nums = 6
    lpg_sess_stats = {}
    for j in range(page_nums):
        lpg_sess_stats[j] = [(x[j] * 100.0) / x[6] for x in lsess_window_stats]
    return lpg_sess_stats

# ----------------------------------------------------------------------
# Purpose: Create a stacked bar chart of the percentage of sessions with
#   X unique URLs for different window sizes. X is 1, 2, 3, 4, 5, and 6+
#   URLs.
# Input: A dict of the window stats indexed by number of URL hits
#   (1,2,3,4,5,6=6+) and with the value being a list holding the
#   percentage of sessions with those hits for each of the session windows.
#   Also input file name to save bar chart to.
# Output: A stacked bar chart for each session window saved to the pdf file.
def plotSessionWindowStats(lpg_sess_stats, pdf_file):

    lsess_window_len = len(lpg_sess_stats.values()[0])
    x_vals = range(1, lsess_window_len + 1)
    page_nums = 6
    llabels = ['1 URL in Session', '2 URLs in Session', '3 URLs in Session', \
               '4 URLs in Session', '5 URLs in Session', '6+ URLs in Session']

    width = 0.35  # width of bars
    sum_bott = [0] * lsess_window_len
    lcolors = ['b', 'r', 'g', 'y', 'k', 'c']
    for k in range(page_nums):
        plt.bar(x_vals, lpg_sess_stats[k], width, color=lcolors[k], bottom=sum_bott, label=llabels[k])
        sum_bott = [a + b for a, b in zip(sum_bott, lpg_sess_stats[k])]

    plt.legend()
    plt.xlabel('Session Period (mins)')
    plt.ylabel('Percentage of Sessions')
    plt.title('Plot of Page View Percentage by Session Period')
    plt.savefig(pdf_file, bbox_inches='tight')
    plt.close()
    return

# ----------------------------------------------------------------------
# Purpose: Create a 2d scatterplot of passed in params and save to file
# Input: Two lists of data to plot along with labels and plot title,
#   and a file to save to in pdf format.
# Output: A scatterplot is created and saved to the file in pdf format.
def plot2d(pdf_file, lx_vals, ly_vals, x_label, y_label, title):
    plt.plot(lx_vals, ly_vals, 'ro')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.savefig(pdf_file, bbox_inches='tight')
    plt.close()

# ----------------------------------------------------------------------
# Purpose: Create a 3d scatterplot of passed in params and save to file
# Input: Three lists of data to plot along with labels and plot title
#   and a file to save to in pdf format.
# Output: A scatterplot is created and saved to the file in pdf format.
def plot3d(pdf_file, lx_vals, ly_vals, lz_vals, x_label, y_label, z_label, title):

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d') #
    ax.scatter(lx_vals, ly_vals, lz_vals, c='r', marker='o')
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_zlabel(z_label)
    ax.set_xlim([0, max(lx_vals)])
    ax.set_ylim([0, max(ly_vals) ])
    plt.title(title)
    plt.savefig(pdf_file, bbox_inches='tight')
    plt.close()


# ----------------------------------------------------------------------
# Purpose: Dump the contents of a list of tuples to a text file
# Input: Filename to save to local dir and a list of tuples of data
#   likely collected from a RDD. Tuple is of the form (key, value)
# Output: Each key and value in the list of tuples, is dumped to a
#   line in a text file.
def outputLocal(filname, lst):
    f = open(filname,'w')
    for ele in lst:
        f.write(ele[0] + " " + str(ele[1]) + "\n")
    f.close()



#------------------------------------------------------------------------


if __name__ == '__main__':

    # All params are optional however the working dir path param should be set as appropriate
    # Expected input file is web log data file and is defaulted to '2015_07_22_mktplace_shop_web_log_sample.log' in
    # '<working_dir_path>/data/' directory.
    # Output generates 4 text files in <working_dir_path>/out/ directory showing different stats on different
    # transformations of the data. A couple plots are also generated in this dir to better visualize the data.
    # Also outputs several stats to std output.

    # e.g. run as "bin/spark-submit PaytmLabs_challenge.py --infile '2015_07_22_mktplace_shop_web_log_sample.log' \
    #                                     --working_dir_path '/home/jphilip/PyCharms/Projects/Proj1/' \
    #                                     --session_period 15"

    parser = argparse.ArgumentParser(description='Determine web log statistics for client IPs.');
    parser.add_argument('--infile', default='/2015_07_22_mktplace_shop_web_log_sample.log', dest='infile', type=str, help='Web log input file to parse.')
    parser.add_argument('--working_dir_path', default='/home/jphilip/PyCharms/Projects/Proj1/', dest='working_dir_path', type=str, help='Full path to the dir where data, code and output reside')
    parser.add_argument('--session_period', dest='session_period', type=int, default=15, help='Time of inactivity in Minutes before another Session is deemed to begin.')
    parser.add_argument('--bCalculate_session_window', action='store_true', default=True, help='Use heuristic to determine best session window. Overrides session_period param if set.')
      # params for names of several of the output files generated during processing
    parser.add_argument('--session_window_bar_chart_file', dest='session_window_bar_chart_file', type=str, default='Session_win_bar_chart.pdf', help='File name of bar chart file when calculating optimal session window. Option bCalculate_session_window must be true.')
    parser.add_argument('--sessionized_cust_file', dest='sessionized_cust_file', type=str, default='Sessionized_Customer_File.txt', help='Web file sessionized by customer according to session period and showing URLs, timestamps and page durations.')
    parser.add_argument('--unique_url_visits_file', dest='unique_url_visits_file', type=str, default='Unique_URL_Visits_by_Session.txt', help='File giving the number of unique URL visits by session sorted in descending order.')
    parser.add_argument('--cust_session_duration_file', dest='cust_session_duration_file', type=str, default='Customer_Session_Duration.txt', help='File showing all customer sessions sorted by session duration.')
    parser.add_argument('--user_engagement_stats_file', dest='user_engagement_stats_file', type=str, default='User_Engagement_Stats.txt',help='Name of file to save several stats around user engagement')
    parser.add_argument('--user_engagement_plot_file', dest='user_engagement_plot_file', type=str, default='User_Engagement_Plot.pdf', help='Name of file to save a 3d plot showing several dimensions of user engagement')

    args = parser.parse_args()

    log.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.addFilter(logging.Filter(name='root'))
    ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    log.addHandler(ch)

    log.info('Starting...')
    sc = SparkContext("local[6]", "AAA")

    DATA_DIR = args.working_dir_path + 'data/'
    OUT_DIR = args.working_dir_path + 'out/'

    SESSION_WINDOW_MAX_BC = sc.broadcast(SESSION_WINDOW_MAX)   # Broadcast var to store max session window to test
    WEB_LOG_IP_IND_BC = sc.broadcast(WEB_LOG_IP_IND)      # Broadcast var to store index of IP field
    WEB_LOG_URL_IND_BC = sc.broadcast(WEB_LOG_URL_IND)    # Broadcast var to store index of URL field
    WEB_LOG_TS_IND_BC = sc.broadcast(WEB_LOG_TS_IND)      # Broadcast var to store index of timestamp field

    SUM_SESSION_TIME_ACC = sc.accumulator(0)     #  Accumulator to store sum of all session times
    TOTAL_SESSIONS_ACC = sc.accumulator(0)       #  Accumulator to store total number of sessions

    # parse input file, and collect IP, date and URL, then aggregate by customer IP, and calculate
    # durations between page visits for each customer
    log.info('Reading Input file...')
    d1_lines_RDD = sc.textFile(DATA_DIR + args.infile)
    log.info('Parsing IP, date and URL from Input...')
    d2_date_url_RDD = d1_lines_RDD.flatMap(getLines)
    log.info('Aggregating dates and URLs by customer IP...')
    d3_agg_date_url_RDD = d2_date_url_RDD.reduceByKey(lambda a,b: a+b)
    log.info('Sorting each customer line by date...')
    d4_sort_agg_date_url_RDD = d3_agg_date_url_RDD.map(lambda (cid, dat): (cid, sorted(dat, key=lambda x: x[0]) ))
    log.info('Calculating times between page visits for each customer...')
    d5_cust_duration_RDD = d4_sort_agg_date_url_RDD.map(getPageDurations)
    d5_cust_duration_RDD.persist()

    # session window size is determined from session_period param or is overriden by the calculated
    # heuristic of optimal window size if the bCalculate_session_window param is set
    session_window = args.session_period

    if args.bCalculate_session_window:
        log.info('Calculating heuristic to determine optimal session window.')
         # get stats for each user on unique URL hits / sesssion for different session windows
        d6_url_session_RDD = d5_cust_duration_RDD.map(getSessionsURLHits)

         # add up all user stats for unique URLs for each window size
         # note: would normally use Numpy here for vector addition, but problem with Spark+Numpy integration
        lsess_window_stats = d6_url_session_RDD.map(lambda (a, b): b).reduce(lambda a, b: [map(sum, zip(x, y)) for x, y in zip(a, b)])
         # collect session stats as a dict with key being number of page hits and value being a percentage count of all
         # sessions for that key for each of the different session window sizes
        lpg_sess_stats = getSessionWindowStats(lsess_window_stats)
         # save a bar chart showing distribution of session page hits across different window sizes
        plotSessionWindowStats(lpg_sess_stats, OUT_DIR + args.session_window_bar_chart_file)
         # determine heuristic for optimal window session size based on stats (none may be found)
        lind = getOptimalSessionWindow(lpg_sess_stats, OUT_DIR + one_pg_sess_change_fl)
        if len(lind) > 0:
            log.info('Optimal session period is determined to be ' + str(lind[0]+1) + ' mins.')
            session_window = lind[0] + 1


    SESSION_WINDOW_BC = sc.broadcast(session_window)  # Broadcast var to store session period param

     # sessionize data and write to local file
    log.info('Sessionizing the data based on session period...')
    d7_sessionized_RDD = d5_cust_duration_RDD.flatMap(getSessions)
    d7_sessionized_RDD.persist()  # store results of rdd so not have to recalculate
    lSessionData = d7_sessionized_RDD.collect()
    outputLocal(OUT_DIR + args.sessionized_cust_file, lSessionData)


    log.info('Calculating full duration of each session...')
       # as a side effect this method also calculates total session time and total # of sessions
       # and stores in accumulator vars
    d8_session_duration_RDD = d7_sessionized_RDD.map(getSessionTime)
    lSessionDuration = d8_session_duration_RDD.collect()

        # store accumulator vars (before it is further modified)
    sum_session_time = SUM_SESSION_TIME_ACC.value
    total_num_sessions = TOTAL_SESSIONS_ACC.value

    ## print to stdout the avg session time
    print '\n\nSum of All Session Times (mins): ', str(sum_session_time)
    print 'Total Number of Sessions: ', str(total_num_sessions)
    print 'Avg Session Time (mins): ', str(sum_session_time / (0.0 + total_num_sessions)), '\n'

    # Get number of unique page visits per session and sort them in descending order
    log.info('Calculating number of page hits for each user session...')
    d8b_page_hits_RDD = d7_sessionized_RDD.map(lambda (cid, dat): (cid, len(set(dat[1]))) )
    d8b_page_hits_RDD.persist()     # persist -- will use later to get total hits across sessions
    d8c_sort_page_hits = d8b_page_hits_RDD.sortBy(lambda (cid, session_hits): -session_hits)
    lunique_url_visits = d8c_sort_page_hits.collect()
    print 'lunique url : ' + str(lunique_url_visits)
    outputLocal(OUT_DIR + args.unique_url_visits_file, lunique_url_visits)

    # Now add up session times by user and sort results in descending order
    log.info('Calculating total duration for each user across sessions...')
    d9_total_cust_session_RDD = d8_session_duration_RDD.reduceByKey(lambda a, b: a + b)
    d10_sorted_total_cust_session = d9_total_cust_session_RDD.sortBy(lambda (cid, dur): -dur)
    d10_sorted_total_cust_session.persist()     # persist -- will use later to merge with total page hits
    lTotalSessionDuration = d10_sorted_total_cust_session.collect()
    outputLocal(OUT_DIR + args.cust_session_duration_file, lTotalSessionDuration)

    # print to stdout the top 15 most engaged users with their full session time across sessions
    topIPs = lTotalSessionDuration[0:14]
    print '\n\nTop 15 Most Engaged users by total duration from all their sessions (mins): ' + str(topIPs)


    #-----------------------------------------------------------------------------------------------------
    # As an alternate definition of an 'engaged' user, we collect stats for total time across sessions, avg time
    # across sessions, total page hits across sessions, avg page hits across sessions, and total number of sessions
    # for each user in a single file
    # Many of these stats are correlated but we can pick 2 or more to give a better indicator of user engagement

    # Get total page hits across sessions and total number of sessions per user.
    log.info('Calculating total page hits across sessions and total number of sessions per user...')
    d11_page_sessions_RDD = d8b_page_hits_RDD.map(lambda (cid, page_hit): (cid, [page_hit, 1.0]))
    d12_total_page_sessions_RDD = d11_page_sessions_RDD.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])

    log.info('Joining time across sessions with total page hits per user...')
    d13_engagement_RDD = d12_total_page_sessions_RDD.join(d10_sorted_total_cust_session)
    d14_total_engagement_RDD = d13_engagement_RDD.map(lambda (cid, dat): (cid, [dat[0][0], dat[1], dat[0][0]/dat[0][1],\
                                                                  dat[1]/dat[0][1], dat[0][1] ]))
    lTotalEngagementStats = d14_total_engagement_RDD.collect()
    outputLocal(OUT_DIR + args.user_engagement_stats_file, lTotalEngagementStats)

    # pick 3 stats (avg URL hits, avg session time, # of sessions) and run a 3D scatterplot
    lavg_page_hits = [x[1][2] for x in lTotalEngagementStats]
    lavg_session_time = [x[1][3] for x in lTotalEngagementStats]
    lnum_sessions = [x[1][4] for x in lTotalEngagementStats]

    x_label = 'Avg Session Time (Mins)'
    y_label = 'Avg Number of Page Hits'
    z_label = 'Number of Sessions'
    title = 'Plot of User Engagement'
    plot3d(OUT_DIR + args.user_engagement_plot_file, lavg_session_time, lavg_page_hits, lnum_sessions, x_label, y_label, z_label, title)



