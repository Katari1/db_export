import MySQLdb as mdb
import sys
query1=[]
query2=[]
results='/tmp/vikram_test/export.csv'
#Opening File and Truncating#
filez = open(results, 'w')
filez.truncate()
filez.write("ANI\tRestricted\tCall start time\tCall end time\tDuration\tReference ID\tClaimed Type\tService Provider\tClaimed Geo\tRisk Reason\tRisk Score\tAnalyzed type\tAnalyzed geo\tCase Ref ID\tAssigned Analyst\tOpened by\tCase status\tFraud status\tCustom status\n")


#connecting to FDS
con = mdb.connect('localhost', 'root', 'H3oN9ybyd4ITNINPLbT', 'fds')
cur = con.cursor()
cur.execute("select tid from calls order by tid asc limit 100")
tids=cur.fetchall()
for tid in tids:
    cur.execute("SELECT pid FROM calls where tid = %s" % tid) 
    pid = cur.fetchone()
    cur.execute("select cid from phones where pid = %s" % pid)   
    ani = cur.fetchone()
    filez.write('%s' %ani)
    filez.write("\t")
    cur.execute("SELECT pid_restricted,start_utc,end_utc,duration_sec,ref_id FROM calls where tid = %s" % tid)
    for i in range(cur.rowcount):
        row=cur.fetchone()
        #for z in row:
        restricted = row[0]
        start_utc = row[1]
	end_utc = row[2]
        duration_sec = row[3]
        ref_id = row[4]    
        filez.write('%s\t %s\t %s\t %s\t %s\t' % (restricted, start_utc, end_utc, duration_sec, ref_id))
        #filez.write(" ")

    cur.execute("SELECT type,carrier,geo,reason,final_score FROM prs_info where tid = %s" % tid)
    for i in range(cur.rowcount):
        row=cur.fetchone()
        type1 = row[0]
        carrier = row[1]
        geo = row[2]
        reason = row[3]
        final_score = row[4]
        filez.write('%s\t %s\t %s\t %s\t %s\t' % (type1, carrier,geo,reason,final_score))
        #filez.write(" ")

    cur.execute("SELECT value from labels join windows using (window_id) join audio_files using (audio_file_id) join calls using (tid) where name = 'pd_type1' and calls.tid  = %s" % tid)
    for i in range(cur.rowcount):
        row=cur.fetchone()
        for z in row:
            filez.write('%s' %z)
            filez.write("\t")

    cur.execute("SELECT value from labels join windows using (window_id) join audio_files using (audio_file_id) join calls using (tid) where name = 'pd_geo1' and calls.tid  = %s" % tid)
    for i in range(cur.rowcount):
        row=cur.fetchone()
        for z in row:
            filez.write('%s' %z)
            filez.write("\t")
    
    cur.execute("select case_id,user_id,opened_user_id,case_status,fraud_status,custom_status from cases where tid  = %s" % tid)
    for i in range(cur.rowcount):
        row=cur.fetchone()
        case_id = row[0]
        assigned_analyst = row[1]
        opened_by = row[2]
        case_status = row[3]
        fraud_status = row[4]
        custom_status = row[5]
        filez.write('%s\t %s\t %s\t %s\t %s\t %s\t' % (case_id,assigned_analyst,opened_by,case_status,fraud_status,custom_status))
    filez.write("\n")

#Closing database connection
if con:
    con.close()
#Closing file
filez.close
