import psycopg2
import datetime, time
import utils

try:
    conn = psycopg2.connect(host="10.228.1.81", database="sandbox", user="postgres", password="pass")
except:
    print("Nemuzu se pripojit!!!!")

cur = conn.cursor()
cur.execute("select id from alerts.rules where active = true")
rules_list = cur.fetchall()

for rule_id in rules_list:
    # get rules data into a dict
    metadata_list = "id, name, query_left, operand, value_right, frequency, debug"
    cur.execute("select " + metadata_list + " from alerts.rules "
                "where active = true and id = " + str(rule_id[0]))
    metadata_values = list(cur.fetchall()[0])
    metadata_list = metadata_list.split(", ")
    metadata = dict(zip(metadata_list, metadata_values))

    # calculate result
    start_time = time.process_time()
    cur.execute(metadata['query_left'])
    result_left = cur.fetchone()[0]

    # evaluate result, set status, send slack msg if requested
    if eval(str(result_left) + metadata['operand'] + str(metadata['value_right'])):
        status = 'ok'
    else:
        status = 'alerting'
        cur.execute("select ch.webhook from alerts.rules_channels rch join alerts.channels ch "
                    "on rch.channel_id = ch.id where rch.rule_id = " + str(rule_id[0]))
        webhook_urls = cur.fetchone()
        utils.send_slack_message(rule_id[0], metadata['name'], webhook_urls)

    if metadata['debug']:
        final_query = metadata['query_left'] + metadata['operand'] + str(metadata['value_right'])
    else:
        final_query = None
    end_time = time.process_time()

    # insert result to results table
    cur.execute("insert into alerts.results (rule_id, calculated_at, results, status, duration, query) "
                "VALUES(%s, %s, %s, %s, %s, %s)",
                (rule_id, datetime.datetime.now(), result_left, status, end_time - start_time, final_query))
    conn.commit()

cur.close()
conn.close()




