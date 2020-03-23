import psycopg2
import datetime
import time
import utils
import dotenv
import logging

env = "sandbox"
env_values = dotenv.dotenv_values()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

try:
    conn = psycopg2.connect(host=env_values["sandbox_host"], database=env_values["sandbox_database"],
                            user=env_values["sandbox_user"], password=env_values["sandbox_password"])
except Exception as e:
    logging.exception("Failed to connect to the database.", exc_info=e)

cur = conn.cursor()

try:
    cur.execute("select id from alerts.v_rules_to_validate_now")
    rules_list = cur.fetchall()
except Exception as e:
    logging.error("Failed to get the list of rules to be run.", exc_info=e)

for rule_id in rules_list:
    try:
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
    except Exception as e:
        logging.exception("Failed to validate rule ID " + str(rule_id[0]), exc_info=e)

cur.close()
conn.close()




