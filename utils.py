import requests
import json


def send_slack_message(rule_id, rule_name, webhook_urls):
    for webhook_url in webhook_urls:
        data = {
            'text': 'Rule *' + rule_name + '* failed. Query to see details: `'
                    + "select rul.id, rul.name, res.query, res.calculated_at from alerts.rules rul "
                      "join alerts.results res on rul.id = res.rule_id where res.status = \'alerting\' "
                      "and rule_id = " + str(rule_id) + ' order by calculated_at desc`'
        }
        response = requests.post(webhook_url, data=json.dumps(
            data), headers={'Content-Type': 'application/json'})
