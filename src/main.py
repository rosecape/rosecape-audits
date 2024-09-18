import os
import requests
from datetime import datetime, timedelta
import pytz
from croniter import croniter

# Airflow API settings
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL', None)
AIRFLOW_API_URL = os.environ.get('AIRFLOW_API_URL', 'http://airflow-webserver.airflow.svc.cluster.local:8080/api/v1')
AIRFLOW_USERNAME = os.environ.get('AIRFLOW_API_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.environ.get('AIRFLOW_API_PASSWORD', 'admin')

# Buffer time for checking runs
BUFFER_TIME = timedelta(hours=1)

def get_all_dags():
    endpoint = f"{AIRFLOW_API_URL}/dags"
    response = requests.get(
        endpoint,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
        params={"limit": 100}  # Adjust if you have more than 100 DAGs
    )
    response.raise_for_status()
    return [dag for dag in response.json()["dags"] if not dag["is_paused"]]

def get_dag_details(dag_id):
    endpoint = f"{AIRFLOW_API_URL}/dags/{dag_id}"
    response = requests.get(
        endpoint,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    )
    response.raise_for_status()
    return response.json()

def get_latest_dag_run(dag_id):
    endpoint = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
    response = requests.get(
        endpoint,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
        params={"limit": 1, "order_by": "-execution_date"}
    )
    response.raise_for_status()
    dag_runs = response.json()["dag_runs"]
    return dag_runs[0] if dag_runs else None

def parse_schedule_interval(schedule_interval):
    if isinstance(schedule_interval, dict):
        if schedule_interval.get('__type') == 'TimeDelta':
            return timedelta(
                days=schedule_interval.get('days', 0),
                seconds=schedule_interval.get('seconds', 0),
                microseconds=schedule_interval.get('microseconds', 0)
            )
        elif 'value' in schedule_interval:
            return schedule_interval['value']
    return schedule_interval

def get_next_scheduled_run(last_run_date, schedule_interval):
    if isinstance(schedule_interval, timedelta):
        return last_run_date + schedule_interval
    elif isinstance(schedule_interval, str):
        if schedule_interval.startswith('@'):
            cron_presets = {
                '@hourly': '0 * * * *',
                '@daily': '0 0 * * *',
                '@weekly': '0 0 * * 0',
                '@monthly': '0 0 1 * *',
                '@yearly': '0 0 1 1 *'
            }
            cron_expression = cron_presets.get(schedule_interval, '0 0 * * *')  # Default to daily if unknown preset
        else:
            cron_expression = schedule_interval
        cron = croniter(cron_expression, last_run_date)
        return cron.get_next(datetime)
    else:
        raise ValueError(f"Unsupported schedule interval type: {type(schedule_interval)}")

def format_timedelta(td):
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def check_dag_execution(dag):
    dag_id = dag["dag_id"]
    dag_details = get_dag_details(dag_id)
    schedule_interval = parse_schedule_interval(dag_details["schedule_interval"])

    if not schedule_interval:
        print(f"Skipping DAG {dag_id} with undefined schedule interval.")
        return False, None
    
    latest_run = get_latest_dag_run(dag_id)
    if not latest_run:
        print(f"No runs found for DAG {dag_id}")
        return False, None

    execution_date = datetime.fromisoformat(latest_run["execution_date"])
    current_time = datetime.now(pytz.utc)

    next_scheduled_run = get_next_scheduled_run(execution_date, schedule_interval)
    latest_acceptable_time = next_scheduled_run + BUFFER_TIME

    # Get a timedelta representation of the schedule interval
    schedule_interval_delta = get_schedule_interval_timedelta(schedule_interval)

    # Check if the latest run was successful and within the expected timeframe
    success = (latest_run["state"] == "success" and 
               execution_date <= next_scheduled_run and 
               execution_date >= next_scheduled_run - schedule_interval_delta - BUFFER_TIME)

    if success:
        print(f"DAG {dag_id} was successfully executed on schedule.")
        start_date = datetime.fromisoformat(latest_run["start_date"])
        end_date = datetime.fromisoformat(latest_run["end_date"])
        ago = current_time - end_date
        print(f"Execution date: {start_date}")
        print(f"Ago: {ago}")
    else:
        print(f"DAG {dag_id} was not executed successfully within the expected timeframe.")
        print(f"Latest run state: {latest_run['state']}")
        print(f"Execution date: {execution_date}")
        print(f"Expected next run: {next_scheduled_run}")
        print(f"Latest acceptable time: {latest_acceptable_time}")
        print(f"Current time: {current_time}")

    return success, schedule_interval, ago if success else None

def get_schedule_interval_timedelta(schedule_interval):
    if isinstance(schedule_interval, timedelta):
        return schedule_interval
    elif isinstance(schedule_interval, str):
        if schedule_interval.startswith('@'):
            cron_presets = {
                '@hourly': timedelta(hours=1),
                '@daily': timedelta(days=1),
                '@weekly': timedelta(weeks=1),
                '@monthly': timedelta(days=30),  # Approximate
                '@yearly': timedelta(days=365)  # Approximate
            }
            return cron_presets.get(schedule_interval, timedelta(days=1))
        else:
            # For cron expressions, return a default of 1 day
            # This is a simplification and might need adjustment for your specific use case
            return timedelta(days=1)
    else:
        raise ValueError(f"Unsupported schedule interval type: {type(schedule_interval)}")

def send_slack_alert(results):
    """
    Send a Slack alert with the audit summary in a formatted way.
    """
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Airflow Audit of the Last DAG Runs - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "emoji": True
            }
        },
        {"type": "divider"}  # Divider after the title
    ]
    
    for dag_id, (success, schedule_interval, ago) in results.items():
        status = '✅ Successful' if success else '❌ Failed'
        ago_str = format_timedelta(ago) if ago else 'N/A'
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*DAG ID:* `{dag_id}`\n*Status:* {status}\n*Schedule Interval:* {schedule_interval}\n*Last Run (ago):* {ago_str}"
            }
        })
        blocks.append({"type": "divider"})

    payload = {
        "text": "Airflow DAGs Audit Results",
        "blocks": blocks
    }


    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    response.raise_for_status()

def check_all_dags():
    dags = get_all_dags()
    results = {}
    
    for dag in dags:
        if not dag['schedule_interval']:
            print(f"Skipping DAG {dag['dag_id']} with undefined schedule interval.")
            continue
        results[dag["dag_id"]] = check_dag_execution(dag)

    print("\nSummary:")
    for dag_id, (success, schedule_interval, ago) in results.items():
        status = 'Successful' if success else 'Failed'
        ago_str = format_timedelta(ago) if ago else 'N/A'
        print(f"DAG {dag_id}: {status} - Schedule interval: {schedule_interval} - Ago: {ago_str}")

    # Send summary to Slack
    send_slack_alert(results)

if __name__ == "__main__":
    check_all_dags()