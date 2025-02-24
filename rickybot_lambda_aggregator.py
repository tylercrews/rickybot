# aws stuff
import boto3
from botocore.exceptions import ClientError

# json necessary to parse secret string, and write/read s3 objects
import json

# datetime is necessary for our ddb and s3 schema
import datetime
import zoneinfo

# for github logging
import base64
import requests

# replace prints with logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)  # Ensure errors are logged

REGION = 'us-east-2'
SECRETS_ID = 'Rickybot-Login-Credentials'

DDB = 'dynamodb'
S3 = 's3'
DDB_TABLE = 'rickybot-ddb'
S3_BUCKET = 'rickybot-s3'

PRIMARY_KEY = 'DOW' # the dynamodb table's primary key. there is no sort key
DOW_KEYS = {
		'Sunday': 'SUN',
		'Monday': 'MON',
		'Tuesday': 'TUE',
		'Wednesday': 'WED',
		'Thursday': 'THU',
		'Friday': 'FRI+SAT',
		'Saturday': 'FRI+SAT'
}
USER_TIMEZONE = "US/Eastern"

FILE_PATH = "LOGGING_AGG_02.txt"
BRANCH = "main"

def lambda_handler(event, context):
	# get the day of the week so we know what dynamodb key to pull from and which bucket to aggregate to
	# doing this first because we do not run this on saturday and can bail out early if we get into this code for some reason -- no longer skipping saturdays, so that we can get fridays follows out of the dynamodb
	# also we are running this at about 1am, the following day after all runs have concluded for the previous. so we're aggregating the previous day's results
	cur_timestamp = datetime.datetime.now(zoneinfo.ZoneInfo(USER_TIMEZONE))
	yest_timestamp = cur_timestamp - datetime.timedelta(days=1)
	yesterday = yest_timestamp.strftime("%A")

	# use the day of the week to pull up the corresponding key for our dynamodb entries and our s3 bucket
	ddbs3_key = DOW_KEYS[yesterday]
	logging.info(ddbs3_key)

	# connect to aws
	try:
		aws_session = boto3.session.Session()
	except:
		err = 'ERROR - failed to begin AWS session'
		logging.error(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}
		# this is the only error that we can't log to github, because we never got the credentials

	# then connect to secrets manager
	try:
		secrets_client = aws_session.client('secretsmanager')
		secret_value = secrets_client.get_secret_value(SecretId=SECRETS_ID)
		secret_string = secret_value['SecretString']
		secret_map = json.loads(secret_string)
	except:
		err = 'ERROR - failed to reach aws secrets manager'
		logging.error(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# create constants from the values in the secrets manager
	BSKY_USERNAME = secret_map['bsky_username']
	BSKY_PASS = secret_map['bsky_password']
	GITHUB_TOKEN = secret_map['github_token']
	GITHUB_REPO = secret_map['github_user/repo']
	HUGGING_TOKEN = secret_map['hugging_token']

	# before the program starts let's set up the logging function so we can insert it at any point where our program could break
	def logging_aggregator(logging_text):
		# LOGGING ALL THE CHANGES TO OUR LOGGING FILE IN GITHUB
		datetime_object = datetime.datetime.fromtimestamp(cur_timestamp.timestamp())
		date_only = str(datetime_object.date())
		commit_message = "Logging follow aggregation on " + date_only


		# Step 1: Get the file's current content and SHA
		url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{FILE_PATH}"
		headers = {"Authorization": f"token {GITHUB_TOKEN}"}
		response = requests.get(url, headers=headers)
		response_json = response.json()

		# Decode the content of the file
		file_sha = response_json["sha"]
		content = base64.b64decode(response_json["content"]).decode("utf-8")

		# Step 2: Modify the file content
		new_content = content + date_only + ': ' + logging_text + '\n'
		encoded_content = base64.b64encode(new_content.encode("utf-8")).decode("utf-8")

		# Step 3: Push the updated content
		data = {
			"message": commit_message,
			"content": encoded_content,
			"sha": file_sha,
			"branch": BRANCH,
		}
		update_response = requests.put(url, headers=headers, json=data)

		if update_response.status_code == 200:
			logging.info("Logging file updated successfully! Here's what was added to the logs:")
			logging.info(date_only + ": " + logging_text)
		else:
			logging.error(f"ERROR - github logging failed. {update_response.json()}")

	# initialize dynamodb and s3
	try:
		dynamodb = aws_session.resource(DDB)
		table = dynamodb.Table(DDB_TABLE)
	except:
		err = 'ERROR - failed to get dynamo db table'
		logging.error(err)
		logging_aggregator(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}
	try:
		s3 = aws_session.client(S3)
		buckets = s3.list_buckets()
		bucket = s3.list_objects_v2(Bucket=S3_BUCKET)
	except:
		err = 'ERROR - failed to get s3 bucket'
		logging.error(err)
		logging_aggregator(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# keep track of everything in a set so we don't have duplicates
	follows_aggregation = set()

	# there should NOT be anything in the current s3 object for this bucket. But just in case there is, like one week didn't properly get cleared out or something, we will add it to the beginning of the aggregation
	try:
		s3.head_object(Bucket=S3_BUCKET, Key=ddbs3_key)
		# except on saturdays - there should be the stuff from friday in the bucket when we check on saturday
		if yesterday == 'Saturday':
			good = f"Items were found in the {ddbs3_key} s3 bucket from Friday's runs. Aggregating Saturday's results to that existing data."
			logging.info(good)
			logging_aggregator(good)
		else:
			warning = "WARNING - Object existed in s3 bucket when there should have been nothing found. Aggregating with current results."
			logging.warning(warning)
			logging_aggregator(warning)
		response = s3.get_object(Bucket=S3_BUCKET, Key=ddbs3_key)
		# creates a list from the json info in the s3 bucket
		data = json.loads(response["Body"].read())
		# add all items from the list into our current set
		follows_aggregation.update(data)
	except s3.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "404":
			logging.info("Clear to proceed - object did not exist in s3 bucket")
	except Exception as e:
		err = f'ERROR - failed to get s3 object'
		logging.error(err)
		logging_aggregator(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# now it's time to iterate through our the attributes on our dynamodb key
	count_runs_combined = 0 # for logging purposes
	try:
		ddb_response = table.get_item(
			Key={'DOW': ddbs3_key},
		)
	except Exception as e:
		err = f"ERROR - failed to check key's existence: {e}"
		logging.error(err)
		logging_aggregator(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}
	logging.info('ddb response:', ddb_response)
	# this if else checks to see if there is anything
	if 'Item' not in ddb_response:
		warning = 'WARNING - found no items in this key, runs may have failed yesterday'
		logger.warning(warning)
		logging_aggregator(warning)
	else:
		count_runs_combined = len(ddb_response['Item'])
		for attribute in ddb_response['Item']: # this iterates through all the attributes in the key
			# for val in ddb_response['Item'][attribute]: # and this iterates through all of the values in the value of that key
			# instead of iterating through all the values we'll just add them all into the set directly
			# print('attr', ddb_response['Item'][attribute])
			follows_aggregation.update(ddb_response['Item'][attribute])

		# after finishing iterating through all of the attributes we can delete this key from the dynamodb to clear out all the previous runs
		try:
			table.delete_item(
				Key={'DOW': ddbs3_key}
			)
		except Exception as e:
			err = f"ERROR - failed to delete item {ddbs3_key} from dynamodb: {e}"
			logging.error(err)
			logging_aggregator(err)
			# don't want to skip putting the list in so we won't return here
	# print(follows_aggregation)

	# now we've aggregated all the values, so we just need to put that into s3
	aggregate_list = list(follows_aggregation)
	try:
		s3.put_object(
			Bucket=S3_BUCKET,
			Key=ddbs3_key,
			Body=json.dumps(aggregate_list),
			ContentType="application/json"
		)
		logging_aggregator(f'Successfully aggregated follows from {ddbs3_key}. Today there were {count_runs_combined} runs, with a total of {len(aggregate_list)} follows.')
	except Exception as e:
			err = f"ERROR - failed to upload object to s3: {e}"
			logger.error(err)
			logging_aggregator(err)
			return {
				'statusCode': 500,
				'body': json.dumps(err)
			}

	return {
		'statusCode': 200,
		'body': json.dumps('Successfully aggregated follows.')
	}
