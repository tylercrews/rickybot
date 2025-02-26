"""rickybot micro - DELETE.ipynb
checks all of the follows from this day one week ago, and unfollows any users that did not follow back.
	logging should record stats of follows-follow backs from that day as well as number of accounts that were deleted.
	Should consume as much of the list in the s3 bucket as possible, and return early if there's nothing in the bucket to delete.
	After processing as much as possible the function will log how much it did, how much is remaining, and it will save in the dynamodb deletions stats what the current counts of everything are.
	When the s3 bucket is completely emptied out, the s3 object will be deleted, and the dynamodb deletions stats will be finished and logged for the sum of the day.

	Args:
		None
	Returns:
		None
"""
# aws stuff
import boto3
from botocore.exceptions import ClientError
# json necessary to parse secret string, and write/read s3 objects
import json
# datetime is necessary for our ddb and s3 schema
import datetime
import zoneinfo
# for logging
import base64
import requests
# import bluesky api
from atproto import Client
# this is the exception that is raised when we try to find a
from atproto.exceptions import BadRequestError
# replace prints with logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

REGION = 'us-east-2'
SECRETS_ID = 'Rickybot-Login-Credentials'

DDB = 'dynamodb'
S3 = 's3'
DDB_TABLE = 'rickybot-ddb'
S3_BUCKET = 'rickybot-s3'
DDB_DELSTATS_KEY = 'DEL-STATS'
DDB_ATTR_PROCESSED = 'PROCESSED'
DDB_ATTR_DNE = 'DNE'
DDB_ATTR_FOLLOWBACKS = 'FOLLOWBACKS'
DDB_ATTR_NOFOLLOWBACK = 'NO-FOLLOWBACK'

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

FILE_PATH = "LOGGING_DEL_02.txt"
BRANCH = "main"

MY_DID = 'did:plc:ktkc7jfakxzjpooj52ffc6ra'

def lambda_handler(event, context):
	# get the day of the week so we know what dynamodb key to pull from and which bucket to aggregate to
	# doing this first because we do not run this on saturday and can bail out early if we get into this code for some reason
	# also we are running this at about 1am, the following day after all runs have concluded for the previous. so we're aggregating the previous day's results
	cur_timestamp = datetime.datetime.now(zoneinfo.ZoneInfo(USER_TIMEZONE))
	dow = cur_timestamp.strftime("%A")

	if dow == 'Saturday':
		leave = "It's Saturday, you shouldn't be here."
		logger.warning(leave)
		# return early with no content
		return {
			'statusCode': 204,
			'body': json.dumps(leave)
		}

	# use the day of the week to pull up the corresponding key for our dynamodb entries and our s3 bucket
	s3_key = DOW_KEYS[dow]
	logger.info(s3_key)

	# connect to aws
	try:
		aws_session = boto3.session.Session()
	except Exception as e:
		err = f'ERROR - failed to begin AWS session: {e}'
		logger.error(err)
		# this is the only error that we can't log to github, because we never got the credentials
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# then connect to secrets manager
	try:
		secrets_client = aws_session.client('secretsmanager')
		secret_value = secrets_client.get_secret_value(SecretId=SECRETS_ID)
		secret_string = secret_value['SecretString']
		secret_map = json.loads(secret_string)
	except Exception as e:
		err = f'ERROR - failed to reach aws secrets manager: {e}'
		logger.error(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# create constants from the values in the secrets manager
	BSKY_USERNAME = secret_map['bsky_username']
	BSKY_PASSWORD = secret_map['bsky_password']
	GITHUB_TOKEN = secret_map['github_token']
	GITHUB_REPO = secret_map['github_user/repo']
	# HUGGING_TOKEN = secret_map['hugging_token']

	# before the program starts let's set up the logging function so we can insert it at any point where our program could break
	def logging_deletions(logging_text):
		# LOGGING ALL THE CHANGES TO OUR LOGGING FILE IN GITHUB
		datetime_object = datetime.datetime.fromtimestamp(cur_timestamp.timestamp())
		date_only = str(datetime_object.date())
		commit_message = "Logging follow deletions on " + date_only

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
				logger.info("Logging file updated successfully! Here's what was added to the logs:")
				logger.info(date_only + ": " + logging_text)
		else:
				logger.error(f"ERROR - {update_response.json()}")

	# initialize dynamodb
	try:
		dynamodb = aws_session.resource(DDB)
		table = dynamodb.Table(DDB_TABLE)
	except Exception as e:
		err = f'ERROR - failed to connect to the dynamoDB table: {e}'
		logger.error(err)
		logging_deletions(err)
		# return with error, we cant add the results to the db and we can't retrieve the cache (which isn't as important, but still)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}
	# initialize s3
	try:
		s3 = aws_session.client(S3)
		buckets = s3.list_buckets()
		bucket = s3.list_objects_v2(Bucket=S3_BUCKET)
	except Exception as e:
		err = f'ERROR - failed to connect to the s3 bucket: {e}'
		logger.error(err)
		logging_deletions(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# pull the object from s3 - if there is nothing in there we can return early
	try:
		s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
		logger.info("Object existed in s3 bucket.")
		response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
		# creates a list from the json info in the s3 bucket
		old_follows = json.loads(response["Body"].read())
		logger.info(old_follows, type(old_follows))
	except s3.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "404": # object was not found at the key
			# this isn't necessarily an ERROR because we're going to do more deletion runs than we need, but logging as an error to make sure it shows up
			end = 'There was no users list found in s3. Terminating function call.'
			logger.error(end)
			# RETURNING EARLY
			return {
				'statusCode': 204,
				'body': json.dumps(end)
			}
	except Exception as e:
		err = f"ERROR - failed to get previous follows list from s3 bucket: {e}"
		logger.error(err)
		logging_deletions(err)

	# and now we can log into the bluesky client
	try:
		client = Client()
		client.login(BSKY_USERNAME, BSKY_PASSWORD)
	except Exception as e:
		err = f'ERROR - failed to log in to the bluesky client: {e}'
		logger.error(err)
		logging_deletions(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# just getting a previous count of our followers and following for the logs
	following_before = 0
	try:
		following = client.get_profile(actor=BSKY_USERNAME).follows_count
		following_before = following
	except Exception as e:
		warning = f'WARNING - failed to get previous follow count: {e}'
		logger.warning(warning)
		logging_deletions(warning)
	logger.info(following_before)

	# initialize our deletion stats for our logging and dynamodb record
	processed_count = 0
	failed_to_delete = [] # if any fail we can add them back in and retry later
	count_users_dne = 0 # if we fail to find on lookup the account does not exist anymore
	followed_back = 0
	no_followback = 0
	finished_deleting = False # flag to let us know if we can delete the running deletion stats
	error_count = 0

	# now go through the followers, check if they still exist, see if they followed back, delete if necessary
	for user_did in old_follows:
		processed_count += 1
		if user_did == MY_DID:
			# this shouldn't happen but we'll cover it anyway
			continue
		# first we have to try to get the profile of the user
		try:
			user_profile = client.get_profile(actor=user_did)
		except BadRequestError as e:
			# if we get a bad request error it means that the profile was either banned or deleted. Nothing else to do with them, but I want to keep track of these.
			# we could really validate this by using the next line of code, but it's overkill - I don't want to get hyperspecific
			# if e.response.content.message == 'Profile not found':
			count_users_dne += 1
		except:
			# if we had a general exception then we should retry this user later, probably just timed out or something.
			failed_to_delete.append(user_did)
			error_count += 1
			if error_count > 3: # something's going wrong with this run, either rate limiting or timing out for some reason
				break

		user_didnt_followback = True if user_profile.viewer.followed_by == None else False
		if user_didnt_followback:
			no_followback += 1
			try:
				follow_uri = user_profile.viewer.following
				if follow_uri != None: # maybe I manually unfollowed
					client.delete_follow(follow_uri)
			except:
				# something went wrong and we failed to delete this user, it's extremely rare for this to happen unless you're just rate limited, so save this for later
				failed_to_delete.append(user_did)
				error_count += 1
				if error_count > 3: # we're probably getting rate limited, so stop processing users
					break
			finally:
				if no_followback >= 3500: # this is a hard cap on deletions so you don't get rate limited.
					break
		else:
			followed_back += 1

	# get a new follow count to show the change
	following_after = 0
	try:
		following = client.get_profile(actor=BSKY_USERNAME).follows_count
		following_after = following
	except Exception as e:
		warning = f'WARNING - failed to get updated follow count: {e}'
		logger.warning(warning)
		logging_deletions(warning)

	# ok so we're out of the loop and here's what we need to do
	# log our progress through the list of deletions.
	logging_deletions(f'Processed {processed_count} users from the list of {len(old_follows)}.{"" if len(failed_to_delete) == 0 else f" {len(failed_to_delete)} failures were encountered and need to be retried."} From this batch of deletions {followed_back} users followed back, {no_followback} did not follow back and were deleted, and {count_users_dne} accounts no longer exist.\nFollows count - now: {following_after} | prev: {following_before}')
	# add any users that we failed to delete back to the end of the list. hopefully this should usually be 0
	old_follows.extend(failed_to_delete)
	# now we check to see if we made it to the end of the list.
	if processed_count >= len(old_follows):
		finished_deleting = True
		# now delete the s3 object
		try:
			s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)
			logging_deletions('Finished processing all deletions for today. Object was successfully deleted from s3 bucket.')
		except Exception as e:
			err = f'ERROR - failed to delete the list of follows from the s3 bucket: {e}'
			logger.error(err)
			logging_deletions(err)
	else:
		# take a slice from wherever we got to until the end of the list, and then we'll stick that back into s3
		leftover_follows = old_follows[processed_count : ]
		try:
			s3.put_object(
					Bucket=S3_BUCKET,
					Key=s3_key,
					Body=json.dumps(leftover_follows),
					ContentType="application/json"
			)
			logging_deletions(f'successfully uploaded the list of remaining follows to check to s3.')
		except Exception as e:
			err = f"ERROR - failed to upload list of leftover follows to s3: {e}"
			logger.error(err)
			logging_deletions(err)
			# don't terminate early here, we want the stats still

	# delete s3 if nothing left to put in
	# do logging here

	# pull up dynamodb, see if there were old stats, and add them to our current stats
	try:
		ddb_response = table.get_item(
				Key={'DOW': DDB_DELSTATS_KEY},
		)
	except Exception as e:
		err = f"ERROR - failed to check ddb key's existence: {e}"
		logger.error(err)
		logging_deletions(err)
		# don't want to return here in case this was our final run and we can still print stats

	logger.info('ddb response:', ddb_response)
	# it's not a problem if there was nothing in the response, it just means this was the first deletion of the day
	if 'Item' in ddb_response:
		processed_count += ddb_response['Item'][DDB_ATTR_PROCESSED]
		count_users_dne += ddb_response['Item'][DDB_ATTR_DNE]
		followed_back += ddb_response['Item'][DDB_ATTR_FOLLOWBACKS]
		no_followback += ddb_response['Item'][DDB_ATTR_NOFOLLOWBACK]

		# after finishing iterating through all of the attributes we can delete this key from the dynamodb to clear out all the previous runs
		if finished_deleting:
			try:
				table.delete_item(
					Key={'DOW': DDB_DELSTATS_KEY}
				)
			except Exception as e:
				err = f"ERROR - failed to delete item {DDB_DELSTATS_KEY} from dynamodb: {e}"
				logger.error(err)
				logging_deletions(err)
		else:
			# if we're not finished deleting then we update the ddb delstats for next run
			try:
				table.update_item(
						Key={'DOW': DDB_DELSTATS_KEY},
						UpdateExpression='SET #attr1 = :val1, #attr2 = :val2, #attr3 = :val3, #attr4 = :val4',
						ExpressionAttributeNames={
								'#attr1': DDB_ATTR_PROCESSED,
								'#attr2': DDB_ATTR_DNE,
								'#attr3': DDB_ATTR_FOLLOWBACKS,
								'#attr4': DDB_ATTR_NOFOLLOWBACK
						},
						ExpressionAttributeValues={
								':val1': processed_count,
								':val2': count_users_dne,
								':val3': followed_back,
								':val4': no_followback
						}
				)
			except Exception as e:
				err = f'ERROR - completed deletions but failed to store running deletion statistics in dynamodb.\n{e}'
				logger.error(err)
				logging_deletions(err)
				return {
					'statusCode': 207,
					'body': json.dumps(err)
				}

	# now log the statistics regardless of if there was anything in ddb (there will not be if there was only one deletion run for the day)
	if finished_deleting:
			conversion_rate = round(followed_back / (followed_back + no_followback) * 100, 2)
			logging_deletions(f"STATS - Finished checking last week's follows from {s3_key} for deletions. In total there were {processed_count} follows processed. {followed_back} users followed back. {no_followback} did not follow back and were deleted. {count_users_dne} accounts no longer exist. Conversion Rate was {conversion_rate}%.")

	return {
		'statusCode': 200,
		'body': json.dumps(f'Successfully checked for follows that did not followback and pruned follow list as necessary.')
	}