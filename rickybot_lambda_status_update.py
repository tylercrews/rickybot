from atproto import Client
# aws stuff
import boto3
from botocore.exceptions import ClientError
# json necessary to parse secret string, and write/read s3 objects
import json
# these imports are to use github apis to do logging, base64 is to parse the json
import requests
import base64
# datetime for logging
import datetime
import zoneinfo
# replace prints with logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)  # Ensure errors are logged


REGION = 'us-east-2'
SECRETS_ID = 'Rickybot-Login-Credentials'

# this file doesn't need ddb access
# DDB = 'dynamodb'
S3 = 's3'
# DDB_TABLE = 'rickybot-ddb'
S3_BUCKET = 'rickybot-s3'
S3_KEY_FOLLOWING_YOU = 'STATUS-FOLLOWING-YOU' # unlike the others that have a key determined by the day of the week, this will check the same spot every time.
S3_KEY_WHO_YOU_FOLLOW = 'STATUS-WHO-YOU-FOLLOW'


USER_TIMEZONE = "US/Eastern"
FILE_PATH = "LOGGING_STATUS_02.txt"
BRANCH = "main"

def lambda_handler(event, context):
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
	BSKY_PASSWORD = secret_map['bsky_password']
	GITHUB_TOKEN = secret_map['github_token']
	GITHUB_REPO = secret_map['github_user/repo']
	HUGGING_TOKEN = secret_map['hugging_token']

	# before the program starts let's set up the logging function so we can insert it at any point where our program could break
	def logging_status(logging_text):
		# LOGGING ALL THE CHANGES TO OUR LOGGING FILE IN GITHUB
		cur_timestamp = datetime.datetime.now(zoneinfo.ZoneInfo(USER_TIMEZONE))
		datetime_object = datetime.datetime.fromtimestamp(cur_timestamp.timestamp())
		date_only = str(datetime_object.date())
		commit_message = "Logging status of followers on " + date_only


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
			logging.error(f"Error: {update_response.json()}")

	# initialize the s3 client and get the bucket
	try:
		s3 = aws_session.client(S3)
		buckets = s3.list_buckets()
		bucket = s3.list_objects_v2(Bucket=S3_BUCKET)
	except:
		err = 'ERROR - failed to establish client connection to s3 bucket'
		logging.error(err)
		logging_status(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# log in to bluesky
	try:
		client = Client()
		client.login(BSKY_USERNAME, BSKY_PASSWORD)
	except:
		err = 'ERROR - failed to log in to bluesky'
		logging.error(err)
		logging_status(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# get the current list of bluesky followers we'll create a map of follower_id: follow_uri, and follow_uri will identify if we follow them
	try:
		current_followers = {}
		followers_count = client.get_profile(actor=BSKY_USERNAME).followers_count
		remaining = followers_count
		next_page = ''
		while remaining > 0:
			logging.info(f'starting the next page of followers. {remaining} followers remaining to check.')
			cur_limit = min(remaining, 100)
			remaining -= cur_limit
			followers = client.get_followers(actor=BSKY_USERNAME, cursor= next_page, limit=cur_limit)
			next_page = followers.cursor
			followers = followers.followers
			for user in followers:
				did = user.did
				handle = user.handle
				follow_uri = user.viewer.following
				# logging.info(handle, did, follow_uri)
				# handles can change we only want to deal with the did
				current_followers[handle] = follow_uri
	except:
		err = 'ERROR - failed to gather current followers'
		logging.error(err)
		logging_status(err)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# now we get the old list of our followers from s3
	# then if we find an old list in our bucket we compare it to the new list and unfollow anyone who is not in our new list.
	count_removed = 0
	count_failed_removal = 0
	success_for_followers = False
	try:
		s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY_FOLLOWING_YOU)
		logging.info("Object existed in s3 bucket.")
		response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_FOLLOWING_YOU)
		# creates a list from the json info in the s3 bucket
		old_followers = json.loads(response["Body"].read())
		logging.info(old_followers, type(old_followers))
		for user_did, follow_uri in old_followers.items():
			if user_did not in current_followers:
				logging.info('not in followers', user_did)
				try:
					client.delete_follow(follow_uri)
					count_removed += 1
				except Exception as e:
					# logging.error(f'failed on {i} - uri: {follow_uri} \n {e}')
					count_failed_removal +=1
		follow_diff = len(current_followers) - len(old_followers)
		logging_status(f'followers status - {"up" if follow_diff >= 0 else "down"} {abs(follow_diff)} followers this week. {count_removed + count_failed_removal} users stopped following. {count_removed} were successfully unfollowed, with {count_failed_removal} failures.')
	except s3.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "404": # object was not found at the key
			err = "ERROR - there was no previous followers list found in the s3 bucket. Adding the new list of followers."
			logging.error(err)
			logging_status(err)
		else:
			err = "ERROR - failed to access the s3 bucket to get previous followers."
			logging.error(err)
			logging_status(err)
	finally:
		# regardless of if we were able to perform the comparison or not we want to input our new list into storage
		try:
			s3.put_object(
					Bucket=S3_BUCKET,
					Key=S3_KEY_FOLLOWING_YOU,
					Body=json.dumps(current_followers),
					ContentType="application/json"
			)
			success_for_followers = True
		except Exception as e:
				logging.error(f"ERROR - failed to upload new followers map to s3: {e}")
				logging_status(f"ERROR - failed to upload new followers map to s3: {e}")
				# we will not return here because we still want to try to upload the follows if possible

	# now we're going to repeat this process with our account follows
	# generate our account's follows list
	cur_who_you_follow = {}
	following = client.get_profile(actor=BSKY_USERNAME).follows_count
	logging.info(f'currently following {following} users')
	next_page = ''
	remaining = following
	while remaining > 0:
		logging.info (f'starting the next page of follows. {remaining} follows remaining to check.')
		cur_limit = min(remaining, 100)
		remaining -= cur_limit
		follows = client.get_follows(actor=BSKY_USERNAME, cursor= next_page, limit=cur_limit)
		next_page = follows.cursor
		follows = follows.follows
		for follow in follows:
			did = follow.did
			# handle = follow.handle
			# following is whether they are following you, it looks like followed_by is if they are following you back.
			# follows_you = 'FALSE' if follow.viewer.followed_by == None else 'TRUE' # we actually don't need this value at all, but we need a value in the map so might as well put something hypothetically useful and small in memory
			follow_uri = follow.viewer.following
			cur_who_you_follow[did] = follow_uri

	# with a hashmap of our follows to iterate through we're going to do something very similar to the followers
	# but this time we're going to see if they're in the previous week's follows, and see if they're in the followers hashmap
	# and if we were following them last week but they're still not a follower, then we delete them.
	# now we get the old list of our followers from s3
	count_removed = 0
	count_failed_removal = 0
	success_for_who_we_follow = False
	users_deleted = []
	try:
		s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY_WHO_YOU_FOLLOW)
		logging.info("Object existed in s3 bucket.")
		response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_WHO_YOU_FOLLOW)
		# creates a list from the json info in the s3 bucket
		prev_who_we_were_following = json.loads(response["Body"].read())
		logging.info(prev_who_we_were_following, type(prev_who_we_were_following))
		for user_did, follow_uri in cur_who_you_follow.items():
			# as we iterate what we want to find are people who Were in the previous list and are Not now in the follows list
			if user_did in prev_who_we_were_following and user_did not in current_followers:
				logging.info(f'this user is an old follow but is still not a follower: {user_did}')
				try:
					client.delete_follow(follow_uri)
					count_removed += 1
					# if we're successfully able to delete the follow we shouldn't keep it in our list of follows too.
					users_deleted.append(user_did)
				except Exception as e:
					# logging.error(f'failed on {i} - uri: {follow_uri} \n {e}')
					count_failed_removal +=1
		# now that we're done iterating through the dict we can safely remove all the users that we deleted and should not be included in it
		for user in users_deleted:
			del cur_who_you_follow[user]
		follow_diff = len(cur_who_you_follow) - len(prev_who_we_were_following)
		logging_status(f'who you follow status - {"up" if follow_diff >= 0 else "down"} {abs(follow_diff)} follows this week. {count_removed + count_failed_removal} users have aged out and were necessary to prune. {count_removed} were successfully unfollowed, with {count_failed_removal} failures.')
	except s3.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "404": # object was not found at the key
			err = "ERROR - there was no previous list of who we follow found in the s3 bucket. Adding the new list of follows."
			logging.error(err)
			logging_status(err)
		else:
			err = "ERROR - failed to access the s3 bucket to get previous list of who we follow."
			logging.error(err)
			logging_status(err)
	finally:
		# regardless of if we were able to perform the comparison or not we want to input our new list into storage
		try:
			s3.put_object(
				Bucket=S3_BUCKET,
				Key=S3_KEY_WHO_YOU_FOLLOW,
				Body=json.dumps(cur_who_you_follow),
				ContentType="application/json"
			)
			success_for_who_we_follow = True
		except Exception as e:
			err = f"ERROR - failed to upload new map of who we follow to s3: {e}"
			logging.error(err)
			logging_status(err)

	logging_status(f"s3 update - uploaded followers: {'SUCCESS' if success_for_followers else 'FAILURE'} | uploaded who we follow: {'SUCCESS' if success_for_who_we_follow else 'FAILURE'} ")
	if not success_for_followers and not success_for_who_we_follow:
		return {
			'statusCode': 500,
			'body': json.dumps('ERROR - failed all updates to the s3 with current follows and followers')
		}
	# successfully completed
	return {
		'statusCode': 200,
		'body': json.dumps('Successfully completed status update.')
	}