# aws stuff
import boto3
# json necessary to parse secret string, and write/read s3 objects
import json
# From the transformers package, import ViTImageProcessor and ViTForImageClassification
from transformers import ViTImageProcessor, ViTForImageClassification
# From the PIL package, import Image and Markdown, need bytesio to translate image
from PIL import Image
from io import BytesIO
# import requests
import requests
# import torch
import torch
# url getter for mpl
import urllib
import numpy as np
# import bluesky api
from atproto import Client
# datetime is necessary for caturday check and logging
import datetime
import zoneinfo
# these imports are to use github apis to do logging, base64 is to parse the json
# import requests # already imported for something else
import base64
# replace prints with logging
import logging
# Huggingface requires a place to write in the cache so that transformers will work.
import os

os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface/transformers'
os.environ['HF_HOME'] = '/tmp/huggingface'
os.makedirs('/tmp/huggingface/transformers', exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.WARNING)  # Ensure errors are logged

os.makedirs('/tmp/huggingface/transformers', exist_ok=True)

# input variables - x is target follows y is num of posts we want to look through, whichever end criteria we reach first
EMBEDDED_PIC = 'app.bsky.embed.images#view'
EMBEDDED_VID = 'app.bsky.embed.video#view'
FEED_CATPICS = 'at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.generator/cv:cat'
FEED_CATS = 'at://did:plc:jfhpnnst6flqway4eaeqzj2a/app.bsky.feed.generator/cats'
FEED_TUXEDOCATS = 'at://did:plc:eubjsqnf5edgvcc6zuoyixhw/app.bsky.feed.generator/tuxedo-cats'
# FEED_CATURDAY = 'at://did:plc:pmyqirafcp3jqdhrl7crpq7t/app.bsky.feed.generator/aaad4sb7tyvjw' # this one is old idk why it disappeared but it was still working?
URL_BEGIN = 'https://bsky.app/profile/'
URL_POST = '/post/'
# my did to check against
MY_DID = 'did:plc:ktkc7jfakxzjpooj52ffc6ra'

CATURDAY_DOW = 'Saturday'
USER_TIMEZONE = "US/Eastern" # you should fill this in with your own timezone here

LINE_BREAK = '\n'
END_LOGGING = '____________________\n'

FILE_PATH = "LOGGING_ADD.txt"  # Replace with the file path in your repo
BRANCH = "main"  # Replace with your branch name

REGION = 'us-east-2'
SECRETS_ID = 'Rickybot-Login-Credentials'

DDB = 'dynamodb'
S3 = 's3'
DDB_TABLE = 'rickybot-ddb'
S3_BUCKET = 'rickybot-s3'
DDB_CACHE_KEY = 'CACHE'
DDB_CACHE_ATTRIBUTE = 'CIDS'

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

def lambda_handler(event, context):
	# get the day of the week so we know what dynamodb key to pull from and which bucket to aggregate to
	cur_timestamp = datetime.datetime.now(zoneinfo.ZoneInfo(USER_TIMEZONE))
	dow = cur_timestamp.strftime("%A")
	str_timestamp = str(cur_timestamp) # we'll need this to use as the attribute for ddb

	# use the day of the week to pull up the corresponding key for our dynamodb entries and our s3 bucket
	ddb_key = DOW_KEYS[dow]
	logger.info(ddb_key)

	# also this is where we can initialize our RUNNING LOG string.
	global running_logging_text # declare global variable so we can edit it elsewhere
	running_logging_text = str_timestamp + LINE_BREAK

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
	FEED_CATURDAY = secret_map['feed_caturday']
	FEED_REGDAY = secret_map['feed_regday']
	FEED_NAME_REGDAY = secret_map['feed_name_regday']
	FEED_NAME = {FEED_CATURDAY: "'Caturday'", FEED_REGDAY: FEED_NAME_REGDAY}
	# run settings are also imported from secretesmanager so they can be tuned without updating the function
	POSTS_CATURDAY = int(secret_map['posts_caturday'])
	FOLLOWS_CATURDAY = int(secret_map['follows_caturday']) #350 when automated to 1 run per 1 hour
	POSTS_OTHERCAT = int(secret_map['posts_regday'])
	FOLLOWS_OTHERCAT = int(secret_map['follows_regday']) # 400 when automated to 1 run per 2 hours
	# running these very frequently so we don't need to do too many:
	# by my math cap for day is 9250, so 1 run per hr caps at 385, 2 hours is 770, but we need to save some of that room for deletions, especially on Fridays. So when we automate I want to do 1000-350, 1000-400.
	# not sure about post count, but 1000 is still good I suppose. Shouldn't take more than about 400 posts to get 400 likes, but can't hurt to have more on slower days I suppose



	# now that we have our github token set up we should set up our logging function to use whenever we encounter any errors
	def logging_add(logging_text):
		# LOGGING ALL THE CHANGES TO OUR LOGGING FILE IN GITHUB
		commit_message = "Logging for follower additions on " + str_timestamp

		# Step 1: Get the file's current content and SHA
		url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{FILE_PATH}"
		headers = {"Authorization": f"token {GITHUB_TOKEN}"}
		response = requests.get(url, headers=headers)
		response_json = response.json()

		# Decode the content of the file
		file_sha = response_json["sha"]
		content = base64.b64decode(response_json["content"]).decode("utf-8")

		# Step 2: Modify the file content
		new_content = content + LINE_BREAK + logging_text + END_LOGGING
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
				logger.info("Logging file updated successfully! Here's what was added to the logs:\n")
				logger.info(logging_text + END_LOGGING)
		else:
				logger.error(f"Error: {update_response.json()}")

	# initialize the ViT model
	try:
		# # Load the feature extractor for the vision transformer
		feature_extractor = ViTImageProcessor.from_pretrained('./vit', local_files_only=True)
		# # Load the pre-trained weights from vision transformer
		model = ViTForImageClassification.from_pretrained('./vit', local_files_only=True)
	except Exception as e:
		err = f'ERROR - failed to initialize ViT model:\n{repr(e)}: {e}'
		logger.error(err)
		running_logging_text += err + LINE_BREAK
		logging_add(running_logging_text)
		# this is a critical failure, so return early here
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# next we need to retrieve our cached posts from the dynamodb, so log into dynamodb here
	try:
		dynamodb = aws_session.resource(DDB)
		table = dynamodb.Table(DDB_TABLE)
	except Exception as e:
		err = f'ERROR - failed to connect to the dynamoDB table: {e}'
		logger.error(err)
		running_logging_text += err + LINE_BREAK
		logging_add(running_logging_text)
		# return with error, we cant add the results to the db and we can't retrieve the cache (which isn't as important, but still)
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# pull the cache of post CIDs seen in the previous run from dynamodb - if we fail any step here just leave a warning that we couldn't check the cache
	cached_posts = set() # in case the ddb fails to retrieve, initialize an empty set
	ddb_response = {} # same reason
	seen_posts = set() # initialize the seen posts set, we'll be using it to replace our cache at the end of the run
	try:
		ddb_response = table.get_item(
				Key={'DOW': DDB_CACHE_KEY},
		)
	except Exception as e:
		warning = f"WARNING - failed to check post cache key's existence: {e}"
		logger.warning(warning)
		running_logging_text += warning + LINE_BREAK

	# logger.info('ddb response:', ddb_response)
	# this if else checks to see if there is anything
	if 'Item' not in ddb_response:
		# check the status code to skip a redundant warning, if we errored out before there will be no key
		if 'ResponseMetadata' in ddb_response and 'HTTPStatusCode' in ddb_response['ResponseMetadata'] and ddb_response['ResponseMetadata']['HTTPStatusCode'] == 200:
			warning = 'WARNING - successful response from dynamodb but there were no items in the post cache key.'
			logger.warning(warning)
			running_logging_text += warning + LINE_BREAK
	else:
		if DDB_CACHE_ATTRIBUTE in ddb_response['Item']:
			cached_posts = ddb_response['Item'][DDB_CACHE_ATTRIBUTE]
			logger.info(f'imported {len(cached_posts)} prior seen posts from the dynamodb table')
		else:
			warning = 'WARNING - somehow there were items in the dynamodb cache key, but the attribute for cached posts was not present'
			logger.warning(warning)
			running_logging_text += warning + LINE_BREAK

	# and now we can log into the bluesky client
	try:
		client = Client()
		client.login(BSKY_USERNAME, BSKY_PASSWORD)
	except Exception as e:
		err = f'ERROR - failed to log in to the bluesky client: {e}'
		logger.error(err)
		running_logging_text += err + LINE_BREAK
		logging_add(running_logging_text)
		# return here, cannot proceed without bluesky
		return {
			'statusCode': 500,
			'body': json.dumps(err)
		}

	# this is our code to identify post images as catposts

	# 281: 'tabby, tabby cat'
	# 282: 'tiger cat', 283: 'Persian cat', 284: 'Siamese cat, Siamese', 285: 'Egyptian cat', 286: 'cougar, puma, catamount, mountain lion, painter, panther, Felis concolor', 287: 'lynx, catamount', 288: 'leopard, Panthera pardus', 289: 'snow leopard, ounce, Panthera uncia', 290: 'jaguar, panther, Panthera onca, Felis onca', 291: 'lion, king of beasts, Panthera leo', 292: 'tiger, Panthera tigris', 293: 'cheetah, chetah, Acinonyx jubatus',
	# 281 to 293
	cat_labels = set()
	for i in range(281, 294):
		cat_labels.add(i)

	# these labels are to remove drawings, memes/reposts, and images with a lot of text respectively
	bad_labels = {
	917 : 'comic book', 916 : 'web site, website, internet site, site', 921 : 'book jacket, dust cover, dust jacket, dust wrapper'}

	def test_bsky_image(url):
		f = urllib.request.urlopen(url)
		image_data = f.read()
		image = Image.open(BytesIO(image_data))
		inputs = feature_extractor(images=image, return_tensor="pt")
		pixel_values = inputs["pixel_values"]
		pixel_values = np.array(pixel_values)
		pixel_values = torch.tensor(pixel_values)
		outputs = model(pixel_values)
		logits = outputs.logits
		predicted_class_idx = logits.argmax(-1).item()
		sorted_preds = torch.argsort(logits, descending=True)[0]
		top_predictions = [sorted_preds[i].item() for i in range(50)] # 50 is semi-arbitrary based on our findings from testing pics # could see tuning this down to 40 but can't tell if it would pick up more or less cats
		top_values = [logits[0][pred].item() for pred in top_predictions]
		# logger.info('label predictions', top_predictions)
		# logger.info('values of predictions', top_values)
		found_cat_label = -1
		found_bad_label = -1
		bad_labels_found = []
		cat_score = 0
		for i, pred in enumerate(top_predictions):
			predicted_class = model.config.id2label[pred]
			# logger.info(predicted_class)
			if pred in cat_labels:
				if found_cat_label == -1:
					found_cat_label = i
				cat_score += top_values[i]
			if pred in bad_labels:
				if found_bad_label == -1:
					found_bad_label = i
				bad_labels_found.append(pred)
				bad_labels_found.append(bad_labels[pred])
				cat_score -= top_values[i]
		# print(' ')
		logger.info('    found cat label:', found_cat_label)
		logger.info('    found bad label:', found_bad_label, bad_labels_found)
		would_pass = found_cat_label >= 0 and found_bad_label < 0
		# logger.info('AI cat score: ', cat_score)
		# logger.info('    passed cat test:', would_pass)
		return would_pass

	# just getting a previous count of our followers and following for the logs
	try:
		following = client.get_profile(actor=BSKY_USERNAME).follows_count
		followers = client.get_profile(actor=BSKY_USERNAME).followers_count
		prev_stats = f'prior followers: {str(followers)} | previously following: {str(following)}'
		logger.info(prev_stats)
		running_logging_text += prev_stats + LINE_BREAK
	except Exception as e:
		warning = f'WARNING - failed to get previous following and followers count: {e}'
		logger.warning(warning)
		running_logging_text += warning + LINE_BREAK

	# after successfully identifying a cat post, this function likes the post and follows the user, returning the follow uri string
	def like_post_and_add_user(post):
		user_did = post.author.did
		post_cid = post.cid
		post_uri = post.uri
		followed_user = ''
		try:
			followed_user = client.follow(user_did).uri
			liked_post = client.like(uri=post_uri, cid=post_cid).uri
			logger.info(f'      ✓✓✓ ✅ Successfully liked post and followed user: {post.author.handle}')
		except Exception as e:
			logger.info(f'      ✓✓✗ ❌ Failed at either liking post or following user: {post.author.handle}. error: {e}')
		return followed_user

	def get_post_follow_likers(post_uri, like_count, users_followed, max_new_followers):
		# need to try opening post, get the list of likers, iterate through following them, add each to the users added
		new_follows_count = 0
		likes_remaining = like_count
		try:
			while likes_remaining > 0:
				if (likes_remaining > 100):
					logger.info(f'        Starting new page of likes. {likes_remaining} likes remaining to check on this post.')
				limit = min(likes_remaining, 100)
				likes_remaining -= limit
				next_page = ''
				response = client.get_likes(uri = post_uri, limit= limit, cursor= next_page)
				likes = response.likes
				next_page = response.cursor

				for like in likes:
					you_follow_them = like.actor.viewer.following
					you_are_followed_by = like.actor.viewer.followed_by
					user_did = like.actor.did
					user_handle = like.actor.handle
					user_muted = like.actor.viewer.muted
					if user_muted:
						logger.info(f'        User is muted. DO NOT FOLLOW. handle: {user_handle}')
						continue
					elif you_follow_them or you_are_followed_by or user_did == MY_DID or user_did in users_followed:
						logger.info(f'        Already seen user. handle: {user_handle}')
						continue
					else:
						# like the user
						follow_uri = client.follow(user_did).uri
						users_followed.add(user_did) # now we only need to save the user_did in the set instead of the whole string, and so we don't need a whole second already added dids set
						logger.info(f'        Followed post-liker. handle: {user_handle}')
						new_follows_count += 1
						if new_follows_count >= max_new_followers:
							break
				# logger.info(f'from {len(likes)} likes on this post you followed {new_followers_count}, saw {follow_them_count} users you already follow, and saw {followed_by_count} users that already follow you')
		except Exception as e:
			logger.error(f'ERROR: THERE WAS AN ISSUE CHECKING THIS POST FOR LIKES. \n{e}')
		return new_follows_count

	def createPostUrl(feed_post):
		url_handle = feed_post.post.author.handle
		url_ending_index = feed_post.post.uri.find('.feed.post/') + 11
		url_ending = feed_post.post.uri[url_ending_index : ]
		return URL_BEGIN + url_handle + URL_POST + url_ending

	def follow_more_users(post_count, follows_count, feed):
		if post_count == 0 or follows_count == 0:
			return []
		posts_to_check = post_count
		successful_cat_post_like_count = 3
		max_errors_allowed = 5
		next_page = ''
		new_follow_count_from_posts = 0
		new_follow_count_from_likes = 0
		page_count = 0
		users_followed = set()

		logging_posts = 0
		logging_pics = 0
		logging_errors_count = 0
		logging_errors_description = []
		logging_notcat = 0
		logging_cat = 0
		logging_vid = 0
		logging_nomedia = 0
		logging_alreadyfollowed = 0
		logging_mutuals = 0
		logging_myposts = 0
		logging_seenpost = 0
		global running_logging_text
		running_logging_text += f'Feed {FEED_NAME[feed]}:' + LINE_BREAK

		def log_results():
			global running_logging_text
			sum_new_follows = new_follow_count_from_posts + new_follow_count_from_likes
			sum_skipped_posts = logging_seenpost + logging_alreadyfollowed + logging_myposts
			sum_unprocessed = logging_nomedia + logging_vid
			logger.info(f'followed {sum_new_follows} new users. {new_follow_count_from_posts} posters and {new_follow_count_from_likes} likers.')
			running_logging_text += f'  Followed {sum_new_follows} new user{"s" if sum_new_follows != 1 else ""}{"!" if sum_new_follows > 0 else "."}' + LINE_BREAK
			running_logging_text += f'    Of those follows, {new_follow_count_from_posts} were posters and {new_follow_count_from_likes} were from likes.' + LINE_BREAK
			running_logging_text += f'  {logging_posts} posts in total were viewed during this run.' + LINE_BREAK
			running_logging_text += f'  Skipped Posts: ({sum_skipped_posts}) - {logging_seenpost} posts were previously seen, {logging_alreadyfollowed} were from users already followed, {logging_myposts} were your posts.' + LINE_BREAK
			running_logging_text += f'  Mutuals: {logging_mutuals} posts were from users that follow you, and these posts were liked.' + LINE_BREAK
			running_logging_text += f'  Unprocessed: ({sum_unprocessed}) - {logging_nomedia} posts had no media attached, and {logging_vid} posts had videos attached.' + LINE_BREAK
			running_logging_text += f'  Processed: {logging_pics} posts had pics attached: {logging_cat} were identified as cat pics and {logging_notcat} were not cats.' + LINE_BREAK
			running_logging_text += f'  {"No errors were encountered while processing pics." if logging_errors_count == 0 else str(logging_errors_count) + " ERROR(S) ENCOUNTERED PROCESSING PICS FROM THIS FEED"} ' + LINE_BREAK
			if logging_errors_count > 0:
				running_logging_text += '\n'.join(logging_errors_description) + LINE_BREAK

		while posts_to_check > 0:
			logger.info(f'[checking page {page_count} of feed {FEED_NAME[feed]}, {posts_to_check} posts left to check, and have found {new_follow_count_from_posts + new_follow_count_from_likes} new users to follow]')
			page_count += 1
			limit = min(posts_to_check, 100)
			posts_to_check -= limit
			try:
				# logger.info('next page', next_page)
				data = client.app.bsky.feed.get_feed({
						'feed': 'at://did:plc:jfhpnnst6flqway4eaeqzj2a/app.bsky.feed.generator/cats',
						'limit': limit,
						'cursor': next_page
				}, headers={})
				next_page = data.cursor
				# logger.info(data)

				for i, f in enumerate(data.feed):
					you_follow_them = f.post.author.viewer.following
					you_are_followed_by = f.post.author.viewer.followed_by
					did = f.post.author.did
					post_cid = f.post.cid
					logging_posts += 1
					if did == MY_DID:
						logger.info(f'{i} - 😎 skipped. This was your own post.')
						logging_myposts += 1
						continue
					elif post_cid in cached_posts or post_cid in seen_posts:
						logger.info(f'{i} - 👀 skipped. Post with cid {post_cid} has already been viewed. Found in {"db cache" if post_cid in cached_posts else "current set"}.')
						logging_seenpost += 1
						seen_posts.add(post_cid) # now that we're using a dynamodb cache we want to add the cid to the seen posts set so we have it for next run
						continue
					else:
						seen_posts.add(post_cid)
						# TODO: the way I have it if you are following them you never check the photo to see if it's a good one to get the likes from.
						if you_follow_them and you_are_followed_by:
							logger.info(f'{i} 💕 user: {f.post.author.handle} is a mutual follower. Liking this post. {createPostUrl(f)}')
							# this can break if you get rate limited. So far hasn't broken when posts are deleted but should have been wrapped in one just in case
							try:
								liked_post = client.like(uri=f.post.uri, cid=f.post.cid).uri
							except Exception as e:
								logging_errors_count += 1
								logger.error(f'    ✓✗ ‼️ liking post {i} caused an error. {logging_errors_count} errors seen this run.\n{e}')
								logging_errors_description.append(f'pg{page_count} #{i}. {repr(e)}: {e}')
								# got rate limited and know that if you hit like 100 errors or so you'll eventually get a timeout and the entire nootebook will be borked.
								if logging_errors_count >= max_errors_allowed:
									logger.error(f'seen more errors ({logging_errors_count}) than the acceptable number of errors ({max_errors_allowed}). terminating run.')
									log_results()
									return users_followed
							logging_mutuals += 1
						elif did in users_followed:
							logger.info(f'{i} ✗ 👀 user: {f.post.author.handle} was already followed in this batch.')
							logging_alreadyfollowed += 1
						elif you_follow_them or you_are_followed_by:
							logger.info(f'{i} ✗ 👀 user: {f.post.author.handle} {"already follows you." if you_are_followed_by else ""}{"is already being followed." if you_follow_them else ""}')
							logging_alreadyfollowed += 1
						elif not f.post.embed or f.post.embed.py_type != EMBEDDED_PIC:
							if f.post.embed.py_type == EMBEDDED_VID:
								logger.info(f'{i} ✗ 🎥 video post: {createPostUrl(f)}')
								logging_vid += 1
							else:
								logger.info(f'{i} ✗ 🔲 no pic for post {i}')
								logging_nomedia += 1
						else:
							logger.info(i, '✓', '📷', f.post.embed.images[0].fullsize)
							logger.info(f'    post: {createPostUrl(f)}')
							logging_pics += 1
							try:
								handle = f.post.author.handle
								logger.info(f'    user: {handle}')
								is_cat = test_bsky_image(f.post.embed.images[0].fullsize)
								if is_cat:
									logger.info(f'    ✓✓ 😺 successfully found cat pic at post {i}. It has {f.post.like_count} likes.')
									new_follow_count_from_posts += 1
									logging_cat += 1
									follow_uri = like_post_and_add_user(f.post)
									users_followed.add(did)
									# so we have a cat post. If it is a solid or particularly good cat post it should probably have a lot of likes, and we can go in and follow all those likers
									if f.post.like_count >= successful_cat_post_like_count:
										logger.info(f'      👍🏻 This cat post got {f.post.like_count}, and I would call it successful, so following its likers.')
										likers_added = get_post_follow_likers(f.post.uri, f.post.like_count, users_followed, follows_count - (new_follow_count_from_posts + new_follow_count_from_likes))
										logger.info(f'      {"✅" if likers_added > 0 else "0️⃣"} Added {likers_added} users that liked that post.')
										new_follow_count_from_likes += likers_added
									if new_follow_count_from_posts + new_follow_count_from_likes >= follows_count:
										logger.info(f'Successfully followed the desired number of new users! terminating run.') # break wasn't working here, it kept going around to the while loop instead
										log_results()
										return users_followed
								else:
									logger.info(f'    ✓✗ ❌ post {i} was not a cat pic')
									logging_notcat += 1
							except Exception as e:
								logging_errors_count += 1
								logger.error(f'    ✓✗ ❓ post {i} image caused an error. {logging_errors_count} errors seen this run.\n{e}')
								logger.error(f'errors ({logging_errors_count}), acceptable number of errors ({max_errors_allowed}).')
								logging_errors_description.append(f'pg{page_count} #{i}. {repr(e)}: {e}')
								# got rate limited and know that if you hit like 100 errors or so you'll eventually get a timeout and the entire notebook will be borked.
								if logging_errors_count >= max_errors_allowed:
									logger.error(f'seen more errors ({logging_errors_count}) than the acceptable number of errors ({max_errors_allowed}). terminating run.')
									log_results()
									return users_followed
			except Exception as e:
				logger.error(f'error encountered from trying to get feed. terminating run.\n{repr(e)}: {e}')
				logging_errors_count += 1
				logging_errors_description.append(f'CRITICAL ERROR ENCOUNTERED WHILE GETTING FEED:\n{repr(e)}: {e}')
				log_results()
				return users_followed
		log_results()
		return users_followed

		# logger.info(data.feed[0].post.embed.images[0].fullsize)

	# FINALLY THE ACTUAL RUN! determine whether you're checking the caturday feed or the regular cat feed
	ddb_attr_run_timestamp = str(datetime.datetime.now(zoneinfo.ZoneInfo(USER_TIMEZONE))) # you don't want to use the static string attribute because what if you re-run this cell? won't be necessary when automated though.
	is_caturday = dow == CATURDAY_DOW
	followed_users = set()
	if is_caturday:
		logger.info("IT'S CATURDAY! Checking the Caturday feed for new followers.")
		followed_users = follow_more_users(POSTS_CATURDAY, FOLLOWS_CATURDAY, FEED_CATURDAY)
	else:
		logger.info("Just a regular day, but we're still following more cats. :3")
		followed_users = follow_more_users(POSTS_OTHERCAT, FOLLOWS_OTHERCAT, FEED_REGDAY)

	# just in case we said we followed ourselves somehow, we'll discard that value
	followed_users.discard(MY_DID)
	ddb_update_follows_failed = False
	# and add the results to our dynamodb
	if len(followed_users) > 0:
		try:
			table.update_item(
					Key={'DOW': ddb_key},
					UpdateExpression='SET #attr = :val',
					ExpressionAttributeNames={
							'#attr': ddb_attr_run_timestamp
					},
					ExpressionAttributeValues={
							':val': followed_users
					}
			)
		except Exception as e:
			err = f'ERROR - failed to store followed users in dynamodb.\n{e}'
			logger.error(err)
			running_logging_text += err + LINE_BREAK
			# no point in returning here, might as well try to at least update the cache and do the github logging, but we'll flag it
			ddb_update_follows_failed = True


	# now we also need to update the cached posts with what we saw this run
	ddb_update_cache_failed = False
	try:
		table.update_item(
				Key={'DOW': DDB_CACHE_KEY},
				UpdateExpression='SET #attr = :val',
				ExpressionAttributeNames={
						'#attr': DDB_CACHE_ATTRIBUTE
				},
				ExpressionAttributeValues={
						':val': seen_posts
				}
		)
	except Exception as e:
		warning = f'WARNING - failed to store followed users in dynamodb.\n{e}'
		logger.warning(warning)
		running_logging_text += warning + LINE_BREAK
		# same here, we won't end early to try to log in github, but we can flag it for the response
		ddb_update_cache_failed = True

	end_timestamp = datetime.datetime.now(zoneinfo.ZoneInfo(USER_TIMEZONE))
	time_diff = end_timestamp - cur_timestamp
	running_logging_text += f'time diff: {str(time_diff)} | completed run at: {str(end_timestamp)}' + LINE_BREAK
	logger.info(f'end timestamp: {end_timestamp}\ntime diff (runtime): {time_diff}')

	# now get an updated count of our followers and following for the logs
	try:
		following = client.get_profile(actor=BSKY_USERNAME).follows_count
		followers = client.get_profile(actor=BSKY_USERNAME).followers_count
		cur_stats = f'cur followers: {str(followers)} | now following: {str(following)}'
		logger.info(cur_stats)
		running_logging_text += cur_stats + LINE_BREAK
	except Exception as e:
		warning = 'WARNING - failed to get updated following and followers count: {e}'
		logger.warning(warning)
		running_logging_text += warning + LINE_BREAK

	# a successful run! add our logging string
	logging_add(running_logging_text)

	if ddb_update_follows_failed or ddb_update_cache_failed:
		result = f'Follows were added successfully, but failed to update the dynamoDB with the new {"follows added" if ddb_update_follows_failed else ""}{" and " if ddb_update_follows_failed and ddb_update_cache_failed else ""}{"posts cache" if ddb_update_cache_failed else ""}.'
		return {
				'statusCode': 207,
				'body': json.dumps(result)
			}

	return {
		'statusCode': 200,
		'body': json.dumps(f'Successfully added {len(followed_users)} follows.')
	}
