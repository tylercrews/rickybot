# rickybot
Rickybot is a Bluesky (social media) automation routine intended to manage account growth.

* it browses feeds (lists of posts) to identify posts with media that aligns with your interests
- - (so long as those interests are cats. It uses a Visual Transformer to create labels of images in posts, and I've written an assessment algorithm to see if they are 'real' cat posts. Real being not memes and not drawings. I plan on trying to make this more flexible in the future, but might only be feasible to tune it to a specific use-case basis.)
* it follows users with a high likelihood of reciprocating follows and improving your engagement rate
- - (when a cat post is identified it likes the post, follows the user, and follows users that liked that post if the likes are above a certain threshold)
* performs follower management
- - (after a week the last week's follows are evaluated, and any users that did not follow back are pruned so that your follow:follower ratio does not get out of control)
* automates its tasks using AWS services

To run this program you need to have: 
* AWS account with SecretsManager, S3, and DynamoDB set up.
* A huggingface login (not necessary since ViT is public but it helps).
* A github repository set up if you want to use the logging features.
* Your own BlueSky account
Downloading and running these colabs should not allow you to actually run the code without plugging in colab secrets and/or aws secretmanager secrets where necessary. 
