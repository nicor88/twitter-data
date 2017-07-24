'use strict';

const TwitterStream = require('node-tweet-stream');
const salient = require('salient');
const crate = require('node-crate');
const cratePersist = require('./crate/cratePersist');
const twitterAuth = require('./settings/twitterAuth');

const analyser = new salient.sentiment.BayesSentimentAnalyser();
const keys = ['aws'];
const tableName = 'tweets';

crate.connect('localhost', 4200);

cratePersist.executeStatement(cratePersist.createTweetsTable(tableName));

const stream = new TwitterStream(twitterAuth.authSetting);

stream.on('tweet', (tweet) => {
  console.log(tweet);
  tweet.created_at = new Date(tweet.created_at);
  tweet.user.created_at = new Date(tweet.user.created_at);
  const sentimentScore = analyser.classify(tweet.text);
  tweet.sentiment_tweet_score = sentimentScore;
  console.log(tweet);
  cratePersist.insertTweet(tableName, tweet);
});

// register tracker for multiple keys
keys.forEach((k) => {
  stream.track(k);
});
