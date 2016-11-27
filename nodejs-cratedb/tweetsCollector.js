'use strict';

const TwitterStream = require('node-tweet-stream');
const salient = require('salient');
const crate = require('node-crate');
const cratePersist = require('./crate/cratePersist');
const crateConfig = require('./settings/crateConfig');
const twitterSettings = require('./settings/twitter');

const analyser = new salient.sentiment.BayesSentimentAnalyser();
const keys = ['Referendum', 'votosi', 'votono'];

crate.connect('localhost', 4200);

cratePersist.executeStatement(crateConfig.tweetsTable);

const stream = new TwitterStream(twitterSettings.authSetting);

stream.on('tweet', (tweet) => {
  tweet.created_at = new Date(tweet.created_at);
  tweet.user.created_at = new Date(tweet.user.created_at);
  const sentimentScore = analyser.classify(tweet.text);
  tweet.sentiment_tweet_score = sentimentScore;
  cratePersist.insertTweet('tweets_test', tweet);
});

// register tracker for multiple keys
keys.forEach((k) => {
  stream.track(k);
});
