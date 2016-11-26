'use strict';

const TwitterStream = require('node-tweet-stream');
const crate = require('node-crate');
const cratePersist = require('./crate/cratePersist');
const crateConfig = require('./settings/crateConfig');
const twitterSettings = require('./settings/twitter');

const keys = ['Referendum', 'referendum'];

crate.connect('localhost', 4200);
cratePersist.executeStatement(crateConfig.tweetsTable);

const stream = new TwitterStream(twitterSettings.authSetting);

stream.on('tweet', (tweet) => {
  // filter just the italian twitter
  if(tweet.lang === 'it') {
    tweet.created_at = new Date(tweet.created_at);
    tweet.user.created_at = new Date(tweet.user.created_at);
    cratePersist.insertTweet('tweets_test', tweet);
  }
});

// register tracker for multiple keys
keys.forEach((k) => {
  stream.track(k);
});
