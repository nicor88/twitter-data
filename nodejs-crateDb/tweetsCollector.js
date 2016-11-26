'use strict';

const TwitterStream = require('node-tweet-stream');
const crate = require('node-crate');
const settings = require('./settings');

crate.connect('localhost', 4200);

// todo define a tweet table here
const scheme = { tweets_test: { title: 'string', author: 'string' } };
crate.create(scheme).success((res) => {
  console.log(res);
}).error((error) => {
  console.log(error);
});

const stream = new TwitterStream(settings.twitterAuthSetting);

stream.on('tweet', (tweet) => {
  console.log(Object.keys(tweet));
  console.log(tweet);
});

stream.track('referendum');
