'use strict';

const crate = require('node-crate');

const insertTweet = (tableName, tweet) => {
  crate.insert(tableName, tweet)
  .success(() => {
    console.log(new Date().toISOString() + ' => record inserted successfully!');
    // console.log(res);
  }).error((err) => {
    console.log(err);
    // TODO log error into a specific table
  });
};

const executeStatement = (statement) => {
  crate.execute(statement).success((res) => {
    console.log(statement + ' executed');
  }).error((err) => {
    console.log(err);
  });
};

const createTweetsTable = (tableName) => {
  return 'create table '+ tableName+ ' ( ' +
    'id string,' +
    'created_at timestamp,' +
    'retweeted boolean,' +
    'source string,' +
    'sentiment_tweet_score float,' +
    'text string index using fulltext with (analyzer = "standard"),' +
    'user object(dynamic) as ( ' +
      'id string,' +
      'created_at timestamp,' +
      'description string,' +
      'location string,' +
      'followers_count integer,' +
      'statuses_count integer,' +
      'friends_count integer,' +
      'verified boolean' +
    ')' +
  ')';
};

module.exports = {
  createTweetsTable: createTweetsTable,
  executeStatement: executeStatement,
  insertTweet: insertTweet
};
