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
    console.log(res);
  }).error((err) => {
    console.log(err);
  });
};

module.exports = {
  executeStatement: executeStatement,
  insertTweet: insertTweet
};
