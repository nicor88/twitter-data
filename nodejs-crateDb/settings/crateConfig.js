const tweetsTable = 'create table tweets_test ( ' +
    'id string,' +
    'created_at timestamp,' +
    'retweeted boolean,' +
    'source string,' +
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
module.exports = {
  tweetsTable: tweetsTable
};
