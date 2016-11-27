# Nodejs Tweets collector
## dependencies
*   Node.js
*   npm
*   Python 2.7

## Install
<pre>
npm install
</pre>

## Twitter Auth
Register your app on [Twitter](https://apps.twitter.com/) and get the tokens
Inside settings create a file named __twitter.js__ with this content:

<pre>
module.exports = {
  authSetting: {
    consumer_key: 'your consumer key',
    consumer_secret: 'your consumer secret',
    token: 'your auth token',
    token_secret: 'your secret token'
  }
};
</pre>
