# Nodejs Tweets collector

## dependencies
*   Node.js
*   npm
*   Python 2.7

## Install
<pre>
npm install
</pre>

## Setup
*  Install dependencies `npm install`
*  Register your app on [Twitter](https://apps.twitter.com/) and get the tokens
*  Configure __settings/twitterAuth.js__ with this content:
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

## Start App
<pre>
docker-compose up
npm run collect
</pre>

You can connect to the crate interface at  `localhost:4200`

## Links
*   [node-crate](https://www.npmjs.com/package/node-crate)

## TODO
*   Dockerize Node.js App
