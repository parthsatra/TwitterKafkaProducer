
var express = require('express');
var app = express();

// Creating server socket and listening for client on port 3000.
// Mainly uses 'http' and 'socket.io'
var server = require('http').createServer(app);
var port = 3000;
server.listen(port);
console.log("Server Socket started and listening on port : " + port);

// Using 'Twit' node.js library to fetch twitter stream of tweets currently getting tweeted.
var Twit = require('twit');

// Passing the Twitter Credentials stored as environment variables.
var T = new Twit({
    consumer_key: process.env.TWITTER_CONSUMER_KEY,
    consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
    access_token: process.env.TWITTER_ACCESS_TOKEN,
    access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
});

//Query to fetch the twitter stream with filter Love and Hate.
var stream = T.stream('statuses/filter', { track: ['love', 'hate'] });

//Listening to 'tweet' event which sends callback whenever someone sends a tweet with the specified filter.
stream.on('tweet', function(tweet){
    console.log(tweet);
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});


module.exports = app;
