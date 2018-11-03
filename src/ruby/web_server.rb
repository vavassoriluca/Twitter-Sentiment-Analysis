require 'sinatra'
require_relative 'kafka-producer.rb'
require 'thread'
require 'chartkick'


template :layout do
  <<LAYOUT
<html>
  <head>
    <script src="http://www.google.com/jsapi"></script>
    <script src="chartkick.js"></script>
  </head>
  <body>
    <%= yield %>
  </body>
</html>
LAYOUT
end

template :index do
  <<INDEX
    <%= pie_chart({"Football" => 10, "Basketball" => 5}) %>
INDEX
end

get '/' do
  erb :index
end

get '/ss' do #start stream
  t1 = Thread.new(start_tweet_stream(["brexit", "uk"]))
  redirect "/"
end
  

