require 'sinatra'
require_relative 'kafka-producer.rb'
require 'thread'
require 'chartkick'

@@data = {}

get '/' do
  @data = @@data
  erb :index
end

get '/ss' do #start stream
  t1 = Thread.new { start_tweet_stream(["brexit", "uk"])}
  redirect "/"
end

post '/payload' do
  @@data = JSON.parse(request.body.read)
  puts "post /pyload:  #{@@data.inspect}"
end
  

