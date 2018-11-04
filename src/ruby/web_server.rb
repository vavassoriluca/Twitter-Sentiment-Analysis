require 'sinatra'
require_relative 'kafka-producer.rb'
require 'thread'
require 'chartkick'

@@data = {test:10}

get '/' do
  @data = @@data
  erb :index
end

get '/ss' do #start stream
  puts "started streaming"
  @@t1 = Thread.new { start_tweet_stream(["brexit", "uk"])}
  redirect "/"
end

get '/stop' do
  puts "stopped streaming"
  begin
    Thread.kill(@@t1)
  rescue
    puts "no streaming started"
  end
  redirect "/"
end

post '/payload' do
  @@data = JSON.parse(request.body.read)
  puts "post /payload:  #{@@data.inspect}"
end

get '/json' do
  content_type :json
  @@data.to_json
end
  

