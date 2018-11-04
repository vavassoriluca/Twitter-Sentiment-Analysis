require 'sinatra'
require_relative 'kafka-producer.rb'
require 'thread'
require 'chartkick'

@@data = {"Nothing" => 10}
@@trackers = ["brexit", "uk"]
@@chart_data = {}


get '/' do
  @trackers  = @@trackers
  begin
    @data         = @@data.clone
    @data.delete("tweets") 
    @data.delete("avg")
    @average      = @@data["avg"]
    @total_tweets = @@data["tweets"]
  rescue Exception => e
    puts e
  end
  erb :index
end


get '/ss' do #start stream
  puts "started streaming"
  @@t1 = Thread.new { start_tweet_stream(@@trackers)}
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
  @@chart_data = @@data.clone
  @@chart_data.delete("tweets") 
  @@chart_data.delete("avg")
  @@chart_data.to_json
end
  

get '/input' do
  @input = params[:input]
  @@trackers = @input.split
  redirect "/"
end

get '/tweetsavg' do
  content_type :json
  @@tw_avg = @@data.clone
  @@tw_avg.delete("positive") 
  @@tw_avg.delete("negative")
  @@tw_avg.delete("neutral")
  @@tw_avg.to_json
end
