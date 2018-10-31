require 'tweetstream'
require 'ruby-kafka'
require 'pp'

TweetStream.configure do |config|
  config.consumer_key       =  ENV['consumer_key']
  config.consumer_secret    =  ENV['consumer_secret']
  config.oauth_token        = ENV['access_token']
  config.oauth_token_secret = ENV['access_token_secret']
  config.auth_method        = :oauth
end

logger = Logger.new($stderr)
brokers = ["localhost:9092"]

topic = "twitter"

kafka = Kafka.new(brokers, client_id: "twitter-producer", logger: logger)

producer = kafka.producer

begin
  TweetStream::Client.new.track('italy', 'italian') do |status|
    p "#{status.inspect}"
    print "\n\n\n"
    producer.produce(status.text, topic: topic)
    producer.deliver_messages
  end
  
ensure
    producer.deliver_messages
    producer.shutdown
end
