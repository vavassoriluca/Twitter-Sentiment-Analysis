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

# Make sure to create this topic in your Kafka cluster or configure the
# cluster to auto-create topics.
topic = "twitter"

kafka = Kafka.new(brokers, client_id: "twitter-producer", logger: logger)

producer = kafka.producer

trackers = ['Drake']

begin
    TweetStream::Client.new.track(trackers) do |status|
      p "#{status.user.name}"
      p "#{status.text}"
      print "\n\n\n"
      producer.produce(status.text, topic: topic)
      producer.deliver_messages
    end
    # Send messages for every 10 lines.
    # producer.deliver_messages if index % 10 == 0 
ensure
  producer.deliver_messages
  producer.shutdown
end
