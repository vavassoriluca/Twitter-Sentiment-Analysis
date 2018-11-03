require 'tweetstream'
require 'ruby-kafka'

def start_tweet_stream tracks
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
    TweetStream::Client.new.track(tracks) do |status|
      p "#{status.text}"
      print "\n\n\n"
      producer.produce(status.text, topic: topic)
      producer.deliver_messages
    end
    
  ensure
      producer.deliver_messages
      producer.shutdown
  end
end