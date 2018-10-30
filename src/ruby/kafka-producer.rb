require 'tweetstream'
require 'kafka'

TweetStream.configure do |config|
  config.consumer_key       =  ENV['consumer_key']
  config.consumer_secret    =  ENV['consumer_secret']
  config.oauth_token        = ENV['access_token']
  config.oauth_token_secret = ENV['access_token_secret']
  config.auth_method        = :oauth
end

TweetStream::Client.new.sample do |status|
  puts "#{status.text}"
end




# logger = Logger.new($stderr)
# brokers = ENV.fetch("KAFKA_BROKERS")

# # Make sure to create this topic in your Kafka cluster or configure the
# # cluster to auto-create topics.
# topic = "text"

# kafka = Kafka.new(
#   seed_brokers: brokers,
#   client_id: "simple-producer",
#   logger: logger,
# )

# producer = kafka.producer

# begin
#   $stdin.each_with_index do |line, index|
#     producer.produce(line, topic: topic)

#     # Send messages for every 10 lines.
#     producer.deliver_messages if index % 10 == 0
#   end
# ensure
#   # Make sure to send any remaining messages.
#   producer.deliver_messages

#   producer.shutdown
# end