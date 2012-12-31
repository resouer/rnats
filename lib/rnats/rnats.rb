require 'rubygems'

require 'eventmachine'
require 'amqp'

module RNATS

  class << self
  
    attr_reader :client, :reconnecting
    attr_writer :reconnect_time_wait
  
    def connect(amqp_url=nil, opts={}, &blk)
      # TODO need to use ENV here! see @nats client connect
    end
  
    def start(amqp_url=nil, opts={}, &blk)
	    raise 'EM is not running! RNATS can only be used within EM.run' unless EM.reactor_running?
	    opts = AMQP::Client.parse_amqp_url(amqp_url || 'amqp:///').merge(opts)
	    # we do not use block, because we have to use return value, otherwise, the afterwards will have eager usage.
	    @client = AMQP.connect(opts)do |connection, open_ok|
        @reconnecting = false
        @on_connect.call if @on_connect
      end
	  
      @client.on_error do |conn, connection_close|
        @on_error.call(connection_close.reply_code, connection_close.reply_text) if @on_error
        puts "[connection.close] Reply code = #{connection_close.reply_code}, reply text = #{connection_close.reply_text}"
        if connection_close.reply_code == 320
          @reconnecting = true
          @on_reconnect.call if @on_reconnect
          puts "[connection.close] Setting up a periodic reconnection timer every #{@reconnect_time_wait} seconds"
          # every 30 seconds default
          conn.periodically_reconnect(@reconnect_time_wait || 30)
        end
      end
    
      @on_connect = blk
      # the channel will be delayed until connection is established
	    @channel = AMQP::Channel.new(@client)  
	    @channel.on_error do |ch, channel_close|
        puts "Channel-level error: #{channel_close.reply_text}, shutting down..."
        @client.close { EventMachine.stop }
      end
      @exchange_name = opts[:exchange] || 'rnats'
	    @exchange = @channel.topic(@exchange_name,:auto_delete => true)
	    
	    
      @harryz_channel=AMQP::Channel.new(@client)
      @harryz_channel.on_error do |ch, channel_close|
        puts "Channel-level error: #{channel_close.reply_text}, shutting down..."
        @client.close { EventMachine.stop }
      end
      @harryz_channel.prefetch 1
=begin
by harry:this means we wouldn't dispatch a new message to a worker until it has process or acknowledge the prevous one.Instead it will dispatch it to the next worker(consumer)
=end
      @harryz_queue    = @harryz_channel.queue("amqpgem.examples.hello_world", :auto_delete => true)
      @harryz_exchange = @harryz_channel.default_exchange
      
    end
=begin
the attribute-exclusive means the queue can only be used by one consumer
to share one queue by multi-consumers,we need to initialize a new consumer every time we subscribe
like belows
=end  
    def harryz_sub(&blk)
        puts "here harry_sub is called!"
        consumer1=AMQP::Consumer.new(@harryz_channel,@harryz_queue)       
        consumer1.consume.on_delivery do |metadata,payload| 
           metadata.ack
           puts "Received a message: #{payload}. "
           blk.call(payload)
        end
    end
    
    def harryz_pub(message)
        puts "harryz_pub is called,published a message: #{message}"
        @harryz_exchange.publish message, :routing_key => @harryz_queue.name
    end
    
    def publish(topic, body = '', opts = {}, &blk)
      puts "[pub] #{topic}, #{body}"	  
	    @exchange.publish(' ' + body.to_s, {:key => (topic || ''), :content_type => 'text/plain'}.merge(opts))
	    blk.call if blk
    end
    
    # TODO there's no :queue in sub. Two way:
    # 1. add :queue in this client (not good...)
    # 2. add :queue in ruby-amqp (good but how)
    # 3. add :queue in broker (good but not easy)
    def subscribe(topic, &blk)
      self.start unless @client
      q = @channel.queue("#{@exchange_name}.#{(rand*2E14).floor}", :auto_delete => true, :exclusive => true)
      if blk.arity == 3
        puts "[subs3] #{topic}"
        q.bind(@exchange, :key => (topic || '')).subscribe(:ack => true) do |metadata, payload|
          body = payload[1..-1]
          puts "[subs3] #{topic}, #{payload}"
          blk.call(body, metadata.reply_to, metadata.routing_key)
        end
      elsif blk.arity == 2
        puts "[subs2] #{topic}"
        q.bind(@exchange, :key => (topic || '')).subscribe(:ack => true) do |metadata, payload|
          body = payload[1..-1]
          puts "[subs2] #{topic}, #{payload}"
          blk.call(body, metadata)
        end
      elsif blk.arity == 1
        puts "[subs1] #{topic}"
        q.bind(@exchange, :key => (topic || '')).subscribe do |metadata, payload|
          body = payload[1..-1]
          puts "[subs1] #{topic}, #{payload}"
          blk.call(body)
        end
      elsif blk.arity == 0
        puts "[subs0] #{topic}"
        q.bind(@exchange, :key => (topic || '')).subscribe do |metadata, payload|
          puts "[subs0] #{topic}, #{payload}"
          blk.call
        end
      end
      q
    end
  
    def reply(metadata, body = '', opts = {},  &blk)
      # NOTE: we must use default_exchange here. if not, reply will not work
      @channel.default_exchange.publish( body.to_s, :routing_key => metadata.reply_to, :correlation_id => metadata.message_id, :mandatory => true)
      metadata.ack
      blk.call if blk
    end
  
    def request(topic, data=nil, opts={},  &blk)
      self.start unless @client
      timeout  = opts[:timeout]  || -1
      replies_queue = @channel.queue("#{@exchange_name}.#{(rand*2E14).floor}", :exclusive => true, :auto_delete => true)
      start = Time.now
      replies_queue.subscribe do |metadata, payload|
        finish = Time.now
        if timeout > 0 && (finish - start) >timeout
          blk.call(:time_out) if blk
        else
          blk.call(payload) if blk
        end
      end
      @exchange.publish( ' ' + data.to_s, :routing_key =>  topic, :message_id => Kernel.rand(10101010).to_s, :reply_to => replies_queue.name)
    end
  
    def unsubscribe q
      puts "[unsub] #{q.name}"
      q.unsubscribe
    end
  
    def stop &blk
      @client.close &blk
    end
  
    def on_connect &blk
      @on_connect = blk
    end
  
    def on_reconnect &blk
      @on_reconnect = blk
    end
  
    def on_error &blk
      @on_error = blk
    end
  
  end
end

require 'uri'

module AMQP
  module Client
    def self.parse_amqp_url(amqp_url)
      uri = URI.parse(amqp_url)
      raise("amqp:// uri required!") unless %w{amqp amqps}.include? uri.scheme
      opts = {}
      opts[:user] = URI.unescape(uri.user) if uri.user
      opts[:pass] = URI.unescape(uri.password) if uri.password
      opts[:vhost] = URI.unescape(uri.path) if uri.path
      opts[:host] = uri.host if uri.host
      opts[:port] = uri.port ? uri.port :
                      {"amqp"=>5672, "amqps"=>5671}[uri.scheme]
      opts[:ssl] = uri.scheme == "amqps"
      return opts
    end
  end
end
