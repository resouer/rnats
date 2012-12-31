require '../lib/rnats/rnats'
require 'amqp'
# This is the basic functional test for RNATS class
#
# NOTE: we need to subscribe 'exit' at the very last: 
# queue will process every publish following subscriber declare subsequence
describe RNATS do
  it 'should handle on_connect block call' do
    msgs = []
    EM.run do
      RNATS.start
      
      RNATS.on_connect do
        msgs << 'connected'
        RNATS.stop {EM.stop}
      end
    end
    msgs.should == ['connected']
  end

  it 'should dispatch messages to multi-consumers by round roubin' do
     msgs = []
     EM.run do
       RNATS.start do
          RNATS.harryz_sub{|msg| puts "111mesg is dispatched to consumer1"
          msgs << msg}
          RNATS.harryz_sub{|msg| puts "222mesg is dispatched to consumer2"
          msgs << msg}
          RNATS.harryz_sub{|msg| puts "333mesg is dispatched to consumer3"
          msgs << msg}
          RNATS.harryz_sub{|msg| puts "444mesg is dispatched to consumer4"
          msgs << msg}
          RNATS.harryz_sub{|msg| puts "555mesg is dispatched to consumer5"
          msgs << msg}                 
          RNATS.subscribe('exit') { RNATS.stop {EM.stop} }
          
          RNATS.harryz_pub('mesg1')
          RNATS.harryz_pub('mesg2')
          RNATS.harryz_pub('mesg3')  
          RNATS.harryz_pub('mesg4')
          RNATS.harryz_pub('mesg5')
          
          RNATS.subscribe('a.*') { |msg,_, topic| msgs << topic }
          RNATS.subscribe('a.*.a') { |msg,_,topic| msgs << topic }
          RNATS.subscribe('a.a.#') { |msg,_,topic| msgs << topic }
          RNATS.subscribe('a.*') { |msg,_, topic| msgs << topic }
          RNATS.subscribe('a.*.a') { |msg,_,topic| msgs << topic }
          RNATS.subscribe('a.a.#') { |msg,_,topic| msgs << topic }   
          RNATS.subscribe('a.*') { |msg,_, topic| msgs << topic }
          RNATS.subscribe('a.*.a') { |msg,_,topic| msgs << topic }
          RNATS.subscribe('a.a.#') { |msg,_,topic| msgs << topic }
          RNATS.subscribe('a.*') { |msg,_, topic| msgs << topic }
          RNATS.subscribe('a.*.a') { |msg,_,topic| msgs << topic }
          RNATS.subscribe('a.a.#') { |msg,_,topic| msgs << topic }           
          RNATS.publish('exit')
       end     
     end
     puts "hahaha:#{msgs}"
     msgs.should == ['mesg1','mesg2','mesg3','mesg4','mesg5']
  end
  

  it 'should handle basic pub/sub as well as empty payload in block' do
    msgs = []
    EM.run do
      RNATS.start do
        RNATS.subscribe('x.y') { |msg| msgs << msg }
        RNATS.subscribe('exit') { RNATS.stop {EM.stop} }
        
        RNATS.publish('x.y')
        RNATS.publish('x.y', 'body')
        RNATS.publish('x.y.z', 'error')
        RNATS.publish('exit')
      end
    end
    msgs.should == ['', 'body']
  end
  
  it 'should handle wildcard subscriptions' do
    msgs = []
    EM.run do
      RNATS.start
      
      RNATS.subscribe('#') { |msg| msgs << msg }
      RNATS.subscribe('a.*') { |msg| msgs << msg }
      RNATS.subscribe('exit') { RNATS.stop {EM.stop} } 
      
      RNATS.publish('a', 'a')
      RNATS.publish('b', 'b')
      RNATS.publish('a.b', 'a.b')
      RNATS.publish('exit')
    end
    # NOTE: the default match sequence is by TOPIC!!! So we have to sort it
    msgs.sort.should == ['', 'a', 'a.b', 'a.b', 'b']
  end
  
  
  
  it 'should not complain when publishing to nil' do
    msgs = []
    EM.run do
      RNATS.start
      
      RNATS.subscribe(nil) { |msg| msgs << msg }
      RNATS.subscribe('exit') { RNATS.stop {EM.stop} }
      
      RNATS.publish(nil)
      RNATS.publish(nil, 'hello')
      RNATS.publish('exit')
    end
    msgs.should == ['', 'hello']
  end
 
  it 'should be able to do unsubscribe' do
    msgs = []
    EM.run do
      RNATS.start
      
      # NOTE: this is how to use unsubscribe in CF. The other case is in Kernel.at_exit 
      # unsubscribe must be in the callback block like this  
      s = RNATS.subscribe('x.y') do |msg| 
        msgs << msg
        RNATS.unsubscribe(s)
        RNATS.publish('x.y', 'b')
        RNATS.publish('exit')
      end
      
      RNATS.subscribe('exit') { RNATS.stop {EM.stop}}      
      RNATS.publish('x.y', 'a')
            
    end
    msgs.should == ['a']
  end
  
  it 'should handle complex wildcard subscriptions' do
    msgs = []
    EM::run do
      RNATS.start
      RNATS.subscribe('a.*') { |msg,_, topic| msgs << topic }
      RNATS.subscribe('a.*.a') { |msg,_,topic| msgs << topic }
      RNATS.subscribe('a.a.#') { |msg,_,topic| msgs << topic }
      RNATS.subscribe('exit') { RNATS.stop {EM.stop} }
      
      RNATS.publish('a')
      RNATS.publish('a.a')
      RNATS.publish('a.a.a')
      RNATS.publish('a.a.a.a')
      RNATS.publish('exit')
    end
    msgs.sort.should == ['a.a', 'a.a', 'a.a.a', 'a.a.a', 'a.a.a.a']
  end
  
  it 'should recieve a proper response when requested' do
    msgs = []
    EM::run do
      RNATS.start
      RNATS.subscribe('a') do |msg, reply_to|
        RNATS.reply(reply_to, msg + '_r')
      end
      RNATS.request('a', 'a') { |msg| msgs << msg }
      RNATS.request('a', 'b') { |msg| msgs << msg }
      RNATS.request('a', 'c') { RNATS.stop {EM.stop} }
    end
    msgs.should == ['a_r', 'b_r']
  end

  
  it 'should handle timeout when requested' do
    msgs = []
    
    EM::run do
      RNATS.start
      RNATS.subscribe('a') do |msg, reply_to|
        EM.add_timer(2){
          RNATS.reply(reply_to, msg + '_r')
        }
      end
      RNATS.request('a', 'a', :timeout => 1) { |msg| msgs << msg }
      RNATS.request('a', 'b', :timeout => 5) { |msg| msgs << msg }
      RNATS.request('a', 'c') { RNATS.stop {EM.stop} }
    end
    msgs.should == [:time_out, 'b_r']
  end

  it 'should handle on reconnect block call' do
    msgs = []
    EM.run do
      RNATS.start
      RNATS.reconnect_time_wait = 2
      
      #TODO on_error needs to be implement
      
      RNATS.on_reconnect do
        msgs << RNATS.reconnecting
        msgs << 'reconnecting'
        RNATS.stop {EM.stop}
      end
      
      EM.add_timer(1) {`sudo rabbitmqctl stop`}
      
    end
    msgs.should == [true, 'reconnecting']
  end 

end
