require 'celluloid'

module PipelineActor
  attr_accessor :next_in_pipeline

  def do_process item
    puts "do_process #{item}"
    result = Celluloid::Actor.current.process item
    
    if @next_in_pipeline
      result = do_process_next result
    end

    return result
  end

  def do_process_next item
    puts "do_process_next #{item}"
      result = @next_in_pipeline.do_process item if @next_in_pipeline
  end

end

module PipelineSource
  include PipelineActor

  def process item
    Celluloid::Actor.current.do_process_next item
  end
end

class PipelineSupervisionGroup < Celluloid::SupervisionGroup
  alias :add_without_pipeline :add

  def add klass, options
    existing_end_of_pipeline_member = @members.last
    add_without_pipeline klass, options
    existing_end_of_pipeline_member.actor.next_in_pipeline = @members.last.actor if existing_end_of_pipeline_member
  end
end

class BrentActorSource
  include Celluloid
  include PipelineActor

  def toss
    throw "Urgh!"
  end

  def process str
    puts "Processing [#{str}] in BrentActorSource"
    return "Brent actor processed: [#{str}]"
  end
end

class SlowActor
  include Celluloid
  include PipelineActor

  def process str
    puts "Processing [#{str}] in SlowActorSource"
    return "Processed slow: #{str}"
  end
end



class MyPipeline < PipelineSupervisionGroup 
  supervise BrentActorSource, :as => :head
  pool SlowActor, :size => 2, :as => :slow_actor
  puts "Calling inside MyPipeline"
end

pipeline = MyPipeline.run!
Celluloid::Actor.all.each do |actor|
  puts "#{actor.inspect}"
  #puts "#{actor.name}"
end
puts "calling async 100 times"
(0..2000).each do |i|
  Celluloid::Actor[:head].do_process_next "Async #{i}"
end

puts "Actor Count: #{Celluloid::Actor.all.to_set.length} Alive: #{Celluloid::Actor.all.to_set.select(&:alive?).length}"
sleep 5
puts "Actor Count: #{Celluloid::Actor.all.to_set.length} Alive: #{Celluloid::Actor.all.to_set.select(&:alive?).length}"

Thread.list.each do |t|
  puts "#{t.inspect} #{t.keys} [#{t[:celluloid_actor]}]"
end
puts "Length: #{Thread.list.size} / cores: #{Celluloid.cores}"
