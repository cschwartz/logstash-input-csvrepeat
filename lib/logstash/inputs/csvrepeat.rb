# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "csv"
require 'time'

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::CsvRepeat < LogStash::Inputs::Base
  config_name "csvrepeat"

  default :codec, "plain"

  config :file, :validate => :path, :required => true
  config :headers, :validate => :path, :required => true
  config :time_between_loops, :validate => :number, :required => true
  config :seperator, :validate => :string, :default => ';'
  config :timestamp_field, :validate => :string, :default => 'timestamp'

  public
  def register
    headers = File.open(@headers).each_line.map(&:strip).map(&:downcase)
    p headers
    input = CSV.open(@file, col_sep: @seperator, headers: headers)
    @lines = input.to_enum
  end # def register

  def run(queue)
    current_event, current_event_time = next_event

    while !stop?
      queue << prepare_event(current_event)

      last_event_time = current_event_time
      current_event, current_event_time = next_event
      interval = calculate_delay(current_event_time, last_event_time)

      Stud.stoppable_sleep(interval) { stop? }
    end # loop
  end # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end

  private

  def calculate_delay(current_event_time, last_event_time)
    if current_event_time > last_event_time
      current_event_time - last_event_time
    else
      @time_between_loops
    end
  end #def calculate_delay

  def prepare_event(event)
    event_data = event.to_hash
    event_data.delete('timestamp')
    event_data = event_data.merge(event_data) { |k, v| v || '' }
    event = LogStash::Event.new(event_data)
    decorate(event)
    event
  end #def prepare_event

  def next_event
    begin
      event = @lines.next
    rescue StopIteration
      @lines.rewind
      event = @lines.next
    end

    start_date = event['startdate']
    start_time = event['startdate']
    p start_date ,start_time
    timestamp = combine_time start_date, start_time
    [event, timestamp]
  end #def next_event

  def combine_time(date, time)
    #Time.parse event[@timestamp_field]
    Time.now
  end
end # class LogStash::Inputs::Example
