require 'mrtoolkit'

class MainMap < MapBase
  def declare
    # declare log fields
    field :ip
    field :client_id
    field :user_id
    field :dt_tm
    field :request
    field :status
    field :result_size
    field :referer
    field :ua

    emit :hour
    emit :count
  end

  def process_begin(input, output)
    @hours = Array.new(24, 0)
    nil
  end
  def process(input, output)
    dt_tm = input.dt_tm
    return nil if dt_tm.nil?
    fields = dt_tm.split(':')
    return nil if fields.nil? || fields.size < 2 || fields[1].nil?
    hour = fields[1].to_i      
    if hour >= 0 && hour < 24
      @hours[hour] += 1
    else
      STDERR.puts "bad hour: #{hour}"
    end
    nil
  end
  def process_end(input, output)
    out = []
    @hours.each_index do |hr|
      output = new_output
      output.hour = hr
      output.count = @hours[hr]
      out << output
    end
    out    
  end
end

class MainJob < JobBase
  def job
    mapper MainMap
    reducer UniqueSumReduce, 1
    indir "logs"
    outdir "hour"
  end
end

