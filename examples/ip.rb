require 'mrtoolkit'

class IpMap < MapBase
  def declare
    # declare log fields
    field :ip
    field :client_id
    field :user_id
    field :dt_tm
    field :request
    field :status
    field :result_size
    field :referrer
    field :ua

    emit :ip
    emit :count
  end
  def process(input, output)
    output.ip = input.ip
    output.count = 1
    output
  end
end

class MainJob < JobBase
  def stage1
    mapper IpMap
    reducer UniqueSumReduce, 1
    indir "logs"
    outdir "ip"
  end
end


MainJob.run_command(__FILE__)
