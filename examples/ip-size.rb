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

    emit :ip
    emit :size
  end
  def process(input, output)
    output.ip = input.ip
    output.size = input.result_size
    output
  end
end

class MainJob < JobBase
  def job
    mapper MainMap
    reducer UniqueSumReduce, 1
    indir "logs"
    outdir "ip-size"
  end
end
