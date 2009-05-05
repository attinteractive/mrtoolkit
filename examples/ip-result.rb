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
    emit :result
  end
  def process(input, output)
    output.ip = input.ip
    output.result = input.status
    output
  end
end

class MainJob < JobBase
  def job
    mapper MainMap
    reducer UniqueIndexedCountReduce
    indir "logs"
    outdir "ip-result"
  end
end
