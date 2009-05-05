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

    emit :ip_ua
    emit :ip
    emit :ua
  end
  def process(input, output)
    ua = input.ua.split(/\s/)[0]
    output.ip_ua = "#{input.ip}|#{ua}"
    output.ip = input.ip
    output.ua = ua
    output
  end
end

class MainJob < JobBase
  def job
    mapper MainMap
    reducer UniqueCountReduce, 2
    indir "logs"
    outdir "ip-ua"
  end
end
