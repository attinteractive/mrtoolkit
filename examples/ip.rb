require 'mrtoolkit'

class MainJob < JobBase
  def job
    mapper CopyMap
    reducer UniqueCountReduce
    indir "logs"
    outdir "ip"
  end
end
