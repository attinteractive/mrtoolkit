# Import combined log format
#
# Parse and remove delimiters.
# Add unambiguous field separators.
class ImportLogFile

  def parse(line)
    patt = /(\S*)\s*(\S*)\s*(\S*)\s*\[([^\]]*)\]\s*"([^"]*)"\s*(\S*)\s*(\S*)\s*"([^"]*)"\s*"([^"]*)"/
    md = patt.match(line)
    return nil if md.nil?
    return md[1, 9]
  end

  def parse_all(fp)
    fp.each_line do |line|
      res = parse(line)
      puts res.join("\t")if res
    end
  end
end

ImportLogFile.new.parse_all(STDIN)
