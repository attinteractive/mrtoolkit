# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{mrtoolkit}
  s.version = "0.1.2"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["cchayden", "vadimj"]
  s.date = %q{2009-07-08}
  s.email = %q{cchayden@nyt.com}
  s.extra_rdoc_files = [
    "README.rdoc"
  ]
  s.files = [
    "README.rdoc",
    "Rakefile",
    "VERSION.yml",
    "examples/Rakefile",
    "examples/Readme",
    "examples/hour.rb",
    "examples/import-logs",
    "examples/import.rb",
    "examples/ip-result.rb",
    "examples/ip-size.rb",
    "examples/ip-ua.rb",
    "examples/ip.rb",
    "examples/section.rb",
    "examples/top-file.rb",
    "lib/mrtoolkit.rb",
    "lib/regression.rb",
    "lib/stream_runner.rb",
    "test/Rakefile",
    "test/test-in/test1-in",
    "test/test-in/test2-in",
    "test/test-in/test3-in",
    "test/test-in/test4-in",
    "test/test-in/test5-in",
    "test/test-in/test6-in",
    "test/test-in/test7-in",
    "test/test-in/test8-in",
    "test/test-in/test9-in",
    "test/utest.rb"
  ]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/jashmenn/mrtoolkit}
  s.rdoc_options = ["--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.2}
  s.summary = %q{Simplify the creation of Hadoop Map/Reduce jobs}
  s.test_files = [
    "test/utest.rb",
    "examples/hour.rb",
    "examples/import.rb",
    "examples/ip-result.rb",
    "examples/ip-size.rb",
    "examples/ip-ua.rb",
    "examples/ip.rb",
    "examples/section.rb",
    "examples/top-file.rb"
  ]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
