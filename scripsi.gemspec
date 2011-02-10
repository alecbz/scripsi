# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{scripsi}
  s.version = "0.0.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.2") if s.respond_to? :required_rubygems_version=
  s.authors = ["Alec Benzer"]
  s.date = %q{2011-02-09}
  s.description = %q{a flexible text-searching library built on top of redis}
  s.email = %q{alecbenzer@gmail.com}
  s.extra_rdoc_files = ["README.md", "lib/scripsi.rb"]
  s.files = ["README.md", "Rakefile", "lib/scripsi.rb", "Manifest", "scripsi.gemspec"]
  s.homepage = %q{https://github.com/alecbenzer/scripsi}
  s.rdoc_options = ["--line-numbers", "--inline-source", "--title", "Scripsi", "--main", "README.md"]
  s.require_paths = ["lib"]
  s.rubyforge_project = %q{scripsi}
  s.rubygems_version = %q{1.5.0}
  s.summary = %q{a flexible text-searching library built on top of redis}

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<redis>, [">= 2.1.1"])
    else
      s.add_dependency(%q<redis>, [">= 2.1.1"])
    end
  else
    s.add_dependency(%q<redis>, [">= 2.1.1"])
  end
end
