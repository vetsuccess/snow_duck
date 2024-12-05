# frozen_string_literal: true

require_relative "lib/snow_duck/version"

Gem::Specification.new do |spec|
  spec.name = "snow_duck"
  spec.version = SnowDuck::Version::VERSION
  spec.authors = ["nikola"]
  spec.email = ["nikola@deversity.net"]

  spec.summary = "Ruby bindings for DuckDB using Rust, providing high-performance in-memory analytics capabilities"
  spec.description = "Snow Duck provides Ruby bindings for DuckDB through Rust integration, offering efficient in-memory analytics and data processing. It seamlessly converts between DuckDB and Ruby data types, supporting complex operations including intervals, timestamps, and structured data."
  spec.homepage = "https://github.com/vetsuccess/snow_duck"
  spec.required_ruby_version = ">= 2.7.0"

  # Prevent pushing this gem to RubyGems.org. This is a private gem.
  spec.metadata["allowed_push_host"] = 'https://github.com/vetsuccess/snow_duck'

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/vetsuccess/snow_duck"
  spec.metadata["changelog_uri"] = "https://github.com/vetsuccess/snow_duck/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:test|spec|features)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.extensions = ["ext/snow_duck/extconf.rb"]
  spec.add_dependency "rb_sys", "~> 0.9.39"
  spec.add_dependency "rake-compiler", "~> 1.2.0"
  spec.add_dependency "activesupport", ">= 6.0"
end
