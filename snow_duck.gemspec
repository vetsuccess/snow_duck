# frozen_string_literal: true

require_relative "lib/snow_duck/version"

Gem::Specification.new do |spec|
  spec.name = "snow_duck"
  spec.version = SnowDuck::Version::VERSION
  spec.authors = ["nikola"]
  spec.email = ["nikola@deversity.net"]

  spec.summary = "Write a short summary, because RubyGems requires one."
  spec.description = "Write a longer description or delete this line."
  spec.homepage = "https://example.com"
  spec.required_ruby_version = ">= 2.7.0"

  spec.metadata["allowed_push_host"] = "https://example.com"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://example.com"
  spec.metadata["changelog_uri"] = "https://example.com"

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
  # Magnus says: "needed until rubygems supports Rust support is out of beta" -> https://github.com/matsadler/magnus?tab=readme-ov-file#writing-an-extension-gem-calling-rust-from-ruby
  # But we are not going to be using rake-compile on target machine, we are going to pre-compile it
  # So its not going to be needed on target machine as well
  spec.add_dependency "rb_sys", "~> 0.9.39"

  # only needed when developing or packaging your gem
  spec.add_dependency "rake-compiler", "~> 1.2.0"
end
