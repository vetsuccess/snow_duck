require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("snow_duck/snow_duck") do |config|
    # Create release builds by default. If needed, RB_SYS_CARGO_PROFILE=dev env var can be used for debug builds
    config.profile = ENV.fetch("RB_SYS_CARGO_PROFILE", :release).to_sym
    config.clean_after_install = true
end