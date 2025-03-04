#!/usr/bin/env -S julia -t 120 --project

if abspath(PROGRAM_FILE) == @__FILE__
    using HfxTools
    @info "Using $(Threads.nthreads()) threads"
    HfxTools.main()
end
