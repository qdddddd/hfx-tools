using Pkg
Pkg.resolve()
Pkg.precompile()

using Base: Semaphore, acquire, release, Filesystem
using .Threads, Logging, CSV, ProgressMeter, Printf, Glob, DataFrames, DataStructures, Dates, GZip, JSON, StatsBase
using DbUtils, DfUtils

@info "Using $(nthreads()) threads"

include("structs.jl")
include("common_utils.jl")
include("process_functions.jl")

const UNPROCESSED = 0x0
const PROCESSING = 0x1
const COMPLETED = 0x2

##
function process(args::ProcessArgs, file_lock::LockedDict{String,ReentrantLock}, out_store::Dict{String,Deque{DataFrame}})
    date = args.dates[args.task.dtid]
    ofile = get_out_file(args, date)

    # Check for existing out-file
    if !args.one_file && args.skip_existing && isfile(ofile)
        return
    end

    q_out = out_store[args.task.symbol]
    df_raw = read_input_df(date, args)

    if !isnothing(args.start_time)
        @rsubset!(df_raw, Time(unix2datetime_adj(:Timestamp)) >= args.start_time)
    end

    # Call user-defined function
    args = ProcessArgs(args; df_raw=df_raw, date=date, out_q=q_out, hfx_cols=r"HFX\d+")
    @debug "Processing $(args.task.symbol)@$(date)"
    df_out = args.func(args)
    @debug "Processed $(args.task.symbol)@$(date)"

    # Check if function returned None
    if df_out === nothing || isempty(df_out)
        return
    end

    # Round data (assuming numerical)
    round_df!(df_out, digits=3, exclude=["Index1000Price"])

    # Handle dry run
    if args.dry_run
        return
    end

    # Store output
    @debug "Writing $(args.task.symbol)@$(date)"
    if args.one_file
        if isnothing(args.snapshot_time)
            CSV.write(ofile, df_out, compress=args.compress, append=isfile(ofile))
        else
            if (!haskey(file_lock, ofile))
                file_lock[ofile] = ReentrantLock()
            end

            lock(file_lock[ofile]) do
                CSV.write(ofile, df_out, compress=args.compress, append=isfile(ofile))
            end
        end
    else
        if args.func == corr_label
            df_out[!, :Date] = Dates.format.(Date.(df_out.Timestamp), dateformat"yyyymmdd")
            for g in groupby(df_out, :Date)
                fn = get_out_file(args, g.Date[1])

                out_dir = dirname(fn)
                if !isdir(out_dir)
                    mkpath(out_dir)
                end

                CSV.write(fn, g[!, 1:end-1], compress=args.compress, append=isfile(fn))
            end
        else
            if isfile(ofile)
                rm(ofile)
            end

            out_dir = dirname(ofile)
            if !isdir(out_dir)
                mkpath(out_dir)
            end

            CSV.write(ofile, df_out, compress=args.compress)
        end
    end
    @debug "Written $(args.task.symbol)@$(date)"

    if !isempty(q_out)
        if endswith(string(nameof(args.func)), "cont")
            @warn "Deque not empty for symbol: $(args.task.symbol)@$(date)"
        end
        empty!(q_out)
    end
    push!(q_out, df_out)

    df_raw = nothing
end

##

function main()
    ## Init params
    # Parse arguments
    start_dt = nothing
    end_dt = nothing
    hfx_model_name = "N8000W100Ver2.2.1SZ"
    d = "/minio/hrmspro/prediction/InfinityRound2/HFX"
    time_thres = 0
    shift = 0
    hdf_root = nothing
    hdf_features = nothing
    hdf_labels = nothing
    stock_info = nothing
    output_root = nothing
    output_root_tmp = Filesystem.mktempdir()
    func = corr_downsample
    cor_func = StatsBase.cor
    compress = true
    corr_thres = nothing
    y_corr_thres = nothing
    dry_run = false
    skip_existing = false
    clear = false
    one_file = false
    snapshot_time = nothing
    start_time = nothing
    ##

    for i in eachindex(ARGS)
        arg = ARGS[i]
        if arg == "--corr"
            corr_thres = parse(Float32, ARGS[i+1])
        elseif arg == "--y-corr"
            y_corr_thres = parse(Float32, ARGS[i+1])
        elseif arg == "-s"
            start_dt = ARGS[i+1]
        elseif arg == "-e"
            end_dt = ARGS[i+1]
        elseif arg == "-m"
            hfx_model_name = ARGS[i+1]
        elseif arg == "-d"
            d = ARGS[i+1]
        elseif arg == "-o"
            output_root = ARGS[i+1]
        elseif arg == "-t"
            snapshot_time = ARGS[i+1]
        elseif arg == "--tt"
            time_thres = parse(Int, ARGS[i+1])
        elseif arg == "--start"
            start_time = ARGS[i+1]
        elseif arg == "--no-compress"
            compress = false
        elseif arg == "--skip-existing"
            skip_existing = true
        elseif arg == "--shift"
            shift = parse(Int, ARGS[i+1])
        elseif arg == "--hdf"
            hdf_root = ARGS[i+1]
            schema = JSON.parsefile(joinpath(hdf_root, "description.json"))
            hdf_features = Symbol.(schema["FeatureNames"])
            hdf_labels = Symbol.(schema["LabelNames"])
        elseif arg == "--func"
            func = getfield(Main, Symbol(ARGS[i+1]))
        elseif arg == "--cor-func"
            cor_func = getfield(StatsBase, Symbol(ARGS[i+1]))
        elseif arg == "--dry"
            dry_run = true
        elseif arg == "--clear"
            clear = true
        elseif arg == "--one-file"
            one_file = true
        elseif arg == "--debug"
            ENV["JULIA_DEBUG"] = Main
        end
    end

    ## Generate params
    if isnothing(hfx_model_name) && !isnothing(hdf_root)
        hfx_model_name = split(hdf_root, ",")[1]
    end

    if isnothing(hfx_model_name)
        throw(ArgumentError("Must provide either -m or --hdf"))
    end

    raw_predict_dir = joinpath(d, hfx_model_name)

    if isnothing(output_root) && !isnothing(corr_thres)
        postfix = func == corr_downsample_cont ? "Cont" : ""
        postfix = occursin("_rev", string(nameof(func))) ? "Rev" : postfix

        func_name = "Corr"
        if cor_func == StatsBase.corkendall
            func_name = "Kendall"
        end

        output_root = "$(raw_predict_dir)($(func_name)Selected$(postfix)$(@sprintf("%.2f", corr_thres))$(shift > 0 ? "_$(shift)" : "")$(time_thres > 0 ? "_$(time_thres)s" : ""))"

        if !isnothing(y_corr_thres)
            output_root = output_root[1:end-1] * "_YC$(y_corr_thres))"
        end

        if !isnothing(start_time)
            output_root = output_root[1:end-1] * "_st$(start_time))"
        end

        if !isnothing(snapshot_time)
            output_root = joinpath(output_root, snapshot_time)
        end
    end

    ## Process existing files
    dict_existing_end = nothing

    if isdir(output_root)
        if clear
            @showprogress desc = "Deleting old files" showspeed = true barlen = 60 @threads for dir in readdir(output_root)
                rm(joinpath(output_root, dir); force=true, recursive=true)
            end
        elseif one_file && skip_existing # Find the true starting date based on existing files
            existing_dt = []
            lk = ReentrantLock()
            @showprogress desc = "Scanning existing files" showspeed = true barlen = 60 @threads for file in readdir(output_root)
                lines = eachline(GZip.open(joinpath(output_root, file)))
                header = first(lines)
                header *= ",Symbol"
                line = nothing
                for l in lines
                    line = l
                end

                if line === nothing
                    continue
                end

                line *= ",$(file[1:9])"
                line = header * "\n" * line * "\n"

                lock(lk) do
                    push!(existing_dt, line)
                end
            end

            df = DataFrame(CSV.File(map(IOBuffer, existing_dt); select=["Symbol", "Timestamp"]))
            if eltype(df.Timestamp) <: Int
                df[!, :Timestamp] = unix2datetime_adj.(df.Timestamp)
            end

            dict_existing_end = Dict(zip(df.Symbol, df.Timestamp))
        end
    else
        mkpath(output_root)
    end

    ##
    # Get list of dates
    dates = readdir(raw_predict_dir)
    filter!(x -> occursin(r"^\d{8}$", x), dates)
    sort!(dates)

    st_id = 1
    ed_id = size(dates, 1)

    if start_dt !== nothing
        st_id = findfirst(x -> x >= start_dt, dates)
    end

    if end_dt !== nothing
        ed_id = findlast(x -> x <= end_dt, dates)
    end

    if isnothing(st_id) || isnothing(ed_id)
        @info "No date to process."
        return
    end

    @info "Params" date = "$(dates[st_id]) - $(dates[ed_id])" input_dir = raw_predict_dir output_root output_root_tmp one_file hdf_root dry_run clear skip_existing

    codes = query_df(
        gcli(),
        "
            SELECT DISTINCT S_INFO_WINDCODE Code
            FROM winddb_mirror.ashareeodprices
            WHERE TRADE_DT >= '$(dates[st_id])' AND TRADE_DT <= '$(dates[ed_id])'
                AND right(S_INFO_WINDCODE, 2) IN ('SH', 'SZ')
        "
    ).Code

    if func == corr_label
        hdf_features = nothing
        stock_info = Dict()
        selected_root = "$(raw_predict_dir)(CorrSelectedCont$(@sprintf("%.2f", corr_thres))$(shift > 0 ? "_$(shift)" : "")$(time_thres > 0 ? "_$(time_thres)s" : ""))"
        lk = ReentrantLock()
        @showprogress desc = "Loading downsampled timestamps" showspeed = true barlen = 60 for file in readdir(selected_root)
            df = CSV.read(joinpath(selected_root, file), DataFrame; select=[:AppSeq, :Timestamp])
            df[!, :Timestamp] = unix2datetime_adj.(df.Timestamp)
            if !isnothing(dict_existing_end)
                @rsubset!(df, :Timestamp > dict_existing_end[file[1:9]])
            end
            lock(lk) do
                stock_info[file[1:9]] = df
            end
        end
    elseif func in [corr_downsample_rev, corr_downsample_rev_no_roll]
        stock_info = CSV.read(
            "/ivohf/hrmspro/prediction/InfinityRound2/stock_info.csv", DataFrame;
            types=Dict(:Code => String, :Date => Date), dateformat="yyyymmdd"
        )
    end

    ## Generate GARGS
    GARGS = ProcessArgs(
        dfs_buffer=Dict{String,DataFrame}([c => DataFrame() for c in codes]),
        dfs_buffer_sizes=Dict{String,Queue{Int}}([c => Queue{Int}() for c in codes]),
        stock_info=stock_info,
        dates=dates,
        in_root=raw_predict_dir,
        out_root=output_root,
        out_root_tmp=output_root_tmp,
        corr_thres=corr_thres,
        y_corr_thres=y_corr_thres,
        time_thres=Second(time_thres),
        shift=shift,
        hdf_root=hdf_root,
        hdf_features=hdf_features,
        hdf_labels=hdf_labels,
        func=func,
        cor_func=cor_func,
        compress=compress,
        dry_run=dry_run,
        skip_existing=skip_existing,
        one_file=one_file,
        snapshot_time=(
            isnothing(snapshot_time)
            ? nothing
            : Time(parse(Int, snapshot_time[1:2]), parse(Int, snapshot_time[3:4]), 0)
        ),
        roll_win=50,
        n_steps=20,
        start_time=isnothing(start_time) ? nothing : Time(parse(Int, start_time[1:2]), parse(Int, start_time[3:4]), 0),
        use_tmp=!(one_file && skip_existing),
    )

    if func == corr_downsample_cont
        # Get index prices
        index_df = DataFrame()
        lk = ReentrantLock()
        @showprogress desc = "Loading index prices" showspeed = true barlen = 60 @threads for dt in dates
            df = get_index("1000", dt, unix=true)
            if !isnothing(df)
                lock(lk) do
                    append!(index_df, df, promote=true, cols=:union)
                end
            end
        end

        sort!(index_df, :ExTime)
        index_df[!, :ExTime] .*= 1000
        GARGS.index_df = index_df
    end

    term_signal = "#"
    ##

    task_dict = Dict{String,Channel{ProcessTask}}([c => Channel{ProcessTask}(ed_id - st_id + 1) for c in codes])
    total_files = Atomic{Int}(0)
    pbar = Progress(total_files[]; desc="$(nameof(func)):", showspeed=true, barlen=60)

    n_symbols_started = Atomic{Int}(0)
    supplier = @spawn begin
        valid_symbols = Set()

        for i in st_id:ed_id
            date = dates[i]
            dt = Date(date, dateformat"yyyymmdd")
            files = glob("*.csv*", joinpath(raw_predict_dir, date))
            n_tasks = 0

            for file in files
                _, symbol = get_date_symbol(file)

                if one_file && !isnothing(dict_existing_end)
                    if haskey(dict_existing_end, symbol) && dt <= Date(dict_existing_end[symbol])
                        continue
                    end
                end

                new_task = ProcessTask(i, symbol, raw_predict_dir, UNPROCESSED)

                if !(symbol in valid_symbols)
                    atomic_add!(n_symbols_started, 1)
                    push!(valid_symbols, symbol)
                end

                q = task_dict[symbol]
                put!(q, new_task)
                n_tasks += 1
            end

            atomic_add!(total_files, n_tasks)
            pbar.n = total_files[]
        end

        for symbol in valid_symbols
            put!(task_dict[symbol], ProcessTask(term_signal, COMPLETED))
        end
    end

    while n_symbols_started[] == 0 && !istaskdone(supplier)
        sleep(0.5)
    end

    if istaskfailed(supplier)
        wait(supplier)
        return
    end

    @info "Start processing tasks..."
    sem = Semaphore(nthreads() - 2)
    force_quit = Atomic{Bool}(false)
    n_symbols_terminated = 0
    file_lock = LockedDict{String,ReentrantLock}()
    out_store = Dict{String,Deque{DataFrame}}([c => Deque{DataFrame}() for c in codes])

    while n_symbols_terminated < n_symbols_started[] || !istaskdone(supplier)
        n_start = 0
        n_finish = 0
        n_process = 0
        n_empty = 0
        to_delete = String[]

        for symbol in keys(task_dict)
            if force_quit[]
                return
            end

            task_q = task_dict[symbol]

            if !isready(task_q)
                @debug "Empty queue for $(symbol)"
                n_empty += 1

                if istaskdone(supplier)
                    push!(to_delete, symbol)
                end
                continue
            end

            task = fetch(task_q)

            if task.symbol == term_signal
                take!(task_dict[symbol])
                @assert !isready(task_dict[symbol])
                empty!(GARGS.dfs_buffer[symbol])
                empty!(out_store[symbol])
                n_symbols_terminated += 1
                continue
            end

            if task.status[] == PROCESSING
                @debug "Skipping $(task.symbol)@$(dates[task.dtid])"
                n_process += 1
                continue
            end

            if task.status[] == COMPLETED
                @debug "Completing $(task.symbol)@$(dates[task.dtid])"
                take!(task_q)
                # size_q = GARGS.dfs_buffer_sizes[task.symbol]
                # s = isempty(size_q) ? 0 : maximum(size_q)
                next!(pbar,
                    showvalues=[
                        (:Progress, "$(pbar.counter+1)/$(pbar.n)"),
                        (:NSymbols, "$(n_symbols_terminated)/$(n_symbols_started[])"),
                        # (:ScanSize, "$(s)")
                        (:Counts, "st = $(n_start) | pr = $(n_process) | ed = $(n_finish) | na = $(n_empty)")
                    ]
                )
                n_finish += 1
                continue
            end

            atomic_xchg!(task.status, PROCESSING)

            @spawn begin
                @debug "Aquiring $(task.symbol)@$(dates[task.dtid])"
                acquire(sem) do
                    try
                        process(ProcessArgs(GARGS; task=task), file_lock, out_store)
                        atomic_xchg!(task.status, COMPLETED)
                        # @debug "Completed $(task.symbol)@$(dates[task.dtid])"
                    catch e
                        @error "\nException thrown with $(task.symbol)@$(dates[task.dtid])" exception = (e, catch_backtrace())
                        atomic_xchg!(force_quit, true)
                    end
                end
                @debug "Released $(task.symbol)@$(dates[task.dtid])"
            end
            n_start += 1
        end

        @debug "Deleting $(length(to_delete)) keys"
        for symbol in to_delete
            delete!(task_dict, symbol)
        end

        yield()
    end

    wait(supplier)
    finish!(pbar)

    if GARGS.use_tmp
        @showprogress desc = "Moving tmp to dest" showspeed = true barlen = 60 @threads for dir in readdir(output_root_tmp)
            mv(joinpath(output_root_tmp, dir), joinpath(output_root, dir); force=true)
        end
    end
end

main()
